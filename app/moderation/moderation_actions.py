from __future__ import annotations

import asyncio
import hashlib
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Optional

from pymongo import UpdateOne
from pymongo.errors import BulkWriteError, DuplicateKeyError
from pyrogram.client import Client
from pyrogram.enums import ParseMode
from pyrogram.errors import FloodWait, RPCError

from app.config import settings
from app.core.models import (
    DistributionPriority,
    JobStatus,
    MediaType,
    ModerationDestination,
    ModerationState,
    QueueJob,
)
from app.core.database import DatabaseManager
from app.repositories.queue_repository import QueueRepository
from app.core.exceptions import DuplicateJobError
from app.services.audit_service import get_audit, AuditAction
from app.services.consent_service import ConsentService
from app.core.logger import get_logger

logger = get_logger(__name__)

_MAX_RETRIES = 3
_consent_service = ConsentService()


# ── Destination helpers ───────────────────────────────────────────────────────

def _destination_group_id(dest: str) -> int:
    if dest == ModerationDestination.NSFW:
        return settings.NSFW_GROUP_ID
    if dest == ModerationDestination.PREMIUM:
        return settings.PREMIUM_GROUP_ID
    raise ValueError(f"Unknown destination: {dest}")


def _destination_display_name(dest: str) -> str:
    if dest == ModerationDestination.NSFW:
        return settings.NSFW_DISPLAY_NAME
    if dest == ModerationDestination.PREMIUM:
        return settings.PREMIUM_DISPLAY_NAME
    return dest


def _get_watermark_config(dest: str) -> Optional[dict]:
    if dest == ModerationDestination.NSFW:
        logo_path = settings.WATERMARK_LOGO_PATH_NSFW
        text = settings.WATERMARK_TEXT_NSFW
    elif dest == ModerationDestination.PREMIUM:
        logo_path = settings.WATERMARK_LOGO_PATH_PREMIUM
        text = settings.WATERMARK_TEXT_PREMIUM
    else:
        return None

    logo_exists = Path(logo_path).exists()
    if not logo_exists:
        logger.warning(
            "Watermark logo missing — will use text overlay",
            extra={"ctx_path": logo_path, "ctx_dest": dest},
        )

    return {
        "watermark_image_path": logo_path if logo_exists else None,
        "watermark_text": text,
        "position": settings.WATERMARK_POSITION,
        "opacity": settings.WATERMARK_OPACITY,
        "scale": settings.WATERMARK_SCALE,
        "destination": dest,
    }


def _compute_checksum(file_unique_id: Optional[str], file_size: int) -> Optional[str]:
    if not file_unique_id:
        return None
    raw = f"{file_unique_id}:{file_size}"
    return hashlib.sha256(raw.encode()).hexdigest()


# ── Safe Telegram ops ─────────────────────────────────────────────────────────

async def safe_dm(client: Client, user_id: int, text: str) -> None:
    for attempt in range(_MAX_RETRIES):
        try:
            await client.send_message(
                chat_id=user_id,
                text=text,
                parse_mode=ParseMode.HTML,
            )
            return
        except FloodWait as e:
            await asyncio.sleep(int(e.value) + settings.FLOODWAIT_EXTRA_BUFFER)
        except RPCError as e:
            logger.warning(
                "Failed to DM uploader",
                extra={"ctx_user_id": user_id, "ctx_error": str(e), "ctx_attempt": attempt + 1},
            )
            if attempt == _MAX_RETRIES - 1:
                return
            await asyncio.sleep(2 ** attempt)
        except Exception as e:
            logger.warning(
                "Unexpected error DMing uploader",
                extra={"ctx_user_id": user_id, "ctx_error": str(e)},
            )
            return


async def safe_delete_message(client: Client, chat_id: int, message_id: int) -> None:
    try:
        await client.delete_messages(chat_id=chat_id, message_ids=message_id)
    except Exception as e:
        logger.warning(
            "Could not delete moderation message",
            extra={"ctx_chat_id": chat_id, "ctx_msg_id": message_id, "ctx_error": str(e)},
        )


async def safe_edit_message(
    client: Client,
    chat_id: int,
    message_id: int,
    text: str,
) -> None:
    for attempt in range(_MAX_RETRIES):
        try:
            await client.edit_message_text(
                chat_id=chat_id,
                message_id=message_id,
                text=text,
                parse_mode=ParseMode.HTML,
            )
            return
        except FloodWait as e:
            await asyncio.sleep(int(e.value) + settings.FLOODWAIT_EXTRA_BUFFER)
        except RPCError as e:
            logger.warning(
                "Could not edit moderation message",
                extra={"ctx_chat_id": chat_id, "ctx_msg_id": message_id, "ctx_error": str(e)},
            )
            return


async def post_to_destination(
    client: Client,
    messages: list,
    dest: str,
) -> bool:
    group_id = _destination_group_id(dest)
    if not group_id:
        logger.error("Destination group ID not configured", extra={"ctx_dest": dest})
        return False

    for msg in messages:
        for attempt in range(_MAX_RETRIES):
            try:
                await client.copy_message(
                    chat_id=group_id,
                    from_chat_id=msg.chat.id,
                    message_id=msg.id,
                )
                break
            except FloodWait as e:
                wait = int(e.value) + settings.FLOODWAIT_EXTRA_BUFFER
                logger.warning(
                    "FloodWait posting to destination",
                    extra={"ctx_dest": dest, "ctx_wait": wait, "ctx_attempt": attempt + 1},
                )
                await asyncio.sleep(wait)
            except RPCError as e:
                logger.error(
                    "RPC error posting to destination",
                    extra={"ctx_dest": dest, "ctx_error": str(e), "ctx_attempt": attempt + 1},
                )
                if attempt == _MAX_RETRIES - 1:
                    return False
                await asyncio.sleep(2 ** attempt)

    return True


# ── Vault archival ────────────────────────────────────────────────────────────

async def archive_to_vault(
    client: Client,
    messages: list,
    dest: str,
    submitter_user_id: int,
    consent_record_id: Optional[str] = None,
    # BUG FIX: approve flow sets initial_status=POSTED so the scheduler
    # never re-picks approved content. Queue flow uses QUEUED (default).
    initial_status: str = ModerationState.QUEUED.value,
) -> list[int]:
    """
    Two-step vault archival:
    1. copy_message() to VAULT_CHANNEL_ID
    2. Upsert metadata to MongoDB

    initial_status parameter:
      - QUEUED  (default) : content goes into scheduler distribution pipeline
      - POSTED            : content was already immediately posted (approve flow)
                            prevents double-post by keeping it OUT of scheduler

    Returns list of vault_message_ids. Never raises.
    """
    if not messages:
        return []

    resolved_consent_id = consent_record_id
    if resolved_consent_id is None and submitter_user_id:
        try:
            consent_doc = await _consent_service.get_active_consent(submitter_user_id)
            if consent_doc:
                resolved_consent_id = str(consent_doc["_id"])
            else:
                logger.warning(
                    "No active consent record found for submitter",
                    extra={"ctx_user_id": submitter_user_id},
                )
        except Exception as e:
            logger.warning(
                "Failed to fetch consent record — proceeding without it",
                extra={"ctx_user_id": submitter_user_id, "ctx_error": str(e)},
            )

    vault_message_ids: list[int] = []

    # ── Step 1: Telegram Vault Channel ────────────────────────────────────────
    if settings.VAULT_CHANNEL_ID:
        for msg in messages:
            copied_id = 0
            for attempt in range(_MAX_RETRIES):
                try:
                    result = await client.copy_message(
                        chat_id=settings.VAULT_CHANNEL_ID,
                        from_chat_id=msg.chat.id,
                        message_id=msg.id,
                    )
                    copied_id = result.id
                    break
                except FloodWait as e:
                    await asyncio.sleep(int(e.value) + settings.FLOODWAIT_EXTRA_BUFFER)
                except RPCError as e:
                    logger.error(
                        "Failed to copy message to vault channel",
                        extra={
                            "ctx_msg_id": msg.id,
                            "ctx_error": str(e),
                            "ctx_attempt": attempt + 1,
                        },
                    )
                    if attempt == _MAX_RETRIES - 1:
                        break
                    await asyncio.sleep(2 ** attempt)
            vault_message_ids.append(copied_id)
    else:
        logger.warning("VAULT_CHANNEL_ID not configured — skipping Telegram vault archival")
        vault_message_ids = [0] * len(messages)

    # ── Step 2: MongoDB vault metadata ────────────────────────────────────────
    db = DatabaseManager.get_db()
    vault_col = db[settings.VAULT_COLLECTION]
    now = datetime.now(timezone.utc)
    operations = []

    for i, msg in enumerate(messages):
        media = None
        if msg.media:
            try:
                media = getattr(msg, msg.media.value, None)
            except Exception:
                pass

        file_unique_id = getattr(media, "file_unique_id", None) if media else None
        file_id = getattr(media, "file_id", None) if media else None
        file_size = getattr(media, "file_size", 0) if media else 0
        media_type_str = msg.media.value if msg.media else "text"
        content_id = f"{msg.chat.id}_{msg.id}"
        vault_msg_id = vault_message_ids[i] if i < len(vault_message_ids) else 0

        checksum = _compute_checksum(file_unique_id, file_size or 0)

        operations.append(UpdateOne(
            {"content_id": content_id},
            {
                "$setOnInsert": {
                    "content_id": content_id,
                    "source_chat_id": str(msg.chat.id),
                    "message_id": msg.id,
                    "media_group_id": msg.media_group_id,
                    "media_type": media_type_str,
                    "file_id": file_id,
                    "file_unique_id": file_unique_id,
                    "file_size": file_size,
                    "caption": msg.caption or msg.text or "",
                    "created_at": now,
                    "usage_count": 0,
                    "last_posted_at": None,
                    "cooldown_until": None,
                },
                "$set": {
                    "vault_message_id": vault_msg_id or None,
                    "vault_channel_id": str(settings.VAULT_CHANNEL_ID) if settings.VAULT_CHANNEL_ID else None,
                    "moderation_destination": dest,
                    # BUG FIX: use initial_status param.
                    # approve flow passes POSTED → scheduler never picks this up again.
                    # queue flow passes QUEUED (default) → enters distribution pipeline.
                    "status": initial_status,
                    "distribution_state": ModerationState.PENDING.value,
                    "submitter_user_id": submitter_user_id,
                    "consent_record_id": resolved_consent_id,
                    "checksum": checksum,
                    "updated_at": now,
                    "metadata": {
                        "has_spoiler": getattr(media, "has_spoiler", False) if media else False,
                        "date": msg.date.isoformat() if msg.date else None,
                    },
                },
            },
            upsert=True,
        ))

    if operations:
        try:
            await vault_col.bulk_write(operations, ordered=False)
            logger.info(
                "Vault archival complete",
                extra={
                    "ctx_count": len(operations),
                    "ctx_dest": dest,
                    "ctx_initial_status": initial_status,
                    "ctx_vault_copied": len([v for v in vault_message_ids if v]),
                    "ctx_submitter": submitter_user_id,
                },
            )
        except BulkWriteError as e:
            logger.warning(
                "Partial vault write (duplicates silently ignored)",
                extra={"ctx_details": str(e.details)},
            )
        except Exception:
            logger.error("Vault MongoDB write failed", exc_info=True)

    try:
        await get_audit().log(
            action=AuditAction.VAULT_ARCHIVE,
            performed_by=submitter_user_id,
            details={
                "destination": dest,
                "message_count": len(messages),
                "initial_status": initial_status,
                "consent_record_id": resolved_consent_id,
            },
        )
    except Exception as e:
        logger.warning("Audit log failed for vault archive", extra={"ctx_error": str(e)})

    return vault_message_ids


# ── Queue enqueue ─────────────────────────────────────────────────────────────

async def enqueue_for_distribution(
    messages: list,
    dest: str,
    submitter_user_id: int,
) -> bool:
    db = DatabaseManager.get_db()
    queue_repo = QueueRepository(db)

    target_group_id = _destination_group_id(dest)
    if not target_group_id:
        logger.error("Cannot enqueue: destination group not configured", extra={"ctx_dest": dest})
        return False

    watermark_config = _get_watermark_config(dest)
    watermark_required = watermark_config is not None

    now = datetime.now(timezone.utc)
    deadline = now + timedelta(hours=settings.QUEUE_DEADLINE_HOURS)

    group_id = messages[0].media_group_id if messages else None
    content_id = (
        f"mod_{group_id}"
        if group_id
        else f"mod_{messages[0].chat.id}_{messages[0].id}"
    )

    source_channel_id = f"submission_{dest}"

    for i, msg in enumerate(messages):
        media = None
        if msg.media:
            try:
                media = getattr(msg, msg.media.value, None)
            except Exception:
                pass

        file_id = getattr(media, "file_id", None) if media else None
        media_type_str = msg.media.value if msg.media else "text"

        try:
            media_type = MediaType(media_type_str)
        except ValueError:
            media_type = MediaType.TEXT

        item_content_id = f"{content_id}_{i}" if len(messages) > 1 else content_id
        initial_status = JobStatus.WATERMARKING if watermark_required else JobStatus.PENDING

        job = QueueJob(
            content_id=item_content_id,
            source_channel_id=source_channel_id,
            target_channel_ids=[str(target_group_id)],
            media_type=media_type,
            media_file_id=file_id,
            caption=msg.caption or msg.text or "",
            priority=DistributionPriority.MODERATED,
            status=initial_status,
            max_retries=settings.MAX_RETRY_ATTEMPTS,
            execute_after=now,
            queue_deadline=deadline,
            watermark_required=watermark_required,
            watermark_config=watermark_config,
            metadata={
                "media_group_id": group_id,
                "message_id": msg.id,
                "submitter_user_id": submitter_user_id,
                "destination": dest,
                "moderated_at": now.isoformat(),
            },
        )

        try:
            await queue_repo.enqueue(job)
        except DuplicateJobError:
            logger.debug("Duplicate queue job skipped", extra={"ctx_content_id": item_content_id})
        except Exception:
            logger.error(
                "Failed to enqueue moderated job",
                extra={"ctx_content_id": item_content_id},
                exc_info=True,
            )
            return False

    logger.info(
        "Content enqueued for distribution",
        extra={
            "ctx_content_id": content_id,
            "ctx_dest": dest,
            "ctx_deadline": deadline.isoformat(),
            "ctx_count": len(messages),
            "ctx_watermark": watermark_required,
        },
    )
    return True


# ── Main action executors ─────────────────────────────────────────────────────

async def execute_approve(
    client: Client,
    messages: list,
    submitter_user_id: int,
    dest: str,
    mod_card_chat_id: int,
    mod_card_message_id: int,
    moderator_name: str,
    moderator_id: int,
) -> None:
    """
    Approve flow:
    1. Archive to vault with status=POSTED (NOT QUEUED) — prevents scheduler double-post
    2. Post immediately to destination group
    3. Delete moderation card
    4. Notify uploader
    5. Write audit log

    BUG FIX: original code archived with status=QUEUED, meaning the scheduler
    would later pick up and re-post already-delivered content. Now we pass
    initial_status=ModerationState.POSTED.value so the vault record is marked
    as already delivered and the scheduler ignores it entirely.
    """
    display_name = _destination_display_name(dest)

    # FIX: pass initial_status=POSTED — approved content is posted immediately,
    # must NOT re-enter the scheduler distribution pipeline.
    vault_ids = await archive_to_vault(
        client, messages, dest,
        submitter_user_id=submitter_user_id,
        initial_status=ModerationState.POSTED.value,
    )

    vault_success = any(vid for vid in vault_ids if vid)
    if not vault_success:
        logger.error(
            "Approve aborted: vault archival returned no valid message IDs",
            extra={"ctx_dest": dest, "ctx_submitter": submitter_user_id},
        )
        await safe_edit_message(
            client,
            mod_card_chat_id,
            mod_card_message_id,
            f"⚠️ <b>Vault archival failed</b> — content NOT posted.\n"
            f"Approved by {moderator_name} but vault write returned zero IDs.\n"
            f"👤 Submitter: <code>{submitter_user_id}</code>\n\n"
            f"Please retry or investigate vault channel access.",
        )
        return

    posted = await post_to_destination(client, messages, dest)
    if not posted:
        logger.error(
            "Approve: failed to post to destination",
            extra={"ctx_dest": dest, "ctx_submitter": submitter_user_id},
        )
        await safe_edit_message(
            client,
            mod_card_chat_id,
            mod_card_message_id,
            f"⚠️ <b>Approved</b> by {moderator_name} but delivery to {display_name} failed.\n"
            f"👤 Submitter: <code>{submitter_user_id}</code>",
        )
        await safe_dm(
            client,
            submitter_user_id,
            f"✅ Your content was approved.\n\nDestination:\n{display_name}\n\n"
            "⚠️ Note: delivery encountered an issue. Our team will retry.",
        )
        return

    await safe_delete_message(client, mod_card_chat_id, mod_card_message_id)
    await safe_dm(
        client,
        submitter_user_id,
        f"✅ Your content was approved.\n\nDestination:\n{display_name}",
    )

    await get_audit().log(
        action=AuditAction.APPROVE,
        performed_by=moderator_id,
        details={
            "destination": dest,
            "submitter_user_id": submitter_user_id,
            "vault_message_ids": vault_ids,
        },
    )

    logger.info(
        "Approve flow complete",
        extra={
            "ctx_submitter": submitter_user_id,
            "ctx_dest": dest,
            "ctx_moderator": moderator_id,
        },
    )


async def execute_queue(
    client: Client,
    messages: list,
    submitter_user_id: int,
    dest: str,
    mod_card_chat_id: int,
    mod_card_message_id: int,
    moderator_name: str,
    moderator_id: int,
) -> None:
    """
    Queue flow:
    1. Archive to vault with status=QUEUED — enters scheduler distribution pipeline
    2. Enqueue MODERATED-priority job
    3. Delete moderation card
    4. Notify uploader
    5. Write audit log
    """
    display_name = _destination_display_name(dest)

    # Queue flow correctly uses QUEUED (default) so scheduler picks it up
    vault_ids = await archive_to_vault(
        client, messages, dest,
        submitter_user_id=submitter_user_id,
        initial_status=ModerationState.QUEUED.value,
    )

    vault_success = any(vid for vid in vault_ids if vid)
    if not vault_success:
        logger.error(
            "Queue aborted: vault archival returned no valid message IDs",
            extra={"ctx_dest": dest, "ctx_submitter": submitter_user_id},
        )
        await safe_edit_message(
            client,
            mod_card_chat_id,
            mod_card_message_id,
            f"⚠️ <b>Vault archival failed</b> — content NOT queued.\n"
            f"Queued by {moderator_name} but vault write returned zero IDs.\n"
            f"👤 Submitter: <code>{submitter_user_id}</code>\n\n"
            f"Please retry or investigate vault channel access.",
        )
        return

    queued = await enqueue_for_distribution(messages, dest, submitter_user_id)
    if not queued:
        logger.error(
            "Queue: failed to enqueue distribution job",
            extra={"ctx_dest": dest, "ctx_submitter": submitter_user_id},
        )
        await safe_edit_message(
            client,
            mod_card_chat_id,
            mod_card_message_id,
            f"⚠️ <b>Queued</b> by {moderator_name} but enqueue failed.\n"
            f"👤 Submitter: <code>{submitter_user_id}</code>",
        )
        return

    await safe_delete_message(client, mod_card_chat_id, mod_card_message_id)
    await safe_dm(
        client,
        submitter_user_id,
        f"✅ Your content has been queued for posting.\n\nDestination:\n{display_name}",
    )

    await get_audit().log(
        action=AuditAction.QUEUE,
        performed_by=moderator_id,
        details={
            "destination": dest,
            "submitter_user_id": submitter_user_id,
        },
    )

    logger.info(
        "Queue flow complete",
        extra={
            "ctx_submitter": submitter_user_id,
            "ctx_dest": dest,
            "ctx_moderator": moderator_id,
            "ctx_deadline_hours": settings.QUEUE_DEADLINE_HOURS,
        },
    )


async def execute_reject(
    client: Client,
    submitter_user_id: int,
    mod_card_chat_id: int,
    mod_card_message_id: int,
    moderator_name: str,
    moderator_id: int,
) -> None:
    """
    Reject flow:
    - Content not archived to vault, not distributed
    - Moderation card updated
    - Uploader notified
    - Audit log written
    """
    await safe_edit_message(
        client,
        mod_card_chat_id,
        mod_card_message_id,
        f"❌ <b>Rejected</b> by {moderator_name} (<code>{moderator_id}</code>)\n"
        f"👤 Submitter: <code>{submitter_user_id}</code>",
    )
    await safe_dm(
        client,
        submitter_user_id,
        "❌ Your submission was rejected by moderation.",
    )

    await get_audit().log(
        action=AuditAction.REJECT,
        performed_by=moderator_id,
        target_user_id=submitter_user_id,
        details={"submitter_user_id": submitter_user_id},
    )

    logger.info(
        "Reject flow complete",
        extra={"ctx_submitter": submitter_user_id, "ctx_moderator": moderator_id},
    )
