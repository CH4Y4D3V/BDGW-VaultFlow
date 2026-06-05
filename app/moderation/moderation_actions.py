from __future__ import annotations

import asyncio
import hashlib
import random
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Optional

from pymongo import UpdateOne
from pymongo.errors import BulkWriteError
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
    # Guard: if watermarking is globally disabled, return None so no job
    # ever enters WATERMARKING status. The scheduler and enqueue paths both
    # treat None watermark_config as watermark_required=False.
    if not settings.WATERMARK_ENABLED:
        return None

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
            "Watermark logo missing and WATERMARK_ENABLED=true — returning None to skip watermarking",
            extra={"ctx_path": logo_path, "ctx_dest": dest},
        )
        return None  # No asset = no watermark, not a broken watermark job

    return {
        "watermark_image_path": logo_path,
        "watermark_text": text,
        "position": random.choice(["TOP_LEFT", "TOP_RIGHT", "BOTTOM_LEFT", "BOTTOM_RIGHT", "CENTER"]),
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
    """
    Bug 1 fix: use copy_message() instead of forward_messages().
    copy_message() produces a clean copy with no metadata leak.
    
    RC-12: Use copy_media_group for albums to prevent fragmentation.
    """
    group_id = _destination_group_id(dest)
    if not group_id:
        logger.error("Destination group ID not configured", extra={"ctx_dest": dest})
        return False

    is_album = len(messages) > 1 and all(m.media_group_id for m in messages)
    
    if is_album:
        try:
            for attempt in range(_MAX_RETRIES):
                try:
                    await client.copy_media_group(
                        chat_id=group_id,
                        from_chat_id=messages[0].chat.id,
                        message_id=messages[0].id,
                    )
                    return True
                except FloodWait as e:
                    await asyncio.sleep(int(e.value) + settings.FLOODWAIT_EXTRA_BUFFER)
                except RPCError:
                    if attempt == _MAX_RETRIES - 1:
                        raise
                    await asyncio.sleep(2 ** attempt)
        except Exception as e:
            logger.warning(
                "copy_media_group_failed_in_post_to_destination_fallback_to_sequential",
                extra={"ctx_error": str(e), "ctx_dest": dest}
            )

    # Sequential Fallback
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


def _generate_content_id(chat_id: int, message_id: int, file_unique_id: Optional[str]) -> str:
    """Deterministic SHA256 content_id generation."""
    raw = f"{chat_id}:{message_id}:{file_unique_id or 'none'}"
    return hashlib.sha256(raw.encode()).hexdigest()


# ── Vault archival ────────────────────────────────────────────────────────────

async def archive_to_vault(
    client: Client,
    messages: list,
    dest: str,
    submitter_user_id: int,
    consent_record_id: Optional[str] = None,
    initial_status: str = ModerationState.QUEUED.value,
) -> list[int]:
    """
    Two-step vault archival:
    1. copy_message() to VAULT_CHANNEL_ID
    2. Upsert metadata to MongoDB
    """
    if not messages:
        return []

    resolved_consent_id = consent_record_id
    if resolved_consent_id is None and submitter_user_id:
        try:
            consent_doc = await _consent_service.get_active_consent(submitter_user_id)
            if consent_doc:
                resolved_consent_id = str(consent_doc["_id"])
        except Exception as e:
            logger.warning("Failed to fetch consent record", extra={"ctx_error": str(e)})

    vault_message_ids: list[int] = []
    db = DatabaseManager.get_db()
    vault_col = db[settings.VAULT_COLLECTION]
    now = datetime.now(timezone.utc)
    
    logger.info(
        "vault_insert_started",
        extra={
            "ctx_submitter": submitter_user_id,
            "ctx_dest": dest,
            "ctx_count": len(messages)
        }
    )

    # ── RC-12: Atomic Vault Archival ────────────────────────────────────────
    # Attempt to copy as an album to preserve media group integrity in the vault.
    
    is_album = len(messages) > 1 and all(m.media_group_id for m in messages)
    copied_messages: list = []
    
    if is_album:
        try:
            for attempt in range(_MAX_RETRIES):
                try:
                    copied_messages = await client.copy_media_group(
                        chat_id=settings.VAULT_CHANNEL_ID,
                        from_chat_id=messages[0].chat.id,
                        message_id=messages[0].id,
                    )
                    break
                except FloodWait as e:
                    await asyncio.sleep(int(e.value) + settings.FLOODWAIT_EXTRA_BUFFER)
                except RPCError:
                    if attempt == _MAX_RETRIES - 1:
                        raise
                    await asyncio.sleep(2 ** attempt)
        except Exception as e:
            logger.warning(
                "copy_media_group_failed_in_vault_archival_fallback_to_sequential",
                extra={"ctx_error": str(e), "ctx_submitter": submitter_user_id}
            )
            copied_messages = []

    # If not an album or album copy failed, copy individually
    if not copied_messages:
        for msg in messages:
            copied_msg = None
            if settings.VAULT_CHANNEL_ID:
                for attempt in range(_MAX_RETRIES):
                    try:
                        copied_msg = await client.copy_message(
                            chat_id=settings.VAULT_CHANNEL_ID,
                            from_chat_id=msg.chat.id,
                            message_id=msg.id,
                        )
                        break
                    except FloodWait as e:
                        await asyncio.sleep(int(e.value) + settings.FLOODWAIT_EXTRA_BUFFER)
                    except RPCError:
                        if attempt == _MAX_RETRIES - 1:
                            break
                        await asyncio.sleep(2 ** attempt)
            
            # Even if copy failed (copied_msg is None), we still add it to keep indices aligned
            copied_messages.append(copied_msg)

    vault_message_ids = [m.id if m else 0 for m in copied_messages]

    # Now update MongoDB for each message
    for i, msg in enumerate(messages):
        media = getattr(msg, str(msg.media.value), None) if msg.media else None
        file_unique_id = getattr(media, "file_unique_id", None) if media else None
        file_id = getattr(media, "file_id", None) if media else None
        file_size = getattr(media, "file_size", 0) if media else 0
        media_type_str = msg.media.value if msg.media else "text"

        content_id = _generate_content_id(msg.chat.id, msg.id, file_unique_id)
        checksum = _compute_checksum(file_unique_id, file_size or 0)
        
        # ── Flow I: Hashing Deduplication ──
        content_hash = None
        if msg.photo:
            try:
                photo_bytes = await client.download_media(msg, in_memory=True)
                from app.utils.media_hashing import calculate_image_hash
                content_hash = calculate_image_hash(photo_bytes)
                
                # Check for duplicates in Vault
                duplicate = await vault_col.find_one({"content_hash": content_hash})
                if duplicate:
                    logger.warning(
                        "duplicate_content_hash_detected",
                        extra={"ctx_content_id": content_id, "ctx_existing": duplicate["content_id"]}
                    )
                    # We still archive but tag it as duplicate
            except Exception as e:
                logger.warning("hashing_failed_during_archival", extra={"ctx_error": str(e)})

        vault_msg_id = vault_message_ids[i] if i < len(vault_message_ids) else 0

        update_doc = {
            "$setOnInsert": {
                "content_id": content_id,
                "created_at": now,
                "usage_count": 0,
            },
            "$set": {
                "source_chat_id": str(msg.chat.id),
                "source_message_id": msg.id,
                "vault_message_id": vault_msg_id if vault_msg_id else None,
                "vault_channel_id": str(settings.VAULT_CHANNEL_ID) if vault_msg_id else None,
                "media_group_id": msg.media_group_id,
                "album_sequence_index": i if msg.media_group_id else None,
                "media_type": media_type_str,
                "file_id": file_id,
                "file_unique_id": file_unique_id,
                "file_size": file_size,
                "caption": msg.caption or msg.text or "",
                "moderation_destination": dest,
                "status": initial_status,
                "distribution_state": ModerationState.PENDING.value,
                "submitter_user_id": submitter_user_id,
                "consent_record_id": resolved_consent_id,
                "checksum": checksum,
                "content_hash": content_hash,
                "updated_at": now,
                "metadata": {
                    "has_spoiler": getattr(media, "has_spoiler", False) if media else False,
                    "date": msg.date.isoformat() if msg.date else None,
                },
            },
        }

        await vault_col.update_one({"content_id": content_id}, update_doc, upsert=True)

    logger.info(
        "vault_insert_completed",
        extra={
            "ctx_submitter": submitter_user_id,
            "ctx_dest": dest,
            "ctx_vault_ids": vault_message_ids
        }
    )

    return vault_message_ids

# ── Queue enqueue ─────────────────────────────────────────────────────────────

async def enqueue_for_distribution(
    messages: list,
    dest: str,
    submitter_user_id: int,
    vault_message_ids: list[int],
) -> bool:
    db = DatabaseManager.get_db()
    queue_repo = QueueRepository(db)

    target_group_id = _destination_group_id(dest)
    if not target_group_id:
        return False

    watermark_config = _get_watermark_config(dest)
    watermark_required = watermark_config is not None

    now = datetime.now(timezone.utc)
    deadline = now + timedelta(hours=settings.QUEUE_DEADLINE_HOURS)
    group_id = messages[0].media_group_id if messages else None
    
    source_channel_id = f"submission_{dest}"
    
    # Validation: Ensure all items have vault references
    if not all(vid > 0 for vid in vault_message_ids) or len(vault_message_ids) != len(messages):
        logger.error("Enqueue aborted: missing vault references for album items")
        return False

    for i, msg in enumerate(messages):
        media = getattr(msg, str(msg.media.value), None) if msg.media else None
        file_unique_id = getattr(media, "file_unique_id", None) if media else None
        
        content_id = _generate_content_id(msg.chat.id, msg.id, file_unique_id)
        vault_msg_id = vault_message_ids[i]

        media_type_str = msg.media.value if msg.media else "text"
        try:
            media_type = MediaType(media_type_str)
        except ValueError:
            media_type = MediaType.TEXT

        initial_status = JobStatus.WATERMARKING if watermark_required else JobStatus.PENDING

        job = QueueJob(
            schema_version=1,
            content_id=content_id,
            source_channel_id=source_channel_id,
            source_message_id=msg.id,
            vault_chat_id=settings.VAULT_CHANNEL_ID,
            vault_message_id=vault_msg_id,
            media_group_id=group_id,
            target_channel_ids=[str(target_group_id)],
            media_type=media_type,
            media_file_id=getattr(media, "file_id", None) if media else None,
            caption=msg.caption or msg.text or "",
            priority=DistributionPriority.MODERATED,
            status=initial_status,
            max_retries=settings.MAX_RETRY_ATTEMPTS,
            execute_after=now,
            queue_deadline=deadline,
            watermark_required=watermark_required,
            watermark_config=watermark_config,
            album_sequence_index=i if group_id else None,
            metadata={
                "submitter_user_id": submitter_user_id,
                "destination": dest,
                "moderated_at": now.isoformat(),
                "source_chat_id": msg.chat.id,
            },
        )

        try:
            await queue_repo.enqueue(job)
        except DuplicateJobError:
            continue
        except Exception:
            logger.error("Failed to enqueue moderated job", exc_info=True)
            return False

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
    1. Archive to vault with status=POSTED — prevents scheduler double-post
    2. Verify vault write succeeded (Bug 4 fix)
    3. Post immediately to destination group
    4. Delete moderation card
    5. Notify uploader
    6. Write audit log
    """
    display_name = _destination_display_name(dest)

    logger.info(
        "moderation_approved",
        extra={
            "ctx_moderator_id": moderator_id,
            "ctx_submitter_id": submitter_user_id,
            "ctx_dest": dest,
            "ctx_action": "approve_immediate"
        }
    )

    vault_ids = await archive_to_vault(
        client, messages, dest,
        submitter_user_id=submitter_user_id,
        initial_status=ModerationState.POSTED.value,
    )

    # Bug 4 fix: verify vault write before proceeding.
    # If all IDs are zero/empty the vault copy failed — abort, do NOT post publicly.
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
    
    # ── SYSTEM 13: HUB CLEANUP ──
    try:
        hub_msg_ids = [m.id for m in messages if m.chat.id == mod_card_chat_id]
        if hub_msg_ids:
            await client.delete_messages(mod_card_chat_id, hub_msg_ids)
    except Exception:
        pass

    await safe_dm(
        client,
        submitter_user_id,
        f"✅ Your content was approved.\n\nDestination:\n{display_name}",
    )

    # ── Audit & Activity ──
    try:
        from app.repositories.activity_repository import ActivityRepository
        from app.models.activity import ActivityAction
        activity_repo = ActivityRepository()
        await activity_repo.log_activity(
            user_id=submitter_user_id,
            action=ActivityAction.UPLOAD,
            metadata={"content_id": content_id if 'content_id' in locals() else "unknown"}
        )
    except Exception:
        pass

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
    2. Verify vault write succeeded (Bug 4 fix)
    3. Enqueue MODERATED-priority job
    4. Delete moderation card
    5. Notify uploader
    6. Write audit log
    """
    display_name = _destination_display_name(dest)

    logger.info(
        "moderation_approved",
        extra={
            "ctx_moderator_id": moderator_id,
            "ctx_submitter_id": submitter_user_id,
            "ctx_dest": dest,
            "ctx_action": "queue_for_distribution"
        }
    )

    vault_ids = await archive_to_vault(
        client, messages, dest,
        submitter_user_id=submitter_user_id,
        initial_status=ModerationState.QUEUED.value,
    )

    # Bug 4 fix: verify vault write before proceeding.
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

    queued = await enqueue_for_distribution(messages, dest, submitter_user_id, vault_ids)
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
    
    # ── SYSTEM 13: HUB CLEANUP ──
    try:
        hub_msg_ids = [m.id for m in messages if m.chat.id == mod_card_chat_id]
        if hub_msg_ids:
            await client.delete_messages(mod_card_chat_id, hub_msg_ids)
    except Exception:
        pass

    await safe_dm(
        client,
        submitter_user_id,
        f"✅ Your content has been queued for posting.\n\nDestination:\n{display_name}",
    )

    # ── Audit & Activity ──
    try:
        from app.repositories.activity_repository import ActivityRepository
        from app.models.activity import ActivityAction
        activity_repo = ActivityRepository()
        await activity_repo.log_activity(
            user_id=submitter_user_id,
            action=ActivityAction.UPLOAD,
            metadata={"content_id": content_id if 'content_id' in locals() else "unknown"}
        )
    except Exception:
        pass

    await get_audit().log(
        action=AuditAction.QUEUE,
        performed_by=moderator_id,
        details={
            "destination": dest,
            "submitter_user_id": submitter_user_id,
            "moderator_name": moderator_name,
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
    messages: Optional[list] = None,
    reason: str = "No reason provided",
) -> None:
    """
    Reject flow:
    - Update vault status to REJECTED (if messages provided)
    - Open support ticket for user with rejection context (System 11/13)
    - Delete Hub media messages (System 13 cleanup)
    - Moderation card updated
    - Uploader notified
    - Audit log written
    """
    if messages:
        try:
            db = DatabaseManager.get_db()
            vault_col = db[settings.VAULT_COLLECTION]
            now = datetime.now(timezone.utc)

            for msg in messages:
                media = getattr(msg, str(msg.media.value), None) if msg.media else None
                file_unique_id = getattr(media, "file_unique_id", None) if media else None
                content_id = _generate_content_id(msg.chat.id, msg.id, file_unique_id)

                await vault_col.update_one(
                    {"content_id": content_id},
                    {"$set": {"status": ModerationState.REJECTED, "updated_at": now}}
                )
        except Exception as e:
            logger.warning("Failed to update vault status to REJECTED", extra={"ctx_error": str(e)})

    # ── SYSTEM 11/13: AUTO-SUPPORT ON REJECT ──
    try:
        from app.services.support_service import get_support_service
        from app.services.topic_manager import TOPIC_SUPPORT

        # Ensure topic exists and notify user
        support_service = get_support_service()
        # We simulate a "message" from admin to start the topic
        await support_service.handle_user_message(
            client, 
            type("MockMsg", (), {"from_user": type("MockUser", (), {"id": submitter_user_id})(), "text": f"SYSTEM: Submission Rejected\nReason: {reason}"})()
        )
    except Exception as e:
        logger.warning("Failed to open support ticket on reject", extra={"ctx_error": str(e)})

    await safe_edit_message(
        client,
        mod_card_chat_id,
        mod_card_message_id,
        f"❌ <b>Rejected</b> by {moderator_name}\n"
        f"👤 Submitter: <code>{submitter_user_id}</code>\n"
        f"📝 Reason: <i>{reason}</i>",
    )

    # ── SYSTEM 13: HUB CLEANUP ──
    try:
        if messages:
            hub_msg_ids = [m.id for m in messages if m.chat.id == mod_card_chat_id]
            if hub_msg_ids:
                await client.delete_messages(mod_card_chat_id, hub_msg_ids)
    except Exception:
        pass

    await safe_dm(
        client,
        submitter_user_id,
        f"❌ <b>Your submission was rejected.</b>\n\n<b>Reason:</b> {reason}\n\n"
        "A support ticket has been opened for you to discuss this decision.",
    )

    # ── Audit & Activity ──
    try:
        from app.repositories.activity_repository import ActivityRepository
        from app.models.activity import ActivityAction
        activity_repo = ActivityRepository()
        await activity_repo.log_activity(
            user_id=submitter_user_id,
            action=ActivityAction.UPLOAD,
            metadata={"content_id": content_id if 'content_id' in locals() else "unknown"}
        )
    except Exception:
        pass

    await get_audit().log(
        action=AuditAction.REJECT,
        performed_by=moderator_id,
        target_user_id=submitter_user_id,
        details={"submitter_user_id": submitter_user_id, "reason": reason},
    )