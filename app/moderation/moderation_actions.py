from __future__ import annotations

# ── moderation_actions.py ──────────────────────────────────────────────────────
# Responsible for executing the three moderation verdicts:
#   • execute_approve()  — immediately posts content, archives to vault,
#                          AND enqueues for future vault_fill distribution.
#   • execute_queue()    — archives to vault and enqueues without
#                          immediately posting.
#   • execute_reject()   — marks vault record REJECTED, notifies submitter.
#
# Spec references:
#   Section 10.3 (Submission Flow — approval and rejection)
#   Section 11   (Vault System)
#   Section 12   (Queue Distribution Engine)
#   Section 22   (Audit Logging — dual: MongoDB + Admin Logs topic)
# ──────────────────────────────────────────────────────────────────────────────

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


def _resolve_vault_channel_id(dest: str) -> int:
    """
    Resolve the correct vault CHANNEL ID for a given moderation destination.

    ROOT CAUSE FIX (spec Section 11 — "Content must NEVER cross between
    vaults... There is no shared or generic vault"): archive_to_vault()
    previously hardcoded settings.VAULT_CHANNEL_ID for every single copy
    operation regardless of dest, meaning ALL approved content — NSFW and
    Premium alike — was always copied into the same legacy generic channel,
    in direct violation of the spec's strict separation rule. Premium-vault
    content was never actually reaching PREMIUM_VAULT_CHANNEL_ID at all.

    Args:
        dest: ``ModerationDestination.NSFW.value`` ("nsfw") or
              ``ModerationDestination.PREMIUM.value`` ("premium").

    Returns:
        The configured NSFW or Premium vault channel ID. Falls back to the
        legacy ``VAULT_CHANNEL_ID`` only if the destination-specific channel
        is not configured (0), preserving backward compatibility for
        deployments that haven't set the dedicated IDs yet.
    """
    if dest == ModerationDestination.NSFW.value:
        return settings.NSFW_VAULT_CHANNEL_ID or settings.VAULT_CHANNEL_ID
    if dest == ModerationDestination.PREMIUM.value:
        return settings.PREMIUM_VAULT_CHANNEL_ID or settings.VAULT_CHANNEL_ID
    return settings.VAULT_CHANNEL_ID


# ── Destination helpers ───────────────────────────────────────────────────────

def _destination_group_id(dest: str) -> int:
    """Return the Telegram group ID for the given moderation destination.

    Raises:
        ValueError: If *dest* is not a recognised ``ModerationDestination``.
    """
    if dest == ModerationDestination.NSFW:
        return settings.NSFW_GROUP_ID
    if dest == ModerationDestination.PREMIUM:
        return settings.PREMIUM_GROUP_ID
    raise ValueError(f"Unknown destination: {dest}")


def _destination_display_name(dest: str) -> str:
    """Return a human-readable display name for *dest*.

    Falls back to the raw value if the destination is not recognised.
    """
    if dest == ModerationDestination.NSFW:
        return settings.NSFW_DISPLAY_NAME
    if dest == ModerationDestination.PREMIUM:
        return settings.PREMIUM_DISPLAY_NAME
    return dest


def _get_watermark_config(dest: str) -> Optional[dict]:
    """Build a watermark configuration dict for *dest*, or return ``None``.

    Returns ``None`` (watermarking skipped) when:
      • ``settings.WATERMARK_ENABLED`` is False, OR
      • the watermark logo file is missing on disk.

    Callers should treat ``None`` as ``watermark_required=False``.
    """
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

    if not Path(logo_path).exists():
        logger.warning(
            "Watermark logo missing and WATERMARK_ENABLED=true — skipping watermarking",
            extra={"ctx_path": logo_path, "ctx_dest": dest},
        )
        return None

    return {
        "watermark_image_path": logo_path,
        "watermark_text": text,
        "position": random.choice(
            ["TOP_LEFT", "TOP_RIGHT", "BOTTOM_LEFT", "BOTTOM_RIGHT", "CENTER"]
        ),
        "opacity": settings.WATERMARK_OPACITY,
        "scale": settings.WATERMARK_SCALE,
        "destination": dest,
    }


def _compute_checksum(file_unique_id: Optional[str], file_size: int) -> Optional[str]:
    """Compute a deterministic SHA-256 checksum from *file_unique_id* and *file_size*.

    Returns ``None`` if *file_unique_id* is absent (e.g. text messages).
    """
    if not file_unique_id:
        return None
    raw = f"{file_unique_id}:{file_size}"
    return hashlib.sha256(raw.encode()).hexdigest()


# ── Safe Telegram ops ─────────────────────────────────────────────────────────

async def safe_dm(client: Client, user_id: int, text: str) -> None:
    """Send an HTML-formatted direct message to *user_id*, retrying on FloodWait.

    Failures are logged but never re-raised so that a DM failure never aborts
    a moderation flow.
    """
    for attempt in range(_MAX_RETRIES):
        try:
            await client.send_message(
                chat_id=user_id,
                text=text,
                parse_mode=ParseMode.HTML,
            )
            return
        except FloodWait as e:
            wait = int(e.value) + settings.FLOODWAIT_EXTRA_BUFFER
            logger.warning(
                "FloodWait DMing user",
                extra={"ctx_user_id": user_id, "ctx_wait": wait, "ctx_attempt": attempt + 1},
            )
            await asyncio.sleep(wait)
        except RPCError as e:
            logger.warning(
                "RPCError DMing user",
                extra={"ctx_user_id": user_id, "ctx_error": str(e), "ctx_attempt": attempt + 1},
            )
            if attempt == _MAX_RETRIES - 1:
                logger.error(
                    "Failed to DM user after all retries",
                    extra={"ctx_user_id": user_id, "ctx_error": str(e)},
                )
                return
            await asyncio.sleep(2 ** attempt)
        except Exception as e:
            logger.error(
                "Unexpected error DMing user",
                extra={"ctx_user_id": user_id, "ctx_error": str(e)},
            )
            return


async def safe_delete_message(client: Client, chat_id: int, message_id: int) -> None:
    """Attempt to delete *message_id* in *chat_id*.

    Failure is logged and silently swallowed so that a failed delete never
    blocks a moderation flow.
    """
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
    """Edit *message_id* in *chat_id* to *text*, retrying on FloodWait.

    Failures are logged.  After exhausting all retries the function returns
    without raising so that callers are not interrupted.
    """
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
            wait = int(e.value) + settings.FLOODWAIT_EXTRA_BUFFER
            logger.warning(
                "FloodWait editing moderation message",
                extra={
                    "ctx_chat_id": chat_id,
                    "ctx_msg_id": message_id,
                    "ctx_wait": wait,
                    "ctx_attempt": attempt + 1,
                },
            )
            await asyncio.sleep(wait)
        except RPCError as e:
            logger.warning(
                "RPCError editing moderation message",
                extra={
                    "ctx_chat_id": chat_id,
                    "ctx_msg_id": message_id,
                    "ctx_error": str(e),
                    "ctx_attempt": attempt + 1,
                },
            )
            if attempt == _MAX_RETRIES - 1:
                logger.error(
                    "Failed to edit moderation message after all retries",
                    extra={"ctx_chat_id": chat_id, "ctx_msg_id": message_id, "ctx_error": str(e)},
                )
            return
        except Exception as e:
            logger.error(
                "Unexpected error editing moderation message",
                extra={"ctx_chat_id": chat_id, "ctx_msg_id": message_id, "ctx_error": str(e)},
            )
            return


async def post_to_destination(
    client: Client,
    messages: list,
    dest: str,
) -> bool:
    """Copy submitted content to the distribution group for *dest*.

    Uses ``copy_message`` (not ``forward_messages``) to produce a clean copy
    with no Telegram "Forwarded from …" attribution.

    Albums (RC-12) are copied atomically via ``copy_media_group``.  If that
    call fails after all retries, the function falls back to sequential
    ``copy_message`` per item.

    Args:
        client:   Active Pyrogram client.
        messages: Submitted messages to copy (1 or more).
        dest:     Moderation destination (``ModerationDestination.NSFW`` or
                  ``ModerationDestination.PREMIUM``).

    Returns:
        True if all messages were delivered, False on unrecoverable error.
    """
    group_id = _destination_group_id(dest)
    if not group_id:
        logger.error("Destination group ID not configured", extra={"ctx_dest": dest})
        return False

    is_album = len(messages) > 1 and all(m.media_group_id for m in messages)
    album_succeeded = False

    if is_album:
        try:
            for attempt in range(_MAX_RETRIES):
                try:
                    await client.copy_media_group(
                        chat_id=group_id,
                        from_chat_id=messages[0].chat.id,
                        message_id=messages[0].id,
                    )
                    album_succeeded = True
                    return True
                except FloodWait as e:
                    wait = int(e.value) + settings.FLOODWAIT_EXTRA_BUFFER
                    logger.warning(
                        "FloodWait during copy_media_group to destination",
                        extra={"ctx_dest": dest, "ctx_wait": wait, "ctx_attempt": attempt + 1},
                    )
                    await asyncio.sleep(wait)
                except RPCError as e:
                    logger.warning(
                        "RPCError during copy_media_group to destination",
                        extra={"ctx_dest": dest, "ctx_error": str(e), "ctx_attempt": attempt + 1},
                    )
                    if attempt == _MAX_RETRIES - 1:
                        raise
                    await asyncio.sleep(2 ** attempt)
        except Exception as e:
            logger.warning(
                "copy_media_group_failed_in_post_to_destination_fallback_to_sequential",
                extra={"ctx_error": str(e), "ctx_dest": dest},
            )

    if album_succeeded:
        # Album copy already returned True above; this path is unreachable
        # but guards against any future refactoring.
        return True

    # Sequential fallback (also handles single-item submissions)
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
                    "RPCError posting to destination",
                    extra={"ctx_dest": dest, "ctx_error": str(e), "ctx_attempt": attempt + 1},
                )
                if attempt == _MAX_RETRIES - 1:
                    return False
                await asyncio.sleep(2 ** attempt)
            except Exception as e:
                logger.error(
                    "Unexpected error posting to destination",
                    extra={"ctx_dest": dest, "ctx_error": str(e)},
                )
                return False

    return True


def _generate_content_id(chat_id: int, message_id: int, file_unique_id: Optional[str]) -> str:
    """Generate a deterministic SHA-256 content ID from source coordinates.

    The content ID is stable across restarts and survives message edits
    because it is based on immutable source identifiers.
    """
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
    """Archive submitted messages to the vault channel and record metadata in MongoDB.

    Two-step process per message:
      1. ``copy_message`` (or ``copy_media_group`` for albums) to the vault
         channel configured for *dest*.
      2. Upsert a document into the ``VAULT_COLLECTION`` MongoDB collection.

    The upsert is idempotent — running again with the same *chat_id* /
    *message_id* pair will overwrite mutable fields but not re-copy the media.

    Args:
        client:             Active Pyrogram client.
        messages:           Submitted messages to archive.
        dest:               Moderation destination — controls which vault
                            channel receives the content.
        submitter_user_id:  Used for metadata and logging.
        consent_record_id:  Optional reference to the user's consent record.
                            Looked up from ConsentService if omitted.
        initial_status:     Initial ``status`` value written to MongoDB.
                            Callers should pass either ``QUEUED`` (for queue
                            flow) or ``POSTED`` (for immediate-approve flow).

    Returns:
        A list of vault channel message IDs, one per input message.  An entry
        is 0 if the media copy for that message failed.
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
            logger.warning(
                "Failed to fetch consent record",
                extra={"ctx_submitter": submitter_user_id, "ctx_error": str(e)},
            )

    vault_message_ids: list[int] = []
    db = DatabaseManager.get_db()
    vault_col = db[settings.VAULT_COLLECTION]
    now = datetime.now(timezone.utc)

    # ROOT CAUSE FIX (spec Section 11): resolve the destination-specific
    # vault channel ONCE here, instead of hardcoding settings.VAULT_CHANNEL_ID
    # at every copy_message/copy_media_group call site below.
    target_vault_channel_id = _resolve_vault_channel_id(dest)

    logger.info(
        "vault_insert_started",
        extra={
            "ctx_submitter": submitter_user_id,
            "ctx_dest": dest,
            "ctx_count": len(messages),
        },
    )

    # ── RC-12: Atomic Vault Archival ────────────────────────────────────────
    # Attempt album copy first; fall back to sequential on failure.
    is_album = len(messages) > 1 and all(m.media_group_id for m in messages)
    copied_messages: list = []

    if is_album:
        try:
            for attempt in range(_MAX_RETRIES):
                try:
                    copied_messages = await client.copy_media_group(
                        chat_id=target_vault_channel_id,
                        from_chat_id=messages[0].chat.id,
                        message_id=messages[0].id,
                    )
                    break
                except FloodWait as e:
                    wait = int(e.value) + settings.FLOODWAIT_EXTRA_BUFFER
                    logger.warning(
                        "FloodWait during copy_media_group in vault archival",
                        extra={
                            "ctx_submitter": submitter_user_id,
                            "ctx_wait": wait,
                            "ctx_attempt": attempt + 1,
                        },
                    )
                    await asyncio.sleep(wait)
                except RPCError as e:
                    logger.warning(
                        "RPCError during copy_media_group in vault archival",
                        extra={
                            "ctx_submitter": submitter_user_id,
                            "ctx_error": str(e),
                            "ctx_attempt": attempt + 1,
                        },
                    )
                    if attempt == _MAX_RETRIES - 1:
                        raise
                    await asyncio.sleep(2 ** attempt)
        except Exception as e:
            logger.warning(
                "copy_media_group_failed_in_vault_archival_fallback_to_sequential",
                extra={"ctx_error": str(e), "ctx_submitter": submitter_user_id},
            )
            copied_messages = []

    # Individual copy (non-album or album fallback)
    if not copied_messages:
        if not target_vault_channel_id:
            logger.error(
                "vault channel not configured for destination — cannot archive to vault",
                extra={"ctx_submitter": submitter_user_id, "ctx_dest": dest},
            )
            return []

        for msg in messages:
            copied_msg = None
            for attempt in range(_MAX_RETRIES):
                try:
                    copied_msg = await client.copy_message(
                        chat_id=target_vault_channel_id,
                        from_chat_id=msg.chat.id,
                        message_id=msg.id,
                    )
                    break
                except FloodWait as e:
                    wait = int(e.value) + settings.FLOODWAIT_EXTRA_BUFFER
                    logger.warning(
                        "FloodWait during individual vault copy",
                        extra={
                            "ctx_submitter": submitter_user_id,
                            "ctx_msg_id": msg.id,
                            "ctx_wait": wait,
                            "ctx_attempt": attempt + 1,
                        },
                    )
                    await asyncio.sleep(wait)
                except RPCError as e:
                    logger.warning(
                        "RPCError during individual vault copy",
                        extra={
                            "ctx_submitter": submitter_user_id,
                            "ctx_msg_id": msg.id,
                            "ctx_error": str(e),
                            "ctx_attempt": attempt + 1,
                        },
                    )
                    if attempt == _MAX_RETRIES - 1:
                        logger.error(
                            "Vault copy failed after all retries",
                            extra={"ctx_submitter": submitter_user_id, "ctx_msg_id": msg.id},
                        )
                    await asyncio.sleep(2 ** attempt)
                except Exception as e:
                    logger.error(
                        "Unexpected error during individual vault copy",
                        extra={
                            "ctx_submitter": submitter_user_id,
                            "ctx_msg_id": msg.id,
                            "ctx_error": str(e),
                        },
                    )
                    break
            # Append even if None to keep index alignment with messages[]
            copied_messages.append(copied_msg)

    vault_message_ids = [m.id if m else 0 for m in copied_messages]

    # Upsert MongoDB documents for each message
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
        if media_type_str in (MediaType.PHOTO.value, "photo"):
            try:
                photo_bytes = await client.download_media(msg, in_memory=True)
                from app.utils.media_hashing import calculate_image_hash
                content_hash = calculate_image_hash(photo_bytes)

                duplicate = await vault_col.find_one({"content_hash": content_hash})
                if duplicate:
                    logger.warning(
                        "duplicate_content_hash_detected",
                        extra={
                            "ctx_content_id": content_id,
                            "ctx_existing": duplicate["content_id"],
                        },
                    )
                    # Still archived, tagged implicitly via duplicate hash match
            except Exception as e:
                logger.warning(
                    "image_hashing_failed",
                    extra={"ctx_media_type": media_type_str, "ctx_error": str(e)},
                )

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
                "vault_channel_id": str(target_vault_channel_id) if vault_msg_id else None,
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

        try:
            await vault_col.update_one({"content_id": content_id}, update_doc, upsert=True)
        except Exception as e:
            logger.error(
                "Failed to upsert vault document",
                extra={"ctx_content_id": content_id, "ctx_error": str(e)},
            )

    logger.info(
        "vault_insert_completed",
        extra={
            "ctx_submitter": submitter_user_id,
            "ctx_dest": dest,
            "ctx_vault_ids": vault_message_ids,
        },
    )

    return vault_message_ids


# ── Queue enqueue ─────────────────────────────────────────────────────────────

async def enqueue_for_distribution(
    messages: list,
    dest: str,
    submitter_user_id: int,
    vault_message_ids: list[int],
    execute_after: Optional[datetime] = None,
) -> bool:
    """Create ``QueueJob`` documents so the distribution worker can deliver content.

    Each message in *messages* becomes one ``QueueJob`` record.  All jobs
    share the same watermark config derived from *dest*.

    Args:
        messages:           Submitted messages (must align 1-to-1 with
                            *vault_message_ids*).
        dest:               Moderation destination — controls target channel
                            and watermark config.
        submitter_user_id:  Embedded in job metadata for tracing.
        vault_message_ids:  Vault channel message IDs returned by
                            ``archive_to_vault()``.  Must be non-zero for
                            every item; if any are 0 the function aborts.
        execute_after:      Optional earliest execution time.  Defaults to
                            ``datetime.now(utc)`` (process immediately).
                            Pass a future timestamp to implement a re-post
                            cooldown (e.g. for vault_fill after an immediate
                            approve).

    Returns:
        True if all jobs were enqueued (duplicate jobs are skipped, not
        counted as failures), False if any non-duplicate error occurred.
    """
    db = DatabaseManager.get_db()
    queue_repo = QueueRepository(db)

    target_group_id = _destination_group_id(dest)
    if not target_group_id:
        logger.error(
            "enqueue_for_distribution: target group ID not configured",
            extra={"ctx_dest": dest},
        )
        return False

    # Require all vault references to be present before creating any job
    if not vault_message_ids or len(vault_message_ids) != len(messages):
        logger.error(
            "enqueue_for_distribution aborted: vault_message_ids count mismatch",
            extra={
                "ctx_dest": dest,
                "ctx_submitter": submitter_user_id,
                "ctx_vault_count": len(vault_message_ids),
                "ctx_msg_count": len(messages),
            },
        )
        return False

    if not all(vid > 0 for vid in vault_message_ids):
        logger.error(
            "enqueue_for_distribution aborted: one or more vault IDs are 0",
            extra={"ctx_dest": dest, "ctx_submitter": submitter_user_id},
        )
        return False

    watermark_config = _get_watermark_config(dest)
    watermark_required = watermark_config is not None

    # ROOT CAUSE FIX (spec Section 11 / distribution-side counterpart to the
    # archive_to_vault fix above): this previously hardcoded
    # settings.VAULT_CHANNEL_ID into every QueueJob's vault_chat_id field,
    # regardless of dest. After the archive_to_vault fix, Premium-destination
    # content is correctly copied into PREMIUM_VAULT_CHANNEL_ID, but the
    # delivery worker reads `vault_chat_id` from THIS job document to know
    # which channel to copy the message FROM when distributing to the target
    # group. A mismatched vault_chat_id means the worker looks for
    # vault_message_id inside the WRONG channel — the message simply isn't
    # there, the copy fails, and approved content never reaches its target
    # group even though it sits correctly archived in the vault.
    resolved_vault_chat_id = _resolve_vault_channel_id(dest)

    now = datetime.now(timezone.utc)
    effective_execute_after = execute_after if execute_after is not None else now
    deadline = now + timedelta(hours=settings.QUEUE_DEADLINE_HOURS)
    group_id = messages[0].media_group_id if messages else None
    source_channel_id = f"submission_{dest}"

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
            vault_chat_id=resolved_vault_chat_id,
            vault_message_id=vault_msg_id,
            media_group_id=group_id,
            target_channel_ids=[str(target_group_id)],
            media_type=media_type,
            media_file_id=getattr(media, "file_id", None) if media else None,
            caption=msg.caption or msg.text or "",
            priority=DistributionPriority.MODERATED,
            status=initial_status,
            max_retries=settings.MAX_RETRY_ATTEMPTS,
            execute_after=effective_execute_after,
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
            logger.info(
                "enqueue_for_distribution: duplicate job skipped",
                extra={"ctx_content_id": content_id, "ctx_dest": dest},
            )
            continue
        except Exception as e:
            logger.error(
                "enqueue_for_distribution: failed to enqueue job",
                extra={"ctx_content_id": content_id, "ctx_dest": dest, "ctx_error": str(e)},
                exc_info=True,
            )
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
    """Execute the immediate-approve moderation verdict.

    Flow (per spec 10.3):
      1. Archive content to the vault channel with ``status=POSTED``.
      2. Post immediately to the distribution group for *dest*.
      3. Enqueue a vault_fill queue job (``execute_after`` = now + cooldown)
         so the item is eligible for future random re-distribution.
         This is the KEY FIX for the audit finding: ``execute_approve()``
         previously skipped ``enqueue_for_distribution()``, so items with
         ``status=POSTED`` were invisible to ``fetch_distribution_content()``
         which queries only for ``status=QUEUED``.
      4. Delete the moderation card and hub media messages.
      5. DM the submitter.
      6. Post a status update to the user's hub topic.
      7. Emit to Admin Logs topic and ``audit_logs`` collection.

    Args:
        client:              Active Pyrogram client.
        messages:            Submitted messages being approved.
        submitter_user_id:   Submitter's Telegram user ID.
        dest:                Moderation destination (NSFW or PREMIUM).
        mod_card_chat_id:    Chat ID of the moderation card message.
        mod_card_message_id: Message ID of the moderation card.
        moderator_name:      Display name of the approving moderator.
        moderator_id:        Telegram user ID of the approving moderator.
    """
    display_name = _destination_display_name(dest)

    logger.info(
        "moderation_approved",
        extra={
            "ctx_moderator_id": moderator_id,
            "ctx_submitter_id": submitter_user_id,
            "ctx_dest": dest,
            "ctx_action": "approve_immediate",
        },
    )

    # Step 1 — Vault archival (status=POSTED records the immediate post)
    vault_ids = await archive_to_vault(
        client,
        messages,
        dest,
        submitter_user_id=submitter_user_id,
        initial_status=ModerationState.POSTED.value,
    )

    vault_success = any(vid for vid in vault_ids if vid)
    if not vault_success:
        logger.error(
            "execute_approve aborted: vault archival failed",
            extra={"ctx_dest": dest, "ctx_submitter": submitter_user_id},
        )
        await safe_edit_message(
            client,
            mod_card_chat_id,
            mod_card_message_id,
            f"⚠️ <b>Vault archival failed</b> — content NOT posted.\n"
            f"Approved by {moderator_name}.\n"
            f"👤 Submitter: <code>{submitter_user_id}</code>",
        )
        return

    # Step 2 — Immediate post to destination group
    posted = await post_to_destination(client, messages, dest)
    if not posted:
        logger.error(
            "execute_approve: failed to post to destination",
            extra={"ctx_dest": dest, "ctx_submitter": submitter_user_id},
        )
        await safe_edit_message(
            client,
            mod_card_chat_id,
            mod_card_message_id,
            f"⚠️ <b>Approved</b> by {moderator_name} but delivery failed.\n"
            f"👤 Submitter: <code>{submitter_user_id}</code>",
        )
        await safe_dm(
            client,
            submitter_user_id,
            f"✅ Your content was approved.\n\nDestination:\n{display_name}\n\n"
            "⚠️ Note: delivery encountered an issue.",
        )
        return

    # Step 3 — Enqueue for future vault_fill distribution.
    # AUDIT FIX (HIGH): Previously this call was absent.  Items archived with
    # status=POSTED were never seen by fetch_distribution_content() (which
    # queries status=QUEUED).  By enqueuing with a cooldown execute_after, the
    # item enters the distribution pool without being immediately re-posted.
    cooldown_hours = getattr(settings, "VAULT_FILL_COOLDOWN_HOURS", 24)
    vault_fill_execute_after = datetime.now(timezone.utc) + timedelta(hours=cooldown_hours)

    queued_for_fill = await enqueue_for_distribution(
        messages,
        dest,
        submitter_user_id,
        vault_ids,
        execute_after=vault_fill_execute_after,
    )
    if not queued_for_fill:
        # Non-fatal: content was already posted; vault_fill entry is best-effort.
        logger.warning(
            "execute_approve: vault_fill enqueue failed — content posted but not in re-post pool",
            extra={"ctx_dest": dest, "ctx_submitter": submitter_user_id},
        )

    # Set cooldown_until and lock distribution_state on vault docs so the
    # provider does not return them before the cooldown window expires.
    #
    # WHY: enqueue_for_distribution() creates a queue job with
    # execute_after = vault_fill_execute_after. However, vault docs do not
    # carry execute_after — the scheduler reads vault docs through provider.py
    # and creates NEW jobs. Without cooldown_until on the vault doc the
    # provider returns the item immediately every cycle, and the scheduler
    # keeps hitting vault_ref_unique DuplicateKeyError (because an active
    # job already exists) until the watermark + delivery completes. Setting
    # cooldown_until = vault_fill_execute_after means the provider will not
    # return this item until the active job has completed, eliminating the
    # redundant DuplicateKeyError attempts entirely.
    #
    # The cooldown is also set here (not only in mark_completed) because this
    # item was just immediately posted — we want the replay cooldown to start
    # from now, not from when the fill job eventually runs.
    try:
        db = DatabaseManager.get_db()
        vault_col = db[getattr(settings, "VAULT_COLLECTION", "vault")]
        resolved_vault_channel = str(_resolve_vault_channel_id(dest))
        valid_vault_ids = [vid for vid in vault_ids if vid and vid > 0]
        if valid_vault_ids:
            await vault_col.update_many(
                {
                    "vault_message_id": {"$in": valid_vault_ids},
                    "vault_channel_id": resolved_vault_channel,
                },
                {
                    "$set": {
                        "distribution_state": "pending_delivery",
                        "cooldown_until": vault_fill_execute_after,
                        "updated_at": datetime.now(timezone.utc),
                    }
                },
            )
    except Exception as cooldown_err:
        # Non-fatal: content was already posted and enqueued.
        logger.warning(
            "execute_approve: vault cooldown_until write failed (non-fatal)",
            extra={"ctx_dest": dest, "ctx_submitter": submitter_user_id, "ctx_error": str(cooldown_err)},
        )

    # Step 4 — Clean up moderation card and hub media
    await safe_delete_message(client, mod_card_chat_id, mod_card_message_id)
    try:
        hub_msg_ids = [m.id for m in messages if m.chat.id == mod_card_chat_id]
        if hub_msg_ids:
            await client.delete_messages(mod_card_chat_id, hub_msg_ids)
    except Exception as e:
        logger.warning(
            "execute_approve: could not delete hub media messages",
            extra={"ctx_chat_id": mod_card_chat_id, "ctx_error": str(e)},
        )

    # Step 5 — Notify submitter
    await safe_dm(
        client,
        submitter_user_id,
        f"✅ Your content was approved.\n\nDestination:\n{display_name}",
    )

    # Step 6 — Log to user's hub topic
    try:
        from app.services.topic_manager import get_topic_manager
        topic_id = await get_topic_manager().get_or_create_user_topic(client, submitter_user_id)

        await client.send_message(
            chat_id=settings.VERIFICATION_GROUP_ID,
            text=(
                f"✅ <b>CONTENT APPROVED</b>\n\n"
                f"<b>Destination:</b> {display_name}\n"
                f"<b>Moderator:</b> {moderator_name}"
            ),
            message_thread_id=topic_id,
            parse_mode=ParseMode.HTML,
        )
    except Exception as e:
        logger.warning(
            "execute_approve: failed to route approval notice to user topic",
            extra={"ctx_submitter": submitter_user_id, "ctx_error": str(e)},
        )

    # Step 7a — Admin Logs topic (spec 9.4 + 22: dual logging required)
    action_label = (
        "CONTENT APPROVED NSFW"
        if dest == ModerationDestination.NSFW
        else "CONTENT APPROVED PREMIUM"
    )
    try:
        from app.services.admin_logger import get_admin_logger
        await get_admin_logger().log(
            client=client,
            action=action_label,
            admin_id=moderator_id,
            admin_name=moderator_name,
            target_user_id=submitter_user_id,
            details=f"Destination: {display_name}",
        )
    except Exception as e:
        logger.warning(
            "execute_approve: failed to write Admin Logs entry",
            extra={"ctx_moderator_id": moderator_id, "ctx_error": str(e)},
        )

    # Step 7b — Activity log
    last_content_id = "unknown"
    try:
        if messages:
            ref_msg = messages[0]
            media = getattr(ref_msg, str(ref_msg.media.value), None) if ref_msg.media else None
            file_unique_id = getattr(media, "file_unique_id", None) if media else None
            last_content_id = _generate_content_id(ref_msg.chat.id, ref_msg.id, file_unique_id)

        from app.repositories.activity_repository import ActivityRepository
        from app.models.activity import ActivityAction
        activity_repo = ActivityRepository()
        await activity_repo.log_activity(
            user_id=submitter_user_id,
            action=ActivityAction.QUEUE, # NEW-09 FIX
            metadata={"content_id": last_content_id},
        )
    except Exception as e:
        logger.warning(
            "execute_approve: failed to write activity log",
            extra={"ctx_submitter": submitter_user_id, "ctx_error": str(e)},
        )

    # Step 7c — Audit log (MongoDB)
    await get_audit().log(
        action=AuditAction.APPROVE,
        performed_by=moderator_id,
        details={
            "destination": dest,
            "submitter_user_id": submitter_user_id,
            "vault_message_ids": vault_ids,
            "vault_fill_enqueued": queued_for_fill,
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
    """Execute the queue-for-later moderation verdict.

    Flow (per spec 10.3):
      1. Archive content to the vault channel with ``status=QUEUED``.
      2. Create a distribution queue job (JobStatus.PENDING or WATERMARKING).
      3. Delete the moderation card and hub media messages.
      4. DM the submitter.
      5. Post a status update to the user's hub topic.
      6. Emit to Admin Logs topic and ``audit_logs`` collection.

    Args:
        client:              Active Pyrogram client.
        messages:            Submitted messages being queued.
        submitter_user_id:   Submitter's Telegram user ID.
        dest:                Moderation destination (NSFW or PREMIUM).
        mod_card_chat_id:    Chat ID of the moderation card message.
        mod_card_message_id: Message ID of the moderation card.
        moderator_name:      Display name of the moderator who queued.
        moderator_id:        Telegram user ID of the moderator.
    """
    display_name = _destination_display_name(dest)

    # FIX: was incorrectly "moderation_approved"; now correctly "moderation_queued"
    logger.info(
        "moderation_queued",
        extra={
            "ctx_moderator_id": moderator_id,
            "ctx_submitter_id": submitter_user_id,
            "ctx_dest": dest,
            "ctx_action": "queue_for_distribution",
        },
    )

    # Step 1 — Vault archival
    vault_ids = await archive_to_vault(
        client,
        messages,
        dest,
        submitter_user_id=submitter_user_id,
        initial_status=ModerationState.QUEUED.value,
    )

    vault_success = any(vid for vid in vault_ids if vid)
    if not vault_success:
        logger.error(
            "execute_queue aborted: vault archival failed",
            extra={"ctx_dest": dest, "ctx_submitter": submitter_user_id},
        )
        await safe_edit_message(
            client,
            mod_card_chat_id,
            mod_card_message_id,
            f"⚠️ <b>Vault archival failed</b> — content NOT queued.\n"
            f"Queued by {moderator_name}.\n"
            f"👤 Submitter: <code>{submitter_user_id}</code>",
        )
        return

    # Step 2 — Queue for distribution
    queued = await enqueue_for_distribution(messages, dest, submitter_user_id, vault_ids)
    if not queued:
        logger.error(
            "execute_queue: failed to enqueue distribution job",
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

    # Lock vault docs so provider skips them while the active job exists.
    # Without this the scheduler hits vault_ref_unique DuplicateJobError every
    # cycle (harmless but noisy). mark_completed() releases the lock and sets
    # cooldown_until after delivery.
    try:
        db = DatabaseManager.get_db()
        vault_col = db[getattr(settings, "VAULT_COLLECTION", "vault")]
        resolved_vault_channel = str(_resolve_vault_channel_id(dest))
        valid_vault_ids = [vid for vid in vault_ids if vid and vid > 0]
        if valid_vault_ids:
            await vault_col.update_many(
                {
                    "vault_message_id": {"$in": valid_vault_ids},
                    "vault_channel_id": resolved_vault_channel,
                },
                {
                    "$set": {
                        "distribution_state": "pending_delivery",
                        "updated_at": datetime.now(timezone.utc),
                    }
                },
            )
    except Exception as lock_err:
        logger.warning(
            "execute_queue: vault distribution_state lock write failed (non-fatal)",
            extra={"ctx_dest": dest, "ctx_submitter": submitter_user_id, "ctx_error": str(lock_err)},
        )

    # Step 3 — Clean up moderation card and hub media
    await safe_delete_message(client, mod_card_chat_id, mod_card_message_id)
    try:
        hub_msg_ids = [m.id for m in messages if m.chat.id == mod_card_chat_id]
        if hub_msg_ids:
            await client.delete_messages(mod_card_chat_id, hub_msg_ids)
    except Exception as e:
        logger.warning(
            "execute_queue: could not delete hub media messages",
            extra={"ctx_chat_id": mod_card_chat_id, "ctx_error": str(e)},
        )

    # Step 4 — Notify submitter
    await safe_dm(
        client,
        submitter_user_id,
        f"✅ Your content has been queued for posting.\n\nDestination:\n{display_name}",
    )

    # Step 5 — Log to user's hub topic
    try:
        from app.services.topic_manager import get_topic_manager
        topic_id = await get_topic_manager().get_or_create_user_topic(client, submitter_user_id)

        await client.send_message(
            chat_id=settings.VERIFICATION_GROUP_ID,
            text=(
                f"⏳ <b>CONTENT QUEUED</b>\n\n"
                f"<b>Destination:</b> {display_name}\n"
                f"<b>Moderator:</b> {moderator_name}"
            ),
            message_thread_id=topic_id,
            parse_mode=ParseMode.HTML,
        )
    except Exception as e:
        logger.warning(
            "execute_queue: failed to route queued notice to user topic",
            extra={"ctx_submitter": submitter_user_id, "ctx_error": str(e)},
        )

    # Step 6a — Admin Logs topic
    action_label = (
        "CONTENT APPROVED NSFW"
        if dest == ModerationDestination.NSFW
        else "CONTENT APPROVED PREMIUM"
    )
    try:
        from app.services.admin_logger import get_admin_logger
        await get_admin_logger().log(
            client=client,
            action=action_label,
            admin_id=moderator_id,
            admin_name=moderator_name,
            target_user_id=submitter_user_id,
            details=f"Queued for: {display_name}",
        )
    except Exception as e:
        logger.warning(
            "execute_queue: failed to write Admin Logs entry",
            extra={"ctx_moderator_id": moderator_id, "ctx_error": str(e)},
        )

    # Step 6b — Activity log
    last_content_id = "unknown"
    try:
        if messages:
            ref_msg = messages[0]
            media = getattr(ref_msg, str(ref_msg.media.value), None) if ref_msg.media else None
            file_unique_id = getattr(media, "file_unique_id", None) if media else None
            last_content_id = _generate_content_id(ref_msg.chat.id, ref_msg.id, file_unique_id)

        from app.repositories.activity_repository import ActivityRepository
        from app.models.activity import ActivityAction
        activity_repo = ActivityRepository()
        await activity_repo.log_activity(
            user_id=submitter_user_id,
            action=ActivityAction.QUEUE, # NEW-09 FIX
            metadata={"content_id": last_content_id},
        )
    except Exception as e:
        logger.warning(
            "execute_queue: failed to write activity log",
            extra={"ctx_submitter": submitter_user_id, "ctx_error": str(e)},
        )

    # Step 6c — Audit log (MongoDB)
    await get_audit().log(
        action=AuditAction.QUEUE,
        performed_by=moderator_id,
        details={
            "destination": dest,
            "submitter_user_id": submitter_user_id,
            "moderator_name": moderator_name,
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
    """Execute the reject moderation verdict.

    Flow (per spec 10.3):
      1. Update vault document(s) to ``status=REJECTED``.
      2. Log rejection to the user's hub topic and re-flag topic as pending
         for support follow-up.
      3. Emit to Admin Logs topic.
      4. Update the moderation card to show rejected state.
      5. Delete hub media messages.
      6. DM the submitter with the rejection reason.
      7. Emit to ``audit_logs`` collection.

    Args:
        client:              Active Pyrogram client.
        submitter_user_id:   Submitter's Telegram user ID.
        mod_card_chat_id:    Chat ID of the moderation card message.
        mod_card_message_id: Message ID of the moderation card.
        moderator_name:      Display name of the rejecting moderator.
        moderator_id:        Telegram user ID of the rejecting moderator.
        messages:            Submitted messages (used to update vault status).
        reason:              Mandatory rejection reason (spec 10.3).
    """
    # Step 1 — Update vault status to REJECTED
    last_content_id = "unknown"
    if messages:
        try:
            db = DatabaseManager.get_db()
            vault_col = db[settings.VAULT_COLLECTION]
            now = datetime.now(timezone.utc)

            for msg in messages:
                media = getattr(msg, str(msg.media.value), None) if msg.media else None
                file_unique_id = getattr(media, "file_unique_id", None) if media else None
                content_id = _generate_content_id(msg.chat.id, msg.id, file_unique_id)
                last_content_id = content_id

                await vault_col.update_one(
                    {"content_id": content_id},
                    {"$set": {"status": ModerationState.REJECTED.value, "updated_at": now}},
                )
        except Exception as e:
            logger.warning(
                "execute_reject: failed to update vault status to REJECTED",
                extra={"ctx_submitter": submitter_user_id, "ctx_error": str(e)},
            )

    # Step 2 — Log to user's hub topic and re-flag as pending
    try:
        from app.services.topic_manager import get_topic_manager
        topic_id = await get_topic_manager().get_or_create_user_topic(client, submitter_user_id)

        await client.send_message(
            chat_id=settings.VERIFICATION_GROUP_ID,
            text=(
                f"❌ <b>CONTENT REJECTED</b>\n\n"
                f"<b>Moderator:</b> {moderator_name}\n"
                f"<b>Reason:</b> {reason}"
            ),
            message_thread_id=topic_id,
            parse_mode=ParseMode.HTML,
        )

        db = DatabaseManager.get_db()
        await db["user_topics"].update_one(
            {"user_id": submitter_user_id},
            {"$set": {"status": "pending", "updated_at": datetime.now(timezone.utc)}},
        )
    except Exception as e:
        logger.warning(
            "execute_reject: failed to route rejection notice to user topic",
            extra={"ctx_submitter": submitter_user_id, "ctx_error": str(e)},
        )

    # Step 3 — Admin Logs topic
    try:
        from app.services.admin_logger import get_admin_logger
        await get_admin_logger().log(
            client=client,
            action="CONTENT REJECTED",
            admin_id=moderator_id,
            admin_name=moderator_name,
            target_user_id=submitter_user_id,
            details=f"Reason: {reason}",
        )
    except Exception as e:
        logger.warning(
            "execute_reject: failed to write Admin Logs entry",
            extra={"ctx_moderator_id": moderator_id, "ctx_error": str(e)},
        )

    # Step 4 — Update moderation card
    await safe_edit_message(
        client,
        mod_card_chat_id,
        mod_card_message_id,
        f"❌ <b>Rejected</b> by {moderator_name}\n"
        f"👤 Submitter: <code>{submitter_user_id}</code>\n"
        f"📝 Reason: <i>{reason}</i>",
    )

    # Step 5 — Delete hub media messages
    if messages:
        try:
            hub_msg_ids = [m.id for m in messages if m.chat.id == mod_card_chat_id]
            if hub_msg_ids:
                await client.delete_messages(mod_card_chat_id, hub_msg_ids)
        except Exception as e:
            logger.warning(
                "execute_reject: could not delete hub media messages",
                extra={"ctx_chat_id": mod_card_chat_id, "ctx_error": str(e)},
            )

    # Step 6 — Notify submitter
    await safe_dm(
        client,
        submitter_user_id,
        f"❌ <b>Your submission was rejected.</b>\n\n<b>Reason:</b> {reason}\n\n"
        "A support ticket has been opened for you to discuss this decision.",
    )

    # Step 7a — Activity log
    # FIX: was incorrectly ActivityAction.UPLOAD; now correctly ActivityAction.REJECT
    try:
        from app.repositories.activity_repository import ActivityRepository
        from app.models.activity import ActivityAction
        activity_repo = ActivityRepository()
        await activity_repo.log_activity(
            user_id=submitter_user_id,
            action=ActivityAction.REJECT,
            metadata={"content_id": last_content_id},
        )
    except Exception as e:
        logger.warning(
            "execute_reject: failed to write activity log",
            extra={"ctx_submitter": submitter_user_id, "ctx_error": str(e)},
        )

    # Step 7b — Audit log (MongoDB)
    await get_audit().log(
        action=AuditAction.REJECT,
        performed_by=moderator_id,
        target_user_id=submitter_user_id,
        details={"submitter_user_id": submitter_user_id, "reason": reason},
    )
