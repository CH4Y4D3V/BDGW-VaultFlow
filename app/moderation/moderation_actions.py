from __future__ import annotations

import asyncio
from datetime import datetime, timedelta, timezone
from typing import Optional

from pyrogram.client import Client
from pyrogram.enums import ParseMode
from pyrogram.errors import FloodWait, RPCError

from app.bot.ingestion import MediaIngestionPipeline
from app.config import settings
from app.core.models import (
    DistributionPriority,
    JobStatus,
    MediaType,
    ModerationDestination,
    QueueJob,
)
from app.core.database import DatabaseManager
from app.repositories.queue_repository import QueueRepository
from app.core.exceptions import DuplicateJobError
from app.utils.logger import get_logger

logger = get_logger(__name__)

_MAX_RETRIES = 3

# Shared ingestion pipeline — module-level singleton
_pipeline = MediaIngestionPipeline()


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


# ── Safe Telegram ops ─────────────────────────────────────────────────────────

async def safe_dm(client: Client, user_id: int, text: str) -> None:
    """
    Send a DM notification to the uploader.
    Never raises — failures are logged and swallowed so the moderation
    pipeline is never blocked by a blocked or unavailable user.
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
            await asyncio.sleep(wait)
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
    """Delete the moderation card from the verification group. Best-effort."""
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
    """Edit the moderation card in place. Best-effort."""
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
    Forward all messages in the submission directly to the destination group.
    Returns True on success, False on failure.
    """
    group_id = _destination_group_id(dest)
    if not group_id:
        logger.error(
            "Destination group ID not configured",
            extra={"ctx_dest": dest},
        )
        return False

    for attempt in range(_MAX_RETRIES):
        try:
            msg_ids = [m.id for m in messages]
            source_chat_id = messages[0].chat.id
            await client.forward_messages(
                chat_id=group_id,
                from_chat_id=source_chat_id,
                message_ids=msg_ids,
            )
            return True
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

    return False


# ── Vault archival ────────────────────────────────────────────────────────────

async def archive_to_vault(messages: list, dest: str) -> None:
    """
    Archive all messages via the ingestion pipeline.
    The pipeline handles deduplication, album preservation, and vault writes.
    """
    for msg in messages:
        source_channel_id = str(msg.chat.id)
        try:
            await _pipeline.ingest(msg, source_channel_id)
        except Exception as e:
            logger.error(
                "Vault archival failed for message",
                extra={"ctx_msg_id": msg.id, "ctx_error": str(e)},
                exc_info=True,
            )


# ── Queue enqueue ─────────────────────────────────────────────────────────────

async def enqueue_for_distribution(
    messages: list,
    dest: str,
    submitter_user_id: int,
) -> bool:
    """
    Archive to vault then enqueue a MODERATED priority distribution job.
    Moderator-queued content has priority MODERATED (3) — above scheduler content.
    Queue deadline is enforced via queue_deadline field (within QUEUE_DEADLINE_HOURS).
    """
    db = DatabaseManager.get_db()
    queue_repo = QueueRepository(db)

    target_group_id = _destination_group_id(dest)
    if not target_group_id:
        logger.error("Cannot enqueue: destination group not configured", extra={"ctx_dest": dest})
        return False

    now = datetime.now(timezone.utc)
    deadline = now + timedelta(hours=settings.QUEUE_DEADLINE_HOURS)

    # Group messages by media_group_id so albums stay atomic
    group_id = messages[0].media_group_id if messages else None
    content_id = (
        f"mod_{group_id}"
        if group_id
        else f"mod_{messages[0].chat.id}_{messages[0].id}"
    )

    for i, msg in enumerate(messages):
        media = getattr(msg, str(msg.media.value)) if msg.media else None
        file_id = getattr(media, "file_id", None) if media else None
        media_type_str = str(msg.media.value) if msg.media else MediaType.TEXT.value

        try:
            media_type = MediaType(media_type_str)
        except ValueError:
            media_type = MediaType.TEXT

        # Each message in an album gets its own job, linked by media_group_id in metadata
        item_content_id = f"{content_id}_{i}" if len(messages) > 1 else content_id

        job = QueueJob(
            content_id=item_content_id,
            source_channel_id=str(msg.chat.id),
            target_channel_ids=[str(target_group_id)],
            media_type=media_type,
            media_file_id=file_id,
            caption=msg.caption or msg.text or "",
            priority=DistributionPriority.MODERATED,
            status=JobStatus.PENDING,
            max_retries=settings.MAX_RETRY_ATTEMPTS,
            execute_after=now,
            queue_deadline=deadline,
            watermark_required=False,
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
            logger.debug(
                "Duplicate queue job skipped",
                extra={"ctx_content_id": item_content_id},
            )
        except Exception as e:
            logger.error(
                "Failed to enqueue moderated job",
                extra={"ctx_content_id": item_content_id, "ctx_error": str(e)},
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
) -> None:
    """
    Full approve flow:
    1. Archive to vault
    2. Post immediately to destination group
    3. Delete moderation card
    4. Notify uploader
    """
    display_name = _destination_display_name(dest)

    # 1. Archive
    await archive_to_vault(messages, dest)

    # 2. Post immediately
    posted = await post_to_destination(client, messages, dest)
    if not posted:
        logger.error(
            "Approve: failed to post to destination",
            extra={"ctx_dest": dest, "ctx_submitter": submitter_user_id},
        )
        # Don't block — still clean up and notify
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

    # 3. Delete moderation card
    await safe_delete_message(client, mod_card_chat_id, mod_card_message_id)

    # 4. Notify uploader
    await safe_dm(
        client,
        submitter_user_id,
        f"✅ Your content was approved.\n\nDestination:\n{display_name}",
    )

    logger.info(
        "Approve flow complete",
        extra={
            "ctx_submitter": submitter_user_id,
            "ctx_dest": dest,
            "ctx_moderator": moderator_name,
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
) -> None:
    """
    Full queue flow:
    1. Archive to vault
    2. Enqueue MODERATED priority distribution job (deadline: QUEUE_DEADLINE_HOURS)
    3. Delete moderation card
    4. Notify uploader
    """
    display_name = _destination_display_name(dest)

    # 1. Archive
    await archive_to_vault(messages, dest)

    # 2. Enqueue
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

    # 3. Delete moderation card
    await safe_delete_message(client, mod_card_chat_id, mod_card_message_id)

    # 4. Notify uploader
    await safe_dm(
        client,
        submitter_user_id,
        f"✅ Your content was approved.\n\nDestination:\n{display_name}",
    )

    logger.info(
        "Queue flow complete",
        extra={
            "ctx_submitter": submitter_user_id,
            "ctx_dest": dest,
            "ctx_moderator": moderator_name,
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
    Full reject flow:
    - Content stays in verification group (not deleted)
    - Not archived
    - Not distributed
    - Uploader notified
    """
    # Edit the card to show rejected state — content remains visible to mods
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

    logger.info(
        "Reject flow complete",
        extra={
            "ctx_submitter": submitter_user_id,
            "ctx_moderator": moderator_id,
        },
    )
