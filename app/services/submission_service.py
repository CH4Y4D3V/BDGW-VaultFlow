from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Optional

from pyrogram.types import Message

from app.bot.ingestion import MediaIngestionPipeline
from app.config import settings
from app.core.database import DatabaseManager
from app.utils.logger import get_logger

logger = get_logger(__name__)

# ── Module-level singletons ───────────────────────────────────────────────────

_pipeline: MediaIngestionPipeline = MediaIngestionPipeline()

# In-memory pending submission registry — fast-path cache.
# Key   : first_msg_id (int)
# Value : (submitter_user_id, messages)
#
# Lifecycle:
#   register_pending() → populated after successful forward to verification group
#   pop_pending()      → consumed on any moderator action (approve/queue/reject)
#
# NOTE: Message objects are NOT persisted to DB and cannot be reconstructed on
# restart. The DB record (PENDING_COLLECTION) is metadata-only for auditing.
# Submissions in-flight at restart time are simply lost from the active registry —
# operators must re-submit. The DB record survives for logging purposes.
_pending_submissions: dict[int, tuple[int, list[Message]]] = {}


# ── Internal DB helpers ───────────────────────────────────────────────────────

async def _persist_pending(
    key: int,
    submitter_user_id: int,
    messages: list[Message],
) -> None:
    """
    Write pending submission metadata to MongoDB.
    Never raises — DB failure is logged but does not block the in-memory path.
    Schema: {key, submitter_user_id, chat_id, message_ids, expires_at}
    """
    try:
        db = DatabaseManager.get_db()
        col = db[settings.PENDING_COLLECTION]
        now = datetime.now(timezone.utc)
        # TTL set to 24 hours — long enough for any moderation cycle
        expires_at = now + timedelta(hours=24)
        doc = {
            "key": key,
            "submitter_user_id": submitter_user_id,
            "chat_id": messages[0].chat.id if messages else 0,
            "message_ids": [m.id for m in messages],
            "expires_at": expires_at,
            "created_at": now,
        }
        await col.update_one(
            {"key": key},
            {"$set": doc},
            upsert=True,
        )
        logger.info(
            "Pending submission persisted to DB",
            extra={"ctx_key": key, "ctx_user_id": submitter_user_id},
        )
    except Exception as e:
        logger.warning(
            "Failed to persist pending submission to DB — in-memory cache still valid",
            extra={"ctx_key": key, "ctx_error": str(e)},
        )


async def _delete_pending_from_db(key: int) -> None:
    """
    Remove pending submission record from MongoDB on consumption.
    Never raises — DB failure is logged only.
    """
    try:
        db = DatabaseManager.get_db()
        col = db[settings.PENDING_COLLECTION]
        result = await col.delete_one({"key": key})
        if result.deleted_count:
            logger.info(
                "Pending submission removed from DB",
                extra={"ctx_key": key},
            )
    except Exception as e:
        logger.warning(
            "Failed to delete pending submission from DB",
            extra={"ctx_key": key, "ctx_error": str(e)},
        )


# ── Public API ────────────────────────────────────────────────────────────────

async def register_pending(
    submitter_user_id: int,
    messages: list[Message],
) -> int:
    """
    Store a pending submission after it has been forwarded to the verification group.
    Writes to both in-memory cache and MongoDB (metadata only).
    Returns the registry key (first message ID).
    """
    if not messages:
        raise ValueError("register_pending requires at least one message")

    key = messages[0].id

    # 1. Write to in-memory fast-path cache
    _pending_submissions[key] = (submitter_user_id, messages)

    # 2. Persist metadata to MongoDB for auditing / restart logging
    await _persist_pending(key, submitter_user_id, messages)

    logger.info(
        "Submission registered as pending moderation",
        extra={
            "ctx_user_id": submitter_user_id,
            "ctx_key": key,
            "ctx_count": len(messages),
        },
    )
    return key


def pop_pending(msg_id: int) -> Optional[tuple[int, list[Message]]]:
    """
    Atomically remove and return a pending submission from in-memory cache.
    Also schedules deletion from MongoDB (fire-and-forget via asyncio task if event loop
    is running, otherwise logs warning).
    Used by callback_handler on any moderation action.

    Returns (submitter_user_id, messages) or None if not found.
    This is the single consumption point — once popped, the entry is gone.
    """
    entry = _pending_submissions.pop(msg_id, None)
    if entry is None:
        logger.warning(
            "pop_pending: no entry found",
            extra={"ctx_msg_id": msg_id},
        )
        return None

    # Schedule async DB deletion without blocking the synchronous pop path.
    # The DB record is metadata/audit only — fire and forget is acceptable here.
    import asyncio
    try:
        loop = asyncio.get_running_loop()
        loop.create_task(_delete_pending_from_db(msg_id))
    except RuntimeError:
        # No running event loop — log and move on (should not happen in normal operation)
        logger.warning(
            "pop_pending: no running event loop for DB cleanup",
            extra={"ctx_msg_id": msg_id},
        )

    return entry


async def ingest_approved(msg_id: int) -> Optional[int]:
    """
    Legacy path kept for any direct callers.
    Prefer pop_pending() + moderation_actions.archive_to_vault() in new code.

    Pops the pending entry and runs it through the ingestion pipeline.
    Returns submitter_user_id or None.
    """
    entry = pop_pending(msg_id)
    if entry is None:
        return None

    submitter_user_id, messages = entry

    for msg in messages:
        source_channel_id = str(msg.chat.id)
        await _pipeline.ingest(msg, source_channel_id)

    logger.info(
        "Submission approved and ingested (legacy path)",
        extra={"ctx_user_id": submitter_user_id, "ctx_msg_id": msg_id, "ctx_count": len(messages)},
    )
    return submitter_user_id


async def reject_pending(msg_id: int) -> Optional[int]:
    """
    Legacy path kept for any direct callers.
    Prefer pop_pending() in new code.

    Pops and discards the pending entry. No vault writes.
    Returns submitter_user_id or None.
    """
    entry = pop_pending(msg_id)
    if entry is None:
        return None

    submitter_user_id, messages = entry
    logger.info(
        "Submission rejected and discarded (legacy path)",
        extra={"ctx_user_id": submitter_user_id, "ctx_msg_id": msg_id},
    )
    return submitter_user_id


def get_pending_count() -> int:
    """Return the number of submissions currently awaiting moderation."""
    return len(_pending_submissions)