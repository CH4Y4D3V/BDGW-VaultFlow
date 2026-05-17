from __future__ import annotations

from typing import Optional

from pyrogram.types import Message

from app.bot.ingestion import MediaIngestionPipeline
from app.utils.logger import get_logger

logger = get_logger(__name__)

# ── Module-level singletons ───────────────────────────────────────────────────

_pipeline: MediaIngestionPipeline = MediaIngestionPipeline()

# In-memory pending submission registry.
# Key   : first_msg_id (int)
# Value : (submitter_user_id, messages)
#
# Lifecycle:
#   register_pending() → populated after successful forward to verification group
#   pop_pending()      → consumed on any moderator action (approve/queue/reject)
#
# In-process only. Lost on restart — operators must re-submit.
_pending_submissions: dict[int, tuple[int, list[Message]]] = {}


# ── Public API ────────────────────────────────────────────────────────────────

async def register_pending(
    submitter_user_id: int,
    messages: list[Message],
) -> int:
    """
    Store a pending submission after it has been forwarded to the verification group.
    Returns the registry key (first message ID).
    """
    if not messages:
        raise ValueError("register_pending requires at least one message")

    key = messages[0].id
    _pending_submissions[key] = (submitter_user_id, messages)

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
    Atomically remove and return a pending submission.
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
