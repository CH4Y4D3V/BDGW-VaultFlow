from __future__ import annotations

from typing import Optional

from pyrogram.types import Message

from app.bot.ingestion import MediaIngestionPipeline
from app.utils.logger import get_logger

logger = get_logger(__name__)

# ── Module-level singletons ───────────────────────────────────────────────────

# Shared MediaIngestionPipeline — its internal album buffer must be long-lived
# so it is instantiated once at module load, not per-request.
_pipeline: MediaIngestionPipeline = MediaIngestionPipeline()

# In-memory pending submission registry.
# Key   : first_msg_id (int) — the primary message ID in the submitter's private chat.
# Value : (submitter_user_id, messages) — the user's ID and their raw Message objects.
#
# Lifecycle:
#   register_pending()  → populated on successful forward to verification group
#   ingest_approved()   → consumed and ingested on moderator approval
#   reject_pending()    → consumed and discarded on moderator rejection
#
# The registry is intentionally in-process memory.  If the process restarts,
# any pending submissions are lost — operators must re-submit.  This is
# acceptable given the interactive nature of the moderation flow.
_pending_submissions: dict[int, tuple[int, list[Message]]] = {}


# ── Public API ────────────────────────────────────────────────────────────────

async def register_pending(
    submitter_user_id: int,
    messages: list[Message],
) -> int:
    """
    Store a pending submission in the registry after it has been forwarded to
    the verification group.

    Returns the registry key (first message ID) that the moderation callback
    will use to look up and action the submission.
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


async def ingest_approved(msg_id: int) -> Optional[int]:
    """
    Approve a pending submission:
    1. Removes it from the pending registry.
    2. Passes each message to MediaIngestionPipeline for vault archival.

    Returns the submitter's user_id on success, or None if no pending entry
    exists for the given msg_id (already actioned or registry cleared).
    """
    entry = _pending_submissions.pop(msg_id, None)
    if entry is None:
        logger.warning(
            "Approval attempted but pending entry not found",
            extra={"ctx_msg_id": msg_id},
        )
        return None

    submitter_user_id, messages = entry

    # Delegate fully to MediaIngestionPipeline — no direct DB writes here.
    # The pipeline handles album buffering, deduplication, and vault archival.
    for msg in messages:
        source_channel_id = str(msg.chat.id)
        await _pipeline.ingest(msg, source_channel_id)

    logger.info(
        "Submission approved and handed to ingestion pipeline",
        extra={
            "ctx_user_id": submitter_user_id,
            "ctx_msg_id": msg_id,
            "ctx_count": len(messages),
        },
    )
    return submitter_user_id


async def reject_pending(msg_id: int) -> Optional[int]:
    """
    Reject a pending submission by removing it from the registry.
    No vault writes are performed.

    Returns the submitter's user_id on success, or None if no pending entry
    exists for the given msg_id.
    """
    entry = _pending_submissions.pop(msg_id, None)
    if entry is None:
        logger.warning(
            "Rejection attempted but pending entry not found",
            extra={"ctx_msg_id": msg_id},
        )
        return None

    submitter_user_id, messages = entry
    logger.info(
        "Submission rejected and discarded",
        extra={
            "ctx_user_id": submitter_user_id,
            "ctx_msg_id": msg_id,
            "ctx_count": len(messages),
        },
    )
    return submitter_user_id


def get_pending_count() -> int:
    """Return the number of submissions currently awaiting moderation."""
    return len(_pending_submissions)
