"""
message_tracker.py
──────────────────
Track and delete user-facing bot messages.

CRITICAL RULES:
  • Only tracks messages sent to user private chats (chat_id == user_id).
  • NEVER touches admin group messages, topics, or audit logs.
  • All Telegram deletions are best-effort — errors are silently swallowed.
  • DB tracking errors are logged but never raise.

CONTEXTS (use these constants everywhere):
  CONTEXT_ACCESS_DELIVERY    — access links sent after payment approval
  CONTEXT_PAYMENT_INTENT     — 20-minute timer warning messages
  CONTEXT_PAYMENT_SUBMISSION — payment proof received confirmation
  CONTEXT_REJECTION          — payment rejection messages
  CONTEXT_TRIAL              — trial expiry/reminder notifications
  CONTEXT_SUPPORT            — support session messages
  CONTEXT_GENERAL            — uncategorised
"""

import asyncio
import logging
from typing import Optional
from datetime import datetime, timezone

from pyrogram import Client
from pyrogram.errors import BadRequest

from app.core.database import DatabaseManager

log = logging.getLogger(__name__)

# ── Context identifiers ────────────────────────────────────────────────────────
CONTEXT_ACCESS_DELIVERY    = "access_delivery"
CONTEXT_PAYMENT_INTENT     = "payment_intent"
CONTEXT_PAYMENT_SUBMISSION = "payment_submission"
CONTEXT_REJECTION          = "rejection"
CONTEXT_TRIAL              = "trial_notification"
CONTEXT_SUPPORT            = "support"
CONTEXT_GENERAL            = "general"

# Messages whose Telegram error means "already gone — safe to mark deleted"
_ALREADY_DELETED_PHRASES = frozenset({
    "message to delete not found",
    "message can't be deleted",
    "message cant be deleted",
    "message is too old",
})


async def track_message(
    user_id: int,
    message_id: int,
    context: str = CONTEXT_GENERAL,
) -> None:
    """
    Record a user-facing bot message for potential cleanup.
    """
    try:
        db = DatabaseManager.get_db()
        await db["message_tracker"].insert_one({
            "user_id": user_id,
            "message_id": message_id,
            "context": context,
            "is_deleted": False,
            "created_at": datetime.now(timezone.utc)
        })
    except Exception as e:
        log.warning(f"Failed to track message {message_id} for user {user_id}: {e}")


async def delete_user_messages(
    client: Client,
    user_id: int,
    context: Optional[str] = None,
) -> int:
    """
    Delete all tracked undeleted messages for a user from Telegram.
    """
    db = DatabaseManager.get_db()
    query = {"user_id": user_id, "is_deleted": False}
    if context:
        query["context"] = context
        
    messages_cursor = db["message_tracker"].find(query)
    messages = await messages_cursor.to_list(length=None)
    
    if not messages:
        return 0

    deleted_count = 0
    deleted_ids: list[int] = []

    for msg in messages:
        msg_id: int = msg["message_id"]
        try:
            await client.delete_messages(chat_id=user_id, message_ids=msg_id)
            deleted_ids.append(msg_id)
            deleted_count += 1
        except BadRequest as exc:
            lowered = str(exc).lower()
            if any(phrase in lowered for phrase in _ALREADY_DELETED_PHRASES):
                deleted_ids.append(msg_id)
                deleted_count += 1
            else:
                log.debug(
                    "[MSG TRACKER] Could not delete msg %d for user %d: %s",
                    msg_id, user_id, exc,
                )
        except Exception as exc:
            log.debug(
                "[MSG TRACKER] Unexpected error deleting msg %d user %d: %s",
                msg_id, user_id, exc,
            )

    if deleted_ids:
        await db["message_tracker"].update_many(
            {"user_id": user_id, "message_id": {"$in": deleted_ids}},
            {"$set": {"is_deleted": True, "deleted_at": datetime.now(timezone.utc)}}
        )

    log.info(
        "[MSG TRACKER] user=%d context=%s: deleted %d/%d messages",
        user_id, context or "all", deleted_count, len(messages),
        extra={"ctx_user_id": user_id, "ctx_context": context or "all"}
    )
    return deleted_count


def schedule_deletion(
    client: Client,
    user_id: int,
    delay_seconds: int,
    context: Optional[str] = None,
) -> None:
    """
    Fire-and-forget: schedule deletion of user messages after a delay.
    """
    async def _delayed() -> None:
        await asyncio.sleep(delay_seconds)
        try:
            await delete_user_messages(client, user_id, context)
        except Exception as exc:
            log.warning(
                "[MSG TRACKER] Scheduled deletion failed for user %d (ctx=%s): %s",
                user_id, context or "all", exc,
            )

    asyncio.create_task(_delayed())
