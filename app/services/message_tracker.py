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

from aiogram import Bot
from aiogram.exceptions import TelegramBadRequest

from database.repository import Database

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
    db: Database,
    user_id: int,
    message_id: int,
    context: str = CONTEXT_GENERAL,
) -> None:
    """
    Record a user-facing bot message for potential cleanup.

    Call immediately after bot.send_message / message.answer returns.
    Never raises — tracking failure is non-fatal.
    """
    await db.track_user_message(user_id, message_id, context)


async def delete_user_messages(
    bot: Bot,
    db: Database,
    user_id: int,
    context: Optional[str] = None,
) -> int:
    """
    Delete all tracked undeleted messages for a user from Telegram.

    Args:
        context: If provided, only delete messages with this context.
                 None → delete ALL tracked messages for the user.

    Returns:
        Count of messages successfully deleted (or already gone).

    Never raises. All Telegram errors are handled gracefully.
    Admin group messages are NEVER in this table, so they cannot be affected.
    """
    messages = await db.get_undeleted_user_messages(user_id, context)
    if not messages:
        log.debug(
            "[MSG TRACKER] No tracked messages for user %d (context=%s)",
            user_id, context or "all",
        )
        return 0

    deleted_count = 0
    deleted_ids: list[int] = []

    for msg in messages:
        msg_id: int = msg["message_id"]
        try:
            await bot.delete_message(chat_id=user_id, message_id=msg_id)
            deleted_ids.append(msg_id)
            deleted_count += 1
        except TelegramBadRequest as exc:
            lowered = str(exc).lower()
            if any(phrase in lowered for phrase in _ALREADY_DELETED_PHRASES):
                # Message already gone from Telegram — clean up DB record anyway
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
        await db.mark_user_messages_deleted(user_id, deleted_ids)

    log.info(
        "[MSG TRACKER] user=%d context=%s: deleted %d/%d messages",
        user_id, context or "all", deleted_count, len(messages),
    )
    return deleted_count


def schedule_deletion(
    bot: Bot,
    db: Database,
    user_id: int,
    delay_seconds: int,
    context: Optional[str] = None,
) -> None:
    """
    Fire-and-forget: schedule deletion of user messages after a delay.
    Creates an asyncio background task. Never blocks the caller.

    Args:
        delay_seconds: How many seconds to wait before deleting.
        context: Which context to delete. None → delete all.
    """
    async def _delayed() -> None:
        await asyncio.sleep(delay_seconds)
        try:
            await delete_user_messages(bot, db, user_id, context)
        except Exception as exc:
            log.warning(
                "[MSG TRACKER] Scheduled deletion failed for user %d (ctx=%s): %s",
                user_id, context or "all", exc,
            )

    asyncio.create_task(_delayed())
    log.debug(
        "[MSG TRACKER] Scheduled deletion for user %d in %ds (context=%s)",
        user_id, delay_seconds, context or "all",
    )