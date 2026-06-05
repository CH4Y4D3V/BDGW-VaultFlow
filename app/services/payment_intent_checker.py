"""
payment_intent_checker.py
─────────────────────────
Background async task — enforces the payment intent timer.

Runs every CHECK_INTERVAL_SECONDS. For each user with an active
``intent_time``:
  • At 5 min elapsed  → Warning 1/2 (15 minutes remaining)
  • At 10 min elapsed → Warning 2/2 (10 minutes remaining)
  • At 20 min elapsed → Auto-ban + kick from all protected groups

All state transitions use find_one_and_update with atomic filters to ensure
idempotency across concurrent scheduler passes. See _run_check() for details.

I18N: All user-facing strings are hardcoded to English per Spec v1.0.
"""

import asyncio
import logging
from datetime import datetime, timezone
from typing import Optional

from pymongo import ReturnDocument
from pyrogram import Client
from pyrogram.enums import ParseMode
from pyrogram.errors import BadRequest, FloodWait, Forbidden

from app.config import settings
from app.core.database import DatabaseManager

log = logging.getLogger("payment_intent_checker")

CHECK_INTERVAL_SECONDS = 120    # run every 2 minutes
WARN_1_SECONDS         = 300    # 5 minutes  → Warning 1/2 (15 min remaining)
WARN_2_SECONDS         = 600    # 10 minutes → Warning 2/2 (10 min remaining)
BAN_SECONDS            = 1200   # 20 minutes → auto-ban

_MAX_FLOOD_WAIT_SECONDS = 60    # cap any FloodWait sleep to 60 s

# Maximum users to process per pass — prevents unbounded memory usage
# on large installs. Increase if needed; consider cursor pagination beyond ~1 000.
_BATCH_LIMIT = 500


async def _safe_notify(client: Client, user_id: int, text: str) -> Optional[int]:
    """
    Send a single HTML-formatted message to a user.

    Handles FloodWait with one capped retry, Forbidden (bot blocked), and
    BadRequest explicitly. Returns the message_id on success, None on failure.

    Prior bug fixed: original code called send_message twice — once without
    parse_mode (result discarded) and once with ParseMode.HTML, delivering
    every notification twice. Only the HTML send is kept.
    """
    try:
        msg = await client.send_message(
            chat_id=user_id,
            text=text,
            parse_mode=ParseMode.HTML,
        )
        return msg.id
    except FloodWait as exc:
        wait = min(exc.value, _MAX_FLOOD_WAIT_SECONDS)
        log.warning(
            "[INTENT CHECKER] FloodWait %ds before notifying user %d — sleeping.",
            wait, user_id,
        )
        await asyncio.sleep(wait)
        try:
            msg = await client.send_message(
                chat_id=user_id,
                text=text,
                parse_mode=ParseMode.HTML,
            )
            return msg.id
        except Exception as retry_exc:
            log.warning(
                "[INTENT CHECKER] Retry after FloodWait failed for user %d: %s",
                user_id, retry_exc,
            )
            return None
    except Forbidden:
        log.warning("[INTENT CHECKER] User %d has blocked the bot.", user_id)
        return None
    except BadRequest as exc:
        log.warning("[INTENT CHECKER] BadRequest for user %d: %s", user_id, exc)
        return None
    except Exception as exc:
        log.warning("[INTENT CHECKER] Could not notify user %d: %s", user_id, exc)
        return None


async def _notify_admins_intent_ban(client: Client, user_id: int) -> None:
    """
    Emit an auto-ban notification to the user's Verification Hub topic
    AND directly to every admin DM as a failsafe.

    FloodWait is handled explicitly on each admin DM send. Topic routing
    failure is non-fatal — admin DMs serve as the fallback.
    """
    text = (
        f"🚫 <b>Payment Intent Auto-Ban</b>\n\n"
        f"👤 User: <code>{user_id}</code>\n"
        f"📝 Reason: Did not complete payment within 20 minutes of receiving details.\n"
        f"⚡ Action: Banned + kicked from all protected groups.\n"
        f"🗑 Intent warning messages deleted from user's chat."
    )

    try:
        from app.services.support_service import get_support_service
        support_service = get_support_service()
        await support_service.notify_to_topic(client, user_id, text)
    except Exception as exc:
        log.warning(
            "[INTENT CHECKER] Topic routing failed for auto-ban alert user %d: %s",
            user_id, exc,
        )

    for admin_id in settings.ADMIN_IDS:
        try:
            await client.send_message(
                chat_id=admin_id,
                text=text,
                parse_mode=ParseMode.HTML,
            )
        except FloodWait as exc:
            wait = min(exc.value, _MAX_FLOOD_WAIT_SECONDS)
            log.warning(
                "[INTENT CHECKER] FloodWait %ds notifying admin %d — sleeping.",
                wait, admin_id,
            )
            await asyncio.sleep(wait)
            try:
                await client.send_message(
                    chat_id=admin_id,
                    text=text,
                    parse_mode=ParseMode.HTML,
                )
            except Exception as retry_exc:
                log.warning(
                    "[INTENT CHECKER] Retry after FloodWait failed for admin %d: %s",
                    admin_id, retry_exc,
                )
        except Exception as exc:
            log.warning(
                "[INTENT CHECKER] Failed to notify admin %d of auto-ban for user %d: %s",
                admin_id, user_id, exc,
            )


async def _run_check(client: Client) -> None:
    """
    Single pass over at most _BATCH_LIMIT users with an active payment intent.

    All state transitions use find_one_and_update with atomic filter clauses
    to guarantee idempotency across concurrent scheduler passes:

      • Ban:      filter {intent_time: {$ne: None}, is_banned: {$ne: True}}
                  Only the winning pass proceeds with kick + notify.

      • Warning N: filter {intent_warn_count: {$lt: N}, intent_time: {$ne: None}}
                  Only the winning pass sends the message and tracks it.

    Processing is ordered: ban check first, then warn 2, then warn 1. The
    ``continue`` after each action ensures a user is never double-processed
    within a single pass.
    """
    db = DatabaseManager.get_db()

    users = await db["users"].find(
        {"intent_time": {"$ne": None}}
    ).limit(_BATCH_LIMIT).to_list(length=_BATCH_LIMIT)

    if not users:
        return

    now = datetime.now(timezone.utc)

    for user in users:
        user_id: int = user["_id"]
        warn_count: int = user.get("intent_warn_count", 0)
        intent_time_raw = user.get("intent_time")

        if not intent_time_raw:
            await db["users"].update_one(
                {"_id": user_id},
                {"$set": {"intent_time": None, "intent_warn_count": 0}},
            )
            continue

        try:
            if isinstance(intent_time_raw, datetime):
                intent_time = intent_time_raw
            else:
                intent_time = datetime.fromisoformat(str(intent_time_raw))

            if intent_time.tzinfo is None:
                intent_time = intent_time.replace(tzinfo=timezone.utc)
        except (ValueError, TypeError):
            log.error(
                "[INTENT CHECKER] Invalid intent_time for user %d: %r — clearing",
                user_id, intent_time_raw,
            )
            await db["users"].update_one(
                {"_id": user_id},
                {"$set": {"intent_time": None, "intent_warn_count": 0}},
            )
            continue

        elapsed = (now - intent_time).total_seconds()

        # ── BAN threshold ──────────────────────────────────────────────────────
        if elapsed >= BAN_SECONDS:
            # Atomically claim the ban. The filter requires intent_time != None
            # AND is_banned != True. A concurrent pass that already set
            # is_banned=True returns None here and we skip.
            ban_result = await db["users"].find_one_and_update(
                {
                    "_id": user_id,
                    "intent_time": {"$ne": None},
                    "is_banned": {"$ne": True},
                },
                {
                    "$set": {
                        "is_banned": True,
                        "intent_time": None,
                        "intent_warn_count": 0,
                    }
                },
                return_document=ReturnDocument.AFTER,
            )

            if ban_result is None:
                log.info(
                    "[INTENT CHECKER] Ban for user %d already claimed by concurrent pass — skipping.",
                    user_id,
                )
                continue

            log.warning(
                "[INTENT CHECKER] Auto-banning user %d (elapsed=%.0fs, warns=%d)",
                user_id, elapsed, warn_count,
            )

            # Delete intent warning messages from user chat
            try:
                from app.services.message_tracker import (
                    CONTEXT_PAYMENT_INTENT,
                    delete_user_messages,
                )
                await delete_user_messages(client, user_id, CONTEXT_PAYMENT_INTENT)
            except Exception as exc:
                log.warning(
                    "[INTENT CHECKER] Could not delete intent warnings for user %d: %s",
                    user_id, exc,
                )

            # Kick from all protected chats with explicit FloodWait handling
            protected_chats = [
                settings.NSFW_GROUP_ID,
                settings.PREMIUM_GROUP_ID,
                settings.VERIFICATION_GROUP_ID,
            ]
            for chat_id in protected_chats:
                if not chat_id:
                    continue
                try:
                    await client.ban_chat_member(chat_id, user_id)
                except FloodWait as exc:
                    wait = min(exc.value, _MAX_FLOOD_WAIT_SECONDS)
                    log.warning(
                        "[INTENT CHECKER] FloodWait %ds kicking user %d from chat %d.",
                        wait, user_id, chat_id,
                    )
                    await asyncio.sleep(wait)
                    try:
                        await client.ban_chat_member(chat_id, user_id)
                    except Exception as retry_exc:
                        log.warning(
                            "[INTENT CHECKER] Kick retry failed for user %d in chat %d: %s",
                            user_id, chat_id, retry_exc,
                        )
                except Exception as exc:
                    log.warning(
                        "[INTENT CHECKER] Could not kick user %d from chat %d: %s",
                        user_id, chat_id, exc,
                    )

            ban_msg = (
                "🚫 <b>You have been banned.</b>\n\n"
                "You failed to complete payment within the 20-minute window "
                "after receiving details."
            )
            await _safe_notify(client, user_id, ban_msg)
            await _notify_admins_intent_ban(client, user_id)
            continue

        # ── Warning 2/2 ────────────────────────────────────────────────────────
        if elapsed >= WARN_2_SECONDS and warn_count < 2:
            # Atomically increment warn_count only if still < 2 and intent is active.
            warn2_result = await db["users"].find_one_and_update(
                {
                    "_id": user_id,
                    "intent_warn_count": {"$lt": 2},
                    "intent_time": {"$ne": None},
                },
                {"$inc": {"intent_warn_count": 1}},
                return_document=ReturnDocument.AFTER,
            )
            if warn2_result is None:
                log.info(
                    "[INTENT CHECKER] Warning 2/2 for user %d already claimed by concurrent pass — skipping.",
                    user_id,
                )
                continue

            warn_msg = (
                "⚠️ <b>Final Warning</b>\n\n"
                "You have 10 minutes left to submit your payment proof "
                "before an automatic ban is issued."
            )
            msg_id = await _safe_notify(client, user_id, warn_msg)
            if msg_id:
                try:
                    await db["message_tracker"].insert_one({
                        "user_id": user_id,
                        "message_id": msg_id,
                        "context": "payment_intent",
                        "created_at": datetime.now(timezone.utc),
                    })
                except Exception as exc:
                    log.warning(
                        "[INTENT CHECKER] Failed to track warn-2 message for user %d: %s",
                        user_id, exc,
                    )
            log.info(
                "[INTENT CHECKER] Warning 2/2 sent to user %d (elapsed=%.0fs)",
                user_id, elapsed,
            )
            continue

        # ── Warning 1/2 ────────────────────────────────────────────────────────
        if elapsed >= WARN_1_SECONDS and warn_count < 1:
            warn1_result = await db["users"].find_one_and_update(
                {
                    "_id": user_id,
                    "intent_warn_count": {"$lt": 1},
                    "intent_time": {"$ne": None},
                },
                {"$inc": {"intent_warn_count": 1}},
                return_document=ReturnDocument.AFTER,
            )
            if warn1_result is None:
                log.info(
                    "[INTENT CHECKER] Warning 1/2 for user %d already claimed by concurrent pass — skipping.",
                    user_id,
                )
                continue

            warn_msg = (
                "⚠️ <b>Payment Pending</b>\n\n"
                "Please submit your TXID and screenshot. "
                "You have 15 minutes remaining."
            )
            msg_id = await _safe_notify(client, user_id, warn_msg)
            if msg_id:
                try:
                    await db["message_tracker"].insert_one({
                        "user_id": user_id,
                        "message_id": msg_id,
                        "context": "payment_intent",
                        "created_at": datetime.now(timezone.utc),
                    })
                except Exception as exc:
                    log.warning(
                        "[INTENT CHECKER] Failed to track warn-1 message for user %d: %s",
                        user_id, exc,
                    )
            log.info(
                "[INTENT CHECKER] Warning 1/2 sent to user %d (elapsed=%.0fs)",
                user_id, elapsed,
            )
            continue


async def payment_intent_checker(client: Client) -> None:
    """
    Main background loop.

    Runs indefinitely, checking for expired payment intents every
    CHECK_INTERVAL_SECONDS. Never crashes the bot — all exceptions from
    _run_check are caught and logged. Handles asyncio.CancelledError
    for clean shutdown on bot termination.
    """
    log.info(
        "[INTENT CHECKER] Started. Interval: %ds | Warn at: %ds/%ds | Ban at: %ds | Batch: %d",
        CHECK_INTERVAL_SECONDS, WARN_1_SECONDS, WARN_2_SECONDS, BAN_SECONDS, _BATCH_LIMIT,
    )

    while True:
        try:
            await asyncio.sleep(CHECK_INTERVAL_SECONDS)
            await _run_check(client)
        except asyncio.CancelledError:
            log.info("[INTENT CHECKER] Cancelled — shutting down.")
            break
        except Exception as exc:
            log.error("[INTENT CHECKER] Unexpected error: %s", exc, exc_info=True)
            await asyncio.sleep(60)
