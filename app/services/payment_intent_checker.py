"""
payment_intent_checker.py
─────────────────────────
Background async task — enforces the payment intent timer.

I18N FIX: All user-facing strings hardcoded to English per Spec v1.0.
"""

import asyncio
import logging
from datetime import datetime, timezone
from typing import Optional

from pyrogram import Client
from pyrogram.errors import Forbidden, BadRequest

from app.config import settings
from app.core.database import DatabaseManager

log = logging.getLogger("payment_intent_checker")

CHECK_INTERVAL_SECONDS = 120    # run every 2 minutes
WARN_1_SECONDS         = 300    # 5 minutes
WARN_2_SECONDS         = 600    # 10 minutes
BAN_SECONDS            = 1200   # 20 minutes


async def _safe_notify(client: Client, user_id: int, text: str) -> Optional[int]:
    """
    Send message to user. Returns message_id on success, None on any failure.
    """
    try:
        msg = await client.send_message(chat_id=user_id, text=text, parse_mode=None) # Pyrogram default is HTML if not set, but we usually use ParseMode.HTML
        # Wait, Pyrogram enums are better
        from pyrogram.enums import ParseMode
        msg = await client.send_message(chat_id=user_id, text=text, parse_mode=ParseMode.HTML)
        return msg.id
    except Forbidden:
        log.warning("[INTENT CHECKER] User %d has blocked the bot.", user_id)
        return None
    except BadRequest as exc:
        log.warning("[INTENT CHECKER] BadRequest for user %d: %s", user_id, exc)
        return None
    except Exception as exc:
        log.warning("[INTENT CHECKER] Could not notify user %d: %s", user_id, exc)
        return None


async def _notify_admins_intent_ban(
    client: Client, user_id: int
) -> None:
    text = (
        f"🚫 <b>Payment Intent Auto-Ban</b>\n\n"
        f"👤 User: <code>{user_id}</code>\n"
        f"📝 Reason: Did not complete payment within 20 minutes of receiving details.\n"
        f"⚡ Action: Banned + kicked from all protected groups.\n"
        f"🗑 Intent warning messages deleted from user's chat."
    )

    # Note: using support_service instead of direct topic routing for consistency
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
            from pyrogram.enums import ParseMode
            await client.send_message(chat_id=admin_id, text=text, parse_mode=ParseMode.HTML)
        except Exception:
            pass


async def _run_check(client: Client) -> None:
    """Single pass over all users with active payment intent."""
    db = DatabaseManager.get_db()
    
    # SYSTEM 14 logic: find users with intent_time set
    # Using direct Motor query as Database repository might be aiogram-based
    users_cursor = db["users"].find({"intent_time": {"$ne": None}})
    users = await users_cursor.to_list(length=None)
    
    if not users:
        return

    now = datetime.now(timezone.utc)

    for user in users:
        user_id: int = user["_id"]
        warn_count: int = user.get("intent_warn_count", 0)
        intent_time_str: str = user.get("intent_time", "")

        if not intent_time_str:
            await db["users"].update_one({"_id": user_id}, {"$set": {"intent_time": None, "intent_warn_count": 0}})
            continue

        try:
            if isinstance(intent_time_str, datetime):
                intent_time = intent_time_str
            else:
                intent_time = datetime.fromisoformat(intent_time_str)
                
            if intent_time.tzinfo is None:
                intent_time = intent_time.replace(tzinfo=timezone.utc)
        except (ValueError, TypeError):
            log.error(
                "[INTENT CHECKER] Invalid intent_time for user %d: %r — clearing",
                user_id, intent_time_str,
            )
            await db["users"].update_one({"_id": user_id}, {"$set": {"intent_time": None, "intent_warn_count": 0}})
            continue

        elapsed = (now - intent_time).total_seconds()

        # ── BAN threshold ──────────────────────────────────────────────────────
        if elapsed >= BAN_SECONDS:
            log.warning(
                "[INTENT CHECKER] Auto-banning user %d (elapsed=%.0fs, warns=%d)",
                user_id, elapsed, warn_count,
            )
            
            # Ban logic
            await db["users"].update_one({"_id": user_id}, {"$set": {"is_banned": True, "intent_time": None, "intent_warn_count": 0}})

            try:
                from app.services.message_tracker import delete_user_messages, CONTEXT_PAYMENT_INTENT
                # Ensure tracker uses Pyrogram
                await delete_user_messages(client, user_id, CONTEXT_PAYMENT_INTENT)
            except Exception as exc:
                log.warning(
                    "[INTENT CHECKER] Could not delete intent warnings for user %d: %s",
                    user_id, exc,
                )

            # Kick logic (simplified, should be in a service)
            protected_chats = [settings.NSFW_GROUP_ID, settings.PREMIUM_GROUP_ID, settings.VERIFICATION_GROUP_ID]
            for chat_id in protected_chats:
                if chat_id:
                    try:
                        await client.ban_chat_member(chat_id, user_id)
                    except Exception:
                        pass

            ban_msg = "🚫 <b>You have been banned.</b>\n\nYou failed to complete payment within the 20-minute window after receiving details."
            await _safe_notify(client, user_id, ban_msg)
            await _notify_admins_intent_ban(client, user_id)
            continue

        # ── Warning 2 ──────────────────────────────────────────────────────────
        if elapsed >= WARN_2_SECONDS and warn_count < 2:
            await db["users"].update_one({"_id": user_id}, {"$inc": {"intent_warn_count": 1}})
            warn_msg = "⚠️ <b>Final Warning</b>\n\nYou have 10 minutes left to submit your payment proof before an automatic ban is issued."
            msg_id = await _safe_notify(client, user_id, warn_msg)
            if msg_id:
                # Track for cleanup
                await db["message_tracker"].insert_one({
                    "user_id": user_id,
                    "message_id": msg_id,
                    "context": "payment_intent",
                    "created_at": datetime.now(timezone.utc)
                })
            log.info(
                "[INTENT CHECKER] Warning 2/3 sent to user %d (elapsed=%.0fs)",
                user_id, elapsed,
            )
            continue

        # ── Warning 1 ──────────────────────────────────────────────────────────
        if elapsed >= WARN_1_SECONDS and warn_count < 1:
            await db["users"].update_one({"_id": user_id}, {"$inc": {"intent_warn_count": 1}})
            warn_msg = "⚠️ <b>Payment Pending</b>\n\nPlease submit your TXID and screenshot. You have 15 minutes remaining."
            msg_id = await _safe_notify(client, user_id, warn_msg)
            if msg_id:
                await db["message_tracker"].insert_one({
                    "user_id": user_id,
                    "message_id": msg_id,
                    "context": "payment_intent",
                    "created_at": datetime.now(timezone.utc)
                })
            log.info(
                "[INTENT CHECKER] Warning 1/3 sent to user %d (elapsed=%.0fs)",
                user_id, elapsed,
            )


async def payment_intent_checker(client: Client) -> None:
    """
    Main background loop. Never crashes the bot.
    All exceptions are caught and logged.
    """
    log.info(
        "[INTENT CHECKER] Started. Check interval: %ds | Warn at: %ds/%ds | Ban at: %ds",
        CHECK_INTERVAL_SECONDS, WARN_1_SECONDS, WARN_2_SECONDS, BAN_SECONDS,
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
