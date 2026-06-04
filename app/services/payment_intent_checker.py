"""
payment_intent_checker.py
─────────────────────────
Background async task — enforces the payment intent timer.

I18N FIX: All user-facing warning/ban strings now fetched per user from locales
using the user's stored language preference. Previously all messages were
hardcoded Bangla regardless of the user's selected language.
"""

import asyncio
import logging
from datetime import datetime, timezone
from typing import Optional

from aiogram import Bot
from aiogram.exceptions import TelegramForbiddenError, TelegramBadRequest

from config import BotConfig
from database.repository import Database

log = logging.getLogger("payment_intent_checker")

CHECK_INTERVAL_SECONDS = 120    # run every 2 minutes
WARN_1_SECONDS         = 300    # 5 minutes
WARN_2_SECONDS         = 600    # 10 minutes
BAN_SECONDS            = 1200   # 20 minutes


async def _safe_notify(bot: Bot, user_id: int, text: str) -> Optional[int]:
    """
    Send message to user. Returns message_id on success, None on any failure.
    """
    try:
        msg = await bot.send_message(chat_id=user_id, text=text, parse_mode="HTML")
        return msg.message_id
    except TelegramForbiddenError:
        log.warning("[INTENT CHECKER] User %d has blocked the bot.", user_id)
        return None
    except TelegramBadRequest as exc:
        log.warning("[INTENT CHECKER] BadRequest for user %d: %s", user_id, exc)
        return None
    except Exception as exc:
        log.warning("[INTENT CHECKER] Could not notify user %d: %s", user_id, exc)
        return None


async def _notify_admins_intent_ban(
    bot: Bot, settings: BotConfig, db: Database, user_id: int
) -> None:
    text = (
        f"🚫 <b>Payment Intent Auto-Ban</b>\n\n"
        f"👤 User: <code>{user_id}</code>\n"
        f"📝 Reason: Did not complete payment within 20 minutes of receiving details.\n"
        f"⚡ Action: Banned + kicked from all protected groups.\n"
        f"🗑 Intent warning messages deleted from user's chat."
    )

    if settings.admin_group_id:
        try:
            from services.support_topics import notify_to_topic
            sent = await notify_to_topic(bot, db, settings, user_id, text)
            if sent is not None:
                return
        except Exception as exc:
            log.warning(
                "[INTENT CHECKER] Topic routing failed for auto-ban alert user %d: %s",
                user_id, exc,
            )

    for admin_id in settings.admin_ids:
        try:
            await bot.send_message(chat_id=admin_id, text=text, parse_mode="HTML")
        except Exception:
            pass


async def _run_check(bot: Bot, db: Database, settings: BotConfig) -> None:
    """Single pass over all users with active payment intent."""
    users = await db.get_active_intent_users()
    if not users:
        return

    now = datetime.now(timezone.utc)

    for user in users:
        user_id: int = user["user_id"]
        warn_count: int = user.get("intent_warn_count", 0)
        intent_time_str: str = user.get("intent_time", "")

        if not intent_time_str:
            await db.clear_payment_intent(user_id)
            continue

        try:
            intent_time = datetime.fromisoformat(intent_time_str)
            if intent_time.tzinfo is None:
                intent_time = intent_time.replace(tzinfo=timezone.utc)
        except (ValueError, TypeError):
            log.error(
                "[INTENT CHECKER] Invalid intent_time for user %d: %r — clearing",
                user_id, intent_time_str,
            )
            await db.clear_payment_intent(user_id)
            continue

        elapsed = (now - intent_time).total_seconds()

        # ── BAN threshold ──────────────────────────────────────────────────────
        if elapsed >= BAN_SECONDS:
            # I18N FIX: fetch user's language before sending ban message
            from locales import get_text, get_user_lang
            _lang = (await get_user_lang(db, user_id)) or "en"

            log.warning(
                "[INTENT CHECKER] Auto-banning user %d (elapsed=%.0fs, warns=%d)",
                user_id, elapsed, warn_count,
            )
            await db.ban_user(user_id)
            await db.clear_payment_intent(user_id)

            try:
                from services.message_tracker import (
                    delete_user_messages as _del_intent,
                    CONTEXT_PAYMENT_INTENT,
                )
                await _del_intent(bot, db, user_id, CONTEXT_PAYMENT_INTENT)
            except Exception as exc:
                log.warning(
                    "[INTENT CHECKER] Could not delete intent warnings for user %d: %s",
                    user_id, exc,
                )

            try:
                from handlers.admin import kick_from_all_groups
                await kick_from_all_groups(bot, settings, db, user_id)
            except Exception as exc:
                log.warning(
                    "[INTENT CHECKER] kick_from_all_groups failed for user %d: %s",
                    user_id, exc,
                )

            # I18N FIX: language-aware ban message
            await _safe_notify(bot, user_id, get_text("intent_ban_msg", _lang))
            await _notify_admins_intent_ban(bot, settings, db, user_id)
            continue

        # ── Warning 2 ──────────────────────────────────────────────────────────
        if elapsed >= WARN_2_SECONDS and warn_count < 2:
            # I18N FIX: fetch user's language before sending warning
            from locales import get_text, get_user_lang
            _lang = (await get_user_lang(db, user_id)) or "en"

            await db.increment_intent_warn(user_id)
            msg_id = await _safe_notify(bot, user_id, get_text("intent_warn_2", _lang))
            if msg_id:
                await db.track_user_message(user_id, msg_id, "payment_intent")
            log.info(
                "[INTENT CHECKER] Warning 2/3 sent to user %d (elapsed=%.0fs)",
                user_id, elapsed,
            )
            continue

        # ── Warning 1 ──────────────────────────────────────────────────────────
        if elapsed >= WARN_1_SECONDS and warn_count < 1:
            # I18N FIX: fetch user's language before sending warning
            from locales import get_text, get_user_lang
            _lang = (await get_user_lang(db, user_id)) or "en"

            await db.increment_intent_warn(user_id)
            msg_id = await _safe_notify(bot, user_id, get_text("intent_warn_1", _lang))
            if msg_id:
                await db.track_user_message(user_id, msg_id, "payment_intent")
            log.info(
                "[INTENT CHECKER] Warning 1/3 sent to user %d (elapsed=%.0fs)",
                user_id, elapsed,
            )


async def payment_intent_checker(bot: Bot, db: Database, settings: BotConfig) -> None:
    """
    Main background loop. Never crashes the bot.
    All exceptions are caught and logged.
    Restart-safe: state persists in SQLite.
    """
    log.info(
        "[INTENT CHECKER] Started. Check interval: %ds | Warn at: %ds/%ds | Ban at: %ds",
        CHECK_INTERVAL_SECONDS, WARN_1_SECONDS, WARN_2_SECONDS, BAN_SECONDS,
    )

    while True:
        try:
            await asyncio.sleep(CHECK_INTERVAL_SECONDS)
            await _run_check(bot, db, settings)
        except asyncio.CancelledError:
            log.info("[INTENT CHECKER] Cancelled — shutting down.")
            break
        except Exception as exc:
            log.error("[INTENT CHECKER] Unexpected error: %s", exc, exc_info=True)
            await asyncio.sleep(60)