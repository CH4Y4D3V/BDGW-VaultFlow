"""
bot/middlewares/antispam.py
───────────────────────────
BaseMiddleware that runs before every message update.

I18N: All user-facing messages use get_text() with per-user language from DB.
"""

import logging
from collections import defaultdict, deque
from datetime import datetime, timedelta, timezone
from typing import Any, Awaitable, Callable, Deque

from aiogram import BaseMiddleware
from aiogram.types import Message, TelegramObject

from locales import get_text, t

log = logging.getLogger(__name__)

_user_message_times: dict[int, Deque[float]] = defaultdict(deque)


async def _get_lang(db, user_id: int) -> str:
    try:
        raw = await db.get_user_language(user_id)
        if raw in ("en", "bn"):
            return raw
    except Exception:
        pass
    return "en"


class AntiSpamMiddleware(BaseMiddleware):

    async def __call__(
        self,
        handler: Callable[[TelegramObject, dict], Awaitable[Any]],
        event: TelegramObject,
        data: dict,
    ) -> Any:

        if not isinstance(event, Message):
            return await handler(event, data)

        message: Message = event
        user = message.from_user
        if user is None:
            return await handler(event, data)

        settings = data.get("settings")
        db = data.get("db")

        if settings is None or db is None:
            return await handler(event, data)

        user_id = user.id

        if user_id in settings.admin_ids:
            return await handler(event, data)

        await db.upsert_user(user_id, user.username, user.full_name)

        if await db.is_muted(user_id):
            lang = await _get_lang(db, user_id)
            await message.answer(get_text("muted_message", lang))
            return

        now = datetime.now(timezone.utc).timestamp()
        window = settings.spam_window_seconds
        timestamps: Deque[float] = _user_message_times[user_id]

        while timestamps and now - timestamps[0] > window:
            timestamps.popleft()

        timestamps.append(now)

        if len(timestamps) > settings.spam_max_messages:
            strikes = await db.add_strike(user_id)
            lang = await _get_lang(db, user_id)

            log.warning(
                "Spam detected: user %d, %d messages in %ds. Strikes: %d",
                user_id, len(timestamps), window, strikes,
            )

            if strikes >= settings.strike_limit_for_ban:
                await db.ban_user(user_id)
                # Clear orphaned FSM state — banned user must not resume stale flows after unban
                try:
                    _storage = getattr(message.bot, "fsm_storage", None)
                    if _storage:
                        from aiogram.fsm.storage.base import StorageKey
                        _key = StorageKey(
                            bot_id=message.bot.id, chat_id=user_id, user_id=user_id
                        )
                        await _storage.set_state(key=_key, state=None)
                        await _storage.set_data(key=_key, data={})
                except Exception as _exc:
                    log.warning(
                        "[ANTISPAM] Could not clear FSM for banned user %d: %s",
                        user_id, _exc,
                    )
                await message.answer(get_text("spam_banned", lang))
                log.warning("User %d permanently banned after %d strikes.", user_id, strikes)
                return

            if strikes >= settings.strike_limit_for_mute:
                mute_minutes = settings.mute_duration_seconds // 60
                mute_until = datetime.now(timezone.utc) + timedelta(minutes=mute_minutes)
                await db.mute_user(user_id, mute_until)
                await message.answer(
                    t("spam_muted", lang,
                      minutes=mute_minutes,
                      strikes=strikes,
                      ban_limit=settings.strike_limit_for_ban)
                )
                log.warning(
                    "User %d muted until %s (strike %d).", user_id, mute_until.isoformat(), strikes
                )
                return

            await message.answer(
                t("spam_strike", lang,
                  strikes=strikes,
                  ban_limit=settings.strike_limit_for_ban)
            )
            return

        return await handler(event, data)