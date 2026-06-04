"""
payment_intent_warning.py
─────────────────────────
Shared helper: sends the anti-spam payment warning to user when payment intent
is first armed.

I18N FIX: removed hardcoded Bangla _PAYMENT_INTENT_WARNING string.
Now uses get_text("intent_warning_initial", lang) where lang is fetched from DB.
Falls back to "en" if DB is unavailable. English users no longer see Bangla.
"""

from aiogram import Bot
from locales import get_text, get_user_lang


async def send_intent_warning(
    bot: Bot,
    user_id: int,
    db=None,
) -> None:
    """
    Send the payment intent warning message to the user in their stored language.

    Call immediately after set_payment_intent() returns True (newly set).
    The `db` parameter is required for language resolution and message tracking.
    If db is None, defaults to English and skips tracking.
    """
    # Resolve language from DB — fallback to "en" if DB unavailable
    lang = "en"
    if db is not None:
        try:
            lang = (await get_user_lang(db, user_id)) or "en"
        except Exception:
            pass

    warning_text = get_text("intent_warning_initial", lang)

    try:
        msg = await bot.send_message(
            chat_id=user_id,
            text=warning_text,
            parse_mode="HTML",
        )
        # Track for cleanup when intent is cleared
        if db is not None and msg is not None:
            try:
                await db.track_user_message(user_id, msg.message_id, "payment_intent")
            except Exception:
                pass
    except Exception:
        pass  # Non-critical — the scheduler warnings still fire on their own