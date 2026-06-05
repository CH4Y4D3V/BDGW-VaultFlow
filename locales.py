from __future__ import annotations
import logging

logger = logging.getLogger(__name__)

async def get_user_lang(db, user_id: int) -> str:
    """
    Fetch user language preference from database.
    Defaults to 'en'.
    """
    try:
        user = await db.users.find_one({"_id": user_id})
        if user and "language" in user:
            return user["language"]
    except Exception as e:
        logger.warning(f"Failed to fetch user language for {user_id}: {e}")
    return "en"

def get_text(key: str, lang: str = "en", **kwargs) -> str:
    """
    Get localized text for a given key and language.
    """
    texts = {
        "en": {
            "support_already_active": "⚠️ <b>Support Already Active</b>\n\nYou already have an open support ticket. Please wait for an admin to respond.",
            "support_session_started": "✅ <b>Support Ticket Opened</b>\n\nYour request has been sent to our team. Please describe your issue in detail.",
            "support_session_expired": "⚠️ <b>Session Expired</b>\n\nYour support session has timed out due to inactivity. Please start a new one with /support.",
            "support_message_received": "✅ Message received.",
            "support_cant_reach": "⚠️ <b>Delivery Failed</b>\n\nWe couldn't deliver your message to the support team. Please try again later.",
            "support_session_closed_user": "✅ <b>Support Ticket Closed</b>\n\nThis support session has been closed by an admin. Thank you!",
            "support_connected": "✅ <b>Admin Connected</b>\n\nYou are now connected to a live admin. You can send messages directly.",
            "support_reply_header": "💬 <b>Admin Reply:</b>\n",
            "banned_message": "🚫 <b>You have been banned.</b>\n\nYou can no longer use this bot.",
            "warn_received": "⚠️ <b>Warning Received</b>\n\nYou have received a warning ({count}/{max}). {remaining} more and you will be banned{s}.",
            "warn_banned": "🚫 <b>Banned</b>\n\nYou have been banned after reaching the maximum number of warnings.",
            "support_already_accepted": "⚠️ Support session for user <code>{user_id}</code> is already accepted by {handler}.",
            "support_not_active": "⚠️ No active support session for user <code>{user_id}</code>.",
        }
    }
    
    # Fallback to English if language not found
    lang_texts = texts.get(lang, texts["en"])
    # Fallback to key itself if key not found
    template = lang_texts.get(key, texts["en"].get(key, key))
    
    try:
        return template.format(**kwargs)
    except Exception:
        return template

def t(key: str, lang: str = "en", **kwargs) -> str:
    """Alias for get_text."""
    return get_text(key, lang, **kwargs)
