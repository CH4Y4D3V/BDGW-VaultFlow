"""
payment_intent_warning.py
─────────────────────────
Shared helper: sends the anti-spam payment warning to user when payment intent
is first armed.

I18N FIX: All user-facing strings hardcoded to English per Spec v1.0.
"""

from pyrogram import Client
from pyrogram.enums import ParseMode
from datetime import datetime, timezone

async def send_intent_warning(
    client: Client,
    user_id: int,
) -> None:
    """
    Send the payment intent warning message to the user.
    """
    warning_text = (
        "⚠️ <b>Action Required: Payment Details Sent</b>\n\n"
        "You have received payment details. You must complete your payment and submit "
        "the TXID and screenshot within <b>20 minutes</b>.\n\n"
        "Failure to do so will result in an <b>automatic permanent ban</b> to prevent spam."
    )

    try:
        msg = await client.send_message(
            chat_id=user_id,
            text=warning_text,
            parse_mode=ParseMode.HTML,
        )
        
        # Track for cleanup when intent is cleared
        if msg:
            from app.core.database import DatabaseManager
            db = DatabaseManager.get_db()
            try:
                await db["message_tracker"].insert_one({
                    "user_id": user_id,
                    "message_id": msg.id,
                    "context": "payment_intent",
                    "created_at": datetime.now(timezone.utc)
                })
            except Exception:
                pass
    except Exception:
        pass
