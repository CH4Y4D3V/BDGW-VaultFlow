from __future__ import annotations

"""
Bidirectional message router — topic_router.py (User-Centric Version)

Routes admin replies in a user's unified forum topic back to the user's private DMs.
"""

import asyncio
from datetime import datetime, timezone

from pyrogram import Client, filters
from pyrogram.enums import ParseMode
from pyrogram.errors import (
    FloodWait,
    InputUserDeactivated,
    PeerIdInvalid,
    RPCError,
    UserIsBlocked,
)
from pyrogram.types import Message

from app.config import settings
from app.services.topic_manager import get_topic_manager
from app.utils.logger import get_logger

logger = get_logger(__name__)

_FLOOD_BUFFER = settings.FLOODWAIT_EXTRA_BUFFER
_MAX_RETRIES = 3


def _get_thread_id(message: Message) -> int | None:
    return (
        getattr(message, "message_thread_id", None)
        or getattr(message, "reply_to_top_message_id", None)
    )


def _is_moderation_card(message: Message) -> bool:
    if not message.reply_markup:
        return False
    try:
        for row in message.reply_markup.inline_keyboard:
            for btn in row:
                data = getattr(btn, "callback_data", "") or ""
                if data.startswith(("mod_", "support_", "bc_")):
                    return True
    except Exception:
        pass
    return False


async def _deliver_to_user(
    client: Client, user_id: int, message: Message
) -> bool:
    """Copy the admin's reply to the user's private chat."""
    for attempt in range(_MAX_RETRIES):
        try:
            await client.copy_message(
                chat_id=user_id,
                from_chat_id=message.chat.id,
                message_id=message.id,
            )
            return True

        except (UserIsBlocked, PeerIdInvalid, InputUserDeactivated):
            logger.warning(
                "_deliver_to_user: user unreachable",
                extra={"ctx_user_id": user_id},
            )
            return False

        except FloodWait as e:
            await asyncio.sleep(int(e.value) + _FLOOD_BUFFER)

        except (RPCError, Exception) as e:
            logger.warning(
                "_deliver_to_user: error",
                extra={
                    "ctx_user_id": user_id,
                    "ctx_error": str(e),
                    "ctx_attempt": attempt + 1,
                },
            )
            if attempt == _MAX_RETRIES - 1:
                return False
            await asyncio.sleep(2 ** attempt)

    return False


@Client.on_message(filters.chat(settings.VERIFICATION_GROUP_ID))
async def route_admin_reply_to_user(client: Client, message: Message) -> None:
    """
    Routes any non-command message from a user-centric topic to the user's DMs.
    """
    try:
        # 1. Gate: must be inside a topic
        thread_id = _get_thread_id(message)
        if not thread_id:
            return

        # 2. Gate: must be from a human user (admin)
        if not message.from_user or message.from_user.is_bot:
            return

        # 3. Gate: skip commands (all admin commands in topics start with /)
        if message.text and message.text.startswith("/"):
            return

        # 4. Gate: never re-route cards or buttons
        if _is_moderation_card(message):
            return

        # 5. Gate: look up user for this topic
        topic_manager = get_topic_manager()
        topic_doc = await topic_manager.get_user_by_topic(thread_id)

        if not topic_doc:
            return  # Not a user-centric topic

        user_id: int = topic_doc["user_id"]

        # 6. Support Session Status Check (Optional, but kept for flow control)
        # If the admin is replying, we assume they have accepted or are ignoring the 'pending' state.
        # But we'll log it as a support interaction.

        logger.info(
            "ROUTING: admin to user",
            extra={
                "ctx_admin_id": message.from_user.id,
                "ctx_user_id": user_id,
                "ctx_topic_id": thread_id,
            },
        )

        # 7. Deliver
        delivered = await _deliver_to_user(client, user_id, message)

        if delivered:
            # Log as support message for history purposes
            try:
                db = DatabaseManager.get_db()
                await db["support_messages"].insert_one({
                    "user_id": user_id,
                    "topic_id": thread_id,
                    "hub_message_id": message.id,
                    "direction": "admin_to_user",
                    "created_at": datetime.now(timezone.utc),
                    "admin_id": message.from_user.id,
                })
            except Exception:
                pass

            # Payment Auto-Advance Logic
            try:
                from app.payments import get_payment_service
                from app.payments.models import PaymentStatus

                payment_service = get_payment_service()
                session = await payment_service.get_active_session(user_id)

                if session and session.status == PaymentStatus.PENDING_DETAILS:
                    await payment_service.update_status(
                        session.id, PaymentStatus.AWAITING_PAYMENT
                    )
                    await payment_service.start_timeout(session.id)
                    await client.send_message(
                        chat_id=message.chat.id,
                        text="✅ <b>Payment session activated</b>. User has 20 minutes to pay.",
                        message_thread_id=thread_id,
                        reply_to_message_id=message.id,
                        parse_mode=ParseMode.HTML
                    )
            except Exception:
                pass

        else:
            try:
                await client.send_message(
                    chat_id=message.chat.id,
                    text="⚠️ <b>Delivery Failed</b>\nUser may have blocked the bot.",
                    message_thread_id=thread_id,
                    reply_to_message_id=message.id,
                    parse_mode=ParseMode.HTML
                )
            except Exception:
                pass

    except Exception as e:
        logger.error(f"topic_router crashed: {e}", exc_info=True)
