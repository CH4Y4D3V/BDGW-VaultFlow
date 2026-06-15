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
from app.core.database import DatabaseManager  # FIX CRITICAL: was missing, caused NameError
from app.services.topic_manager import get_topic_manager
from app.utils.logger import get_logger

logger = get_logger(__name__)

_FLOOD_BUFFER = settings.FLOODWAIT_EXTRA_BUFFER
_MAX_RETRIES = 3


def _get_thread_id(message: Message) -> int | None:
    """Extract the forum thread/topic ID from a message, trying both attributes."""
    return (
        getattr(message, "message_thread_id", None)
        or getattr(message, "reply_to_top_message_id", None)
    )


def _is_moderation_card(message: Message) -> bool:
    """
    Returns True if the message contains inline buttons that belong to
    moderation, support, or broadcast card flows.  These must never be
    re-routed to users.
    """
    if not message.reply_markup:
        return False
    try:
        for row in message.reply_markup.inline_keyboard:
            for btn in row:
                data = getattr(btn, "callback_data", "") or ""
                if data.startswith(("mod_", "support_", "bc_")):
                    return True
    except Exception as e:
        logger.warning(
            "_is_moderation_card: unexpected error parsing markup",
            extra={"ctx_error": str(e)},
        )
    return False


async def _deliver_to_user(
    client: Client, user_id: int, message: Message
) -> bool:
    """
    Copy the admin's reply to the user's private chat.

    Retries up to _MAX_RETRIES times with exponential backoff on transient
    errors.  Returns True on success, False on permanent failure.
    """
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
                "_deliver_to_user: user permanently unreachable",
                extra={"ctx_user_id": user_id},
            )
            return False

        except FloodWait as e:
            wait = int(e.value) + _FLOOD_BUFFER
            logger.info(
                "_deliver_to_user: FloodWait",
                extra={"ctx_user_id": user_id, "ctx_wait": wait},
            )
            await asyncio.sleep(wait)

        except (RPCError, Exception) as e:
            logger.warning(
                "_deliver_to_user: transient error",
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


async def _send_to_hub(
    client: Client,
    chat_id: int,
    text: str,
    thread_id: int,
    reply_to: int,
) -> None:
    """
    Send a status message back into the hub topic, with FloodWait handling.

    Non-fatal — errors are logged but not re-raised.
    """
    for attempt in range(_MAX_RETRIES):
        try:
            await client.send_message(
                chat_id=chat_id,
                text=text,
                message_thread_id=thread_id,
                reply_to_message_id=reply_to,
                parse_mode=ParseMode.HTML,
            )
            return
        except FloodWait as e:
            wait = int(e.value) + _FLOOD_BUFFER
            logger.info(
                "_send_to_hub: FloodWait",
                extra={"ctx_wait": wait, "ctx_thread_id": thread_id},
            )
            await asyncio.sleep(wait)
        except Exception as e:
            logger.warning(
                "_send_to_hub: failed to send status message",
                extra={"ctx_error": str(e), "ctx_attempt": attempt + 1},
            )
            return  # Non-fatal; do not retry on non-FloodWait errors


@Client.on_message(filters.chat(settings.VERIFICATION_GROUP_ID))
async def route_admin_reply_to_user(client: Client, message: Message) -> None:
    """
    Routes any non-command, non-card message from a user-centric hub topic
    to the corresponding user's private DMs.

    Gates (in order):
      1. Message must be inside a forum topic (thread_id present).
      2. Sender must be a human user (not a bot).
      3. Message must not be a slash command.
      4. Message must not be a moderation/support card.
      5. Topic must map to a known user (via topic_manager).

    On successful delivery, logs the interaction to support_messages and
    optionally advances a PENDING_DETAILS payment session.
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
            return  # Not a user-centric topic — ignore silently

        user_id: int = topic_doc["user_id"]

        logger.info(
            "ROUTING: admin reply → user DM",
            extra={
                "ctx_admin_id": message.from_user.id,
                "ctx_user_id": user_id,
                "ctx_topic_id": thread_id,
            },
        )

        # 6. Deliver to user's DM
        delivered = await _deliver_to_user(client, user_id, message)

        if delivered:
            # 6a. Log as support interaction in DB (non-fatal)
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
            except Exception as e:
                logger.warning(
                    "route_admin_reply: support_messages insert failed",
                    extra={"ctx_user_id": user_id, "ctx_error": str(e)},
                )

            # 6b. Payment auto-advance: when admin sends payment details directly
            # in the topic (not via the FSM button), advance the session from
            # WAITING_PAYMENT_DETAILS → WAITING_TXID and start the 20-min timer.
            #
            # BUG FIX: previous code checked PaymentStatus.PENDING_DETAILS and
            # advanced to PaymentStatus.AWAITING_PAYMENT. Neither of these is
            # the status actually used by the payment service:
            #   - Sessions are created with status WAITING_PAYMENT_DETAILS
            #   - handle_payment_inputs expects WAITING_TXID or WAITING_SCREENSHOT
            # The wrong status meant the auto-advance never triggered, the session
            # stayed in WAITING_PAYMENT_DETAILS, and when the user sent their TXID
            # the payment handler raised ContinuePropagation → support caught it.
            try:
                from app.payments import get_payment_service
                from app.payments.models import PaymentStatus

                payment_service = get_payment_service()
                session = await payment_service.get_active_session(user_id)

                if session and session.status == PaymentStatus.WAITING_PAYMENT_DETAILS:
                    advanced = await payment_service.update_status(
                        session.id, PaymentStatus.WAITING_TXID
                    )
                    if advanced:
                        await payment_service.start_timeout(
                            session.id, confirmed_delivery=True
                        )
                        # Send user the "submit your TXID" prompt
                        await client.send_message(
                            chat_id=user_id,
                            text=(
                                "👆 <b>Payment details received above.</b>\n\n"
                                "Once you've sent the payment, reply here with your "
                                "<b>Transaction ID (TXID)</b> as a text message to "
                                "submit your proof for review."
                            ),
                            parse_mode="html",
                        )
                        await _send_to_hub(
                            client=client,
                            chat_id=message.chat.id,
                            text="✅ <b>Payment session activated.</b> User has 20 minutes to submit TXID.",
                            thread_id=thread_id,
                            reply_to=message.id,
                        )
            except Exception as e:
                logger.warning(
                    "route_admin_reply: payment auto-advance failed",
                    extra={"ctx_user_id": user_id, "ctx_error": str(e)},
                )

        else:
            # Delivery failed — notify admin in topic
            await _send_to_hub(
                client=client,
                chat_id=message.chat.id,
                text="⚠️ <b>Delivery Failed</b>\nUser may have blocked the bot.",
                thread_id=thread_id,
                reply_to=message.id,
            )

    except Exception as e:
        logger.error(
            "topic_router: route_admin_reply_to_user crashed",
            extra={"ctx_error": str(e)},
            exc_info=True,
        )
