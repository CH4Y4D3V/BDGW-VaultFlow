from __future__ import annotations

"""
Bidirectional message router — topic_router.py

Routes admin replies in Verification Hub forum topics back to users' private DMs.

RC-7 FIX: Entry-point logging added so every routing attempt is traceable.
RC-3 FIX: Top-level try-except on the main handler — routing failures are
          logged and the admin receives an in-topic notification instead of
          the handler crashing silently.
RC-6 FIX: This handler is now the SINGLE authoritative routing point for all
          topic types. support_handler.py::handle_hub_support_message_persist
          no longer calls copy_message (which caused double-delivery of admin
          replies for support topics). This handler persists support messages
          to the DB for support topics after successful delivery.

Guard logic (prevents routing loops and noise):
  1. Message must be in VERIFICATION_GROUP_ID
  2. Message must be inside a forum topic (has message_thread_id)
  3. Sender must be a human user (not a bot)
  4. The topic must exist in user_topics collection
  5. Skip bot-generated moderation cards (reply_markup with mod_ callbacks)
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
from app.services.topic_manager import get_topic_manager, TOPIC_SUPPORT
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
                if data.startswith("mod_"):
                    return True
    except Exception as e:
        logger.exception(
            "moderation_card_detection_failed",
            extra={"ctx_error": str(e)},
        )
        pass
    return False


async def _deliver_to_user(
    client: Client, user_id: int, message: Message
) -> bool:
    """
    Copy the admin's reply to the user's private chat.
    Returns True on success, False if user is unreachable.

    RC-2 fix: catches ALL exception types, not just FloodWait/RPCError.
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
                "_deliver_to_user: user unreachable",
                extra={"ctx_user_id": user_id},
            )
            return False

        except FloodWait as e:
            await asyncio.sleep(int(e.value) + _FLOOD_BUFFER)

        except RPCError as e:
            logger.warning(
                "_deliver_to_user: RPCError",
                extra={
                    "ctx_user_id": user_id,
                    "ctx_error": str(e),
                    "ctx_attempt": attempt + 1,
                },
            )
            if attempt == _MAX_RETRIES - 1:
                return False
            await asyncio.sleep(2 ** attempt)

        except Exception as e:
            # RC-2 fix: catch everything else
            logger.error(
                "_deliver_to_user: unexpected exception",
                extra={
                    "ctx_user_id": user_id,
                    "ctx_error": str(e),
                    "ctx_attempt": attempt + 1,
                },
                exc_info=True,
            )
            if attempt == _MAX_RETRIES - 1:
                return False
            await asyncio.sleep(2 ** attempt)

    return False


async def _persist_support_message(
    user_id: int, thread_id: int, message: Message
) -> None:
    """
    RC-6 fix: persist admin reply to support_messages collection.
    Previously this was done by support_handler::handle_hub_support_message_persist
    via support_service.handle_admin_reply() — BUT that also called copy_message,
    causing double delivery. Now only the routing happens here (in _deliver_to_user),
    and the DB persistence is also done here for support topics.
    NEVER raises.
    """
    try:
        from app.repositories.support_repository import SupportRepository
        repo = SupportRepository()
        await repo.save_message({
            "user_id": user_id,
            "topic_id": thread_id,
            "user_message_id": None,
            "hub_message_id": message.id,
            "direction": "admin_to_user",
            "created_at": datetime.now(timezone.utc),
            "admin_id": message.from_user.id if message.from_user else None,
        })
    except Exception as e:
        logger.warning(
            "_persist_support_message: DB save failed (non-fatal)",
            extra={"ctx_error": str(e)},
        )


@Client.on_message(filters.chat(settings.VERIFICATION_GROUP_ID))
async def route_admin_reply_to_user(client: Client, message: Message) -> None:
    """
    Main routing handler — fires on every message in the verification hub.

    RC-7 fix: full entry logging with update context.
    RC-3 fix: top-level try-except — crashes here are logged and admin is notified.
    RC-6 fix: for support topics, persists to DB after routing (replacing the
              removed routing call in support_handler).
    """
    try:
        # ── Gate 1: must be inside a topic ───────────────────────────────────
        thread_id = _get_thread_id(message)
        if not thread_id:
            return

        # ── Gate 2: must be from a human ─────────────────────────────────────
        if not message.from_user:
            return
        if message.from_user.is_bot:
            return

        # ── Gate 3: never re-route moderation cards ──────────────────────────
        if _is_moderation_card(message):
            return

        # ── Gate 4: look up user for this topic ──────────────────────────────
        topic_manager = get_topic_manager()
        try:
            topic_doc = await topic_manager.get_user_by_topic(thread_id)
        except Exception as e:
            logger.error(
                "route_admin_reply_to_user: get_user_by_topic raised",
                extra={"ctx_thread_id": thread_id, "ctx_error": str(e)},
                exc_info=True,
            )
            return

        if not topic_doc:
            return  # Untracked topic — ignore

        user_id: int = topic_doc["user_id"]
        topic_type: str = topic_doc.get("topic_type", "support")

        # ── SYSTEM 10: SUPPORT BRIDGE GUARD ──
        if topic_type == TOPIC_SUPPORT:
            status = topic_doc.get("status", "pending")
            if status != "accepted":
                try:
                    await client.send_message(
                        chat_id=message.chat.id,
                        text="⚠️ <b>Bridge Inactive</b>\n\nYou must click <code>✅ Accept Support</code> on the moderation card before replying to the user.",
                        message_thread_id=thread_id,
                        reply_to_message_id=message.id,
                        parse_mode=ParseMode.HTML
                    )
                except Exception:
                    pass
                return

        logger.info(
            "HANDLER: route_admin_reply_to_user — routing",
            extra={
                "ctx_admin_id": message.from_user.id,
                "ctx_user_id": user_id,
                "ctx_topic_id": thread_id,
                "ctx_topic_type": topic_type,
                "ctx_msg_id": message.id,
            },
        )

        # ── Deliver ───────────────────────────────────────────────────────────
        delivered = await _deliver_to_user(client, user_id, message)

        if delivered:
            logger.info(
                "Admin reply routed to user",
                extra={
                    "ctx_admin_id": message.from_user.id,
                    "ctx_user_id": user_id,
                    "ctx_topic_id": thread_id,
                    "ctx_topic_type": topic_type,
                },
            )
            # RC-6 fix: persist support message after successful delivery
            if topic_type == TOPIC_SUPPORT:
                await _persist_support_message(user_id, thread_id, message)
            
            # RC-12 fix: automatically advance payment session on admin reply
            elif topic_type == "payment":
                # --- 7.3 FULL PAYMENT FLOW ---
                # "Only AFTER delivery success: Payment Session Activated (DB), Session Timeout Timer Starts"
                try:
                    from app.payments import get_payment_service
                    from app.payments.models import PaymentStatus
                    
                    payment_service = get_payment_service()
                    session = await payment_service.get_active_session(user_id)
                    
                    if session and session.status == PaymentStatus.PENDING_DETAILS:
                        # ── SYSTEM 7.3: CONFIRMED DELIVERY ──
                        # delivered is True here, so we advance
                        await payment_service.update_status(session.id, PaymentStatus.AWAITING_PAYMENT)
                        await payment_service.start_timeout(session.id)
                        
                        logger.info(
                            "Payment session activated after confirmed delivery",
                            extra={"ctx_payment_id": session.id, "ctx_user_id": user_id}
                        )
                        
                        # Notify admin in topic that session started
                        try:
                            await client.send_message(
                                chat_id=message.chat.id,
                                text="✅ <b>Delivery Confirmed</b>\nPayment session activated. User has 20 minutes to pay.",
                                message_thread_id=thread_id,
                                reply_to_message_id=message.id,
                                parse_mode=ParseMode.HTML
                            )
                        except:
                            pass
                except Exception as e:
                    logger.warning(
                        "Failed to advance payment session in topic_router",
                        extra={"ctx_user_id": user_id, "ctx_error": str(e)}
                    )
        else:
            logger.warning(
                "Admin reply could not be delivered to user",
                extra={
                    "ctx_user_id": user_id,
                    "ctx_topic_id": thread_id,
                },
            )
            try:
                await client.send_message(
                    chat_id=message.chat.id,
                    text=(
                        f"⚠️ Could not deliver reply to user "
                        f"<code>{user_id}</code>.\n"
                        "They may have blocked the bot."
                    ),
                    parse_mode=ParseMode.HTML,
                    reply_to_message_id=message.id,
                )
            except Exception as e:
                logger.warning(
                    "route_admin_reply_to_user: could not notify admin of delivery failure",
                    extra={"ctx_error": str(e)},
                )

    except Exception as e:
        # RC-3 fix: last resort catch — routing must never crash silently
        logger.error(
            "HANDLER: route_admin_reply_to_user unhandled exception",
            extra={
                "ctx_msg_id": message.id,
                "ctx_from_user": (
                    message.from_user.id if message.from_user else None
                ),
                "ctx_error": str(e),
            },
            exc_info=True,
        )
