from __future__ import annotations

"""
Support handler.

RC-4 FIX: ~filters.command([]) replaced with ~filters.regex(r"^/").
RC-6 FIX: admin->user routing handled by topic_router.py only.
SUPPORT-FIX: handle_private_message_support now creates topic on demand
             instead of returning early when topic is None. Fixes silently
             dropped messages when forum topic creation failed in handle_support_menu.
"""

import asyncio
from datetime import datetime, timezone

from pyrogram import Client, filters
from pyrogram.enums import ParseMode
from pyrogram.errors import FloodWait, RPCError
from pyrogram.types import CallbackQuery, Message

from app.config import settings
from app.core.redis_client import get_redis
from app.core.permissions import is_support_admin
from app.repositories.support_repository import SupportRepository
from app.services.support_service import get_support_service
from app.services.topic_service import get_topic_service, TOPIC_SUPPORT
from app.utils.logger import get_logger

logger = get_logger(__name__)

_FLOOD_BUFFER = settings.FLOODWAIT_EXTRA_BUFFER
_MAX_RETRIES = 3


def _get_support_repo():
    return SupportRepository()


async def _safe_reply(
    message: Message,
    text: str,
    parse_mode: ParseMode = ParseMode.HTML,
) -> None:
    for attempt in range(_MAX_RETRIES):
        try:
            await message.reply_text(text, parse_mode=parse_mode)
            return
        except FloodWait as e:
            await asyncio.sleep(int(e.value) + _FLOOD_BUFFER)
        except RPCError as e:
            logger.warning(
                "_safe_reply: RPCError",
                extra={"ctx_error": str(e), "ctx_attempt": attempt + 1},
            )
            if attempt == _MAX_RETRIES - 1:
                return
            await asyncio.sleep(2 ** attempt)
        except Exception as e:
            logger.error(
                "_safe_reply: unexpected exception",
                extra={"ctx_error": str(e), "ctx_attempt": attempt + 1},
                exc_info=True,
            )
            if attempt == _MAX_RETRIES - 1:
                return
            await asyncio.sleep(2 ** attempt)


# ── Callback: menu:support ────────────────────────────────────────────────────

@Client.on_callback_query(filters.regex(r"^menu:support$"))
async def handle_support_menu(client: Client, callback: CallbackQuery) -> None:
    user_id = callback.from_user.id if callback.from_user else 0

    redis = get_redis()
    spam_key = f"menu:spam:{user_id}"
    try:
        if await redis.exists(spam_key):
            await callback.answer("Slow down! Processing...", show_alert=False)
            return
        await redis.set(spam_key, "1", ex=1)
    except Exception:
        pass

    await callback.answer()

    logger.info(
        "HANDLER: handle_support_menu entered",
        extra={"ctx_from_user": user_id},
    )

    try:
        # Try to pre-create the topic so the first message routes immediately.
        # If it fails we still show the prompt — the topic will be created
        # lazily when the user's first message arrives.
        topic_service = get_topic_service()
        try:
            topic_id = await topic_service.get_or_create_user_topic(
                client, user_id, TOPIC_SUPPORT
            )
            logger.info(
                "Support topic ready",
                extra={"ctx_user_id": user_id, "ctx_topic_id": topic_id},
            )
        except Exception as e:
            logger.warning(
                "handle_support_menu: topic pre-creation failed (will retry on first message)",
                extra={"ctx_user_id": user_id, "ctx_error": str(e)},
            )

        await callback.message.edit_text(
            "🆘 <b>Support</b>\n\n"
            "Send your message below and our team will respond shortly.\n\n"
            "<i>Type your question or describe your issue.</i>",
            parse_mode=ParseMode.HTML,
        )

        logger.info(
            "Support ticket flow initiated",
            extra={"ctx_user_id": user_id},
        )

    except Exception as e:
        logger.error(
            "HANDLER: handle_support_menu unhandled exception",
            extra={"ctx_error": str(e)},
            exc_info=True,
        )
        try:
            await callback.answer("⚠️ Could not open support. Please try again.", show_alert=True)
        except Exception:
            pass


# ── Private message routing ───────────────────────────────────────────────────

@Client.on_message(filters.private & ~filters.regex(r"^/"))
async def handle_private_message_support(client: Client, message: Message) -> None:
    """
    Route non-command private text messages to support.

    SUPPORT-FIX: Previously returned early if topic_id was None (topic creation
    had failed in handle_support_menu). Now creates the topic on demand so the
    very first message a user sends always reaches the verification hub.
    """
    if not message.from_user:
        return

    # Skip media — handled by submission_handler
    if message.photo or message.video or message.document or message.animation:
        logger.debug(
            "handle_private_message_support: skipping media",
            extra={"ctx_from_user": message.from_user.id},
        )
        return

    user_id = message.from_user.id

    try:
        topic_service = get_topic_service()
        topic_id = await topic_service.get_user_topic_id(user_id, TOPIC_SUPPORT)

        if topic_id is None:
            # SUPPORT-FIX: topic missing — create it now instead of dropping message
            logger.info(
                "handle_private_message_support: no topic found, creating on demand",
                extra={"ctx_user_id": user_id},
            )
            try:
                topic_id = await topic_service.get_or_create_user_topic(
                    client, user_id, TOPIC_SUPPORT
                )
                logger.info(
                    "handle_private_message_support: topic created",
                    extra={"ctx_user_id": user_id, "ctx_topic_id": topic_id},
                )
            except Exception as e:
                logger.error(
                    "handle_private_message_support: on-demand topic creation failed",
                    extra={"ctx_user_id": user_id, "ctx_error": str(e)},
                    exc_info=True,
                )
                # Still route — SupportService will forward to group main chat
                topic_id = None

        if topic_id is None:
            # No topic at all (forum topics disabled or bot lacks permission).
            # Route directly to verification group main chat as fallback.
            logger.info(
                "handle_private_message_support: routing without topic (fallback)",
                extra={"ctx_user_id": user_id},
            )

        logger.info(
            "HANDLER: handle_private_message_support routing to support",
            extra={"ctx_user_id": user_id, "ctx_topic_id": topic_id},
        )

        support_service = get_support_service()
        await support_service.handle_user_message(client, message)

    except Exception as e:
        logger.error(
            "HANDLER: handle_private_message_support unhandled exception",
            extra={"ctx_user_id": user_id, "ctx_error": str(e)},
            exc_info=True,
        )


# ── Admin command: /close_ticket ──────────────────────────────────────────────

@Client.on_message(
    filters.command("close_ticket")
    & filters.chat(settings.VERIFICATION_GROUP_ID)
)
async def handle_close_ticket(client: Client, message: Message) -> None:
    logger.info(
        "HANDLER: handle_close_ticket entered",
        extra={"ctx_from_user": message.from_user.id if message.from_user else None},
    )

    try:
        if not message.from_user or not is_support_admin(message.from_user.id):
            return

        thread_id = (
            getattr(message, "message_thread_id", None)
            or getattr(message, "reply_to_top_message_id", None)
        )
        if not thread_id:
            await message.reply_text("❌ This command must be used inside a topic thread.")
            return

        topic_service = get_topic_service()
        topic_doc = await topic_service.get_user_by_topic(thread_id)
        if not topic_doc or topic_doc.get("topic_type") != TOPIC_SUPPORT:
            await message.reply_text("❌ This is not a support topic.")
            return

        user_id: int = topic_doc["user_id"]

        try:
            await client.send_message(
                chat_id=user_id,
                text=(
                    "✅ <b>Your support ticket has been closed.</b>\n\n"
                    "If you have further questions, start a new conversation anytime."
                ),
                parse_mode=ParseMode.HTML,
            )
        except Exception as e:
            logger.warning(
                "handle_close_ticket: could not notify user",
                extra={"ctx_user_id": user_id, "ctx_error": str(e)},
            )

        try:
            await client.close_forum_topic(
                chat_id=settings.VERIFICATION_GROUP_ID,
                message_thread_id=thread_id,
            )
        except Exception as e:
            logger.warning(
                "handle_close_ticket: could not close forum topic",
                extra={"ctx_thread_id": thread_id, "ctx_error": str(e)},
            )

        await message.reply_text(
            f"✅ Ticket closed. User <code>{user_id}</code> has been notified.",
            parse_mode=ParseMode.HTML,
        )

        logger.info(
            "Support ticket closed",
            extra={
                "ctx_user_id": user_id,
                "ctx_topic_id": thread_id,
                "ctx_admin": message.from_user.id,
            },
        )

    except Exception as e:
        logger.error(
            "HANDLER: handle_close_ticket unhandled exception",
            extra={"ctx_error": str(e)},
            exc_info=True,
        )
        await _safe_reply(message, "⚠️ Failed to close ticket. Please try again.")