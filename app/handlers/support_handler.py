from __future__ import annotations

"""
Support handler — routes menu:support callbacks and manages the support ticket lifecycle.
"""

import asyncio
from datetime import datetime, timezone

from pyrogram import Client, filters
from pyrogram.enums import ParseMode
from pyrogram.errors import FloodWait, RPCError
from pyrogram.types import CallbackQuery, Message

from app.config import settings
from app.core.permissions import is_support_admin
from app.services.support_service import get_support_service
from app.services.topic_service import get_topic_service, TOPIC_SUPPORT
from app.utils.logger import get_logger

logger = get_logger(__name__)

_FLOOD_BUFFER = settings.FLOODWAIT_EXTRA_BUFFER
_MAX_RETRIES = 3


# ── Callback: menu:support ────────────────────────────────────────────────────

@Client.on_callback_query(filters.regex(r"^menu:support$") & filters.private)
async def handle_support_menu(client: Client, callback: CallbackQuery) -> None:
    user_id = callback.from_user.id

    await callback.message.edit_text(
        "🆘 <b>Support</b>\n\n"
        "Send your message and we'll connect you with our support team.\n\n"
        "<i>Just type your question or issue below.</i>",
        parse_mode=ParseMode.HTML,
    )
    await callback.answer()

    topic_service = get_topic_service()
    try:
        await topic_service.get_or_create_user_topic(client, user_id, TOPIC_SUPPORT)
    except Exception as e:
        logger.warning(
            "Support: pre-create topic failed, will retry on first message",
            extra={"ctx_user_id": user_id, "ctx_error": str(e)},
        )

    logger.info("Support ticket flow initiated", extra={"ctx_user_id": user_id})


# ── Private message routing — user has active support topic ───────────────────

@Client.on_message(filters.private & ~filters.command([]))
async def handle_private_message_support(client: Client, message: Message) -> None:
    if not message.from_user:
        return

    user_id = message.from_user.id
    topic_service = get_topic_service()
    topic_id = await topic_service.get_user_topic_id(user_id, TOPIC_SUPPORT)

    if topic_id is None:
        return

    support_service = get_support_service()
    await support_service.handle_user_message(client, message)


# ── Verification hub — admin replies in support topics ────────────────────────

@Client.on_message(filters.chat(settings.VERIFICATION_GROUP_ID))
async def handle_hub_message_support(client: Client, message: Message) -> None:
    thread_id = (
        getattr(message, "message_thread_id", None)
        or getattr(message, "reply_to_top_message_id", None)
    )
    if not thread_id:
        return

    if not message.from_user or message.from_user.is_bot:
        return

    if message.reply_markup:
        try:
            for row in message.reply_markup.inline_keyboard:
                for btn in row:
                    if getattr(btn, "callback_data", "").startswith("mod_"):
                        return
        except Exception:
            pass

    topic_service = get_topic_service()
    topic_doc = await topic_service.get_user_by_topic(thread_id)
    if not topic_doc or topic_doc.get("topic_type") != TOPIC_SUPPORT:
        return

    support_service = get_support_service()
    await support_service.handle_admin_reply(client, message)


# ── Admin command: /close_ticket ──────────────────────────────────────────────

@Client.on_message(
    filters.command("close_ticket")
    & filters.chat(settings.VERIFICATION_GROUP_ID)
)
async def handle_close_ticket(client: Client, message: Message) -> None:
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
                "If you have further questions, start a new conversation via /start."
            ),
            parse_mode=ParseMode.HTML,
        )
    except Exception as e:
        logger.warning(
            "Could not notify user of ticket close",
            extra={"ctx_user_id": user_id, "ctx_error": str(e)},
        )

    try:
        await client.close_forum_topic(
            chat_id=settings.VERIFICATION_GROUP_ID,
            message_thread_id=thread_id,
        )
    except Exception as e:
        logger.warning(
            "Could not close forum topic",
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