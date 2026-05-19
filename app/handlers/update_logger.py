from __future__ import annotations

from pyrogram import Client, filters
from pyrogram.types import CallbackQuery, ChatMemberUpdated, Message

from app.core.logger import get_logger

logger = get_logger(__name__)


@Client.on_message(group=-1)
async def trace_message_update(client: Client, message: Message) -> None:
    try:
        logger.info(
            "UPDATE_TRACE: message",
            extra={
                "ctx_msg_id": message.id,
                "ctx_chat_id": message.chat.id if message.chat else None,
                "ctx_chat_type": str(message.chat.type) if message.chat else None,
                "ctx_from_user_id": message.from_user.id if message.from_user else None,
                "ctx_sender_chat_id": message.sender_chat.id if message.sender_chat else None,
                "ctx_media_type": str(message.media) if message.media else None,
                "ctx_text_preview": (message.text or "")[:80] if message.text else None,
                "ctx_caption_preview": (message.caption or "")[:80] if message.caption else None,
                "ctx_media_group_id": message.media_group_id,
                "ctx_is_command": bool(message.text and message.text.startswith("/")),
            },
        )
    except Exception as e:
        logger.error("UPDATE_TRACE: failed to log message update", exc_info=e)


@Client.on_callback_query(group=-1)
async def trace_callback_update(client: Client, callback: CallbackQuery) -> None:
    # FIX: CallbackQuery.message can be None (e.g. when the message is too old,
    # was deleted, or the callback originates from an inline query result).
    # Previously this crashed with "'CallbackQuery' object has no attribute 'chat'"
    # because the code tried to access callback.message.chat without guarding.
    try:
        chat_id = None
        message_id = None

        if callback.message is not None:
            # message.chat may itself be None on certain update types
            chat_id = callback.message.chat.id if getattr(callback.message, "chat", None) else None
            message_id = callback.message.id

        logger.info(
            "UPDATE_TRACE: callback_query",
            extra={
                "ctx_callback_id": callback.id,
                "ctx_from_user_id": callback.from_user.id if callback.from_user else None,
                "ctx_data": callback.data,
                "ctx_chat_id": chat_id,
                "ctx_message_id": message_id,
            },
        )
    except Exception as e:
        logger.error("UPDATE_TRACE: failed to log callback update", exc_info=e)


@Client.on_chat_member_updated(group=-1)
async def trace_member_update(client: Client, update: ChatMemberUpdated) -> None:
    try:
        logger.info(
            "UPDATE_TRACE: chat_member_updated",
            extra={
                "ctx_chat_id": update.chat.id if update.chat else None,
                "ctx_from_user_id": update.from_user.id if update.from_user else None,
                "ctx_old_status": (
                    str(update.old_chat_member.status) if update.old_chat_member else None
                ),
                "ctx_new_status": (
                    str(update.new_chat_member.status) if update.new_chat_member else None
                ),
            },
        )
    except Exception as e:
        logger.error("UPDATE_TRACE: failed to log member update", exc_info=e)
