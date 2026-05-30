from __future__ import annotations

"""
Global Telegram update interceptor — update_logger.py

Registered at group=-1, which fires BEFORE all other handlers.
Provides structured entry-point logging for every incoming Telegram update.

This is the PRIMARY observability tool for diagnosing silent handler failures.
Without this, a handler crash is invisible — Pyrogram's internal dispatcher
catches the exception and drops the update with no structured trace.

Rules:
  - NEVER raises.
  - NEVER calls stop_propagation() — must not block downstream handlers.
  - Logs the full update envelope so you can always confirm the bot received
    an update even when the downstream handler fails.
"""

import asyncio
from pyrogram import Client, filters
from pyrogram.types import CallbackQuery, ChatMemberUpdated, Message

from app.utils.logger import get_logger

logger = get_logger(__name__)

# ── SYSTEM 20: FRAUD/BAN GUARD ──
@Client.on_message(filters.private, group=-2)
@Client.on_callback_query(group=-2)
async def handle_ban_guard(client: Client, update: Message | CallbackQuery):
    """
    Prevents banned users from interacting with any bot logic.
    Group -2 ensures this runs BEFORE everything else.
    """
    user_id = update.from_user.id if update.from_user else None
    if not user_id:
        return

    # Skip owner/admins from ban check for safety
    from app.core.permissions import is_sudo
    if is_sudo(user_id):
        return

    from app.repositories.user_repository import UserRepository
    user_repo = UserRepository()
    user_doc = await user_repo.get_user(user_id)
    
    if user_doc and user_doc.get("is_banned"):
        if isinstance(update, Message):
            try:
                await update.reply_text("❌ <b>Access Denied</b>\n\nYour account has been permanently banned due to a violation of our terms.", parse_mode=ParseMode.HTML)
            except:
                pass
        else:
            try:
                await update.answer("❌ You are banned.", show_alert=True)
            except:
                pass
        
        update.stop_propagation()


# ── SYSTEM 4: DOT-SLASH PREFIX AUTO-DELETE ──
@Client.on_message(filters.private & filters.regex(r"^\./"), group=-1)
async def handle_prefix_auto_delete(client: Client, message: Message):
    """
    Catch messages starting with ./ and delete them after 10 seconds.
    Group -1 ensures this runs alongside tracing and doesn't block downstream.
    """
    await asyncio.sleep(10)
    try:
        await message.delete()
    except Exception:
        pass

# ── Idempotency Cache ──
# Prevents duplicate UPDATE_TRACE logs if the plugin is loaded multiple times
# or if multiple handlers catch the same update.
_trace_cache: set[str] = set()
_MAX_CACHE_SIZE = 100

def _is_duplicate(update_id: str) -> bool:
    if update_id in _trace_cache:
        return True
    _trace_cache.add(update_id)
    if len(_trace_cache) > _MAX_CACHE_SIZE:
        # Simple FIFO-ish cleanup
        try:
            _trace_cache.remove(next(iter(_trace_cache)))
        except (StopIteration, KeyError):
            pass
    return False


@Client.on_message(group=-1)
async def trace_message_update(client: Client, message: Message) -> None:
    """
    Fires for EVERY incoming Message update before any other handler.
    RC-1 fix: global trace so we always know the bot received the update.
    """
    update_key = f"msg:{message.id}:{message.chat.id if message.chat else 0}"
    if _is_duplicate(update_key):
        return

    try:
        chat_id = message.chat.id if message.chat else None
        chat_type = str(message.chat.type) if message.chat else None
        from_user_id = message.from_user.id if message.from_user else None
        sender_chat_id = message.sender_chat.id if message.sender_chat else None
        media_type = str(message.media) if message.media else None
        text_preview = (message.text or "")[:80] if message.text else None
        caption_preview = (message.caption or "")[:80] if message.caption else None

        logger.info(
            "UPDATE_TRACE: message",
            extra={
                "ctx_msg_id": message.id,
                "ctx_chat_id": chat_id,
                "ctx_chat_type": chat_type,
                "ctx_from_user_id": from_user_id,
                "ctx_sender_chat_id": sender_chat_id,
                "ctx_media_type": media_type,
                "ctx_text_preview": text_preview,
                "ctx_caption_preview": caption_preview,
                "ctx_media_group_id": message.media_group_id,
                "ctx_is_command": bool(
                    message.text and message.text.startswith("/")
                ),
            },
        )
    except Exception as e:
        # Safety net — this handler must NEVER crash
        logger.error("UPDATE_TRACE: failed to log message update", exc_info=e)


@Client.on_callback_query(group=-1)
async def trace_callback_update(client: Client, callback: CallbackQuery) -> None:
    """
    Fires for EVERY incoming CallbackQuery before any other handler.
    RC-1 fix: confirms callback delivery even when the actual handler crashes.
    """
    update_key = f"cb:{callback.id}"
    if _is_duplicate(update_key):
        return

    try:
        logger.info(
            "UPDATE_TRACE: callback_query",
            extra={
                "ctx_callback_id": callback.id,
                "ctx_from_user_id": (
                    callback.from_user.id if callback.from_user else None
                ),
                "ctx_data": callback.data,
                "ctx_chat_id": (
                    callback.message.chat.id
                    if callback.message and callback.message.chat
                    else None
                ),
                "ctx_message_id": (
                    callback.message.id if callback.message else None
                ),
            },
        )
    except Exception as e:
        logger.error("UPDATE_TRACE: failed to log callback update", exc_info=e)


@Client.on_chat_member_updated(group=-1)
async def trace_member_update(client: Client, update: ChatMemberUpdated) -> None:
    """
    Fires for EVERY ChatMemberUpdated event before other handlers.
    """
    user_id = update.new_chat_member.user.id if update.new_chat_member else 0
    update_key = f"member:{update.chat.id}:{user_id}:{update.date}"
    if _is_duplicate(update_key):
        return

    try:
        logger.info(
            "UPDATE_TRACE: chat_member_updated",
            extra={
                "ctx_chat_id": update.chat.id if update.chat else None,
                "ctx_from_user_id": (
                    update.from_user.id if update.from_user else None
                ),
                "ctx_old_status": (
                    str(update.old_chat_member.status)
                    if update.old_chat_member
                    else None
                ),
                "ctx_new_status": (
                    str(update.new_chat_member.status)
                    if update.new_chat_member
                    else None
                ),
            },
        )
    except Exception as e:
        logger.error("UPDATE_TRACE: failed to log member update", exc_info=True)
