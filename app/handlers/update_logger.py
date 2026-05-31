from __future__ import annotations

"""
Global Telegram update interceptor — update_logger.py

Registered at group=-1 (fires BEFORE all other handlers except ban guard at -2).
Provides structured entry-point logging for every incoming Telegram update.

FIX (GAP 7 — BD Phone Number Detection):
  Previous check: `any(p in text for p in ["017", "018", "019", "014", "013"])`
  This matched ANY text containing those substrings, e.g. "2017 was a good year"
  or "01389 is a postcode" — causing false-positive deletions.

  Fix: Use the same compiled regex as cleanup_service.py:
    r"(?<!\d)01[3-9]\d{8}(?!\d)"
  This requires exactly 11 digits starting with 01[3-9] — matches real BD numbers only.
"""

import asyncio
import re

from pyrogram import Client, filters
from pyrogram.enums import ChatType, ParseMode
from pyrogram.types import CallbackQuery, ChatMemberUpdated, Message

from app.utils.logger import get_logger

logger = get_logger(__name__)

# FIX: Added r prefix to make this a raw string, eliminating SyntaxWarning
# for \d inside a regular string in Python 3.12.
_BD_PHONE_REGEX = re.compile(r"(?<!\d)01[3-9]\d{8}(?!\d)")


# ── Ban guard (group -2 — highest priority) ───────────────────────────────────

@Client.on_message(filters.private, group=-2)
@Client.on_callback_query(group=-2)
async def handle_ban_guard(client: Client, update: Message | CallbackQuery) -> None:
    """
    Prevents banned users from interacting with any bot logic.
    Fires before all handlers. Stops propagation on banned users.
    """
    user_id = update.from_user.id if update.from_user else None
    if not user_id:
        return

    # Owner and sudo are never banned
    from app.core.permissions import is_sudo
    if is_sudo(user_id):
        return

    try:
        from app.repositories.user_repository import UserRepository
        user_repo = UserRepository()
        user_doc = await user_repo.get_user(user_id)
    except Exception:
        return  # DB error — allow through rather than lock everyone out

    if user_doc and user_doc.get("is_banned"):
        if isinstance(update, Message):
            try:
                await update.reply_text(
                    "❌ <b>Access Denied</b>\n\n"
                    "Your account has been permanently banned due to a "
                    "violation of our terms.",
                    parse_mode=ParseMode.HTML,
                )
            except Exception:
                pass
        elif isinstance(update, CallbackQuery):
            try:
                await update.answer("❌ You are banned.", show_alert=True)
            except Exception:
                pass

        update.stop_propagation()


# ── BD phone number cleanup (group -2 alongside ban guard) ───────────────────

@Client.on_message(filters.private, group=-2)
async def handle_phone_number_cleanup(client: Client, message: Message) -> None:
    """
    Section 20: BD phone numbers → auto-delete after 7 minutes.
    FIX GAP 7: Uses regex instead of substring match to avoid false positives.
    """
    if not message.from_user:
        return

    text = message.text or message.caption or ""
    if not text:
        return

    if _BD_PHONE_REGEX.search(text):
        user_id = message.from_user.id
        try:
            from app.services.cleanup_service import get_cleanup_service
            await get_cleanup_service().log_message(
                user_id=user_id,
                message_id=message.id,
                text=text,
                category="phone",
            )
        except Exception as e:
            logger.warning(
                "phone_cleanup_log_failed",
                extra={"ctx_user_id": user_id, "ctx_error": str(e)},
            )


# ── Dot-slash prefix auto-delete (group -1) ───────────────────────────────────

@Client.on_message(filters.private & filters.regex(r"^\./"), group=-1)
async def handle_prefix_auto_delete(client: Client, message: Message) -> None:
    """
    Section 4.3 / Section 20: Messages starting with ./ deleted after 10 seconds.
    Silent — no notification to user.
    """
    await asyncio.sleep(10)
    try:
        await message.delete()
    except Exception:
        pass


# ── Idempotency cache ─────────────────────────────────────────────────────────

_trace_cache: set[str] = set()
_MAX_CACHE_SIZE = 200


def _is_duplicate(update_id: str) -> bool:
    if update_id in _trace_cache:
        return True
    _trace_cache.add(update_id)
    if len(_trace_cache) > _MAX_CACHE_SIZE:
        try:
            _trace_cache.remove(next(iter(_trace_cache)))
        except (StopIteration, KeyError):
            pass
    return False


# ── Message trace (group -1) ──────────────────────────────────────────────────

@Client.on_message(group=-1)
async def trace_message_update(client: Client, message: Message) -> None:
    """Fires for EVERY incoming Message before any other handler (except ban guard)."""
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
        logger.error("UPDATE_TRACE: failed to log message update", exc_info=e)


# ── Callback query trace (group -1) ──────────────────────────────────────────

@Client.on_callback_query(group=-1)
async def trace_callback_update(client: Client, callback: CallbackQuery) -> None:
    """Fires for EVERY CallbackQuery before any other handler."""
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


# ── Chat member update trace (group -1) ──────────────────────────────────────

@Client.on_chat_member_updated(group=-1)
async def trace_member_update(client: Client, update: ChatMemberUpdated) -> None:
    """Fires for EVERY ChatMemberUpdated event."""
    user_id = update.new_chat_member.user.id if update.new_chat_member else 0
    update_key = f"member:{update.chat.id if update.chat else 0}:{user_id}:{update.date}"
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
