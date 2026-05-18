from __future__ import annotations

import asyncio
from collections import defaultdict

from pyrogram import Client, filters
from pyrogram.enums import ParseMode
from pyrogram.errors import FloodWait, RPCError
from pyrogram.types import (
    CallbackQuery,
    InlineKeyboardButton,
    InlineKeyboardMarkup,
    Message,
)

from app.config import settings
from app.handlers.creator_onboarding import check_and_gate_creator
from app.moderation import verification_hub
from app.services import submission_service
from app.utils.logger import get_logger

logger = get_logger(__name__)

_album_buffer: dict[str, list[Message]] = defaultdict(list)
_album_tasks: dict[str, asyncio.Task] = {}
_album_lock = asyncio.Lock()

_FLOOD_BUFFER = settings.FLOODWAIT_EXTRA_BUFFER
_MAX_REPLY_RETRIES = 3


# ── RC-2 FIX: _safe_reply catches ALL exception types, not just FloodWait/RPCError ──

async def _safe_reply(
    message: Message,
    text: str,
    parse_mode: ParseMode = ParseMode.HTML,
    reply_markup=None,
) -> bool:
    """
    Send a reply with full retry coverage.

    RC-2 fix: the original only caught FloodWait and RPCError.
    Pyrogram can raise asyncio.TimeoutError, ConnectionError,
    MessageTooLong, or other non-RPCError exceptions during reply.
    These were previously swallowed by Pyrogram's dispatcher — user got silence.

    Now catches ALL exceptions. Returns True on success, False on all-attempts-failed.
    NEVER raises.
    """
    for attempt in range(_MAX_REPLY_RETRIES):
        try:
            await message.reply_text(
                text, parse_mode=parse_mode, reply_markup=reply_markup
            )
            return True

        except FloodWait as e:
            wait = int(e.value) + _FLOOD_BUFFER
            logger.warning(
                "_safe_reply: FloodWait",
                extra={
                    "ctx_msg_id": message.id,
                    "ctx_wait": wait,
                    "ctx_attempt": attempt + 1,
                },
            )
            await asyncio.sleep(wait)

        except RPCError as e:
            logger.warning(
                "_safe_reply: RPCError",
                extra={
                    "ctx_msg_id": message.id,
                    "ctx_error": str(e),
                    "ctx_attempt": attempt + 1,
                },
            )
            if attempt == _MAX_REPLY_RETRIES - 1:
                return False
            await asyncio.sleep(2 ** attempt)

        except Exception as e:
            # RC-2 fix: catch everything else — asyncio.TimeoutError,
            # ConnectionError, AttributeError, etc.
            logger.error(
                "_safe_reply: unexpected exception",
                extra={
                    "ctx_msg_id": message.id,
                    "ctx_error": str(e),
                    "ctx_attempt": attempt + 1,
                },
                exc_info=True,
            )
            if attempt == _MAX_REPLY_RETRIES - 1:
                return False
            await asyncio.sleep(2 ** attempt)

    return False


# ── Internal pipeline ────────────────────────────────────────────────────────

async def _submit_for_review(
    client: Client,
    messages: list[Message],
    user_id: int,
) -> None:
    """
    RC-8 fix: every exit path sends user feedback.
    If register_pending or forward_to_verification fails for any reason,
    the user always receives a visible acknowledgement.
    """
    reference_message = messages[0]

    # Register in pending cache
    try:
        await submission_service.register_pending(user_id, messages)
    except Exception as e:
        logger.error(
            "_submit_for_review: register_pending raised",
            extra={"ctx_user_id": user_id, "ctx_error": str(e)},
            exc_info=True,
        )
        await _safe_reply(
            reference_message,
            "⚠️ We couldn't register your submission right now. Please try again.",
        )
        return

    # Forward to verification group
    success = False
    try:
        success = await verification_hub.forward_to_verification(
            client, messages, user_id
        )
    except Exception as e:
        logger.error(
            "_submit_for_review: forward_to_verification raised",
            extra={"ctx_user_id": user_id, "ctx_error": str(e)},
            exc_info=True,
        )

    if success:
        sent = await _safe_reply(
            reference_message,
            "📨 Your submission is <b>under review</b>. "
            "You'll be notified once a decision is made.",
        )
        if not sent:
            logger.error(
                "_submit_for_review: acknowledgement reply failed to send",
                extra={"ctx_user_id": user_id},
            )
        logger.info(
            "Submission forwarded and acknowledged",
            extra={"ctx_user_id": user_id, "ctx_count": len(messages)},
        )
    else:
        # Clean up pending entry
        try:
            await submission_service.reject_pending(reference_message.id)
        except Exception as e:
            logger.warning(
                "_submit_for_review: reject_pending failed after forwarding failure",
                extra={"ctx_user_id": user_id, "ctx_error": str(e)},
            )

        sent = await _safe_reply(
            reference_message,
            "⚠️ We couldn't forward your submission right now. "
            "Please try again in a moment.",
        )
        if not sent:
            logger.error(
                "_submit_for_review: error reply also failed to send",
                extra={"ctx_user_id": user_id},
            )
        logger.error(
            "_submit_for_review: verification forwarding failed",
            extra={"ctx_user_id": user_id},
        )


async def _flush_album(group_id: str, user_id: int, client: Client) -> None:
    try:
        await asyncio.sleep(settings.MEDIA_GROUP_TIMEOUT_SECONDS)
    except asyncio.CancelledError:
        return

    async with _album_lock:
        messages = _album_buffer.pop(group_id, [])
        _album_tasks.pop(group_id, None)

    if not messages:
        return

    messages.sort(key=lambda m: m.id)
    logger.info(
        "Album flushed for review",
        extra={
            "ctx_group_id": group_id,
            "ctx_user_id": user_id,
            "ctx_count": len(messages),
        },
    )
    await _submit_for_review(client, messages, user_id)


# ── Handlers ─────────────────────────────────────────────────────────────────

@Client.on_message(filters.command("start") & filters.private)
async def handle_start(client: Client, message: Message) -> None:
    """
    RC-7 fix: entry logging.
    RC-3 fix: top-level try-except with fallback reply.
    """
    logger.info(
        "HANDLER: handle_start entered",
        extra={
            "ctx_from_user": (
                message.from_user.id if message.from_user else None
            ),
            "ctx_chat_id": message.chat.id if message.chat else None,
        },
    )

    try:
        if not message.from_user:
            logger.warning("handle_start: no from_user, ignoring")
            return

        name = message.from_user.first_name or "there"

        keyboard = InlineKeyboardMarkup([
            [InlineKeyboardButton("💎 Join Premium", callback_data="menu:premium")],
            [
                InlineKeyboardButton(
                    "📤 Send Content Anonymously", callback_data="menu:submit"
                )
            ],
            [InlineKeyboardButton("🆘 Need Help", callback_data="menu:support")],
        ])

        sent = await _safe_reply(
            message,
            f"👋 Hello, <b>{name}</b>!\n\n"
            "Send me a photo, video, document, or animation and I'll forward it "
            "to our team for review.\n\n"
            "You'll receive a notification once a decision has been made.\n\n"
            "Use the menu below to get started:",
            reply_markup=keyboard,
        )

        if sent:
            logger.info(
                "HANDLER: handle_start replied successfully",
                extra={"ctx_user_id": message.from_user.id},
            )
        else:
            logger.error(
                "HANDLER: handle_start — _safe_reply returned False (all retries failed)",
                extra={"ctx_user_id": message.from_user.id},
            )

    except Exception as e:
        # RC-3 fix: catch everything, always give user feedback
        logger.error(
            "HANDLER: handle_start unhandled exception",
            extra={
                "ctx_user_id": (
                    message.from_user.id if message.from_user else None
                ),
                "ctx_error": str(e),
            },
            exc_info=True,
        )
        await _safe_reply(
            message,
            "⚠️ Something went wrong. Please try again or send /start.",
        )


@Client.on_callback_query(filters.regex(r"^menu:submit$") & filters.private)
async def handle_submit_menu(client: Client, callback: CallbackQuery) -> None:
    """
    RC-7 fix: handles the 'Send Content Anonymously' button.
    This callback had NO registered handler — clicking it silently timed out.
    """
    logger.info(
        "HANDLER: handle_submit_menu entered",
        extra={
            "ctx_from_user": (
                callback.from_user.id if callback.from_user else None
            ),
        },
    )

    try:
        await callback.answer()
        try:
            await callback.message.edit_text(
                "📤 <b>Send Your Content</b>\n\n"
                "Just send your photo, video, document, or animation directly "
                "in this chat now.\n\n"
                "If this is your first submission, you'll be asked to complete "
                "a quick consent confirmation first.\n\n"
                "<i>Go ahead — send your content below.</i>",
                parse_mode=ParseMode.HTML,
            )
        except Exception as e:
            logger.warning(
                "handle_submit_menu: could not edit message",
                extra={"ctx_error": str(e)},
            )
            await callback.answer(
                "Send your photo, video, or file directly in this chat.",
                show_alert=True,
            )

    except Exception as e:
        logger.error(
            "HANDLER: handle_submit_menu unhandled exception",
            extra={"ctx_error": str(e)},
            exc_info=True,
        )
        try:
            await callback.answer("⚠️ Error. Please try again.", show_alert=True)
        except Exception:
            pass


@Client.on_message(
    (filters.photo | filters.video | filters.document | filters.animation)
    & filters.private
)
async def handle_media_submission(client: Client, message: Message) -> None:
    """
    RC-3 fix: full top-level exception boundary.
    RC-5 fix: check_and_gate_creator is wrapped so DB errors give user feedback.
    RC-7 fix: entry logging.
    RC-9 fix: every code path acknowledges the user.
    """
    logger.info(
        "HANDLER: handle_media_submission entered",
        extra={
            "ctx_from_user": (
                message.from_user.id if message.from_user else None
            ),
            "ctx_media_type": str(message.media) if message.media else None,
            "ctx_media_group_id": message.media_group_id,
        },
    )

    try:
        if not message.from_user:
            logger.warning("handle_media_submission: no from_user, ignoring")
            return

        user_id = message.from_user.id

        # RC-5 fix: consent gate wrapped in its own try-except
        try:
            is_verified = await check_and_gate_creator(client, message)
        except Exception as e:
            logger.error(
                "HANDLER: check_and_gate_creator raised unexpectedly",
                extra={"ctx_user_id": user_id, "ctx_error": str(e)},
                exc_info=True,
            )
            await _safe_reply(
                message,
                "⚠️ We couldn't verify your creator status right now. "
                "Please try again in a moment.",
            )
            return

        if not is_verified:
            # Onboarding prompt already sent by check_and_gate_creator
            logger.info(
                "HANDLER: handle_media_submission — creator gate: not verified, "
                "onboarding prompt shown",
                extra={"ctx_user_id": user_id},
            )
            return

        group_id = message.media_group_id

        if not group_id:
            logger.info(
                "Single media submission",
                extra={"ctx_user_id": user_id, "ctx_msg_id": message.id},
            )
            await _submit_for_review(client, [message], user_id)
            return

        async with _album_lock:
            _album_buffer[group_id].append(message)
            existing_task = _album_tasks.get(group_id)
            if existing_task and not existing_task.done():
                existing_task.cancel()
            task = asyncio.create_task(
                _flush_album(group_id, user_id, client),
                name=f"album-flush-{group_id}",
            )
            _album_tasks[group_id] = task

        logger.debug(
            "Album message buffered",
            extra={
                "ctx_user_id": user_id,
                "ctx_group_id": group_id,
                "ctx_msg_id": message.id,
                "ctx_buffer_size": len(_album_buffer[group_id]),
            },
        )

    except Exception as e:
        # RC-3 fix: last resort catch
        logger.error(
            "HANDLER: handle_media_submission unhandled exception",
            extra={
                "ctx_user_id": (
                    message.from_user.id if message.from_user else None
                ),
                "ctx_error": str(e),
            },
            exc_info=True,
        )
        await _safe_reply(
            message,
            "⚠️ An unexpected error occurred processing your submission. "
            "Please try again.",
        )