from __future__ import annotations

import asyncio
import time
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

# FIX 17: Simple in-memory per-user rate limit.
# Max 10 submissions per user per 60 seconds.
# Intentionally single-process / in-memory to match the existing architecture.
_submission_rate: dict[int, list[float]] = {}
_RATE_LIMIT_MAX = 10
_RATE_LIMIT_WINDOW = 60.0


# ── RC-2 FIX: _safe_reply catches ALL exception types ────────────────────────

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


# ── Payment state check helper ────────────────────────────────────────────────

async def _has_active_payment_session(user_id: int) -> bool:
    """
    Middleware conflict fix: check whether the user is mid-payment before
    routing media to the submission pipeline.

    Replicates the same DB lookup used by payment_handler._get_payment_state()
    so no circular import is needed. Returns True if a payment session is active.
    Never raises — returns False on any DB error (fail-open is safer here).
    """
    try:
        from app.core.database import DatabaseManager
        db = DatabaseManager.get_db()
        doc = await db["bot_config"].find_one({"key": f"payment_state:{user_id}"})
        return doc is not None
    except Exception as e:
        logger.warning(
            "_has_active_payment_session: DB error, defaulting to False",
            extra={"ctx_user_id": user_id, "ctx_error": str(e)},
        )
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


@Client.on_callback_query(filters.regex(r"^menu:(submit|anonymous)$") & filters.private)
async def handle_submit_menu(client: Client, callback: CallbackQuery) -> None:
    """
    RC-7 fix: handles the 'Send Content Anonymously' button.
    """
    action = callback.data.split(":")[1]
    logger.info(
        "HANDLER: handle_submit_menu entered",
        extra={
            "ctx_from_user": (
                callback.from_user.id if callback.from_user else None
            ),
            "ctx_action": action,
        },
    )

    try:
        await callback.answer()
        
        if action == "anonymous":
            text = (
                "🕵️ <b>Anonymous Submission</b>\n\n"
                "Your identity will be completely hidden from the public channel. "
                "Only our moderation team will see the source for verification purposes.\n\n"
                "<i>Just send your content directly in this chat now.</i>"
            )
        else:
            text = (
                "📤 <b>Send Your Content</b>\n\n"
                "Just send your photo, video, document, or animation directly "
                "in this chat now.\n\n"
                "If this is your first submission, you'll be asked to complete "
                "a quick consent confirmation first.\n\n"
                "<i>Go ahead — send your content below.</i>"
            )

        try:
            await callback.message.edit_text(
                text,
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
    Middleware conflict fix: skip if user is in an active payment flow.
    FIX 17: per-user rate limiting (max 10 submissions per 60 seconds).
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

        # ── Middleware conflict fix: skip if user is in active payment flow ──
        # payment_handler.handle_payment_proof_capture() owns media messages
        # while the user is mid-payment. Without this guard, both handlers fire
        # and the photo is incorrectly routed to both the submission pipeline and
        # the payment proof capture path.
        try:
            payment_active = await _has_active_payment_session(user_id)
        except Exception as e:
            logger.warning(
                "handle_media_submission: payment state check failed, proceeding",
                extra={"ctx_user_id": user_id, "ctx_error": str(e)},
            )
            payment_active = False

        if payment_active:
            logger.info(
                "HANDLER: handle_media_submission — user in payment flow, skipping",
                extra={"ctx_user_id": user_id},
            )
            return

        # ── FIX 17: Per-user rate limiting ────────────────────────────────────
        # Simple in-memory sliding window: max 10 submissions per user per 60s.
        # Applied after the payment check (payment messages are already excluded)
        # and before the consent gate so rate-limited users get a fast reply
        # without hitting MongoDB.
        now = time.monotonic()
        timestamps = _submission_rate.get(user_id, [])
        timestamps = [t for t in timestamps if now - t < _RATE_LIMIT_WINDOW]
        if len(timestamps) >= _RATE_LIMIT_MAX:
            logger.warning(
                "HANDLER: handle_media_submission — rate limit hit",
                extra={"ctx_user_id": user_id, "ctx_count": len(timestamps)},
            )
            await message.reply(
                "You're submitting too quickly. Please wait before sending more content."
            )
            return
        timestamps.append(now)
        _submission_rate[user_id] = timestamps

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