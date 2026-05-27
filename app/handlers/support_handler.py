from __future__ import annotations

"""
Support handler — routes menu:support callbacks and manages the support ticket lifecycle.

RC-4 FIX: `~filters.command([])` with an empty list never matches any command
(empty command set intersection is always False), so its inverse matched ALL
messages. Replaced with an explicit `~filters.regex(r'^/')` to correctly
exclude slash commands.

RC-6 FIX: `handle_hub_message_support` was duplicating the admin → user
routing that `topic_router.py::route_admin_reply_to_user` already performs.
For support topics, users were receiving every admin reply TWICE. This handler
now only persists the support message to the DB — routing is handled exclusively
by topic_router.py.
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

_support_repo = SupportRepository()


async def _safe_reply(
    message: Message,
    text: str,
    parse_mode: ParseMode = ParseMode.HTML,
) -> None:
    """
    RC-2 fix: catches ALL exception types, not just FloodWait/RPCError.
    """
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
            # RC-2 fix: catch everything else
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
    
    # ── Anti-Spam / Debounce ──
    redis = get_redis()
    spam_key = f"menu:spam:{user_id}"
    if await redis.exists(spam_key):
        await callback.answer("Slow down! Processing...", show_alert=False)
        return
    await redis.set(spam_key, "1", ex=1)
    
    logger.info(
        "HANDLER: handle_support_menu entered",
        extra={
            "ctx_from_user": user_id,
        },
    )

    try:
        await callback.message.edit_text(
            "🆘 <b>Support</b>\n\n"
            "Send your message and we'll connect you with our support team.\n\n"
            "<i>Just type your question or issue below.</i>",
            parse_mode=ParseMode.HTML,
        )
        await callback.answer()

        user_id = callback.from_user.id
        topic_service = get_topic_service()
        try:
            await topic_service.get_or_create_user_topic(
                client, user_id, TOPIC_SUPPORT
            )
        except Exception as e:
            logger.warning(
                "handle_support_menu: pre-create topic failed — will retry on first message",
                extra={"ctx_user_id": user_id, "ctx_error": str(e)},
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
            await callback.answer(
                "⚠️ Could not open support. Please try again.", show_alert=True
            )
        except Exception:
            pass


# ── RC-4 FIX: Private message routing — correct command exclusion ─────────────
#
# BEFORE (buggy):
#   @Client.on_message(filters.private & ~filters.command([]))
#
# WHY IT WAS BROKEN:
#   filters.command([]) checks if message.text starts with "/" AND the command
#   name is in the provided list. With an empty list [], no command name can
#   ever match — so filters.command([]) NEVER fires. Its inverse (~) therefore
#   ALWAYS fires, meaning this handler ran on ALL private messages including
#   /start, /ping, etc.
#
# FIX:
#   Use ~filters.regex(r"^/") to explicitly exclude slash-prefixed messages.
#   This correctly targets only non-command private messages.

@Client.on_message(filters.private & ~filters.regex(r"^/"))
async def handle_private_message_support(client: Client, message: Message) -> None:
    """
    Route non-command private messages to support if user has an active topic.

    RC-4 fix: filter now correctly excludes commands using regex instead of
    the broken ~filters.command([]) which matched everything.

    Returns early (no-op) for users without an existing support topic.
    For users WITH a support topic, their private text/media is routed there.
    """
    if not message.from_user:
        return

    # Also skip media submissions — those are handled by handle_media_submission
    # in submission_handler.py. We only want text/non-media messages here.
    if message.photo or message.video or message.document or message.animation:
        logger.debug(
            "handle_private_message_support: skipping media (handled by submission_handler)",
            extra={"ctx_from_user": message.from_user.id},
        )
        return

    user_id = message.from_user.id

    try:
        topic_service = get_topic_service()
        topic_id = await topic_service.get_user_topic_id(user_id, TOPIC_SUPPORT)

        if topic_id is None:
            # No support topic — this is not a support conversation
            return

        logger.info(
            "HANDLER: handle_private_message_support routing to support topic",
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


# ── RC-6 FIX: Verification hub — admin replies in support topics ───────────────
#
# BEFORE (buggy):
#   This handler called support_service.handle_admin_reply() which internally
#   calls _copy_message_safe() to deliver the message to the user.
#   topic_router.py::route_admin_reply_to_user ALSO delivers the message to
#   the user for ALL topic types (including support).
#   Result: user received every admin support reply TWICE.
#
# FIX:
#   This handler now ONLY persists the admin reply to the support_messages
#   collection for audit/history purposes. It does NOT deliver to the user.
#   Delivery is handled exclusively by topic_router.py::route_admin_reply_to_user.

@Client.on_message(filters.chat(settings.VERIFICATION_GROUP_ID))
async def handle_hub_support_message_persist(
    client: Client, message: Message
) -> None:
    """
    Persist admin replies in support topics to the support_messages collection.

    RC-6 fix: DOES NOT route the message to the user. That is done by
    topic_router.py::route_admin_reply_to_user which fires for all topic types.
    This handler's sole responsibility is the DB persistence audit record.
    """
    try:
        thread_id = (
            getattr(message, "message_thread_id", None)
            or getattr(message, "reply_to_top_message_id", None)
        )
        if not thread_id:
            return

        if not message.from_user or message.from_user.is_bot:
            return

        # Only skip the bot's own moderation cards
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

        # Only persist for support topics — other topic types don't use this store
        if not topic_doc or topic_doc.get("topic_type") != TOPIC_SUPPORT:
            return

        user_id: int = topic_doc["user_id"]

        # RC-6 fix: DB persistence only — NO copy_message / routing here
        try:
            await _support_repo.save_message({
                "user_id": user_id,
                "topic_id": thread_id,
                "user_message_id": None,
                "hub_message_id": message.id,
                "direction": "admin_to_user",
                "created_at": datetime.now(timezone.utc),
            })
            logger.debug(
                "handle_hub_support_message_persist: saved admin reply record",
                extra={
                    "ctx_user_id": user_id,
                    "ctx_topic_id": thread_id,
                    "ctx_msg_id": message.id,
                    "ctx_admin": message.from_user.id,
                },
            )
        except Exception as e:
            logger.warning(
                "handle_hub_support_message_persist: DB save failed (non-fatal)",
                extra={"ctx_error": str(e)},
            )

    except Exception as e:
        logger.error(
            "HANDLER: handle_hub_support_message_persist unhandled exception",
            extra={"ctx_error": str(e)},
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
        extra={
            "ctx_from_user": (
                message.from_user.id if message.from_user else None
            ),
        },
    )

    try:
        if not message.from_user or not is_support_admin(message.from_user.id):
            return

        thread_id = (
            getattr(message, "message_thread_id", None)
            or getattr(message, "reply_to_top_message_id", None)
        )
        if not thread_id:
            await message.reply_text(
                "❌ This command must be used inside a topic thread."
            )
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