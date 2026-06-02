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
from app.services.topic_manager import get_topic_manager, TOPIC_SUPPORT
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


from app.ui.support_cards import build_support_welcome_card, build_ticket_created_card
from app.ui.common import build_back_button

# ── Callback: menu:support ────────────────────────────────────────────────────

@Client.on_callback_query(filters.regex(r"^menu:support$"))
async def handle_support_menu(client: Client, callback: CallbackQuery) -> None:
    if not callback.message or not callback.message.chat:
        return

    user_id = callback.from_user.id if callback.from_user else 0

    # ── Anti-Spam / Debounce ──
    redis = get_redis()
    spam_key = f"menu:spam:{user_id}"
    if await redis.exists(spam_key):
        await callback.answer("Slow down! Processing...", show_alert=False)
        return
    await redis.set(spam_key, "1", ex=1)

    await callback.answer()

    logger.info(
        "HANDLER: handle_support_menu entered",
        extra={
            "ctx_from_user": user_id,
        },
    )

    try:
        text, reply_markup = build_support_welcome_card()
        sent_msg = await callback.message.edit_text(
            text,
            reply_markup=reply_markup,
            parse_mode=ParseMode.HTML,
        )

        # ── SYSTEM 20: CLEANUP ──
        try:
            from app.services.cleanup_service import get_cleanup_service
            await get_cleanup_service().log_message(user_id, sent_msg.id, text, category="general")
        except Exception:
            pass

        await callback.answer()

        user_id = callback.from_user.id
        topic_manager = get_topic_manager()
        try:
            await topic_manager.get_or_create_user_topic(
                client, user_id, TOPIC_SUPPORT
            )
        except Exception as e:
            logger.exception(
                "forum_topic_creation_failed",
                extra={"ctx_user_id": user_id, "ctx_error": str(e)},
            )
            # Notify user of failure
            await callback.message.edit_text(
                "Support is temporarily unavailable. Please try again in a few minutes.",
                parse_mode=ParseMode.HTML,
            )
            return

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
        except Exception as e:
            logger.exception(
                "support_menu_error_answer_failed",
                extra={"ctx_error": str(e)},
            )
            pass


# ── RC-4 FIX: Private message routing — correct command exclusion ─────────────

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

    user_id = message.from_user.id

    try:
        topic_manager = get_topic_manager()
        topic_id = await topic_manager.get_user_topic_id(user_id, TOPIC_SUPPORT)

        if topic_id is None:
            # No support topic — this is not a support conversation
            return

        logger.info(
            "HANDLER: handle_private_message_support routing to support topic",
            extra={"ctx_user_id": user_id, "ctx_topic_id": topic_id},
        )

        support_service = get_support_service()
        is_first = await support_service.handle_user_message(client, message)

        if is_first:
            ticket_id = f"T-{user_id}-{topic_id}"
            text = build_ticket_created_card(ticket_id)
            await message.reply_text(text, parse_mode=ParseMode.HTML)

    except Exception as e:
        logger.error(
            "HANDLER: handle_private_message_support unhandled exception",
            extra={"ctx_user_id": user_id, "ctx_error": str(e)},
            exc_info=True,
        )


# RC-6 FIX: Verification hub — admin replies in support topics
#
# This handler previously persisted admin replies to the support_messages
# collection. However, topic_router.py now handles both routing and persistence
# for all topic types to ensure exactly-once delivery and consistent audit logs.
# This handler is removed to centralize routing logic.


# ── Admin callbacks: support:reply, resolve, close ────────────────────────────

@Client.on_callback_query(filters.regex(r"^support:accept:(?P<uid>\d+)$"))
async def handle_support_accept_callback(client: Client, callback: CallbackQuery) -> None:
    """Updates ticket status to accepted and notifies the user."""
    user_id = int(callback.matches[0].group("uid"))
    admin_name = callback.from_user.first_name

    from app.repositories.support_repository import SupportRepository
    repo = SupportRepository()

    # 1. Update DB
    success = await repo.update_ticket_status(user_id, TOPIC_SUPPORT, "accepted")
    if not success:
        await callback.answer("❌ Failed to accept ticket. Already accepted or closed?", show_alert=True)
        return

    # ── SYSTEM 18: AUDIT LOG ──
    from app.services.audit_service import get_audit
    await get_audit().log(
        action="support_accept",
        performed_by=callback.from_user.id,
        target_user_id=user_id,
        details={"admin_name": admin_name}
    )

    # 2. Notify user
    try:
        await client.send_message(
            chat_id=user_id,
            text=(
                f"✅ <b>Admin {admin_name} has accepted your support request.</b>\n\n"
                "You can now chat directly with the support team here."
            ),
            parse_mode=ParseMode.HTML,
        )
    except Exception:
        pass

    # 3. Update admin card
    try:
        import re
        ticket_id_match = re.search(r"Ticket ID: ([\w-]+)", callback.message.text or "")
        ticket_id = ticket_id_match.group(1) if ticket_id_match else "unknown"

        from app.ui.support_cards import build_admin_support_actions
        await callback.message.edit_reply_markup(
            reply_markup=build_admin_support_actions(ticket_id, user_id, status="accepted")
        )

        await callback.message.edit_text(
            callback.message.text.replace("⏳ Awaiting Response", f"✅ <b>Accepted by {admin_name}</b>"),
            reply_markup=build_admin_support_actions(ticket_id, user_id, status="accepted"),
            parse_mode=ParseMode.HTML
        )
    except Exception as e:
        logger.warning("Failed to update card after accept", extra={"ctx_error": str(e)})

    await callback.answer("Ticket accepted.")


@Client.on_callback_query(filters.regex(r"^support:reply:(?P<uid>\d+)$"))
async def handle_support_reply_callback(client: Client, callback: CallbackQuery) -> None:
    """Alerts the admin that they should reply directly in the topic."""
    await callback.answer(
        "💬 Type your reply directly in this topic to send it to the user.",
        show_alert=True
    )


@Client.on_callback_query(filters.regex(r"^support:(resolve|close):(?P<tid>.+)$"))
async def handle_support_closure_callback(client: Client, callback: CallbackQuery) -> None:
    """Closes the ticket and notifies the user."""
    action = callback.matches[0].group(1)
    tid = callback.matches[0].group("tid")

    # Extract user_id from the card text or via topic_manager
    import re
    user_id_match = re.search(r"User ID: (\d+)", callback.message.text or "")
    user_id = int(user_id_match.group(1)) if user_id_match else None

    if not user_id:
        # Fallback to topic mapping
        thread_id = (
            getattr(callback.message, "message_thread_id", None)
            or getattr(callback.message, "reply_to_top_message_id", None)
        )
        if thread_id:
            topic_manager = get_topic_manager()
            topic_doc = await topic_manager.get_user_by_topic(thread_id)
            if topic_doc:
                user_id = topic_doc["user_id"]

    if not user_id:
        await callback.answer("❌ Could not identify user to notify.", show_alert=True)
        return

    admin_name = callback.from_user.first_name or "Admin"

    # Notify user
    try:
        status_text = "resolved" if action == "resolve" else "closed"

        # ── SYSTEM 18: AUDIT LOG ──
        from app.services.audit_service import get_audit
        await get_audit().log(
            action=f"support_{action}",
            performed_by=callback.from_user.id,
            target_user_id=user_id,
            details={"ticket_id": tid}
        )

        await callback.answer(f"✅ Ticket {status_text}! (Admin: {admin_name})", show_alert=True)

        await client.send_message(
            chat_id=user_id,
            text=(
                f"✅ <b>Your support ticket has been {status_text}.</b>\n\n"
                "If you have further questions, start a new conversation via /start."
            ),
            parse_mode=ParseMode.HTML,
        )

        # ── SYSTEM 15.5: USER-SIDE DELETION (GAP 7 FIX) ──
        try:
            from app.services.cleanup_service import get_cleanup_service
            await get_cleanup_service().delete_user_support_history(user_id)
        except Exception:
            pass
    except Exception as e:
        logger.warning(
            "handle_support_closure_callback: could not notify user",
            extra={"ctx_user_id": user_id, "ctx_error": str(e)},
        )

    # Close topic
    try:
        thread_id = (
            getattr(callback.message, "message_thread_id", None)
            or getattr(callback.message, "reply_to_top_message_id", None)
        )
        if thread_id:
            await client.close_forum_topic(
                chat_id=settings.VERIFICATION_GROUP_ID,
                message_thread_id=thread_id,
            )
    except Exception as e:
        logger.warning(
            "handle_support_closure_callback: could not close forum topic",
            extra={"ctx_error": str(e)},
        )

    # Update admin card
    try:
        await callback.message.edit_text(
            callback.message.text + f"\n\n✅ <b>Ticket {action.capitalize()}d</b> by {callback.from_user.first_name}",
            reply_markup=None,
            parse_mode=ParseMode.HTML
        )
    except Exception:
        pass

    await callback.answer(f"Ticket {action}d.")


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

        topic_manager = get_topic_manager()
        topic_doc = await topic_manager.get_user_by_topic(thread_id)
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