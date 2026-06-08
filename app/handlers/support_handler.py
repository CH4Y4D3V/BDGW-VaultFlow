# app/handlers/support_handler.py — COMPLETE FIXED FILE
"""
Handles the user-facing support system.
"""
from __future__ import annotations

import asyncio
import random
from datetime import datetime, timezone

from pyrogram import Client, filters
from pyrogram.enums import ParseMode
from pyrogram.errors import FloodWait, RPCError
from pyrogram.types import Message

from app.config import settings
from app.core.database import DatabaseManager
from app.core.permissions import Role, permission_required
from app.services.support_service import get_support_service
from app.utils.logger import get_logger

logger = get_logger(__name__)


# ─── User-Facing Handlers ───────────────────────────────────────────────────

@Client.on_message(filters.command("help") & filters.private)
async def handle_help_command(client: Client, message: Message) -> None:
    """Entry point for /help command."""
    logger.info("help_command_received", extra={"ctx_user_id": message.from_user.id if message.from_user else None})
    await route_to_support_topic(client, message)


@Client.on_message(
    filters.private
    & ~filters.command(["start", "rules", "mystatus", "ping", "help", "takedown", "cancel", "become_creator"])
    & ~filters.bot
)
async def handle_private_message(client: Client, message: Message) -> None:
    """Catch-all for private messages — routes to support topic."""
    if not message.from_user:
        return
    await route_to_support_topic(client, message)


# ─── Admin-Facing Handlers ──────────────────────────────────────────────────

@Client.on_message(
    filters.command("close")
    & filters.group
    & filters.chat(settings.VERIFICATION_GROUP_ID)
)
@permission_required(Role.ADMIN)
async def handle_close_command(client: Client, message: Message) -> None:
    """Handles the /close command from an admin in the verification hub."""
    if not message.reply_to_message:
        await message.reply_text("Please reply to a message in the support thread to close it.")
        return

    db = DatabaseManager.get_db()
    support_service = get_support_service()

    thread_id = getattr(message.reply_to_message, "message_thread_id", None)
    if not thread_id:
        await message.reply_text("Could not determine topic thread ID.")
        return

    # Find user from topic
    topic_doc = await db["user_topics"].find_one({"topic_id": thread_id})
    if not topic_doc:
        await message.reply_text("This does not appear to be a support thread.")
        return

    user_id = topic_doc["user_id"]
    closed_by_name = message.from_user.first_name if message.from_user else "Admin"

    # Close the session
    now = datetime.now(timezone.utc)
    result = await db["support_sessions"].update_one(
        {"user_id": user_id, "status": {"$in": ["PENDING", "ACTIVE"]}},
        {"$set": {"status": "CLOSED", "closed_at": now, "closed_by": message.from_user.id if message.from_user else 0}},
        sort=[("created_at", -1)],
    )

    if result.modified_count == 0:
        await message.reply_text("Could not close support session. It may already be closed.")
        return

    # User-facing notification
    try:
        await client.send_message(
            user_id,
            f"✅ <b>Support Ticket Closed</b>\n\nYour support session has been closed by {closed_by_name}. Thank you!",
            parse_mode=ParseMode.HTML,
        )
    except Exception as e:
        logger.warning("failed_to_send_close_notice_to_user", extra={"ctx_user_id": user_id, "ctx_error": str(e)})

    # Delete user-side support messages
    try:
        from app.services.cleanup_service import get_cleanup_service
        await get_cleanup_service().delete_user_support_history(user_id)
    except Exception as e:
        logger.warning("cleanup_on_close_failed", extra={"ctx_error": str(e)})

    await message.reply_text(
        f"✅ Support session closed for user <code>{user_id}</code>.",
        parse_mode=ParseMode.HTML,
    )

    logger.info("support_session_closed", extra={"ctx_user_id": user_id, "ctx_admin": message.from_user.id if message.from_user else 0})


# ─── Core Routing Logic ─────────────────────────────────────────────────────

async def route_to_support_topic(client: Client, message: Message) -> None:
    """
    Routes a user's message to their dedicated support topic in the hub.
    """
    user = message.from_user
    if not user:
        return

    db = DatabaseManager.get_db()
    support_service = get_support_service()

    # Get or create session with retry on race condition
    session_doc = None
    is_new = False

    for attempt in range(3):
        try:
            existing = await db["support_sessions"].find_one(
                {"user_id": user.id, "status": {"$in": ["PENDING", "ACTIVE"]}},
                sort=[("created_at", -1)],
            )

            if existing:
                session_doc = existing
                is_new = False
                break
            else:
                # Get or create hub topic
                from app.services.topic_manager import get_topic_manager
                topic_manager = get_topic_manager()
                topic_id = await topic_manager.get_or_create_user_topic(
                    client, user.id,
                    full_name=user.full_name or user.first_name or str(user.id),
                    username=user.username,
                )

                if not topic_id:
                    logger.error("support_topic_creation_failed", extra={"ctx_user_id": user.id})
                    await message.reply_text("Sorry, there was an error creating your support session. Please try again.")
                    return

                now = datetime.now(timezone.utc)
                result = await db["support_sessions"].insert_one({
                    "user_id": user.id,
                    "topic_id": topic_id,
                    "status": "PENDING",
                    "created_at": now,
                    "notified_unattended": False,
                })
                session_doc = {"_id": result.inserted_id, "user_id": user.id, "topic_id": topic_id, "status": "PENDING"}
                is_new = True
                break
        except Exception as e:
            if "duplicate key" in str(e).lower() and attempt < 2:
                await asyncio.sleep(random.uniform(0.1, 0.5))
                continue
            logger.exception("failed_to_get_or_create_support_session", extra={"ctx_user_id": user.id})
            return

    if not session_doc:
        logger.error("support_session_creation_exhausted", extra={"ctx_user_id": user.id})
        return

    topic_id = session_doc.get("topic_id")
    if not topic_id:
        logger.error("support_session_missing_topic_id", extra={"ctx_user_id": user.id})
        await message.reply_text("Sorry, there was an error with your support session. Please try again.")
        return

    # Forward the user's message to their topic
    try:
        await client.forward_messages(
            chat_id=settings.VERIFICATION_GROUP_ID,
            from_chat_id=user.id,
            message_ids=message.id,
            message_thread_id=topic_id,
        )
    except Exception as e:
        logger.error(
            "failed_to_forward_support_message",
            extra={"ctx_user_id": user.id, "ctx_topic_id": topic_id, "ctx_error": str(e)},
        )
        await message.reply_text("Sorry, there was an error processing your message. Please try again.")
        return

    # Track for cleanup
    try:
        from app.services.cleanup_service import get_cleanup_service
        await get_cleanup_service().log_support_message(user_id=user.id, message_id=message.id)
    except Exception as e:
        logger.warning("support_cleanup_log_failed", extra={"ctx_error": str(e)})

    # If new session, post the card for admins
    if is_new:
        try:
            card_text = await support_service.build_user_support_card(
                db=db, user_id=user.id, from_user=user, message=message
            )
            from app.services.support_service import build_accept_markup
            await client.send_message(
                chat_id=settings.VERIFICATION_GROUP_ID,
                text=card_text,
                reply_markup=build_accept_markup(user.id),
                message_thread_id=topic_id,
                parse_mode=ParseMode.HTML,
            )
        except Exception as e:
            logger.error("failed_to_post_support_card", extra={"ctx_user_id": user.id, "ctx_error": str(e)})

        # Log activity
        try:
            from app.services.activity_service import ActivityService
            activity_service = ActivityService(db)
            await activity_service.log_support_session_start(user.id, str(session_doc.get("_id", "")))
        except Exception as e:
            logger.warning("support_activity_log_failed", extra={"ctx_error": str(e)})

    # Reply to user
    try:
        await message.reply_text("✅ Your message has been sent to the support team. They will reply here.")
    except Exception as e:
        logger.warning("support_ack_failed", extra={"ctx_error": str(e)})


# ─── Callback: Accept Support ───────────────────────────────────────────────

@Client.on_callback_query(filters.regex(r"^support_accept:(\d+)$"))
async def handle_support_accept(client, callback_query) -> None:
    """Admin accepts a support ticket."""
    from app.core.permissions import is_admin_or_owner
    if not callback_query.from_user or not await is_admin_or_owner(callback_query.from_user.id):
        await callback_query.answer("Unauthorized.", show_alert=True)
        return

    user_id = int(callback_query.matches[0].group(1))
    admin_id = callback_query.from_user.id
    admin_name = callback_query.from_user.first_name or f"Admin {admin_id}"

    db = DatabaseManager.get_db()
    result = await db["support_sessions"].update_one(
        {"user_id": user_id, "status": "PENDING"},
        {"$set": {"status": "ACTIVE", "accepted_by": admin_id, "accepted_at": datetime.now(timezone.utc)}},
        sort=[("created_at", -1)],
    )

    if result.modified_count == 0:
        await callback_query.answer("Session not found or already accepted.", show_alert=True)
        return

    await callback_query.answer(f"Support session accepted.")

    try:
        await callback_query.message.edit_reply_markup(None)
        await callback_query.message.reply(
            f"✅ <b>Accepted by {admin_name}</b>\n\nYou are now connected to the user.",
            parse_mode=ParseMode.HTML,
        )
    except Exception:
        pass

    try:
        await client.send_message(
            user_id,
            f"✅ <b>Admin Connected</b>\n\nYou are now connected to a support agent. You can send messages directly.",
            parse_mode=ParseMode.HTML,
        )
    except Exception as e:
        logger.warning("support_accept_notify_user_failed", extra={"ctx_user_id": user_id, "ctx_error": str(e)})