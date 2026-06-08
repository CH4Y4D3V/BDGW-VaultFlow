"""
app/handlers/support_handler.py
-------------------------------
Handles the user-facing support system, including /help, direct messages,
and the admin-side /close command.
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
from app.services.activity_service import ActivityService
from app.services.support_service import SupportService
from app.ui.support_cards import (
    format_admin_closed_ticket_card,
    format_new_support_request_card,
    format_user_closed_ticket_card,
)
from app.utils.logger import get_logger

logger = get_logger(__name__)


# ─── User-Facing Handlers ───────────────────────────────────────────────────

@Client.on_message(filters.command("help") & filters.private)
async def handle_help_command(client: Client, message: Message) -> None:
    """Entry point for /help command. Routes to the main support handler."""
    logger.info("help_command_received", extra={"ctx_user_id": message.from_user.id})
    await route_to_support_topic(client, message)


@Client.on_message(
    filters.private
    & ~filters.command(["start", "rules", "mystatus", "ping", "help"])
    & ~filters.bot
)
async def handle_private_message(client: Client, message: Message) -> None:
    """Handles any private message that is not another command."""
    # This acts as the main entry point for users initiating a support chat.
    await route_to_support_topic(client, message)


# ─── Admin-Facing Handlers ──────────────────────────────────────────────────

@Client.on_message(
    filters.command("close")
    & filters.group
    & filters.chat(settings.VERIFICATION_GROUP_ID)
)
@permission_required(Role.MODERATOR)
async def handle_close_command(client: Client, message: Message) -> None:
    """Handles the /close command from an admin in the verification hub."""
    if not message.reply_to_message:
        await message.reply_text("Please reply to a message in the support thread to close it.")
        return

    db = DatabaseManager.get_db()
    support_service = SupportService(db)

    user_id = await support_service.get_user_id_from_topic(message.chat.id, message.reply_to_message.message_thread_id)
    if not user_id:
        await message.reply_text("This does not appear to be a support thread.")
        return

    logger.info(
        "close_command_received",
        extra={
            "ctx_admin_id": message.from_user.id,
            "ctx_user_id": user_id,
            "ctx_topic_id": message.reply_to_message.message_thread_id,
        },
    )

    closed_by_name = message.from_user.first_name or "Admin"
    success = await support_service.close_session_by_topic(
        message.reply_to_message.message_thread_id,
        closed_by_name,
        closed_by_id=message.from_user.id,
    )

    if not success:
        await message.reply_text("Could not close support session. It may already be closed.")
        return

    # User-facing notification
    try:
        user_card = format_user_closed_ticket_card(closed_by_name)
        await client.send_message(user_id, user_card, parse_mode=ParseMode.HTML)
    except Exception as e:
        logger.warning(
            "failed_to_send_close_notice_to_user",
            extra={"ctx_user_id": user_id, "ctx_error": str(e)},
        )

    # Admin-facing notification
    admin_card = format_admin_closed_ticket_card(
        user_id,
        (await client.get_users(user_id)).first_name,
        closed_by_name,
    )
    await message.reply_text(admin_card, parse_mode=ParseMode.HTML)


# ─── Core Routing Logic ─────────────────────────────────────────────────────

async def route_to_support_topic(client: Client, message: Message) -> None:
    """
    Routes a user's message to their dedicated support topic in the hub.

    1.  Retrieves or creates a support session and its corresponding topic.
    2.  Forwards the user's message to the topic.
    3.  If it's a new session, posts a "New Request" card to the topic.
    """
    user = message.from_user
    if not user:
        return

    db = DatabaseManager.get_db()
    support_service = SupportService(db)

    # REG-05 (D-02) FIX: Retry on unique index violation race condition
    for attempt in range(3):
        try:
            session, is_new = await support_service.get_or_create_session(user.id, user.full_name)
            break
        except Exception as e:
            if "duplicate key error" in str(e).lower() and attempt < 2:
                await asyncio.sleep(random.uniform(0.1, 0.5))
                continue
            logger.exception(
                "failed_to_get_or_create_support_session",
                extra={"ctx_user_id": user.id},
            )
            return
    else: # This block runs if the loop completes without breaking
        logger.error("failed_to_get_or_create_support_session_after_retries", extra={"ctx_user_id": user.id})
        return

    # Forward the user's message to their topic
    try:
        await client.forward_messages(
            chat_id=settings.VERIFICATION_GROUP_ID,
            from_chat_id=user.id,
            message_ids=message.id,
            message_thread_id=session.topic_id,
        )
    except Exception as e:
        logger.error(
            "failed_to_forward_support_message",
            extra={
                "ctx_user_id": user.id,
                "ctx_topic_id": session.topic_id,
                "ctx_error": str(e),
            },
        )
        await message.reply_text("Sorry, there was an error processing your message. Please try again.")
        return

    # If it's a brand new session, post the "New Request" card for admins.
    if is_new:
        card = format_new_support_request_card(user.id, user.full_name, user.username)
        try:
            await client.send_message(
                chat_id=settings.VERIFICATION_GROUP_ID,
                text=card,
                message_thread_id=session.topic_id,
                parse_mode=ParseMode.HTML,
            )
            activity_service = ActivityService(db)
            await activity_service.log_support_session_start(user.id, session.id)

        except Exception as e:
            logger.error(
                "failed_to_post_new_support_request_card",
                extra={
                    "ctx_user_id": user.id,
                    "ctx_topic_id": session.topic_id,
                    "ctx_error": str(e),
                },
            )

    # Respond to the user so they know their message was received.
    await message.reply_text("✅ Your message has been sent to the support team. They will reply here.")
