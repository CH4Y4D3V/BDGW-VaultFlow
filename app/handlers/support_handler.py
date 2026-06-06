from __future__ import annotations

import asyncio
from datetime import datetime, timezone
from typing import Optional

from pyrogram import Client, filters
from pyrogram.enums import ParseMode
from pyrogram.types import (
    CallbackQuery,
    Message,
    InlineKeyboardButton,
    InlineKeyboardMarkup,
)

from app.config import settings
from app.core.database import DatabaseManager
from app.services.support_service import get_support_service, build_accept_markup
from app.services.topic_manager import get_topic_manager
from app.utils.logger import get_logger

logger = get_logger(__name__)

# ── Helpers ───────────────────────────────────────────────────────────────

async def _send_to_user_topic(client: Client, user_id: int, text: str, reply_markup=None):
    """Utility to post a message to the user's permanent hub topic."""
    try:
        topic_mgr = get_topic_manager()
        topic_id = await topic_mgr.get_or_create_user_topic(client, user_id)
        await client.send_message(
            chat_id=settings.VERIFICATION_GROUP_ID,
            text=text,
            message_thread_id=topic_id,
            reply_markup=reply_markup,
            parse_mode=ParseMode.HTML,
        )
    except Exception as exc:
        logger.error(
            "support_handler_topic_post_failed",
            extra={"ctx_user_id": user_id, "ctx_error": str(exc)},
        )

# ── Handlers ──────────────────────────────────────────────────────────────

@Client.on_message(filters.command("help") & filters.private)
async def handle_help_command(client: Client, message: Message) -> None:
    """
    User-side entry point: /help in private chat.
    
    Section 15.1: Opens a new support session or re-activates an existing topic.
    """
    user_id = message.from_user.id
    first_name = message.from_user.first_name or "Creator"
    
    logger.info("handle_help_command", extra={"ctx_user_id": user_id})

    service = get_support_service()
    db = DatabaseManager.get_db()
    
    # Check for existing non-closed session
    existing = await db["support_sessions"].find_one(
        {"user_id": user_id, "status": {"$in": ["PENDING", "ACTIVE"]}}
    )
    
    if existing:
        await message.reply_text(
            "⏳ You already have an open support request.\n"
            "An admin will be with you shortly. "
            "You can type your message here now."
        )
        return

    # Create new PENDING session
    session_id = await service.create_session(client, user_id)
    
    # Notify User
    await message.reply_text(
        "🆘 <b>Support Request Opened</b>\n\n"
        "Your request has been sent to our moderators. "
        "Please describe your issue below.\n\n"
        "<i>An admin will join this chat shortly.</i>",
        parse_mode=ParseMode.HTML
    )
    
    # Send Request Card to Hub Topic (Section 15.2)
    card_text = await service.build_user_support_card(db, user_id, message.from_user, message)
    markup = build_accept_markup(user_id)
    
    await _send_to_user_topic(client, user_id, card_text, reply_markup=markup)

@Client.on_callback_query(filters.regex(r"^support_accept:(\d+)$"))
async def handle_accept_callback(client: Client, callback_query: CallbackQuery) -> None:
    """
    Admin-side: Accept a support request.
    
    Section 15.3: Implements ownership lock (A-04/B-04).
    """
    user_id = int(callback_query.matches[0].group(1))
    admin_id = callback_query.from_user.id
    admin_name = callback_query.from_user.first_name or "Admin"
    
    db = DatabaseManager.get_db()
    
    # Atomic Ownership Lock Check
    # We use find_one_and_update to ensure only one admin can claim 'PENDING'
    result = await db["support_sessions"].find_one_and_update(
        {"user_id": user_id, "status": "PENDING"},
        {
            "$set": {
                "status": "ACTIVE",
                "accepted_by": admin_id,
                "accepted_at": datetime.now(timezone.utc),
            }
        },
        return_document=True
    )
    
    if not result:
        # Check if it was already accepted by someone else
        current = await db["support_sessions"].find_one({"user_id": user_id})
        if current and current.get("status") == "ACTIVE":
            other_admin = current.get("accepted_by", "Another admin")
            await callback_query.answer(f"❌ This request was already accepted by ID: {other_admin}", show_alert=True)
            # Update the card to reflect it's taken
            try:
                await callback_query.message.edit_text(
                    callback_query.message.text + f"\n\n✅ <b>Accepted by Admin {other_admin}</b>",
                    reply_markup=None, # Remove button
                    parse_mode=ParseMode.HTML
                )
            except Exception:
                pass
        else:
            await callback_query.answer("❌ This request is no longer valid.", show_alert=True)
        return

    # Success: Notify Admin
    await callback_query.answer("✅ Support Request Accepted!")
    await callback_query.message.edit_text(
        callback_query.message.text.html + f"\n\n✅ <b>Accepted by {admin_name} ({admin_id})</b>",
        reply_markup=None,
        parse_mode=ParseMode.HTML
    )

    # Notify User in DM
    await client.send_message(
        chat_id=user_id,
        text=f"✅ <b>Admin {admin_name}</b> has joined the support session.\n"
             "How can we help you today?",
        parse_mode=ParseMode.HTML
    )
    
    # Dual Audit Log (A-15/B-03/Section 9.4)
    from app.services.support_service import send_admin_log_entry
    await send_admin_log_entry(
        client=client,
        action_type="SUPPORT ACCEPTED",
        admin_user_id=admin_id,
        admin_name=admin_name,
        target_user_id=user_id,
        target_name=None, # Will fetch in service
        target_username=None,
        detail=f"Admin claimed support session for user {user_id}"
    )

@Client.on_message(filters.command("close") & filters.group & filters.chat(settings.VERIFICATION_GROUP_ID))
async def handle_close_command(client: Client, message: Message) -> None:
    """
    Admin-side: Close a support session.
    
    Section 15.5: Terminates bridge and triggers user-side message deletion (Section 20).
    """
    if not message.reply_to_message:
        # In a forum, we can also check the thread_id
        pass
    
    # In this implementation, we rely on the thread (topic_id)
    topic_id = message.message_thread_id
    if not topic_id:
        return

    db = DatabaseManager.get_db()
    session = await db["support_sessions"].find_one({"topic_id": topic_id, "status": "ACTIVE"})
    
    if not session:
        await message.reply_text("❌ No active support session found in this topic.")
        return

    user_id = session["user_id"]
    admin_id = message.from_user.id
    
    # Update DB
    await db["support_sessions"].update_one(
        {"_id": session["_id"]},
        {
            "$set": {
                "status": "CLOSED",
                "closed_at": datetime.now(timezone.utc),
                "closed_by": admin_id
            }
        }
    )

    # Notify Admin topic
    await message.reply_text("🔒 <b>Support session closed.</b>\nUser messages will be deleted shortly.", parse_mode=ParseMode.HTML)

    # Notify User
    await client.send_message(
        chat_id=user_id,
        text="🔒 <b>Support Session Closed</b>\n\n"
             "Thank you for contacting support. This chat history will be cleared.",
        parse_mode=ParseMode.HTML
    )

    # Trigger User-side cleanup (Section 20)
    try:
        from app.services.cleanup_service import get_cleanup_service
        cleanup = get_cleanup_service()
        await cleanup.trigger_user_cleanup(user_id)
    except Exception as exc:
        logger.warning("support_cleanup_trigger_failed", extra={"ctx_user_id": user_id, "ctx_error": str(exc)})

    # Audit
    from app.services.support_service import send_admin_log_entry
    await send_admin_log_entry(
        client=client,
        action_type="SUPPORT CLOSED",
        admin_user_id=admin_id,
        admin_name=message.from_user.first_name,
        target_user_id=user_id,
        target_name=None,
        target_username=None,
        detail=f"Session closed by admin"
    )

async def handle_support_entry(client: Client, callback_query: CallbackQuery) -> None:
    """Handles the 'menu:support' callback from user dashboard."""
    # Mimic /help behavior
    # We create a fake message object to reuse handle_help_command
    class FakeMessage:
        def __init__(self, query: CallbackQuery):
            self.from_user = query.from_user
            self.chat = query.message.chat
        async def reply_text(self, text, **kwargs):
            return await client.send_message(self.from_user.id, text, **kwargs)

    await handle_help_command(client, FakeMessage(callback_query))
    await callback_query.answer()

async def route_support_message(client: Client, message: Message) -> None:
    """
    Entry point for topic_router to handle support messages.
    """
    # This is handled by SupportService.handle_user_message
    # which is called from user_handler / generic message handler
    pass
