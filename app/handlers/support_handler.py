from __future__ import annotations

import logging
from html import escape
from datetime import datetime, timezone
from typing import Optional, Dict

from pyrogram import Client, filters
from pyrogram.types import Message, CallbackQuery
from pyrogram.enums import ParseMode
from pyrogram.errors import PeerIdInvalid

from app.config import settings
from app.core.database import DatabaseManager
from app.services.support_service import get_support_service, build_accept_markup
from app.services.topic_manager import get_topic_manager
from app.utils.logger import get_logger

log = get_logger(__name__)


async def is_session_active(db, user_id: int) -> bool:
    """Checks if a user has an active/accepted topic session."""
    doc = await db["user_topics"].find_one({"user_id": user_id})
    if not doc:
        return False
    
    status = doc.get("status")
    # If the topic is closed, it's inactive. 
    # In the new system, topics are permanent but 'sessions' can be closed.
    if status == "closed":
        return False
            
    return True

@Client.on_message(filters.command(["admin", "support", "help"]) & filters.private)
async def cmd_support(client: Client, message: Message):
    db = DatabaseManager.get_db()
    user_id = message.from_user.id

    if user_id in settings.ADMIN_IDS:
        await message.reply(
            "ℹ️ You are an admin. Use <code>/close</code> "
            "inside a user topic to manage sessions.",
            parse_mode=ParseMode.HTML,
        )
        return

    # In the new system, we always reuse the topic, but we might want to notify admins
    # if it's a "new" request (session was closed or never existed)
    session_active = await is_session_active(db, user_id)
    
    topic_manager = get_topic_manager()
    topic_id = await topic_manager.get_or_create_user_topic(client, user_id)

    if not session_active:
        # Re-open or initialize session
        await db["user_topics"].update_one(
            {"user_id": user_id},
            {
                "$set": {
                    "status": "pending",
                    "updated_at": datetime.now(timezone.utc),
                    "last_activity_at": datetime.now(timezone.utc)
                }
            }
        )
        await message.reply("✅ Support request received. Our team will respond as soon as possible.", parse_mode=ParseMode.HTML)

        user = message.from_user
        user_link = f"tg://user?id={user_id}"
        accept_notice_text = (
            f"📩 <b>Support Message</b>\n\n"
            f"👤 <a href='{user_link}'>{escape(user.full_name)}</a> "
            f"(@{escape(user.username or 'no_username')}) "
            f"[<code>{user_id}</code>]\n\n"
            f"User is waiting for help.\n\n"
            f"👇 Click below to accept:"
        )

        support_service = get_support_service()
        await support_service.notify_to_topic(
            client=client,
            user_id=user_id,
            text=accept_notice_text,
            reply_markup=build_accept_markup(user_id),
        )
    else:
        await message.reply("⚠️ You already have an active support session. An admin will respond shortly.", parse_mode=ParseMode.HTML)


@Client.on_callback_query(filters.regex(r"^support_accept:(\d+)$"))
async def handle_support_accept(client: Client, callback: CallbackQuery):
    user_id = int(callback.data.split(":")[1])
    db = DatabaseManager.get_db()
    
    doc = await db["user_topics"].find_one({"user_id": user_id})
    if not doc or doc.get("status") == "accepted":
        await callback.answer("This ticket is already accepted or invalid.", show_alert=True)
        return

    admin_name = callback.from_user.first_name or "Admin"
    await db["user_topics"].update_one(
        {"user_id": user_id},
        {
            "$set": {
                "status": "accepted",
                "accepted_at": datetime.now(timezone.utc),
                "accepted_by": callback.from_user.id,
                "accepted_by_name": admin_name,
            }
        }
    )

    await callback.message.edit_text(
        f"{callback.message.text}\n\n✅ <b>Accepted by {admin_name}</b>",
        reply_markup=None,
        parse_mode=ParseMode.HTML
    )
    
    try:
        await client.send_message(
            chat_id=user_id,
            text="✅ An admin has accepted your support request. You can now chat freely.",
            parse_mode=ParseMode.HTML
        )
    except Exception:
        pass
    
    await callback.answer("Session accepted.")


@Client.on_message((filters.text | filters.photo | filters.video | filters.document) & filters.private & ~filters.command([]), group=1)
async def private_message_handler(client: Client, message: Message):
    """Routes all private messages to the user's unified topic."""
    user_id = message.from_user.id
    
    # We always forward to the topic if the bot is in 'support mode' for this user
    # or if we just want a complete history.
    support_service = get_support_service()
    await support_service.handle_user_message(client, message)


@Client.on_message(filters.command("closesupport") & filters.chat(settings.VERIFICATION_GROUP_ID))
async def cmd_close_support_legacy(client: Client, message: Message):
    """Legacy alias for /close."""
    from app.handlers.admin_handler import handle_close_command
    await handle_close_command(client, message)
