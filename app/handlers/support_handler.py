from __future__ import annotations

import logging
from html import escape
from datetime import datetime, timedelta, timezone
from typing import Optional, Dict

from pyrogram import Client, filters
from pyrogram.types import Message, CallbackQuery
from pyrogram.enums import ParseMode
from pyrogram.errors import PeerIdInvalid

from app.config import settings
from app.core.database import DatabaseManager
from app.services.support_service import get_support_service, build_accept_markup
from app.services.topic_manager import TOPIC_SUPPORT, get_topic_manager
from app.utils.logger import get_logger
from locales import get_user_lang, get_text

log = get_logger(__name__)

# In-memory FSM for support state as a backup/quick check
user_states: Dict[int, str] = {}
SUPPORT_STATE_ACTIVE = "active"

def _support_session_minutes() -> int:
    import os
    raw = os.getenv("SUPPORT_SESSION_MINUTES", "")
    try:
        return max(1, int(raw)) if raw else 60 # Default 1 hour for production
    except ValueError:
        return 60

async def is_session_active(db, user_id: int) -> bool:
    doc = await db["user_topics"].find_one({"user_id": user_id, "topic_type": TOPIC_SUPPORT})
    if not doc:
        return False
    
    status = doc.get("status")
    if status == "closed":
        return False
        
    expires_at_str = doc.get("expires_at")
    if expires_at_str:
        try:
            expires_at = datetime.fromisoformat(expires_at_str)
            if datetime.now(timezone.utc) > expires_at:
                return False
        except Exception:
            pass
            
    return True

@Client.on_message(filters.command(["admin", "support", "help"]) & filters.private)
async def cmd_support(client: Client, message: Message):
    db = DatabaseManager.get_db()
    user_id = message.from_user.id

    if user_id in settings.admin_ids:
        await message.reply(
            "ℹ️ You are an admin. Use <code>/closesupport &lt;user_id&gt;</code> "
            "to manage user sessions.",
            parse_mode=ParseMode.HTML,
        )
        return

    lang = (await get_user_lang(db, user_id)) or "en"

    if await is_session_active(db, user_id):
        await message.reply(get_text("support_already_active", lang), parse_mode=ParseMode.HTML)
        return

    minutes = _support_session_minutes()
    expires_at = (datetime.now(timezone.utc) + timedelta(minutes=minutes)).isoformat()
    
    await db["user_topics"].update_one(
        {"user_id": user_id, "topic_type": TOPIC_SUPPORT},
        {
            "$set": {
                "status": "pending",
                "expires_at": expires_at,
                "updated_at": datetime.now(timezone.utc),
                "created_at": datetime.now(timezone.utc)
            }
        },
        upsert=True
    )
    user_states[user_id] = SUPPORT_STATE_ACTIVE

    await message.reply(get_text("support_session_started", lang), parse_mode=ParseMode.HTML)

    user = message.from_user
    user_link = f"tg://user?id={user_id}"
    accept_notice_text = (
        f"🔔 <b>New Support Request</b>\n\n"
        f"👤 <a href='{user_link}'>{escape(user.full_name)}</a> "
        f"(@{escape(user.username or 'no_username')}) "
        f"[<code>{user_id}</code>]\n\n"
        f"User is waiting for help.\n\n"
        f"⚠️ <b>Accept the session before replying.</b>\n\n"
        f"👇 Click below:"
    )

    support_service = get_support_service()
    await support_service.notify_to_topic(
        client=client,
        user_id=user_id,
        text=accept_notice_text,
        reply_markup=build_accept_markup(user_id),
    )

@Client.on_callback_query(filters.regex(r"^support_accept:(\d+)$"))
async def handle_support_accept(client: Client, callback: CallbackQuery):
    user_id = int(callback.data.split(":")[1])
    db = DatabaseManager.get_db()
    
    # Verify the session is still pending
    doc = await db["user_topics"].find_one({"user_id": user_id, "topic_type": TOPIC_SUPPORT})
    if not doc or doc.get("status") != "pending":
        handler = doc.get("accepted_by_name", "another admin") if doc else "unknown"
        await callback.answer(get_text("support_already_accepted", "en", user_id=user_id, handler=handler), show_alert=True)
        return

    admin_name = callback.from_user.first_name or "Admin"
    await db["user_topics"].update_one(
        {"user_id": user_id, "topic_type": TOPIC_SUPPORT},
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
    
    # Notify user
    lang = (await get_user_lang(db, user_id)) or "en"
    try:
        await client.send_message(
            chat_id=user_id,
            text=get_text("support_connected", lang),
            parse_mode=ParseMode.HTML
        )
    except Exception:
        pass
    
    await callback.answer("Session accepted.")

async def msg_support_forward(client: Client, message: Message):
    db = DatabaseManager.get_db()
    user_id = message.from_user.id

    if not await is_session_active(db, user_id):
        lang = (await get_user_lang(db, user_id)) or "en"
        await message.reply(get_text("support_session_expired", lang), parse_mode=ParseMode.HTML)
        user_states.pop(user_id, None)
        return

    support_service = get_support_service()
    ok = await support_service.handle_user_message(client, message)
    
    if ok:
        lang = (await get_user_lang(db, user_id)) or "en"
        await message.reply(get_text("support_message_received", lang), parse_mode=ParseMode.HTML)

@Client.on_message((filters.text | filters.photo | filters.video | filters.document) & filters.private & ~filters.command, group=1)
async def private_message_handler(client: Client, message: Message):
    user_id = message.from_user.id
    if user_states.get(user_id) == SUPPORT_STATE_ACTIVE or await is_session_active(DatabaseManager.get_db(), user_id):
        user_states[user_id] = SUPPORT_STATE_ACTIVE
        await msg_support_forward(client, message)

@Client.on_message(filters.command("closesupport") & filters.private)
async def cmd_close_support(client: Client, message: Message):
    db = DatabaseManager.get_db()
    if message.from_user.id not in settings.admin_ids:
        return

    parts = message.text.split()
    user_id = None
    if len(parts) == 2 and parts[1].isdigit():
        user_id = int(parts[1])
    
    if not user_id:
        await message.reply("Usage: <code>/closesupport <user_id></code>")
        return

    await db["user_topics"].update_one(
        {"user_id": user_id, "topic_type": TOPIC_SUPPORT},
        {"$set": {"status": "closed", "closed_at": datetime.now(timezone.utc)}}
    )
    user_states.pop(user_id, None)
    
    await message.reply(f"✅ Support session closed for user {user_id}.")
    
    try:
        lang = (await get_user_lang(db, user_id)) or "en"
        await client.send_message(
            chat_id=user_id,
            text=get_text("support_session_closed_user", lang),
            parse_mode=ParseMode.HTML
        )
    except Exception:
        pass
