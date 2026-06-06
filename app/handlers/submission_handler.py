from __future__ import annotations

import asyncio
from datetime import datetime, timezone
from typing import Optional, List

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
from app.services.onboarding_service import OnboardingService
from app.utils.logger import get_logger

logger = get_logger(__name__)

# ── Album Buffer (Volatile, in-memory per worker) ─────────────────────────
# Section 10.2: Albums must be buffered via media_group_id.
_album_buffer: dict[str, List[Message]] = {}

# ── Handlers ──────────────────────────────────────────────────────────────

@Client.on_callback_query(filters.regex(r"^menu:submit$"))
async def handle_submit_menu(client: Client, callback_query: CallbackQuery) -> None:
    """
    Entry point for submission flow from main menu.
    
    Section 10.1: Main Channel membership verification gate.
    """
    user_id = callback_query.from_user.id
    
    # 1. Verification Gate
    try:
        member = await client.get_chat_member(settings.MAIN_CHANNEL_ID, user_id)
        if member.status.value in ("left", "banned", "kicked"):
             raise ValueError("Not a member")
    except Exception:
        await callback_query.answer(
            "❌ You must join our main channel first to submit content.",
            show_alert=True
        )
        return

    # 2. Terms Check
    db = DatabaseManager.get_db()
    user_doc = await db["users"].find_one({"user_id": user_id})
    if not user_doc or not user_doc.get("terms_accepted"):
        await callback_query.answer(
            "❌ Please accept the terms of service in /start before submitting.",
            show_alert=True
        )
        return

    await callback_query.answer()
    await callback_query.message.edit_text(
        "📤 <b>Anonymous Submission</b>\n\n"
        "Send your photo or video here now. You can also send albums.\n\n"
        "<i>All submissions are reviewed by moderators before posting.</i>",
        reply_markup=InlineKeyboardMarkup([[
            InlineKeyboardButton("⬅️ Back to Menu", callback_data="menu:home")
        ]]),
        parse_mode=ParseMode.HTML
    )

@Client.on_message(filters.private & (filters.photo | filters.video))
async def handle_submission(client: Client, message: Message) -> None:
    """
    Primary media handler. Detects albums and routes to processing.
    """
    if not message.from_user:
        return

    # Section 10.2: Media Group detection
    if message.media_group_id:
        mg_id = message.media_group_id
        if mg_id not in _album_buffer:
            _album_buffer[mg_id] = []
            asyncio.create_task(_process_album_buffer(client, mg_id))
        
        _album_buffer[mg_id].append(message)
        return

    # Single media
    await _register_submission(client, [message])

async def _process_album_buffer(client: Client, mg_id: str):
    """Wait for album completion then register."""
    await asyncio.sleep(2.0) # Buffer window
    messages = _album_buffer.pop(mg_id, [])
    if messages:
        await _register_submission(client, messages)

async def _register_submission(client: Client, messages: List[Message]):
    """
    Process, hash, and route submission to moderation topic.
    """
    lead_msg = messages[0]
    user_id = lead_msg.from_user.id
    first_name = lead_msg.from_user.first_name or "Creator"
    
    # Placeholder for hashing logic (Section 10.2)
    # In full implementation, we'd download, hash, and check content_fingerprints.
    # For now, we simulate success.
    
    db = DatabaseManager.get_db()
    
    # Create DB Record
    submission_id = str(datetime.now().timestamp()) # Real implementation uses ObjectId
    
    # Notify User
    ack = await lead_msg.reply_text(
        "✅ <b>Submission Received</b>\n\n"
        "Your content has been forwarded to our moderation team.\n"
        "Status: <code>PENDING REVIEW</code>",
        parse_mode=ParseMode.HTML
    )
    asyncio.create_task(_delete_after(ack, 10))

    # Route to Verification Hub (Section 10.3)
    from app.services.topic_manager import get_topic_manager
    from app.moderation.verification_hub import post_user_info_card, forward_to_verification
    
    topic_mgr = get_topic_manager()
    topic_id = await topic_mgr.get_or_create_user_topic(client, user_id)
    
    # 1. Post user info card (Section 10.3)
    await post_user_info_card(
        client=client,
        user=lead_msg.from_user,
        chat_id=settings.VERIFICATION_GROUP_ID,
        topic_id=topic_id,
    )
    
    # 2. Forward content with moderation buttons (Section 10.3)
    await forward_to_verification(
        client=client,
        messages=messages,
        submitter_user_id=user_id,
        topic_id=topic_id,
    )

async def _delete_after(msg, delay):
    await asyncio.sleep(delay)
    try:
        await msg.delete()
    except Exception:
        pass
