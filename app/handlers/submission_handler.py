from __future__ import annotations

import asyncio
import time
from collections import defaultdict
from datetime import datetime, timezone

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
from app.core.redis_client import get_redis
from app.core.database import DatabaseManager
from app.moderation.verification_hub import forward_to_verification
from app.services.submission_service import register_pending
from app.services.topic_service import get_topic_service, TOPIC_CONTENT
from app.services.subscription_service import SubscriptionService
from app.models.subscription import Plan
from app.utils.logger import get_logger

logger = get_logger(__name__)

# ── Album Collector State ───────────────────────────────────────────────────

_album_cache: dict[str, list[Message]] = {}
_album_locks: dict[str, asyncio.Lock] = defaultdict(asyncio.Lock)
_ALBUM_WAIT_SECONDS = 2.0


async def _safe_reply(message: Message, text: str, reply_markup=None) -> None:
    try:
        await message.reply_text(text, reply_markup=reply_markup, parse_mode=ParseMode.HTML)
    except Exception:
        pass


@Client.on_callback_query(filters.regex(r"^menu:submit$"))
async def handle_submit_menu(client: Client, callback: CallbackQuery) -> None:
    await callback.answer()
    await callback.message.edit_text(
        "📨 <b>Submit Content</b>\n\n"
        "Please send your photo, video, or album now.\n\n"
        "<i>Note: By sending content, you confirm you have the rights to share it.</i>",
        reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("← Back", callback_data="menu:home")]]),
        parse_mode=ParseMode.HTML
    )


@Client.on_callback_query(filters.regex(r"^menu:anonymous$"))
async def handle_anonymous_toggle(client: Client, callback: CallbackQuery) -> None:
    user_id = callback.from_user.id
    redis = get_redis()
    key = f"user:anon:{user_id}"
    
    is_anon = await redis.exists(key)
    if is_anon:
        await redis.delete(key)
        await callback.answer("Anonymous mode: OFF", show_alert=True)
    else:
        await redis.set(key, "1", ex=86400 * 30)
        await callback.answer("Anonymous mode: ON", show_alert=True)


@Client.on_message(filters.private & (filters.photo | filters.video | filters.document | filters.animation))
async def handle_media_submission(client: Client, message: Message) -> None:
    user_id = message.from_user.id
    
    # B-06 Guard: Skip if in payment flow
    redis = get_redis()
    if await redis.exists(f"pay_session:{user_id}"):
        return

    # 1. Consent Check (F-02)
    db = DatabaseManager.get_db()
    consent = await db["consent_records"].find_one({"user_id": user_id, "is_active": True})
    if not consent:
        await message.reply_text(
            "⚠️ <b>Consent Required</b>\n\n"
            "To submit content, you must agree to our terms and conditions.",
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("✅ I Agree", callback_data="consent:agree")],
                [InlineKeyboardButton("❌ Decline", callback_data="menu:home")]
            ]),
            parse_mode=ParseMode.HTML
        )
        return

    # 2. Daily Cap Check (F-02)
    sub_service = SubscriptionService()
    plan = await sub_service.get_effective_plan(user_id)
    daily_cap = 50 if plan != Plan.FREE else 5
    
    cap_key = f"cap:submit:{user_id}:{time.strftime('%Y-%m-%d')}"
    current_count = int(await redis.get(cap_key) or 0)
    
    if current_count >= daily_cap:
        await _safe_reply(
            message,
            f"🚫 <b>Daily Limit Reached</b>\n\n"
            f"You have reached your daily limit of {daily_cap} submissions.\n"
            f"Upgrade to Premium for higher limits."
        )
        return

    # 3. Album Handling
    if message.media_group_id:
        group_id = message.media_group_id
        async with _album_locks[group_id]:
            if group_id not in _album_cache:
                _album_cache[group_id] = []
                asyncio.create_task(_process_album(client, group_id, user_id, cap_key))
            
            _album_cache[group_id].append(message)
        return

    # 4. Single Media Handling
    await _finalize_submission(client, [message], user_id, cap_key)


async def _process_album(client: Client, group_id: str, user_id: int, cap_key: str) -> None:
    await asyncio.sleep(_ALBUM_WAIT_SECONDS)
    async with _album_locks[group_id]:
        messages = _album_cache.pop(group_id, [])
        if not messages:
            return
        
    await _finalize_submission(client, messages, user_id, cap_key)


async def _finalize_submission(client: Client, messages: list[Message], user_id: int, cap_key: str) -> None:
    try:
        # Increment cap
        redis = get_redis()
        await redis.incr(cap_key)
        await redis.expire(cap_key, 86400)

        # Forward to verification topic
        topic_service = get_topic_service()
        topic_id = await topic_service.get_or_create_user_topic(client, user_id, TOPIC_CONTENT)
        
        success = await forward_to_verification(
            client=client,
            messages=messages,
            submitter_user_id=user_id,
            topic_id=topic_id
        )
        
        if success:
            await register_pending(user_id, messages)
            await _safe_reply(messages[0], "✅ <b>Content submitted!</b>\nOur moderators will review it shortly.")
        else:
            await _safe_reply(messages[0], "❌ <b>Failed to submit content.</b>\nPlease try again later.")

    except Exception as e:
        logger.exception("Submission finalization failed", extra={"ctx_user_id": user_id, "ctx_error": str(e)})
        await _safe_reply(messages[0], "⚠️ An unexpected error occurred. Please try again.")


@Client.on_callback_query(filters.regex(r"^consent:agree$"))
async def handle_consent_agree(client: Client, callback: CallbackQuery) -> None:
    user_id = callback.from_user.id
    db = DatabaseManager.get_db()
    await db["consent_records"].update_one(
        {"user_id": user_id},
        {"$set": {"user_id": user_id, "is_active": True, "agreed_at": datetime.now(timezone.utc)}},
        upsert=True
    )
    await callback.answer("Thank you! You can now submit content.", show_alert=True)
    await callback.message.edit_text(
        "✅ <b>Terms Agreed</b>\n\nYou can now send photos and videos directly to this chat.",
        reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("← Back", callback_data="menu:home")]]),
        parse_mode=ParseMode.HTML
    )
