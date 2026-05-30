from __future__ import annotations

import asyncio
from datetime import datetime, timezone

from pyrogram import Client, filters
from pyrogram.enums import ParseMode
from pyrogram.types import (
    CallbackQuery,
    InlineKeyboardButton,
    InlineKeyboardMarkup,
    Message,
)

from app.config import settings
from app.core.redis_client import get_redis
from app.core.database import DatabaseManager
from app.services.takedown_service import TakedownService
from app.utils.logger import get_logger

logger = get_logger(__name__)

# ── FSM Keys ──
# state:takedown:{user_id} -> current state
# data:takedown:{user_id} -> JSON string with collected data

STATE_IDLE = "idle"
STATE_AWAITING_ID = "awaiting_id"
STATE_AWAITING_REASON = "awaiting_reason"
STATE_AWAITING_LINK = "awaiting_link"

_takedown_service = TakedownService()


async def _get_fsm(user_id: int):
    redis = get_redis()
    state = await redis.get(f"state:takedown:{user_id}") or STATE_IDLE
    import json
    data_raw = await redis.get(f"data:takedown:{user_id}")
    data = json.loads(data_raw) if data_raw else {}
    return state, data


async def _set_fsm(user_id: int, state: str, data: dict):
    redis = get_redis()
    import json
    if state == STATE_IDLE:
        await redis.delete(f"state:takedown:{user_id}", f"data:takedown:{user_id}")
    else:
        await redis.set(f"state:takedown:{user_id}", state, ex=3600)
        await redis.set(f"data:takedown:{user_id}", json.dumps(data), ex=3600)


@Client.on_message(filters.command("takedown") & filters.private)
async def handle_takedown_start(client: Client, message: Message) -> None:
    user_id = message.from_user.id
    await _set_fsm(user_id, STATE_AWAITING_ID, {})
    
    await message.reply_text(
        "⚖️ <b>Takedown Request / DMCA</b>\n\n"
        "Please provide the <b>Content ID</b> you wish to report.\n"
        "<i>(Found in the caption of the shared content)</i>\n\n"
        "Type /cancel to abort.",
        parse_mode=ParseMode.HTML
    )


@Client.on_message(filters.command("cancel") & filters.private)
async def handle_takedown_cancel(client: Client, message: Message) -> None:
    user_id = message.from_user.id
    state, _ = await _get_fsm(user_id)
    if state != STATE_IDLE:
        await _set_fsm(user_id, STATE_IDLE, {})
        await message.reply_text("❌ Takedown request cancelled.")


@Client.on_message(filters.private & ~filters.command(["takedown", "cancel", "start"]))
async def handle_takedown_fsm(client: Client, message: Message) -> None:
    user_id = message.from_user.id
    state, data = await _get_fsm(user_id)
    
    if state == STATE_IDLE:
        return

    if state == STATE_AWAITING_ID:
        content_id = message.text.strip()
        # Basic validation: check if exists in vault
        db = DatabaseManager.get_db()
        exists = await db[settings.VAULT_COLLECTION].find_one({"content_id": content_id})
        if not exists:
            await message.reply_text("❌ Invalid Content ID. Please check and send again.")
            return
            
        data["content_id"] = content_id
        await _set_fsm(user_id, STATE_AWAITING_REASON, data)
        await message.reply_text("📝 <b>Reason for Takedown</b>\n\nPlease describe why this content should be removed (e.g., Copyright, Private, Other).")
        return

    if state == STATE_AWAITING_REASON:
        data["reason"] = message.text.strip()
        await _set_fsm(user_id, STATE_AWAITING_LINK, data)
        await message.reply_text("🔗 <b>Proof / Identity Link</b>\n\nPlease provide a link or description of your identity/proof of ownership for this request.")
        return

    if state == STATE_AWAITING_LINK:
        data["link"] = message.text.strip()
        await _set_fsm(user_id, STATE_IDLE, {})
        
        # Submit to service
        full_reason = f"Reason: {data['reason']}\nProof: {data['link']}"
        record_id = await _takedown_service.submit_report(
            content_id=data["content_id"],
            reported_by=user_id,
            reason=full_reason,
            report_type="takedown"
        )
        
        await message.reply_text(
            "✅ <b>Request Submitted</b>\n\n"
            f"Your request <code>{record_id}</code> has been received and is under review.\n"
            "The content has been automatically locked pending final decision.",
            parse_mode=ParseMode.HTML
        )
        return
