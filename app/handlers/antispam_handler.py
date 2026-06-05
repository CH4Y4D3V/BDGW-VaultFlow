from __future__ import annotations

import logging
from collections import defaultdict, deque
from datetime import datetime, timedelta, timezone
from typing import Deque

from pyrogram import Client, filters
from pyrogram.types import Message

from app.config import settings
from app.core.database import DatabaseManager

logger = logging.getLogger(__name__)

# User message timestamps for rate limiting
# user_id -> deque of timestamps
_user_message_times: dict[int, Deque[float]] = defaultdict(deque)

@Client.on_message(filters.private & ~filters.bot, group=-2)
async def antispam_handler(client: Client, message: Message) -> None:
    """
    High-priority antispam handler.
    Group -2 ensures it runs before most other handlers.
    """
    user = message.from_user
    if not user:
        return

    user_id = user.id
    
    # Admins are exempt
    if user_id in settings.ADMIN_IDS:
        return

    db = DatabaseManager.get_db()
    
    # 1. Check if user is muted
    user_doc = await db["users"].find_one({"_id": user_id})
    if user_doc and user_doc.get("mute_until"):
        mute_until = user_doc["mute_until"]
        if isinstance(mute_until, str):
            mute_until = datetime.fromisoformat(mute_until)
            
        if datetime.now(timezone.utc) < mute_until:
            await message.reply_text("🚫 <b>You are currently muted.</b>\nPlease wait until your mute expires.")
            message.stop_propagation()
            return

    # 2. Rate limiting logic
    now = datetime.now(timezone.utc).timestamp()
    window = getattr(settings, "SPAM_WINDOW_SECONDS", 5)
    max_msgs = getattr(settings, "SPAM_MAX_MESSAGES", 5)
    
    timestamps = _user_message_times[user_id]
    while timestamps and now - timestamps[0] > window:
        timestamps.popleft()
    
    timestamps.append(now)
    
    if len(timestamps) > max_msgs:
        # Spam detected
        logger.warning(f"Spam detected from user {user_id}", extra={"user_id": user_id})
        
        # Increment strikes
        await db["users"].update_one({"_id": user_id}, {"$inc": {"spam_strikes": 1}})
        user_doc = await db["users"].find_one({"_id": user_id})
        strikes = user_doc.get("spam_strikes", 0)
        
        strike_limit_ban = getattr(settings, "STRIKE_LIMIT_FOR_BAN", 5)
        strike_limit_mute = getattr(settings, "STRIKE_LIMIT_FOR_MUTE", 3)
        
        if strikes >= strike_limit_ban:
            await db["users"].update_one({"_id": user_id}, {"$set": {"is_banned": True}})
            await message.reply_text("🚫 <b>You have been permanently banned for spamming.</b>")
            logger.warning(f"User {user_id} banned for spam", extra={"user_id": user_id})
            message.stop_propagation()
            return
            
        if strikes >= strike_limit_mute:
            mute_mins = getattr(settings, "MUTE_DURATION_MINUTES", 30)
            mute_until = datetime.now(timezone.utc) + timedelta(minutes=mute_mins)
            await db["users"].update_one({"_id": user_id}, {"$set": {"mute_until": mute_until}})
            await message.reply_text(
                f"⚠️ <b>You have been muted for {mute_mins} minutes due to spamming.</b>\n"
                f"Strike {strikes}/{strike_limit_ban}"
            )
            message.stop_propagation()
            return
            
        await message.reply_text(
            f"⚠️ <b>Please slow down!</b>\n"
            f"You are sending messages too fast. Strike {strikes}/{strike_limit_ban}"
        )
        message.stop_propagation()
        return

    # Upsert user info
    await db["users"].update_one(
        {"_id": user_id},
        {"$set": {"username": user.username, "full_name": user.full_name, "last_seen": datetime.now(timezone.utc)}},
        upsert=True
    )
