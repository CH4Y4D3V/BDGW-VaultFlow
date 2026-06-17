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
        try:
            if isinstance(mute_until, str):
                mute_until = datetime.fromisoformat(mute_until)
            if mute_until.tzinfo is None:
                # Defensive normalization: MongoDB/BSON stores datetimes as UTC
                # instants with no tzinfo metadata. If this value was ever read
                # back naive (e.g. an older driver config, a manual DB edit, or
                # a legacy string write), treat it as UTC rather than crashing
                # the comparison below.
                mute_until = mute_until.replace(tzinfo=timezone.utc)

            if datetime.now(timezone.utc) < mute_until:
                await message.reply_text("🚫 <b>You are currently muted.</b>\nPlease wait until your mute expires.")
                message.stop_propagation()
                return
            else:
                # Mute has expired — clear it so we don't keep re-checking stale state.
                await db["users"].update_one({"_id": user_id}, {"$unset": {"mute_until": ""}})
        except (ValueError, TypeError) as exc:
            logger.error(
                f"Invalid mute_until value for user {user_id}: {mute_until!r} — clearing",
                extra={"user_id": user_id, "error": str(exc)},
            )
            await db["users"].update_one({"_id": user_id}, {"$unset": {"mute_until": ""}})

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
            logger.warning(f"User {user_id} banned for spam", extra={"user_id": user_id})
            
            # Dual Audit Log (B-17 / §9.5 / §22)
            try:
                from app.services.support_service import send_admin_log_entry
                await send_admin_log_entry(
                    client=client,
                    action_type="AUTO BAN (SPAM)",
                    admin_user_id=0, # System
                    admin_name="System",
                    target_user_id=user_id,
                    target_name=user.full_name,
                    target_username=user.username,
                    detail=f"Auto-banned after {strikes} spam strikes."
                )
            except Exception:
                pass

            message.stop_propagation()
            return
            
        if strikes >= strike_limit_mute:
            mute_mins = getattr(settings, "MUTE_DURATION_MINUTES", 30)
            mute_until = datetime.now(timezone.utc) + timedelta(minutes=mute_mins)
            await db["users"].update_one({"_id": user_id}, {"$set": {"mute_until": mute_until}})
            logger.warning(f"User {user_id} muted for spam", extra={"user_id": user_id})

            # Dual Audit Log (B-17 / §9.5 / §22)
            try:
                from app.services.support_service import send_admin_log_entry
                await send_admin_log_entry(
                    client=client,
                    action_type="AUTO MUTE (SPAM)",
                    admin_user_id=0, # System
                    admin_name="System",
                    target_user_id=user_id,
                    target_name=user.full_name,
                    target_username=user.username,
                    detail=f"Auto-muted for {mute_mins}m after {strikes} spam strikes."
                )
            except Exception:
                pass

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
