from __future__ import annotations

import asyncio
import re
from datetime import datetime, timedelta, timezone
from pyrogram import Client
from app.config import settings
from app.core.database import DatabaseManager
from app.utils.logger import get_logger

logger = get_logger(__name__)

# BD Phone number regex (017x, 018x, 019x, 014x, 013x)
BD_PHONE_REGEX = r"(?:017|018|019|014|013)\d{8}"

class CleanupService:
    """
    Enforces SECTION 20 — MESSAGE CLEANUP POLICY.
    - General bot conversation: 60 mins
    - Payment sessions: 20 mins
    - BD Phone numbers: 7 mins
    """

    def __init__(self, bot: Client):
        self._bot = bot
        self._db = DatabaseManager.get_db()
        self._history = self._db["message_history"]

    async def log_message(self, user_id: int, message_id: int, text: str = "", category: str = "general"):
        """Records a message to be cleaned up later."""
        expiry_mins = 60
        if category == "payment":
            expiry_mins = 20
        
        # Check for BD phone numbers in text
        if re.search(BD_PHONE_REGEX, text):
            category = "phone"
            expiry_mins = 7

        expires_at = datetime.now(timezone.utc) + timedelta(minutes=expiry_mins)
        
        await self._history.update_one(
            {"user_id": user_id, "message_id": message_id},
            {
                "$set": {
                    "user_id": user_id,
                    "message_id": message_id,
                    "category": category,
                    "expires_at": expires_at,
                    "deleted": False
                }
            },
            upsert=True
        )

    async def run_cleanup_sweep(self):
        """Scans for expired messages and deletes them from Telegram."""
        try:
            now = datetime.now(timezone.utc)
            cursor = self._history.find({
                "expires_at": {"$lte": now},
                "deleted": False
            })

            async for record in cursor:
                user_id = record["user_id"]
                msg_id = record["message_id"]

                try:
                    await self._bot.delete_messages(user_id, msg_id)
                except Exception:
                    pass # Already deleted or blocked

                await self._history.update_one(
                    {"_id": record["_id"]},
                    {"$set": {"deleted": True}}
                )

        except Exception as e:
            logger.error("CleanupService sweep failed", extra={"ctx_error": str(e)})

    async def delete_user_support_history(self, user_id: int):
        """Rule 15.5: Delete all user-side support messages on closure."""
        try:
            cursor = self._history.find({"user_id": user_id, "category": "support", "deleted": False})
            msg_ids = []
            async for r in cursor:
                msg_ids.append(r["message_id"])
            
            if msg_ids:
                await self._bot.delete_messages(user_id, msg_ids)
                await self._history.update_many(
                    {"user_id": user_id, "category": "support"},
                    {"$set": {"deleted": True}}
                )
        except Exception:
            pass

def get_cleanup_service(bot: Client = None) -> CleanupService:
    # This expects bot to be provided on first call (lifecycle)
    return CleanupService(bot)
