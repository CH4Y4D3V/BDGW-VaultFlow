from __future__ import annotations

import asyncio
from datetime import datetime, timedelta, timezone
from pyrogram import Client
from pyrogram.enums import ParseMode

from app.config import settings
from app.core.database import DatabaseManager
from app.utils.logger import get_logger

logger = get_logger(__name__)

class SupportMonitor:
    """Monitors support tickets for inactivity and alerts admins."""
    
    def __init__(self, bot: Client):
        self._bot = bot
        self._db = DatabaseManager.get_db()

    async def check_inactivity(self):
        """Scans for pending tickets older than 5 minutes."""
        try:
            now = datetime.now(timezone.utc)
            threshold = now - timedelta(minutes=5)
            
            # Find pending support tickets created > 5 mins ago that haven't been warned
            cursor = self._db["user_topics"].find({
                "topic_type": "support",
                "status": "pending",
                "created_at": {"$lte": threshold},
                "inactivity_warned": {"$ne": True}
            })
            
            async for ticket in cursor:
                user_id = ticket["user_id"]
                topic_id = ticket["topic_id"]
                
                logger.info("support_inactivity_detected", extra={"ctx_user_id": user_id, "ctx_topic_id": topic_id})
                
                try:
                    await self._bot.send_message(
                        chat_id=settings.VERIFICATION_GROUP_ID,
                        text=(
                            "🚨 <b>SUPPORT URGENT</b>\n\n"
                            f"Ticket for user <code>{user_id}</code> has been pending for over 5 minutes.\n"
                            "Please click <code>✅ Accept Support</code> to assist the user."
                        ),
                        message_thread_id=topic_id,
                        parse_mode=ParseMode.HTML
                    )
                    
                    # Mark as warned to avoid spam
                    await self._db["user_topics"].update_one(
                        {"_id": ticket["_id"]},
                        {"$set": {"inactivity_warned": True}}
                    )

                    # Per Section 15.3: notify user that no admin is available
                    try:
                        await self._bot.send_message(
                            chat_id=user_id,
                            text=(
                                "⚠️ No admin available currently. "
                                "Your ticket is still open — an admin will respond soon."
                            ),
                            parse_mode=ParseMode.HTML,
                        )
                    except Exception as notify_err:
                        logger.warning(
                            "failed_to_send_inactivity_user_notify",
                            extra={"ctx_user_id": user_id, "ctx_error": str(notify_err)},
                        )
                except Exception as e:
                    logger.warning("failed_to_send_inactivity_warning", extra={"ctx_error": str(e)})

        except Exception as e:
            logger.error("SupportMonitor error", extra={"ctx_error": str(e)})
