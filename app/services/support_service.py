from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import Optional

from pyrogram.client import Client
from pyrogram.types import Message, InlineKeyboardMarkup, InlineKeyboardButton
from pyrogram.enums import ParseMode
from pyrogram.errors import FloodWait, RPCError

from app.config import settings
from app.core.database import DatabaseManager
from app.services.topic_manager import get_topic_manager, TOPIC_SUPPORT

logger = logging.getLogger(__name__)

def build_accept_markup(user_id: int) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([[
        InlineKeyboardButton(
            text="✅ Accept Support",
            callback_data=f"support_accept:{user_id}",
        )
    ]])

class SupportService:
    def __init__(self):
        self.topic_manager = get_topic_manager()

    async def handle_user_message(self, client: Client, message: Message) -> bool:
        """
        Routes user message from bot DM to their support topic in Hub.
        """
        user_id = message.from_user.id
        try:
            topic_id = await self.topic_manager.get_or_create_user_topic(
                client, user_id, TOPIC_SUPPORT
            )
            
            # Forward the message to the topic
            await client.copy_message(
                chat_id=settings.VERIFICATION_GROUP_ID,
                from_chat_id=message.chat.id,
                message_id=message.id,
                message_thread_id=topic_id
            )
            
            # Log the message to DB
            db = DatabaseManager.get_db()
            await db["support_messages"].insert_one({
                "user_id": user_id,
                "topic_id": topic_id,
                "user_message_id": message.id,
                "direction": "user_to_admin",
                "created_at": datetime.now(timezone.utc),
            })
            
            return True
        except Exception as e:
            logger.error(f"Failed to forward message to support topic for {user_id}: {e}")
            return False

    async def notify_to_topic(
        self,
        client: Client,
        user_id: int,
        text: str,
        reply_markup: Optional[InlineKeyboardMarkup] = None,
        **kwargs
    ) -> Optional[Message]:
        """
        Sends a notification message to a user's support topic.
        """
        try:
            topic_id = await self.topic_manager.get_or_create_user_topic(
                client, user_id, TOPIC_SUPPORT
            )
            
            sent = await client.send_message(
                chat_id=settings.VERIFICATION_GROUP_ID,
                text=text,
                reply_markup=reply_markup,
                message_thread_id=topic_id,
                parse_mode=ParseMode.HTML
            )
            return sent
        except Exception as e:
            logger.error(f"Failed to send notification to support topic for {user_id}: {e}")
            return None

_support_service: Optional[SupportService] = None

def get_support_service() -> SupportService:
    global _support_service
    if _support_service is None:
        _support_service = SupportService()
    return _support_service
