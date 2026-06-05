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
from app.services.topic_manager import get_topic_manager

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
        Routes user message from bot DM to their unified user topic in Hub.
        """
        user_id = message.from_user.id
        try:
            topic_id = await self.topic_manager.get_or_create_user_topic(
                client, user_id
            )
            
            # ── ROUTE HEADER TO USER TOPIC ──
            try:
                user_name = f"{message.from_user.first_name or ''} {message.from_user.last_name or ''}".strip()
                msg_text = message.text or message.caption or "-"
                if len(msg_text) > 100:
                    msg_text = msg_text[:97] + "..."

                await client.send_message(
                    chat_id=settings.VERIFICATION_GROUP_ID,
                    text=(
                        f"📩 <b>SUPPORT MESSAGE</b>\n\n"
                        f"<b>User:</b> {user_name}\n"
                        f"<b>Message:</b> {msg_text}"
                    ),
                    message_thread_id=topic_id,
                    parse_mode=ParseMode.HTML
                )

                # Audit log
                from app.services.audit_service import get_audit
                await get_audit().log(
                    action="SUPPORT_MESSAGE",
                    performed_by=user_id,
                    target_user_id=user_id
                )
            except Exception:
                pass
            
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
            logger.error(f"Failed to forward message to user topic for {user_id}: {e}")
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
        Sends a notification message to a user's unified topic.
        """
        try:
            topic_id = await self.topic_manager.get_or_create_user_topic(
                client, user_id
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
            logger.error(f"Failed to send notification to user topic for {user_id}: {e}")
            return None

_support_service: Optional[SupportService] = None

def get_support_service() -> SupportService:
    global _support_service
    if _support_service is None:
        _support_service = SupportService()
    return _support_service
