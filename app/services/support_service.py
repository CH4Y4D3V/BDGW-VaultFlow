from __future__ import annotations

"""
SupportService — bidirectional message routing between users and admins.

User → bot DM  →  copy to user's support topic in verification hub
Admin reply in topic  →  copy back to user DM
"""

import asyncio
from datetime import datetime, timezone
from typing import Optional

from pyrogram.client import Client
from pyrogram.enums import ParseMode
from pyrogram.errors import FloodWait, RPCError, UserIsBlocked, PeerIdInvalid

from app.config import settings
from app.repositories.support_repository import SupportRepository
from app.services.topic_service import get_topic_service, TOPIC_SUPPORT
from app.utils.logger import get_logger

logger = get_logger(__name__)

_FLOOD_BUFFER = settings.FLOODWAIT_EXTRA_BUFFER
_MAX_RETRIES = 3

_support_repo = SupportRepository()


async def _copy_message_safe(
    client: Client,
    to_chat: int,
    from_chat: int,
    message_id: int,
    thread_id: Optional[int] = None,
) -> Optional[int]:
    """Copy a message, returning the new message_id or None on failure."""
    for attempt in range(_MAX_RETRIES):
        try:
            kwargs = {
                "chat_id": to_chat,
                "from_chat_id": from_chat,
                "message_id": message_id,
            }
            if thread_id is not None:
                kwargs["message_thread_id"] = thread_id

            result = await client.copy_message(**kwargs)
            return result.id
        except (UserIsBlocked, PeerIdInvalid):
            return None
        except FloodWait as e:
            await asyncio.sleep(int(e.value) + _FLOOD_BUFFER)
        except RPCError as e:
            if attempt == _MAX_RETRIES - 1:
                logger.warning(
                    "copy_message failed",
                    extra={"ctx_error": str(e), "ctx_to": to_chat},
                )
                return None
            await asyncio.sleep(2 ** attempt)
    return None


class SupportService:
    """
    Routes support messages between user DMs and the verification hub topics.
    """

    async def handle_user_message(self, client: Client, message) -> None:
        """
        Called when a private message arrives and the user has (or needs) a support topic.
        1. Get or create support topic.
        2. Copy message into topic.
        3. Persist to support_messages.
        """
        if not message.from_user:
            return

        user_id = message.from_user.id
        topic_service = get_topic_service()

        try:
            topic_id = await topic_service.get_or_create_user_topic(
                client, user_id, TOPIC_SUPPORT
            )
        except Exception as e:
            logger.error(
                "Support: failed to get/create topic",
                extra={"ctx_user_id": user_id, "ctx_error": str(e)},
            )
            try:
                await client.send_message(
                    chat_id=user_id,
                    text="⚠️ Could not open a support ticket. Please try again in a moment.",
                    parse_mode=ParseMode.HTML,
                )
            except Exception:
                pass
            return

        hub_message_id = await _copy_message_safe(
            client,
            to_chat=settings.VERIFICATION_GROUP_ID,
            from_chat=message.chat.id,
            message_id=message.id,
            thread_id=topic_id,
        )

        await _support_repo.save_message({
            "user_id": user_id,
            "topic_id": topic_id,
            "user_message_id": message.id,
            "hub_message_id": hub_message_id,
            "direction": "user_to_admin",
            "created_at": datetime.now(timezone.utc),
        })

        logger.info(
            "Support message routed to hub",
            extra={
                "ctx_user_id": user_id,
                "ctx_topic_id": topic_id,
                "ctx_hub_msg_id": hub_message_id,
            },
        )

    async def handle_admin_reply(self, client: Client, message) -> None:
        """
        Called when an admin sends a message in a support topic.
        Routes the reply back to the user's DM.
        """
        thread_id = (
            getattr(message, "message_thread_id", None)
            or getattr(message, "reply_to_top_message_id", None)
        )
        if not thread_id:
            return

        topic_service = get_topic_service()
        topic_doc = await topic_service.get_user_by_topic(thread_id)
        if not topic_doc:
            return

        if topic_doc.get("topic_type") != TOPIC_SUPPORT:
            return

        user_id: int = topic_doc["user_id"]

        hub_message_id = await _copy_message_safe(
            client,
            to_chat=user_id,
            from_chat=message.chat.id,
            message_id=message.id,
        )

        await _support_repo.save_message({
            "user_id": user_id,
            "topic_id": thread_id,
            "user_message_id": hub_message_id,
            "hub_message_id": message.id,
            "direction": "admin_to_user",
            "created_at": datetime.now(timezone.utc),
        })

        if hub_message_id is None:
            # Notify admin in-topic that user is unreachable
            try:
                await client.send_message(
                    chat_id=message.chat.id,
                    text=f"⚠️ Could not deliver reply to user <code>{user_id}</code>. They may have blocked the bot.",
                    parse_mode=ParseMode.HTML,
                    reply_to_message_id=message.id,
                )
            except Exception:
                pass

        logger.info(
            "Admin reply routed to user",
            extra={
                "ctx_user_id": user_id,
                "ctx_topic_id": thread_id,
                "ctx_admin": message.from_user.id if message.from_user else "unknown",
            },
        )


# Module-level singleton
_support_service: Optional[SupportService] = None


def get_support_service() -> SupportService:
    global _support_service
    if _support_service is None:
        _support_service = SupportService()
    return _support_service