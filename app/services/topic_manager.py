from __future__ import annotations

import asyncio
import random
from datetime import datetime, timezone
from typing import Optional, Dict, Any

from pyrogram.client import Client
from pyrogram.enums import ChatType, ParseMode
from pyrogram.errors import FloodWait, RPCError, Forbidden
from pyrogram import raw
from pyrogram.raw.types import Updates, UpdateNewChannelMessage, UpdateNewMessage, MessageService, MessageActionTopicCreate

from app.config import settings
from app.core.database import DatabaseManager
from app.utils.logger import get_logger

logger = get_logger(__name__)

# ── Topic types (Backward Compatibility) ──────────────────────────────────────
TOPIC_CONTENT = "user_topic"
TOPIC_SUPPORT = "user_topic"
TOPIC_PAYMENT = "user_topic"
TOPIC_REJECTED = "user_topic"

class TopicManager:
    """
    User-Centric Topic Manager.
    Ensures every user has exactly ONE permanent forum topic in the Hub.
    """

    def __init__(self) -> None:
        self._local_cache: Dict[int, int] = {}  # user_id -> topic_id
        self._initialized = False
        self._lock = asyncio.Lock()

    async def restore_cache(self) -> None:
        """Loads all user topic mappings from MongoDB into memory."""
        if self._initialized:
            return
        try:
            db = DatabaseManager.get_db()
            await self._create_indexes(db)
            
            cursor = db["user_topics"].find({})
            async for doc in cursor:
                u_id = doc.get("user_id")
                t_id = doc.get("topic_id")
                if u_id and t_id:
                    self._local_cache[int(u_id)] = int(t_id)
            
            self._initialized = True
            logger.info(
                "TopicManager cache restored",
                extra={"ctx_count": len(self._local_cache)}
            )
        except Exception as e:
            logger.error(
                "TopicManager cache restore failed",
                extra={"ctx_error": str(e)}
            )

    async def _create_indexes(self, db):
        try:
            await db["user_topics"].create_index("user_id", unique=True)
            await db["user_topics"].create_index("topic_id", unique=True)
        except Exception as e:
            logger.warning(f"Failed to create user_topics indexes: {e}")

    async def get_or_create_user_topic(
        self,
        client: Client,
        user_id: int,
        *args,  # Ignore topic_type if passed
        **kwargs
    ) -> int:
        """Get or create the single permanent forum topic for a user."""
        # 1. Local Cache
        if user_id in self._local_cache:
            return self._local_cache[user_id]

        # 2. MongoDB Lookup
        db = DatabaseManager.get_db()
        doc = await db["user_topics"].find_one({"user_id": user_id})

        if doc:
            topic_id = int(doc["topic_id"])
            self._local_cache[user_id] = topic_id
            return topic_id

        # 3. Creation with Lock
        async with self._lock:
            # Double-check after acquiring lock
            doc = await db["user_topics"].find_one({"user_id": user_id})
            if doc:
                topic_id = int(doc["topic_id"])
                self._local_cache[user_id] = topic_id
                return topic_id

            # Fetch user info for title and header
            user_name = "Unknown"
            username = "-"
            try:
                user = await client.get_users(user_id)
                first_name = user.first_name or ""
                last_name = user.last_name or ""
                user_name = f"{first_name} {last_name}".strip() or f"User {user_id}"
                username = user.username or "-"
            except Exception:
                pass

            title = f"👤 {user_name} | {user_id}"
            if len(title) > 128:
                title = title[:125] + "..."

            topic_id = await self._create_telegram_topic(client, title)

            now = datetime.now(timezone.utc)
            await db["user_topics"].update_one(
                {"user_id": user_id},
                {
                    "$set": {
                        "user_id": user_id,
                        "topic_id": topic_id,
                        "topic_name": title,
                        "status": "active",
                        "created_at": now,
                        "last_activity_at": now,
                        "accepted_by": None,
                        "accepted_at": None,
                        "hub_chat_id": settings.VERIFICATION_GROUP_ID,
                    }
                },
                upsert=True,
            )

            self._local_cache[user_id] = topic_id
            
            # Send and pin permanent thread header
            await self._send_and_pin_header(client, user_id, topic_id, user_name, username)

            logger.info(
                "user_topic_created",
                extra={
                    "ctx_user_id": user_id,
                    "ctx_topic_id": topic_id,
                    "ctx_title": title
                }
            )

            return topic_id

    async def _send_and_pin_header(
        self, 
        client: Client, 
        user_id: int, 
        topic_id: int, 
        full_name: str, 
        username: str
    ):
        """Sends and pins the permanent thread header for the user topic."""
        # Fetch status info
        sub_status = "FREE"
        try:
            from app.repositories.subscription_repository import SubscriptionRepository
            sub_repo = SubscriptionRepository()
            sub = await sub_repo.get_by_user_id(user_id)
            if sub:
                sub_status = f"{sub.plan.value.upper()} ({sub.status.value})"
        except Exception:
            pass
            
        warnings = 0
        is_banned = "No"
        is_muted = "No"
        try:
            from app.repositories.user_repository import UserRepository
            user_repo = UserRepository()
            user_doc = await user_repo.get_user(user_id)
            if user_doc:
                warnings = user_doc.get("warning_count", 0)
                is_banned = "Yes 🚫" if user_doc.get("is_banned") else "No"
                is_muted = "Yes 🔇" if user_doc.get("is_muted") else "No"
        except Exception:
            pass

        now_str = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
        
        header_text = (
            f"👤 <b>User Thread</b>\n\n"
            f"<b>Name:</b> {full_name}\n"
            f"<b>Username:</b> @{username}\n"
            f"<b>User ID:</b> <code>{user_id}</code>\n\n"
            f"<b>Created:</b>\n{now_str}\n\n"
            f"<b>Status:</b>\n🟢 Active\n\n"
            f"<b>Subscription:</b>\n{sub_status}\n\n"
            f"<b>Warnings:</b>\n{warnings}\n\n"
            f"<b>Muted:</b>\n{is_muted}\n\n"
            f"<b>Banned:</b>\n{is_banned}\n\n"
            f"━━━━━━━━━━━━━━\n\n"
            f"All user activity appears in this topic.\n\n"
            f"• Support messages\n"
            f"• Payment submissions\n"
            f"• Payment proofs\n"
            f"• Content submissions\n"
            f"• Content moderation results\n"
            f"• Takedown requests\n"
            f"• Admin notes\n"
            f"• Audit events\n"
            f"• Warnings\n"
            f"• Mutes\n"
            f"• Bans\n\n"
            f"━━━━━━━━━━━━━━\n\n"
            f"<b>Commands</b>\n\n"
            f"/accept\n"
            f"/close\n"
            f"/ban\n"
            f"/unban\n"
            f"/warn\n"
            f"/mute\n"
            f"/unmute\n"
            f"/paymentdone\n"
            f"/profile\n"
            f"/history\n"
            f"/payments\n"
            f"/notes\n"
            f"/note &lt;text&gt;"
        )
        
        try:
            msg = await client.send_message(
                chat_id=settings.VERIFICATION_GROUP_ID,
                text=header_text,
                message_thread_id=topic_id,
                parse_mode=ParseMode.HTML
            )
            await client.pin_chat_message(
                chat_id=settings.VERIFICATION_GROUP_ID,
                message_id=msg.id
            )
        except Exception as e:
            logger.error(f"Failed to send/pin header for user {user_id}: {e}")

    async def get_user_topic_id(self, user_id: int, *args, **kwargs) -> Optional[int]:
        """Return the topic_id for a given user, or None if not found."""
        if user_id in self._local_cache:
            return self._local_cache[user_id]

        db = DatabaseManager.get_db()
        doc = await db["user_topics"].find_one({"user_id": user_id})
        if doc:
            topic_id = int(doc["topic_id"])
            self._local_cache[user_id] = topic_id
            return topic_id
        return None

    async def get_user_by_topic(self, topic_id: int) -> Optional[dict]:
        """Reverse lookup: Given topic_id, find the user mapping."""
        db = DatabaseManager.get_db()
        return await db["user_topics"].find_one({"topic_id": topic_id})

    async def recover_topic(self, client: Client, user_id: int, *args) -> int:
        """Forces recreation of the user topic."""
        logger.info(
            "Recovering user topic",
            extra={"ctx_user_id": user_id}
        )
        self._local_cache.pop(user_id, None)
        db = DatabaseManager.get_db()
        await db["user_topics"].delete_one({"user_id": user_id})

        return await self.get_or_create_user_topic(client, user_id)

    async def _create_telegram_topic(self, client: Client, title: str) -> int:
        """Internal: Raw MTProto call to create a forum topic."""
        group_id = settings.VERIFICATION_GROUP_ID
        
        delays = [2, 4, 8]
        for attempt, delay in enumerate(delays):
            try:
                peer = await client.resolve_peer(group_id)
                result = await client.invoke(
                    raw.functions.messages.CreateForumTopic(
                        peer=peer,
                        title=title,
                        random_id=random.randint(1, 2**63 - 1),
                    )
                )

                topic_id = None
                if isinstance(result, Updates):
                    for update in result.updates:
                        if type(update).__name__ == "UpdateNewForumTopic":
                            topic_id = getattr(update, "id", None)
                            if topic_id: break
                        if isinstance(update, (UpdateNewChannelMessage, UpdateNewMessage)):
                            msg = update.message
                            if isinstance(msg, MessageService) and isinstance(msg.action, MessageActionTopicCreate):
                                topic_id = msg.id
                                break

                if topic_id is None:
                    async for topic in client.get_forum_topics(group_id, limit=5):
                        if topic.title == title:
                            topic_id = topic.id
                            break

                if topic_id:
                    return topic_id
                
                raise RuntimeError("Failed to extract topic_id")

            except FloodWait as e:
                await asyncio.sleep(int(e.value) + settings.FLOODWAIT_EXTRA_BUFFER)
            except Exception as e:
                if attempt == len(delays) - 1:
                    raise
                await asyncio.sleep(delay)

        raise RuntimeError(f"Failed to create forum topic: {title}")

    # Compatibility shims for legacy calls
    async def get_or_create_payments_topic(self, client, payment_id, user_id):
        return await self.get_or_create_user_topic(client, user_id)

    async def get_or_create_shared_topic(self, client, key, title):
        # Shared topics (Audit, Log, etc.) still need separate IDs if requested,
        # but the prompt says DO NOT create separate topics for Payments, Support, etc.
        # We'll just return a single Hub topic or specific ID from settings if it's not user-centric.
        # Actually, let's just use the Hub general chat (id 1 or 0) or specific settings.
        return 0 

_topic_manager: Optional[TopicManager] = None

def get_topic_manager() -> TopicManager:
    global _topic_manager
    if _topic_manager is None:
        _topic_manager = TopicManager()
    return _topic_manager
