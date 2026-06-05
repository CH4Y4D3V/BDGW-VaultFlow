from __future__ import annotations

import asyncio
from datetime import datetime, timezone
from typing import Optional, Dict, Any

from pyrogram.client import Client
from pyrogram.enums import ChatType
from pyrogram.errors import FloodWait, RPCError, Forbidden
from pyrogram import raw

from app.config import settings
from app.core.database import DatabaseManager
from app.utils.logger import get_logger

logger = get_logger(__name__)

# ── Topic types ───────────────────────────────────────────────────────────────

TOPIC_CONTENT = "content"
TOPIC_SUPPORT = "support"
TOPIC_PAYMENT = "payment"
TOPIC_REJECTED = "rejected"

_TOPIC_ICONS = {
    TOPIC_CONTENT: "📤",
    TOPIC_SUPPORT: "🆘",
    TOPIC_PAYMENT: "💎",
    TOPIC_REJECTED: "❌",
}

class TopicManager:
    """
    Unified manager for Telegram Forum Topics.

    Handles:
    - Lazy creation
    - Persistent caching (MongoDB)
    - Automatic recovery (if topic deleted in Telegram)
    - Race-condition safety (Asyncio locks)
    - Distributed safety (MongoDB atomic upserts)
    """

    def __init__(self) -> None:
        self._local_cache: Dict[str, int] = {}
        self._initialized = False
        self._lock = asyncio.Lock()

    async def restore_cache(self) -> None:
        """Loads all active topic mappings from MongoDB into memory (GAP 8 FIX)."""
        if self._initialized:
            return

        try:
            db = DatabaseManager.get_db()
            cursor = db["user_topics"].find({})
            async for doc in cursor:
                u_id = doc["user_id"]
                t_type = doc["topic_type"]
                t_id = doc["topic_id"]

                cache_key = f"user:{u_id}:{t_type}"
                self._local_cache[cache_key] = int(t_id)

            self._initialized = True
            logger.info("TopicManager cache restored", extra={"ctx_count": len(self._local_cache)})
        except Exception as e:
            logger.error("Failed to restore TopicManager cache", extra={"ctx_error": str(e)})

    async def get_or_create_user_topic(
        self,
        client: Client,
        user_id: int,
        topic_type: str,
    ) -> int:
        """Get or create a per-user topic of a specific type."""
        cache_key = f"user:{user_id}:{topic_type}"

        # 1. Local Cache
        if cache_key in self._local_cache:
            return self._local_cache[cache_key]

        # 2. MongoDB Lookup
        db = DatabaseManager.get_db()
        doc = await db["user_topics"].find_one({"user_id": user_id, "topic_type": topic_type})

        if doc:
            topic_id = int(doc["topic_id"])
            self._local_cache[cache_key] = topic_id
            return topic_id

        # 3. Creation with Lock
        async with self._lock:
            # Double-check
            doc = await db["user_topics"].find_one({"user_id": user_id, "topic_type": topic_type})
            if doc:
                topic_id = int(doc["topic_id"])
                self._local_cache[cache_key] = topic_id
                return topic_id

            icon = _TOPIC_ICONS.get(topic_type, "💬")

            # Attempt to get user's first name for a better topic title
            user_name = f"User {user_id}"
            try:
                user = await client.get_users(user_id)
                if user.first_name:
                    user_name = user.first_name
            except Exception:
                pass

            title = f"{icon} {user_name}"

            topic_id = await self._create_telegram_topic(client, title)

            await db["user_topics"].update_one(
                {"user_id": user_id, "topic_type": topic_type},
                {
                    "$set": {
                        "user_id": user_id,
                        "topic_type": topic_type,
                        "topic_id": topic_id,
                        "created_at": datetime.now(timezone.utc),
                        "hub_chat_id": settings.VERIFICATION_GROUP_ID,
                        "status": "pending",
                    }
                },
                upsert=True,
            )

            logger.info(
                "user_topic_persisted",
                extra={
                    "ctx_user_id": user_id,
                    "ctx_topic_type": topic_type,
                    "ctx_topic_id": topic_id
                }
            )

            self._local_cache[cache_key] = topic_id
            return topic_id

    async def get_or_create_shared_topic(
        self,
        client: Client,
        key: str,
        title: str,
    ) -> int:
        """Get or create a shared topic (e.g., 'Rejected Content' or 'Payments')."""
        cache_key = f"shared:{key}"

        if cache_key in self._local_cache:
            return self._local_cache[cache_key]

        db = DatabaseManager.get_db()
        doc = await db["bot_config"].find_one({"key": f"{key}_topic_id"})
        if doc:
            topic_id = int(doc["value"])
            self._local_cache[cache_key] = topic_id
            return topic_id

        async with self._lock:
            # Re-check
            doc = await db["bot_config"].find_one({"key": f"{key}_topic_id"})
            if doc:
                topic_id = int(doc["value"])
                self._local_cache[cache_key] = topic_id
                return topic_id

            new_topic_id = await self._create_telegram_topic(client, title)

            # Atomic upsert to prevent race conditions
            existing_doc = await db["bot_config"].find_one_and_update(
                {"key": f"{key}_topic_id"},
                {"$setOnInsert": {"key": f"{key}_topic_id", "value": str(new_topic_id)}},
                upsert=True,
                return_document=False,  # returns PRE-update doc
            )

            if existing_doc is not None:
                # Lost the race
                existing_topic_id = int(existing_doc["value"])
                try:
                    await client.delete_forum_topic(
                        chat_id=settings.VERIFICATION_GROUP_ID,
                        message_thread_id=new_topic_id,
                    )
                except Exception:
                    pass
                self._local_cache[cache_key] = existing_topic_id
                return existing_topic_id

            self._local_cache[cache_key] = new_topic_id
            return new_topic_id

    async def get_user_topic_id(self, user_id: int, topic_type: str) -> Optional[int]:
        """
        Return the topic_id for a given user+type, or None if not found.
        Read-only — does not create.
        """
        cache_key = f"user:{user_id}:{topic_type}"
        if cache_key in self._local_cache:
            return self._local_cache[cache_key]

        db = DatabaseManager.get_db()
        doc = await db["user_topics"].find_one({"user_id": user_id, "topic_type": topic_type})
        if doc:
            topic_id = int(doc["topic_id"])
            self._local_cache[cache_key] = topic_id
            return topic_id
        return None

    async def get_user_by_topic(self, topic_id: int) -> Optional[dict]:
        """
        Return the user_topics document for a given topic_id, or None if not found.
        Used by topic_router to identify which user an admin reply belongs to.
        """
        db = DatabaseManager.get_db()
        return await db["user_topics"].find_one({"topic_id": topic_id})

    async def recover_topic(self, client: Client, user_id: int, topic_type: str) -> int:
        """Forces recreation of a topic, useful if the old one was deleted."""
        logger.info(
            "Recovering topic",
            extra={"ctx_user_id": user_id, "ctx_topic_type": topic_type}
        )
        cache_key = f"user:{user_id}:{topic_type}"
        self._local_cache.pop(cache_key, None)

        db = DatabaseManager.get_db()
        await db["user_topics"].delete_one({"user_id": user_id, "topic_type": topic_type})

        return await self.get_or_create_user_topic(client, user_id, topic_type)

    async def _create_telegram_topic(self, client: Client, title: str) -> int:
        """Internal: Raw MTProto call to create a topic with retry logic."""
        import random
        from pyrogram.raw.types import Updates, UpdateNewChannelMessage, UpdateNewMessage, MessageService, MessageActionTopicCreate

        group_id = settings.VERIFICATION_GROUP_ID
        logger.info(
            "attempting_forum_topic_creation",
            extra={
                "ctx_group_id": group_id,
                "ctx_title": title,
            }
        )

        # Pre-flight check: Verify group type and forum status
        try:
            chat = await client.get_chat(group_id)
            is_forum = (chat.type == ChatType.FORUM) or getattr(chat, "is_forum", False)
            me = await chat.get_member("me")
            can_manage = me.privileges.can_manage_topics if me.privileges else False

            logger.info(
                "forum_preflight_check",
                extra={
                    "ctx_group_id": group_id,
                    "ctx_chat_type": str(chat.type),
                    "ctx_is_forum": is_forum,
                    "ctx_can_manage_topics": can_manage,
                    "ctx_bot_id": me.user.id
                }
            )

            if not is_forum:
                logger.error("verification_group_is_not_a_forum", extra={"ctx_group_id": group_id})
            if not can_manage:
                logger.error("bot_lacks_manage_topics_permission", extra={"ctx_group_id": group_id})

        except Exception as pre_err:
            logger.warning("forum_preflight_failed", extra={"ctx_error": str(pre_err)})

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
                        # Path A: Explicit UpdateNewForumTopic (if exists in this library version)
                        if type(update).__name__ == "UpdateNewForumTopic":
                            topic_id = getattr(update, "id", None)
                            if topic_id: break

                        # Path B: MessageService with MessageActionTopicCreate
                        if isinstance(update, (UpdateNewChannelMessage, UpdateNewMessage)):
                            msg = update.message
                            if isinstance(msg, MessageService) and isinstance(msg.action, MessageActionTopicCreate):
                                topic_id = msg.id
                                break

                if topic_id is None:
                    # Final Fallback: Search recently created topics by title
                    logger.warning(
                        "topic_id_not_in_updates_attempting_search_fallback",
                        extra={"ctx_title": title}
                    )
                    try:
                        async for topic in client.get_forum_topics(group_id, limit=5):
                            if topic.title == title:
                                topic_id = topic.id
                                break
                    except Exception as fallback_err:
                        logger.warning("topic_search_fallback_failed", extra={"ctx_error": str(fallback_err)})

                if topic_id is None:
                    logger.error(
                        "topic_id_extraction_failed",
                        extra={"ctx_result_type": type(result).__name__}
                    )
                    raise RuntimeError("Failed to extract topic_id from CreateForumTopic response")

                logger.info(
                    "forum_topic_created",
                    extra={"ctx_title": title, "ctx_topic_id": topic_id},
                )
                return topic_id

            except FloodWait as e:
                wait_time = int(e.value) + settings.FLOODWAIT_EXTRA_BUFFER
                logger.warning(
                    "forum_topic_creation_floodwait",
                    extra={"ctx_title": title, "ctx_wait": wait_time, "ctx_attempt": attempt + 1}
                )
                await asyncio.sleep(wait_time)

            except RPCError as e:
                logger.exception(
                    "forum_topic_creation_rpc_error",
                    extra={
                        "ctx_title": title,
                        "ctx_error_code": e.CODE,
                        "ctx_error_name": e.NAME,
                        "ctx_error_message": str(e),
                        "ctx_group_id": group_id,
                        "ctx_attempt": attempt + 1
                    }
                )
                if attempt == len(delays) - 1:
                    raise
                await asyncio.sleep(delay)

            except Exception as e:
                logger.exception(
                    "forum_topic_creation_unexpected_error",
                    extra={
                        "ctx_title": title,
                        "ctx_error_type": type(e).__name__,
                        "ctx_error_message": str(e),
                        "ctx_attempt": attempt + 1,
                        "ctx_group_id": group_id,
                    }
                )
                if attempt == len(delays) - 1:
                    raise
                await asyncio.sleep(delay)

        raise RuntimeError(f"Failed to create forum topic: {title}")

_topic_manager: Optional[TopicManager] = None

def get_topic_manager() -> TopicManager:
    global _topic_manager
    if _topic_manager is None:
        _topic_manager = TopicManager()
    return _topic_manager
