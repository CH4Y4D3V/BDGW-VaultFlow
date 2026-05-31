from __future__ import annotations

import asyncio
from datetime import datetime, timezone
from typing import Optional, Dict

from pyrogram.client import Client
from pyrogram.enums import ChatType  # FIX BUG D: was missing
from pyrogram.errors import FloodWait, RPCError
from pyrogram import raw

from app.config import settings
from app.core.database import DatabaseManager
from app.utils.logger import get_logger

logger = get_logger(__name__)

# ── Topic type constants ──────────────────────────────────────────────────────
TOPIC_CONTENT  = "content"
TOPIC_SUPPORT  = "support"
TOPIC_PAYMENT  = "payment"
TOPIC_REJECTED = "rejected"

_TOPIC_ICONS = {
    TOPIC_CONTENT:  "📤",
    TOPIC_SUPPORT:  "🆘",
    TOPIC_PAYMENT:  "💎",
    TOPIC_REJECTED: "❌",
}


class TopicManager:
    """
    Unified manager for Telegram Forum Topics.

    FIX (BUG D): Added `from pyrogram.enums import ChatType` — was used in
    `_create_telegram_topic` preflight check but never imported.

    FIX (GAP 8 — Restart Safety): `warm_cache_from_db()` populates
    `_local_cache` from MongoDB on startup, preventing spurious new topic
    creation for users whose topics already exist but whose IDs were lost
    when the in-memory cache was cleared on restart.

    Handles:
    - Lazy creation with distributed lock (MongoDB atomic upsert)
    - Persistent caching (MongoDB)
    - In-process caching (Python dict)
    - Automatic recovery if topic deleted in Telegram
    - Race-condition safety (asyncio lock per manager instance)
    """

    _instance: Optional["TopicManager"] = None
    _lock = asyncio.Lock()

    @classmethod
    def get_instance(cls) -> "TopicManager":
        if cls._instance is None:
            cls._instance = TopicManager()
        return cls._instance

    def __init__(self) -> None:
        self._local_cache: Dict[str, int] = {}

    async def warm_cache_from_db(self) -> int:
        """
        FIX (GAP 8): Pre-populate in-memory cache from MongoDB on bot startup.
        Prevents new topic creation for existing user topics after a restart.
        Returns number of topics loaded.
        """
        try:
            db = DatabaseManager.get_db()
            count = 0
            async for doc in db["user_topics"].find({}):
                user_id = doc.get("user_id")
                topic_type = doc.get("topic_type")
                topic_id = doc.get("topic_id")
                if user_id and topic_type and topic_id:
                    cache_key = f"user:{user_id}:{topic_type}"
                    self._local_cache[cache_key] = int(topic_id)
                    count += 1

            # Also warm shared topics
            async for doc in db["bot_config"].find({"key": {"$regex": "_topic_id$"}}):
                key = doc.get("key", "").replace("_topic_id", "")
                value = doc.get("value")
                if key and value:
                    self._local_cache[f"shared:{key}"] = int(value)
                    count += 1

            logger.info(
                "topic_cache_warmed_from_db",
                extra={"ctx_count": count},
            )
            return count
        except Exception as e:
            logger.warning(
                "topic_cache_warm_failed",
                extra={"ctx_error": str(e)},
            )
            return 0

    async def get_or_create_user_topic(
        self,
        client: Client,
        user_id: int,
        topic_type: str,
    ) -> int:
        """Get or create a per-user topic of a specific type."""
        cache_key = f"user:{user_id}:{topic_type}"

        # 1. Local in-process cache
        if cache_key in self._local_cache:
            return self._local_cache[cache_key]

        # 2. MongoDB lookup
        db = DatabaseManager.get_db()
        doc = await db["user_topics"].find_one(
            {"user_id": user_id, "topic_type": topic_type}
        )

        if doc:
            topic_id = int(doc["topic_id"])
            self._local_cache[cache_key] = topic_id
            return topic_id

        # 3. Creation with distributed lock
        async with self._lock:
            # Double-check after acquiring lock
            doc = await db["user_topics"].find_one(
                {"user_id": user_id, "topic_type": topic_type}
            )
            if doc:
                topic_id = int(doc["topic_id"])
                self._local_cache[cache_key] = topic_id
                return topic_id

            icon = _TOPIC_ICONS.get(topic_type, "💬")

            # Fetch user's name for a readable topic title
            user_name = f"User {user_id}"
            try:
                user = await client.get_users(user_id)
                if user.first_name:
                    user_name = user.first_name
                    if user.last_name:
                        user_name += f" {user.last_name}"
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
                "user_topic_created_and_persisted",
                extra={
                    "ctx_user_id": user_id,
                    "ctx_topic_type": topic_type,
                    "ctx_topic_id": topic_id,
                },
            )

            self._local_cache[cache_key] = topic_id
            return topic_id

    async def get_user_topic_id(
        self, user_id: int, topic_type: str
    ) -> Optional[int]:
        """Check if a topic exists without creating one. Returns None if not found."""
        cache_key = f"user:{user_id}:{topic_type}"
        if cache_key in self._local_cache:
            return self._local_cache[cache_key]

        db = DatabaseManager.get_db()
        doc = await db["user_topics"].find_one(
            {"user_id": user_id, "topic_type": topic_type}
        )
        if doc:
            topic_id = int(doc["topic_id"])
            self._local_cache[cache_key] = topic_id
            return topic_id
        return None

    async def get_user_by_topic(self, topic_id: int) -> Optional[dict]:
        """Find the user_topics document for a given topic_id."""
        db = DatabaseManager.get_db()
        return await db["user_topics"].find_one({"topic_id": topic_id})

    async def get_or_create_shared_topic(
        self,
        client: Client,
        key: str,
        title: str,
    ) -> int:
        """Get or create a shared topic (e.g. 'Rejected Content', 'Payments')."""
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

            existing_doc = await db["bot_config"].find_one_and_update(
                {"key": f"{key}_topic_id"},
                {"$setOnInsert": {"key": f"{key}_topic_id", "value": str(new_topic_id)}},
                upsert=True,
                return_document=False,
            )

            if existing_doc is not None:
                # Lost the race — delete the duplicate we just created
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

    async def recover_topic(
        self, client: Client, user_id: int, topic_type: str
    ) -> int:
        """Force recreation of a topic (e.g. if the old one was deleted in Telegram)."""
        logger.info(
            "Recovering topic",
            extra={"ctx_user_id": user_id, "ctx_topic_type": topic_type},
        )
        cache_key = f"user:{user_id}:{topic_type}"
        self._local_cache.pop(cache_key, None)

        db = DatabaseManager.get_db()
        await db["user_topics"].delete_one(
            {"user_id": user_id, "topic_type": topic_type}
        )

        return await self.get_or_create_user_topic(client, user_id, topic_type)

    # ── Internal topic creation ───────────────────────────────────────────────

    async def _create_telegram_topic(self, client: Client, title: str) -> int:
        """Raw MTProto call to create a forum topic. Retries on FloodWait/RPCError."""
        import random
        from pyrogram.raw.types import (
            Updates,
            UpdateNewChannelMessage,
            UpdateNewMessage,
            MessageService,
        )
        from pyrogram.raw.types import MessageActionTopicCreate

        group_id = settings.VERIFICATION_GROUP_ID

        # Pre-flight check
        try:
            chat = await client.get_chat(group_id)
            # FIX BUG D: ChatType is now imported
            is_forum = (chat.type == ChatType.SUPERGROUP) and getattr(
                chat, "is_forum", False
            )
            if not is_forum:
                logger.warning(
                    "verification_group_may_not_be_forum",
                    extra={
                        "ctx_group_id": group_id,
                        "ctx_chat_type": str(chat.type),
                        "ctx_is_forum": getattr(chat, "is_forum", False),
                    },
                )
        except Exception as preflight_err:
            logger.warning(
                "topic_preflight_check_failed",
                extra={"ctx_error": str(preflight_err)},
            )

        delays = [2, 4, 8]
        for attempt, delay in enumerate(delays):
            try:
                peer = await client.resolve_peer(group_id)

                result = await client.invoke(
                    raw.functions.channels.CreateForumTopic(
                        channel=peer,
                        title=title,
                        random_id=random.randint(1, 2**63 - 1),
                    )
                )

                topic_id = None
                if isinstance(result, Updates):
                    for update in result.updates:
                        if type(update).__name__ == "UpdateNewForumTopic":
                            topic_id = getattr(update, "id", None)
                            if topic_id:
                                break

                        if isinstance(
                            update, (UpdateNewChannelMessage, UpdateNewMessage)
                        ):
                            msg = update.message
                            if isinstance(msg, MessageService) and isinstance(
                                msg.action, MessageActionTopicCreate
                            ):
                                topic_id = msg.id
                                break

                if topic_id is None:
                    # Fallback: search recently created topics by title
                    try:
                        async for topic in client.get_forum_topics(group_id, limit=5):
                            if topic.title == title:
                                topic_id = topic.id
                                break
                    except Exception as fallback_err:
                        logger.warning(
                            "topic_search_fallback_failed",
                            extra={"ctx_error": str(fallback_err)},
                        )

                if topic_id is None:
                    raise RuntimeError(
                        f"Failed to extract topic_id from CreateForumTopic "
                        f"response for title '{title}'"
                    )

                logger.info(
                    "forum_topic_created",
                    extra={"ctx_title": title, "ctx_topic_id": topic_id},
                )
                return topic_id

            except FloodWait as e:
                wait_time = int(e.value) + settings.FLOODWAIT_EXTRA_BUFFER
                logger.warning(
                    "forum_topic_creation_floodwait",
                    extra={
                        "ctx_title": title,
                        "ctx_wait": wait_time,
                        "ctx_attempt": attempt + 1,
                    },
                )
                await asyncio.sleep(wait_time)

            except RPCError as e:
                logger.error(
                    "forum_topic_creation_rpc_error",
                    extra={
                        "ctx_title": title,
                        "ctx_error_code": getattr(e, "CODE", "?"),
                        "ctx_error_name": getattr(e, "NAME", "?"),
                        "ctx_error_message": str(e),
                        "ctx_attempt": attempt + 1,
                    },
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
                        "ctx_attempt": attempt + 1,
                    },
                )
                if attempt == len(delays) - 1:
                    raise
                await asyncio.sleep(delay)

        raise RuntimeError(f"Failed to create forum topic after all retries: {title}")


def get_topic_manager() -> TopicManager:
    return TopicManager.get_instance()
