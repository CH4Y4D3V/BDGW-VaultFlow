from __future__ import annotations

import asyncio
from datetime import datetime, timezone
from typing import Optional, Dict, Any

from pyrogram.client import Client
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
    
    _instance: Optional["TopicManager"] = None
    _lock = asyncio.Lock()

    def __clinit__(self):
        pass

    @classmethod
    def get_instance(cls) -> "TopicManager":
        if cls._instance is None:
            cls._instance = TopicManager()
        return cls._instance

    def __init__(self) -> None:
        self._local_cache: Dict[str, int] = {}

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
            # Verify it's still alive (optional, or wait for failure)
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
            title = f"{icon} User {user_id}"
            
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
                    }
                },
                upsert=True,
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
                return_document=False, # returns PRE-update doc
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
        from pyrogram.raw.types import UpdateNewForumTopic, Updates

        delays = [2, 4, 8]
        for attempt, delay in enumerate(delays):
            try:
                peer = await client.resolve_peer(settings.VERIFICATION_GROUP_ID)
                
                # Verify it's a forum (optional but recommended)
                # chat = await client.get_chat(settings.VERIFICATION_GROUP_ID)
                # if not getattr(chat, "is_forum", False):
                #     raise RuntimeError(f"Group {settings.VERIFICATION_GROUP_ID} is not a Forum.")

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
                        if isinstance(update, UpdateNewForumTopic):
                            topic_id = update.id
                            break
                
                if topic_id is None:
                    # Fallback for some pyrofork versions where UpdateNewForumTopic might be nested or different
                    logger.warning(
                        "UpdateNewForumTopic not found in Updates, attempting fallback extraction",
                        extra={"ctx_result_type": type(result).__name__}
                    )
                    for update in getattr(result, "updates", []):
                        if hasattr(update, "id") and "ForumTopic" in type(update).__name__:
                            topic_id = update.id
                            break
                        if hasattr(update, "message") and hasattr(update.message, "reply_to") and hasattr(update.message.reply_to, "reply_to_top_id"):
                            topic_id = update.message.reply_to.reply_to_top_id
                            break

                if topic_id is None:
                    raise RuntimeError("Failed to extract topic_id from CreateForumTopic response")
                
                logger.info(
                    "Forum topic created",
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

            except Forbidden as e:
                logger.error(
                    "forum_topic_creation_forbidden",
                    extra={
                        "ctx_title": title,
                        "ctx_error": repr(e),
                        "ctx_group_id": settings.VERIFICATION_GROUP_ID,
                        "ctx_note": "Bot may lack 'manage_topics' permission or forum topics are disabled in group."
                    },
                    exc_info=True
                )
                raise

            except RPCError as e:
                logger.error(
                    "forum_topic_creation_rpc_error",
                    extra={
                        "ctx_title": title, 
                        "ctx_error_type": type(e).__name__,
                        "ctx_error_message": str(e),
                        "ctx_attempt": attempt + 1
                    },
                    exc_info=True
                )
                if attempt == len(delays) - 1:
                    break
                await asyncio.sleep(delay)

            except Exception as e:
                logger.error(
                    "forum_topic_creation_unexpected_error",
                    extra={
                        "ctx_title": title,
                        "ctx_error_type": type(e).__name__,
                        "ctx_error_message": str(e),
                        "ctx_attempt": attempt + 1,
                        "ctx_group_id": settings.VERIFICATION_GROUP_ID,
                    },
                    exc_info=True,
                )
                if attempt == len(delays) - 1:
                    break
                await asyncio.sleep(delay)
        
        raise RuntimeError(f"Failed to create forum topic: {title}")

def get_topic_manager() -> TopicManager:
    return TopicManager.get_instance()
