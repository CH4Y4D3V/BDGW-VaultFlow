from __future__ import annotations

import asyncio
from datetime import datetime, timezone
from typing import Optional

from pyrogram.client import Client
from pyrogram.errors import FloodWait, RPCError

from app.config import settings
from app.core.database import DatabaseManager
from app.utils.logger import get_logger

logger = get_logger(__name__)

# ── Topic types ───────────────────────────────────────────────────────────────

TOPIC_CONTENT = "content"
TOPIC_SUPPORT = "support"
TOPIC_PAYMENT = "payment"

_TOPIC_ICONS = {
    TOPIC_CONTENT: "📤",
    TOPIC_SUPPORT: "🆘",
    TOPIC_PAYMENT: "💎",
}

# Singleton — shared across all callers in-process
_instance: Optional["TopicService"] = None


def get_topic_service() -> "TopicService":
    global _instance
    if _instance is None:
        _instance = TopicService()
    return _instance


class TopicService:
    """
    Manages per-user Telegram Forum Topics in the Verification Hub.

    Topic model:
      - One topic per (user_id, topic_type) pair — reused across sessions
      - Shared "Rejected Content" topic for all rejections (prevents topic explosion)
      - Topics are created lazily on first use

    IMPORTANT: The Verification Hub must be a Telegram Supergroup with Topics enabled.
    Bot must be admin with 'manage_topics' permission.

    Telegram hard limit: ~9000 topics per supergroup.
    With reuse strategy (one per user), this supports 9000 users before cleanup needed.
    Use `archive_old_topics()` if you need to recycle slots.
    """

    def __init__(self) -> None:
        self._rejected_topic_id: Optional[int] = None
        self._lock = asyncio.Lock()

    # ── Public API ────────────────────────────────────────────────────────────

    async def get_or_create_user_topic(
        self,
        client: Client,
        user_id: int,
        topic_type: str,
    ) -> int:
        """
        Return the existing topic_id for (user_id, topic_type),
        or create a new Forum Topic and persist it.
        """
        existing = await self._fetch_topic(user_id, topic_type)
        if existing:
            return existing["topic_id"]

        async with self._lock:
            # Double-check after acquiring lock (concurrent creation race)
            existing = await self._fetch_topic(user_id, topic_type)
            if existing:
                return existing["topic_id"]

            topic_id = await self._create_topic(client, user_id, topic_type)
            await self._persist_topic(user_id, topic_type, topic_id)
            return topic_id

    async def get_user_by_topic(self, topic_id: int) -> Optional[dict]:
        """Return {user_id, topic_type} for a given topic_id, or None."""
        db = DatabaseManager.get_db()
        return await db["user_topics"].find_one({"topic_id": topic_id})

    async def get_or_create_rejected_topic(self, client: Client) -> int:
        """
        Return the single shared 'Rejected Content' topic ID.
        All rejections go here — never create per-user topics for rejections.

        WARNING fix (TOCTOU race): the previous implementation used a plain
        upsert after creating the Telegram topic, which had a race window where
        two concurrent callers could both create Telegram topics and both try to
        upsert, leaving a duplicate orphaned Telegram topic.

        Fix: use find_one_and_update with upsert=True and $setOnInsert so only
        ONE document is ever written atomically. If the document already existed
        (return value is not None), we use the existing topic_id and delete the
        duplicate Telegram topic we just created.
        """
        if self._rejected_topic_id:
            return self._rejected_topic_id

        db = DatabaseManager.get_db()
        doc = await db["bot_config"].find_one({"key": "rejected_topic_id"})
        if doc:
            self._rejected_topic_id = int(doc["value"])
            return self._rejected_topic_id

        async with self._lock:
            # Re-check inside lock
            doc = await db["bot_config"].find_one({"key": "rejected_topic_id"})
            if doc:
                self._rejected_topic_id = int(doc["value"])
                return self._rejected_topic_id

            # Create the Telegram forum topic first (outside DB transaction scope)
            new_topic_id = await self._create_named_topic(client, "❌ Rejected Content")

            # WARNING fix: atomic findOneAndUpdate with $setOnInsert.
            # return_document=False returns the PRE-UPDATE document.
            # If pre-update doc is not None → the key already existed (race lost),
            # so use the existing value and delete our newly created duplicate.
            existing_doc = await db["bot_config"].find_one_and_update(
                {"key": "rejected_topic_id"},
                {"$setOnInsert": {"key": "rejected_topic_id", "value": str(new_topic_id)}},
                upsert=True,
                return_document=False,  # returns PRE-update doc; None means we inserted
            )

            if existing_doc is not None:
                # Another concurrent caller already created and persisted the topic.
                # Use the existing one and clean up the duplicate we just created.
                existing_topic_id = int(existing_doc["value"])
                logger.warning(
                    "get_or_create_rejected_topic: race lost — existing topic found, "
                    "deleting duplicate Telegram topic",
                    extra={
                        "ctx_existing_topic_id": existing_topic_id,
                        "ctx_duplicate_topic_id": new_topic_id,
                    },
                )
                try:
                    await client.delete_forum_topic(
                        chat_id=settings.VERIFICATION_GROUP_ID,
                        message_thread_id=new_topic_id,
                    )
                except Exception as e:
                    logger.warning(
                        "get_or_create_rejected_topic: could not delete duplicate topic",
                        extra={"ctx_topic_id": new_topic_id, "ctx_error": str(e)},
                    )
                self._rejected_topic_id = existing_topic_id
                return existing_topic_id

            # We won the race — new_topic_id is now the canonical value in DB
            self._rejected_topic_id = new_topic_id
            logger.info(
                "Rejected content topic created",
                extra={"ctx_topic_id": new_topic_id},
            )
            return new_topic_id

    async def get_user_topic_id(self, user_id: int, topic_type: str) -> Optional[int]:
        """Read-only check — does NOT create the topic if missing."""
        doc = await self._fetch_topic(user_id, topic_type)
        return doc["topic_id"] if doc else None

    # ── Internal ──────────────────────────────────────────────────────────────

    async def _fetch_topic(self, user_id: int, topic_type: str) -> Optional[dict]:
        db = DatabaseManager.get_db()
        return await db["user_topics"].find_one(
            {"user_id": user_id, "topic_type": topic_type}
        )

    async def _persist_topic(self, user_id: int, topic_type: str, topic_id: int) -> None:
        db = DatabaseManager.get_db()
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

    async def _create_topic(self, client: Client, user_id: int, topic_type: str) -> int:
        icon = _TOPIC_ICONS.get(topic_type, "💬")
        title = f"{icon} User {user_id}"
        return await self._create_named_topic(client, title)

    async def _create_named_topic(self, client: Client, title: str) -> int:
        for attempt in range(3):
            try:
                topic = await client.create_forum_topic(
                    chat_id=settings.VERIFICATION_GROUP_ID,
                    title=title,
                )
                logger.info("Forum topic created", extra={"ctx_title": title, "ctx_topic_id": topic.id})
                return topic.id
            except FloodWait as e:
                await asyncio.sleep(int(e.value) + settings.FLOODWAIT_EXTRA_BUFFER)
            except RPCError as e:
                logger.error(
                    "Failed to create forum topic",
                    extra={"ctx_title": title, "ctx_error": str(e), "ctx_attempt": attempt + 1},
                )
                if attempt == 2:
                    raise
                await asyncio.sleep(2 ** attempt)
        raise RuntimeError(f"Failed to create topic: {title}")