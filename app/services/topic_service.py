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

# ── Topic types ────────────────────────────────────────────────────────────────

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


# Alias so lifecycle.py can call get_topic_manager() without error
get_topic_manager = get_topic_service


class TopicService:
    """
    Manages per-user Telegram Forum Topics in the Verification Hub.

    Topic model:
      - One topic per (user_id, topic_type) pair — reused across sessions
      - Shared "Rejected Content" topic for all rejections
      - Shared "Payments" topic for all payment moderation
      - Topics are created lazily on first use

    IMPORTANT: The Verification Hub must be a Telegram Supergroup with Topics enabled.
    Bot must be admin with 'manage_topics' permission.
    """

    def __init__(self) -> None:
        self._rejected_topic_id: Optional[int] = None
        self._payments_topic_id: Optional[int] = None
        self._lock = asyncio.Lock()

    # ── Startup cache warm ────────────────────────────────────────────────────

    async def warm_cache_from_db(self) -> None:
        """
        Pre-populate in-memory singleton topic IDs from MongoDB on startup.

        Called once by AppLifecycle.start() before the bot begins handling
        updates. Prevents cold-DB reads on the very first support / payment
        topic lookup and avoids unnecessary Telegram API calls for topics
        that already exist.

        Never raises — failures are logged and the service degrades to
        lazy-load mode (topics are fetched on first use as before).
        """
        try:
            db = DatabaseManager.get_db()

            rejected_doc = await db["bot_config"].find_one({"key": "rejected_topic_id"})
            if rejected_doc and rejected_doc.get("value"):
                self._rejected_topic_id = int(rejected_doc["value"])
                logger.info(
                    "warm_cache_from_db: rejected topic ID loaded",
                    extra={"ctx_topic_id": self._rejected_topic_id},
                )

            payments_doc = await db["bot_config"].find_one({"key": "payments_topic_id"})
            if payments_doc and payments_doc.get("value"):
                self._payments_topic_id = int(payments_doc["value"])
                logger.info(
                    "warm_cache_from_db: payments topic ID loaded",
                    extra={"ctx_topic_id": self._payments_topic_id},
                )

            logger.info(
                "warm_cache_from_db: topic cache warmed",
                extra={
                    "ctx_rejected_id": self._rejected_topic_id,
                    "ctx_payments_id": self._payments_topic_id,
                },
            )
        except Exception as e:
            logger.warning(
                "warm_cache_from_db: failed to pre-load topic cache — "
                "topics will be fetched lazily on first use",
                extra={"ctx_error": str(e)},
            )

    # ── Public API ─────────────────────────────────────────────────────────────

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

        Uses find_one_and_update with $setOnInsert to prevent TOCTOU races
        where two concurrent callers each create a Telegram topic and both
        try to upsert — the loser detects the conflict and deletes its
        duplicate Telegram topic.
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

            new_topic_id = await self._create_named_topic(client, "❌ Rejected Content")

            existing_doc = await db["bot_config"].find_one_and_update(
                {"key": "rejected_topic_id"},
                {"$setOnInsert": {"key": "rejected_topic_id", "value": str(new_topic_id)}},
                upsert=True,
                return_document=False,
            )

            if existing_doc is not None:
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

            self._rejected_topic_id = new_topic_id
            logger.info(
                "Rejected content topic created",
                extra={"ctx_topic_id": new_topic_id},
            )
            return new_topic_id

    async def get_or_create_payments_topic(self, client: Client) -> int:
        """
        Return the single shared 'Payments' topic ID.
        """
        if self._payments_topic_id:
            return self._payments_topic_id

        db = DatabaseManager.get_db()
        doc = await db["bot_config"].find_one({"key": "payments_topic_id"})
        if doc:
            self._payments_topic_id = int(doc["value"])
            return self._payments_topic_id

        async with self._lock:
            doc = await db["bot_config"].find_one({"key": "payments_topic_id"})
            if doc:
                self._payments_topic_id = int(doc["value"])
                return self._payments_topic_id

            new_topic_id = await self._create_named_topic(client, "💎 Payments")

            existing_doc = await db["bot_config"].find_one_and_update(
                {"key": "payments_topic_id"},
                {"$setOnInsert": {"key": "payments_topic_id", "value": str(new_topic_id)}},
                upsert=True,
                return_document=False,
            )

            if existing_doc is not None:
                existing_topic_id = int(existing_doc["value"])
                try:
                    await client.delete_forum_topic(
                        chat_id=settings.VERIFICATION_GROUP_ID,
                        message_thread_id=new_topic_id,
                    )
                except Exception:
                    pass
                self._payments_topic_id = existing_topic_id
                return existing_topic_id

            self._payments_topic_id = new_topic_id
            logger.info("Payments topic created", extra={"ctx_topic_id": new_topic_id})
            return new_topic_id

    async def get_user_topic_id(self, user_id: int, topic_type: str) -> Optional[int]:
        """Read-only check — does NOT create the topic if missing."""
        doc = await self._fetch_topic(user_id, topic_type)
        return doc["topic_id"] if doc else None

    # ── Internal ───────────────────────────────────────────────────────────────

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
        import random
        from pyrogram import raw
        from pyrogram.errors import RPCError, Forbidden

        delays = [2, 4, 8]
        for attempt, delay in enumerate(delays):
            try:
                peer = await client.resolve_peer(settings.VERIFICATION_GROUP_ID)
                result = await client.invoke(
                    raw.functions.channels.CreateForumTopic(
                        channel=peer,
                        title=title,
                        random_id=random.randint(1, 2**31 - 1),
                    )
                )
                topic_id = None
                for update in result.updates:
                    if hasattr(update, "id"):
                        topic_id = update.id
                        break
                if topic_id is None:
                    raise RuntimeError("CreateForumTopic returned no topic id")
                logger.info(
                    "Forum topic created",
                    extra={"ctx_title": title, "ctx_topic_id": topic_id},
                )
                return topic_id

            except FloodWait as e:
                wait_time = int(e.value) + settings.FLOODWAIT_EXTRA_BUFFER
                logger.warning(
                    "forum_topic_creation_floodwait",
                    extra={"ctx_title": title, "ctx_wait": wait_time, "ctx_attempt": attempt + 1},
                )
                await asyncio.sleep(wait_time)

            except Forbidden as e:
                logger.error(
                    "forum_topic_creation_forbidden",
                    extra={
                        "ctx_title": title,
                        "ctx_error": str(e),
                        "ctx_note": "Bot may lack 'manage_topics' permission or forum topics are disabled.",
                    },
                )
                raise

            except RPCError as e:
                logger.error(
                    "forum_topic_creation_rpc_error",
                    extra={"ctx_title": title, "ctx_error": str(e), "ctx_attempt": attempt + 1},
                    exc_info=True,
                )
                if attempt == len(delays) - 1:
                    raise
                await asyncio.sleep(delay)

            except Exception as e:
                logger.error(
                    "forum_topic_creation_unexpected_error",
                    extra={"ctx_title": title, "ctx_error": str(e), "ctx_attempt": attempt + 1},
                    exc_info=True,
                )
                if attempt == len(delays) - 1:
                    raise
                await asyncio.sleep(delay)

        raise RuntimeError(f"Failed to create topic after {len(delays)} attempts: {title}")