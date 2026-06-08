from __future__ import annotations

import asyncio
from datetime import datetime, timedelta, timezone
from typing import Optional

from motor.motor_asyncio import AsyncIOMotorDatabase
from pyrogram.types import Message

from app.bot.ingestion import MediaIngestionPipeline
from app.config import settings
from app.core.database import DatabaseManager
from app.services.consent_service import ConsentService
from app.utils.logger import get_logger

logger = get_logger(__name__)

# ── Global In-Memory Cache (Legacy compatibility) ──────────────────────────────
_pending_submissions: dict[int, tuple[int, list[Message]]] = {}


class SubmissionService:
    """
    Service layer for managing content submissions.
    """

    def __init__(self, db: Optional[AsyncIOMotorDatabase] = None) -> None:
        self._db = db or DatabaseManager.get_db()
        self._consent = ConsentService()
        self._pipeline = MediaIngestionPipeline()

    async def has_consent(self, user_id: int) -> bool:
        """
        Check if the user has accepted the creator terms and has an active profile.
        """
        return await self._consent.is_verified_creator(user_id)

    async def create_pending_submission(
        self,
        user_id: int,
        messages: list[Message],
        hub_topic_id: int,
        hub_card_message_id: int,
    ) -> int:
        """
        Registers a new submission as pending moderation.
        Writes to both in-memory cache and MongoDB.
        """
        if not messages:
            raise ValueError("Submission requires at least one message")

        # The key is the ID of the first message in the submission
        key = messages[0].id

        # 1. Update in-memory cache (for active moderation sessions)
        _pending_submissions[key] = (user_id, messages)

        # 2. Persist to MongoDB
        try:
            col = self._db[settings.PENDING_COLLECTION]
            now = datetime.now(timezone.utc)
            doc = {
                "user_id": user_id,
                "first_msg_id": key,
                "message_ids": [m.id for m in messages],
                "hub_topic_id": hub_topic_id,
                "hub_card_message_id": hub_card_message_id,
                "status": "pending",
                "created_at": now,
                "expires_at": now + timedelta(hours=settings.QUEUE_DEADLINE_HOURS),
            }
            await col.update_one({"first_msg_id": key}, {"$set": doc}, upsert=True)
            
            logger.info(
                "submission_persisted",
                extra={"ctx_user_id": user_id, "ctx_key": key}
            )
        except Exception as e:
            logger.error(
                "submission_persistence_failed",
                extra={"ctx_user_id": user_id, "ctx_error": str(e)}
            )
            # We don't raise here because the in-memory cache is still functional
            # but it means a restart will lose this submission.

        return key

    async def get_pending(self, key: int) -> Optional[tuple[int, list[Message]]]:
        """Retrieve a pending submission from the in-memory cache."""
        return _pending_submissions.get(key)

    async def pop_pending(self, key: int) -> Optional[tuple[int, list[Message]]]:
        """Consumes a pending submission, removing it from cache and DB."""
        entry = _pending_submissions.pop(key, None)
        
        # Cleanup DB
        try:
            col = self._db[settings.PENDING_COLLECTION]
            await col.delete_one({"first_msg_id": key})
        except Exception as e:
            logger.warning("failed_to_cleanup_pending_db", extra={"ctx_key": key, "ctx_error": str(e)})
            
        return entry


# ── Legacy Function Exports (Backward Compatibility) ──────────────────────────

def get_pending_count() -> int:
    return len(_pending_submissions)

async def register_pending(user_id: int, messages: list[Message]) -> int:
    service = SubmissionService()
    # Note: legacy caller doesn't provide hub IDs, we use 0
    return await service.create_pending_submission(user_id, messages, 0, 0)

def pop_pending(msg_id: int) -> Optional[tuple[int, list[Message]]]:
    # This remains sync for legacy callers but DB cleanup will be missed or must be fire-and-forget
    entry = _pending_submissions.pop(msg_id, None)
    if entry:
        asyncio.create_task(SubmissionService()._db[settings.PENDING_COLLECTION].delete_one({"first_msg_id": msg_id}))
    return entry
