from __future__ import annotations

from datetime import datetime, timezone
from typing import Optional

from motor.motor_asyncio import AsyncIOMotorDatabase
from pymongo import ASCENDING, ReturnDocument

from app.payments.models import PaymentSession, PaymentStatus
from app.utils.logger import get_logger

logger = get_logger(__name__)


class PaymentRepository:
    def __init__(self, db: AsyncIOMotorDatabase):
        self._db = db
        self._collection = db["payments"]
        self._audit_collection = db["payment_audit"]
        self._topics_collection = db["payment_topics"]
        self._timeouts_collection = db["payment_timeouts"]
        self._history_collection = db["subscription_history"]

    async def save_session(self, session: PaymentSession) -> None:
        await self._collection.replace_one(
            {"_id": session.id},
            session.to_dict(),
            upsert=True
        )

    async def get_session(self, payment_id: str) -> Optional[PaymentSession]:
        doc = await self._collection.find_one({"_id": payment_id})
        return PaymentSession.from_dict(doc) if doc else None

    async def get_active_session(self, user_id: int) -> Optional[PaymentSession]:
        """Returns the most recent non-final session for a user."""
        doc = await self._collection.find_one(
            {
                "user_id": user_id,
                "status": {"$in": [
                    PaymentStatus.WAITING_PAYMENT_DETAILS.value,
                    PaymentStatus.AWAITING_PAYMENT.value,
                    PaymentStatus.WAITING_SCREENSHOT.value,
                    PaymentStatus.SUBMITTED.value,
                    PaymentStatus.UNDER_REVIEW.value,
                    PaymentStatus.PROCESSING.value
                ]}
            },
            sort=[("created_at", -1)]
        )
        return PaymentSession.from_dict(doc) if doc else None

    async def acquire_processing_lock(self, payment_id: str) -> bool:
        """Atomic lock to prevent duplicate approval/rejection."""
        result = await self._collection.find_one_and_update(
            {
                "_id": payment_id,
                "status": PaymentStatus.UNDER_REVIEW.value
            },
            {
                "$set": {
                    "status": PaymentStatus.PROCESSING.value,
                    "locked_at": datetime.now(timezone.utc)
                }
            },
            return_document=ReturnDocument.AFTER
        )
        return result is not None

    async def schedule_timeout(self, payment_id: str, user_id: int, expires_at) -> None:
        await self._timeouts_collection.update_one(
            {"_id": payment_id},
            {
                "$set": {
                    "payment_id": payment_id,
                    "user_id": user_id,
                    "expires_at": expires_at,
                    "five_minute_warning_sent": False,
                    "ten_minute_warning_sent": False,
                    "created_at": datetime.now(timezone.utc),
                }
            },
            upsert=True,
        )

    async def clear_timeout(self, payment_id: str) -> None:
        await self._timeouts_collection.delete_one({"_id": payment_id})

    async def record_subscription_history(self, payment_id: str, data: dict) -> None:
        await self._history_collection.insert_one({
            "payment_id": payment_id,
            "created_at": datetime.now(timezone.utc),
            **data,
        })

    async def log_event(self, payment_id: str, event: str, metadata: dict) -> None:
        await self._audit_collection.insert_one({
            "payment_id": payment_id,
            "event": event,
            "metadata": metadata,
            "timestamp": datetime.now(timezone.utc)
        })

    async def map_topic(self, topic_id: int, payment_id: str) -> None:
        await self._topics_collection.update_one(
            {"_id": topic_id},
            {"$set": {"payment_id": payment_id}},
            upsert=True
        )

    async def get_payment_by_topic(self, topic_id: int) -> Optional[str]:
        doc = await self._topics_collection.find_one({"_id": topic_id})
        return doc["payment_id"] if doc else None

    async def create_indexes(self) -> None:
        try:
            await self._collection.create_index([("user_id", ASCENDING)], name="pay_user_id")
            await self._collection.create_index([("status", ASCENDING)], name="pay_status")
            await self._collection.create_index([("expires_at", ASCENDING)], name="pay_expires_at")
            await self._audit_collection.create_index([("payment_id", ASCENDING)], name="pay_audit_id")
            await self._topics_collection.create_index([("payment_id", ASCENDING)], name="pay_topic_id")
            await self._timeouts_collection.create_index([("expires_at", ASCENDING)], name="pay_timeout_expiry")
            await self._history_collection.create_index([("user_id", ASCENDING)], name="pay_history_user")
            await self._history_collection.create_index([("payment_id", ASCENDING)], name="pay_history_id")
        except Exception as e:
            logger.error("payment_indexes_creation_failed", extra={"ctx_error": str(e)})
            raise
