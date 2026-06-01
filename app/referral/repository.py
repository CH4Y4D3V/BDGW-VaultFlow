from __future__ import annotations

from datetime import datetime, timezone
from typing import Optional

from motor.motor_asyncio import AsyncIOMotorDatabase
from pymongo import ASCENDING, ReturnDocument

from app.payments.models import PaymentSession, PaymentStatus
from app.utils.logger import get_logger

logger = get_logger(__name__)


class PaymentRepository:
    def __init__(self, db: AsyncIOMotorDatabase) -> None:
        self._db = db
        self._collection = db["payments"]
        self._audit_collection = db["payment_audit"]
        self._topics_collection = db["payment_topics"]
        self._timeouts_collection = db["payment_timeouts"]
        self._history_collection = db["subscription_history"]

    # ── Session CRUD ──────────────────────────────────────────────────────────

    async def save_session(self, session: PaymentSession) -> None:
        await self._collection.replace_one(
            {"_id": session.id},
            session.to_dict(),
            upsert=True,
        )

    async def get_session(self, payment_id: str) -> Optional[PaymentSession]:
        doc = await self._collection.find_one({"_id": payment_id})
        return PaymentSession.from_dict(doc) if doc else None

    async def get_active_session(self, user_id: int) -> Optional[PaymentSession]:
        """Return the most recent non-terminal session for a user."""
        terminal = [
            PaymentStatus.APPROVED.value,
            PaymentStatus.REJECTED.value,
            PaymentStatus.EXPIRED.value,
            PaymentStatus.CANCELLED.value,
        ]
        doc = await self._collection.find_one(
            {
                "user_id": user_id,
                "status": {"$nin": terminal},
            },
            sort=[("created_at", -1)],
        )
        return PaymentSession.from_dict(doc) if doc else None

    # ── TXID uniqueness ───────────────────────────────────────────────────────

    async def get_by_txid(self, txid: str) -> Optional[dict]:
        """
        Return ANY payment document that has this TXID, regardless of status.

        Checking all statuses (including cancelled/expired) is intentional —
        it prevents TXID reuse across separate payment attempts, which is a
        common fraud vector.

        Returns the raw dict (not a PaymentSession) for minimal overhead.
        """
        if not txid:
            return None
        return await self._collection.find_one(
            {"txid": txid.strip()},
            {"_id": 1, "user_id": 1, "status": 1},  # projection — we only need existence
        )

    # ── Atomic processing lock ────────────────────────────────────────────────

    async def acquire_processing_lock(self, payment_id: str) -> bool:
        """
        Atomically transition UNDER_REVIEW → PROCESSING.

        Returns True if the lock was acquired (this admin claimed it first).
        Returns False if another admin already claimed it or the session is
        no longer in UNDER_REVIEW state.

        This is the critical guard against double-approval/double-rejection.
        """
        result = await self._collection.find_one_and_update(
            {
                "_id": payment_id,
                "status": PaymentStatus.UNDER_REVIEW.value,
            },
            {
                "$set": {
                    "status": PaymentStatus.PROCESSING.value,
                    "locked_at": datetime.now(timezone.utc),
                    "updated_at": datetime.now(timezone.utc),
                }
            },
            return_document=ReturnDocument.AFTER,
        )
        return result is not None

    # ── Timeout scheduling ────────────────────────────────────────────────────

    async def schedule_timeout(
        self,
        payment_id: str,
        user_id: int,
        expires_at: datetime,
    ) -> None:
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

    async def get_expired_timeouts(self) -> list[dict]:
        """Return timeout records whose expires_at has passed (for recovery worker)."""
        now = datetime.now(timezone.utc)
        return await self._timeouts_collection.find(
            {"expires_at": {"$lte": now}}
        ).to_list(length=None)

    async def get_pending_warnings(self, warning_field: str, cutoff: datetime) -> list[dict]:
        """
        Return timeout records where the warning hasn't been sent yet
        and the cutoff time has passed.
        Used by the timeout monitor to send 5-min and 10-min warnings.
        """
        return await self._timeouts_collection.find(
            {
                warning_field: False,
                "expires_at": {"$lte": cutoff},
            }
        ).to_list(length=None)

    async def mark_warning_sent(self, payment_id: str, warning_field: str) -> None:
        await self._timeouts_collection.update_one(
            {"_id": payment_id},
            {"$set": {warning_field: True}},
        )

    # ── Audit log ─────────────────────────────────────────────────────────────

    async def log_event(
        self,
        payment_id: str,
        event: str,
        metadata: dict,
    ) -> None:
        await self._audit_collection.insert_one({
            "payment_id": payment_id,
            "event": event,
            "metadata": metadata,
            "timestamp": datetime.now(timezone.utc),
        })

    # ── Topic mapping ─────────────────────────────────────────────────────────

    async def map_topic(self, topic_id: int, payment_id: str) -> None:
        await self._topics_collection.update_one(
            {"_id": topic_id},
            {"$set": {"payment_id": payment_id}},
            upsert=True,
        )

    async def get_payment_by_topic(self, topic_id: int) -> Optional[str]:
        doc = await self._topics_collection.find_one({"_id": topic_id})
        return doc["payment_id"] if doc else None

    # ── Subscription history ──────────────────────────────────────────────────

    async def record_subscription_history(
        self,
        payment_id: str,
        data: dict,
    ) -> None:
        await self._history_collection.insert_one({
            "payment_id": payment_id,
            "created_at": datetime.now(timezone.utc),
            **data,
        })

    # ── Stuck session recovery ────────────────────────────────────────────────

    async def reset_stuck_processing(self) -> int:
        """
        On startup: sessions stuck in PROCESSING state indicate a crash
        during approval/rejection. Reset them to UNDER_REVIEW so admins
        can action them again.
        """
        result = await self._collection.update_many(
            {"status": PaymentStatus.PROCESSING.value},
            {
                "$set": {
                    "status": PaymentStatus.UNDER_REVIEW.value,
                    "updated_at": datetime.now(timezone.utc),
                }
            },
        )
        if result.modified_count:
            logger.warning(
                "Reset stuck PROCESSING sessions to UNDER_REVIEW",
                extra={"ctx_count": result.modified_count},
            )
        return result.modified_count

    # ── Index creation ────────────────────────────────────────────────────────

    async def create_indexes(self) -> None:
        await self._collection.create_index([("user_id", ASCENDING)])
        await self._collection.create_index([("status", ASCENDING)])
        await self._collection.create_index(
            [("txid", ASCENDING)],
            sparse=True,  # sparse: sessions without txid don't pollute the index
        )
        await self._collection.create_index([("created_at", ASCENDING)])
        await self._collection.create_index(
            [("user_id", ASCENDING), ("status", ASCENDING)],
            name="user_active_session_lookup",
        )
        await self._audit_collection.create_index([("payment_id", ASCENDING)])
        await self._timeouts_collection.create_index([("expires_at", ASCENDING)])
        await self._history_collection.create_index([("user_id", ASCENDING)])
        await self._history_collection.create_index([("payment_id", ASCENDING)])
        logger.info("Payment repository indexes created")
