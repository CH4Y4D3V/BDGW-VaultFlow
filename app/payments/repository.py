from __future__ import annotations

from datetime import datetime, timezone
from typing import Optional

from motor.motor_asyncio import AsyncIOMotorDatabase
from pymongo import ASCENDING, ReturnDocument
from pymongo.errors import OperationFailure

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

    async def release_processing_lock(self, payment_id: str) -> None:
        """Release a PROCESSING lock, reverting back to UNDER_REVIEW."""
        await self._collection.update_one(
            {"_id": payment_id, "status": "processing"},
            {
                "$set": {
                    "status": "under_review",
                    "updated_at": datetime.now(timezone.utc),
                },
                "$unset": {"locked_at": ""},
            },
        )

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
        """Returns the most recent non-final session for a user."""
        doc = await self._collection.find_one(
            {
                "user_id": user_id,
                "status": {"$in": [
                    PaymentStatus.WAITING_PAYMENT_DETAILS.value,
                    PaymentStatus.WAITING_TXID.value,
                    PaymentStatus.WAITING_SCREENSHOT.value,
                    PaymentStatus.SUBMITTED.value,
                    PaymentStatus.UNDER_REVIEW.value,
                    PaymentStatus.PROCESSING.value,
                ]},
            },
            sort=[("created_at", -1)],
        )
        return PaymentSession.from_dict(doc) if doc else None

    async def acquire_processing_lock(self, payment_id: str) -> bool:
        """Atomic lock to prevent duplicate approval/rejection."""
        result = await self._collection.find_one_and_update(
            {
                "_id": payment_id,
                "status": PaymentStatus.UNDER_REVIEW.value,
            },
            {
                "$set": {
                    "status": PaymentStatus.PROCESSING.value,
                    "locked_at": datetime.now(timezone.utc),
                }
            },
            return_document=ReturnDocument.AFTER,
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
            "timestamp": datetime.now(timezone.utc),
        })

    async def map_topic(self, topic_id: int, payment_id: str) -> None:
        await self._topics_collection.update_one(
            {"_id": topic_id},
            {"$set": {"payment_id": payment_id}},
            upsert=True,
        )

    async def get_payment_by_topic(self, topic_id: int) -> Optional[str]:
        doc = await self._topics_collection.find_one({"_id": topic_id})
        return doc["payment_id"] if doc else None

    async def get_sessions_by_statuses(self, statuses: list) -> list:
        """Fetch all payment sessions matching any of the given statuses."""
        status_values = [s.value if hasattr(s, "value") else str(s) for s in statuses]
        docs = await self._collection.find(
            {"status": {"$in": status_values}}
        ).to_list(length=None)
        return [PaymentSession.from_dict(d) for d in docs]

    async def reset_stuck_processing(self) -> int:
        """Reset sessions stuck in PROCESSING back to UNDER_REVIEW (crash recovery)."""
        result = await self._collection.update_many(
            {"status": "processing"},
            {
                "$set": {
                    "status": "under_review",
                    "updated_at": datetime.now(timezone.utc),
                }
            },
        )
        return result.modified_count

    async def create_indexes(self) -> None:
        """
        Create payment collection indexes with conflict-safe drop-and-recreate.

        The txid unique index previously failed silently if an old index with a
        different name existed on the same field. This method now drops conflicting
        indexes before retrying, making it idempotent across deployments.
        """
        index_specs = [
            # payments collection
            (self._collection, "payments", [
                {"keys": [("user_id", ASCENDING)], "name": "payments_user_id"},
                {"keys": [("status", ASCENDING)], "name": "payments_status"},
                {"keys": [("user_id", ASCENDING), ("status", ASCENDING)], "name": "payments_user_status"},
                # txid must be globally unique to prevent duplicate transaction abuse
                {
                    "keys": [("txid", ASCENDING)],
                    "name": "payments_txid_unique",
                    "unique": True,
                    "sparse": True,  # NULL txid allowed (session not yet submitted)
                },
            ]),
            # audit collection
            (self._audit_collection, "payment_audit", [
                {"keys": [("payment_id", ASCENDING)], "name": "audit_payment_id"},
            ]),
            # topics collection
            (self._topics_collection, "payment_topics", [
                {"keys": [("payment_id", ASCENDING)], "name": "topic_payment_id"},
            ]),
            # timeouts collection
            (self._timeouts_collection, "payment_timeouts", [
                {"keys": [("expires_at", ASCENDING)], "name": "timeout_expiry"},
            ]),
            # subscription history
            (self._history_collection, "subscription_history", [
                {"keys": [("user_id", ASCENDING)], "name": "history_user_id"},
                {"keys": [("payment_id", ASCENDING)], "name": "history_payment_id"},
            ]),
        ]

        for collection, label, specs in index_specs:
            await self._safe_create_indexes(collection, label, specs)

        logger.info("Payment repository indexes created")

    @staticmethod
    async def _safe_create_indexes(collection, label: str, specs: list) -> None:
        """
        Create indexes from a list of spec dicts. On IndexOptionsConflict (code 85),
        drop conflicting old indexes by name and retry once.
        """
        from pymongo import IndexModel

        # Copy specs to avoid mutating the originals across retry attempts.
        specs_copy = [dict(s) for s in specs]
        index_models = [
            IndexModel(spec.pop("keys"), **spec)
            for spec in specs_copy
        ]

        try:
            # Proactively detect option mismatches before attempting creation.
            existing = await collection.list_indexes().to_list(length=100)
            for spec in specs:
                name = spec.get("name")
                if not name:
                    continue
                found = next((idx for idx in existing if idx["name"] == name), None)
                if found:
                    if (
                        spec.get("unique", False) != found.get("unique", False)
                        or spec.get("sparse", False) != found.get("sparse", False)
                    ):
                        logger.warning(
                            f"Index options mismatch for {label}:{name}. Dropping for recreation.",
                        )
                        await collection.drop_index(name)

            await collection.create_indexes(index_models)

        except OperationFailure as e:
            if e.code == 85:  # IndexOptionsConflict
                logger.warning(
                    f"Index conflict on {label} — dropping all non-id indexes and retrying",
                    extra={"ctx_error": str(e)},
                )
                try:
                    await collection.drop_indexes()
                    # Rebuild index models from original (unmodified) specs.
                    index_models_retry = [
                        IndexModel(
                            s["keys"],
                            **{k: v for k, v in s.items() if k != "keys"},
                        )
                        for s in specs
                    ]
                    await collection.create_indexes(index_models_retry)
                    logger.info(f"{label} indexes reconciled after full drop")
                except Exception as retry_err:
                    logger.error(
                        f"Failed to reconcile indexes for {label}",
                        extra={"ctx_error": str(retry_err)},
                        exc_info=True,
                    )
            else:
                logger.error(
                    f"Index creation error for {label}",
                    extra={"ctx_error": str(e), "ctx_code": e.code},
                    exc_info=True,
                )
