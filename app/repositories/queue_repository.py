# ARCHITECTURE NOTE: Queue is implemented on MongoDB via find_one_and_update atomic claiming.
# Redis is available in the environment but not used for queuing.
# The MongoDB approach provides: atomic job claiming, dead-letter promotion, stale lock recovery.
# Trade-off vs Redis Streams: 2s polling interval instead of pub/sub instant pickup.
# This is acceptable for current volume. Redis Streams can be added later for sub-second latency.

from datetime import datetime, timedelta, timezone
from typing import Optional, List, Any
from motor.motor_asyncio import AsyncIOMotorDatabase, AsyncIOMotorClientSession
from bson import ObjectId
from pymongo import ReturnDocument
from pymongo.errors import DuplicateKeyError
from app.config import settings
from app.core.models import QueueJob, JobStatus, QueueMetrics, WatermarkState
from app.core.exceptions import (
    JobNotFoundError, 
    DuplicateJobError, 
    InvalidQueueJobError, 
    VaultReferenceMissingError,
    ConsistencyViolationError,
    QuarantineError
)
from app.utils.logger import get_logger

logger = get_logger(__name__)


class QueueRepository:
    def __init__(self, db: AsyncIOMotorDatabase):
        self._db = db
        self._queue = db[settings.QUEUE_COLLECTION]
        self._dlq = db[settings.DEAD_LETTER_COLLECTION]
        self._metrics = db[settings.METRICS_COLLECTION]
        self._locks = db[settings.LOCK_COLLECTION]

    # ─── Enqueue ─────────────────────────────────────────────────────────────

    async def enqueue(self, job: QueueJob) -> str:
        """Strictly validates and enqueues a job."""
        if job.schema_version < 1:
            raise InvalidQueueJobError("Job must have schema_version >= 1")
        
        if not job.vault_chat_id or not job.vault_message_id:
            raise VaultReferenceMissingError(f"Job {job.content_id} missing vault references")

        doc = job.model_dump(by_alias=False, exclude={"id"})
        doc["created_at"] = datetime.now(timezone.utc)
        doc["updated_at"] = datetime.now(timezone.utc)

        try:
            result = await self._queue.insert_one(doc)
        except DuplicateKeyError:
            raise DuplicateJobError(
                f"Active job already exists for content_id={job.content_id}"
            )

        job_id = str(result.inserted_id)
        logger.info(
            "Job enqueued",
            extra={
                "ctx_job_id": job_id,
                "ctx_content_id": job.content_id,
                "ctx_vault_msg": job.vault_message_id,
                "ctx_status": job.status,
            },
        )
        return job_id

    # ─── Claim / Lock ─────────────────────────────────────────────────────────

    async def claim_watermark_jobs(self, worker_id: str, batch_size: int = 1) -> List[dict]:
        """Atomically claim jobs for watermarking, supporting media groups."""
        from app.core.database import DatabaseManager
        use_transactions = DatabaseManager.transactions_supported()
        
        now = datetime.now(timezone.utc)
        claimed = []
        claimed_ids = []

        if use_transactions:
            async with await self._db.client.start_session() as session:
                for _ in range(batch_size):
                    # 1. Find an eligible candidate
                    doc = await self._queue.find_one(
                        {
                            "status": JobStatus.WATERMARKING,
                            "_id": {"$nin": claimed_ids},
                            "watermark_required": True,
                            "watermark_state": WatermarkState.PENDING,
                            "locked_by": None,
                        },
                        sort=[("priority", -1), ("created_at", 1)],
                        session=session
                    )
                    if not doc:
                        break

                    group_id = doc.get("media_group_id")
                    
                    async with session.start_transaction():
                        if group_id:
                            # 2. Claim entire group atomically
                            result = await self._queue.update_many(
                                {
                                    "media_group_id": group_id,
                                    "status": JobStatus.WATERMARKING,
                                    "locked_by": None,
                                },
                                {
                                    "$set": {
                                        "status": JobStatus.LOCKED,
                                        "locked_by": worker_id,
                                        "locked_at": now,
                                        "updated_at": now,
                                        "watermark_state": WatermarkState.PROCESSING,
                                    }
                                },
                                session=session
                            )
                            if result.modified_count > 0:
                                cursor = self._queue.find({
                                    "media_group_id": group_id,
                                    "locked_by": worker_id,
                                }).sort("album_sequence_index", 1)
                                async for g_doc in cursor:
                                    if g_doc["_id"] not in claimed_ids:
                                        claimed.append(g_doc)
                                        claimed_ids.append(g_doc["_id"])
                        else:
                            # 2. Claim single job
                            res = await self._queue.find_one_and_update(
                                {"_id": doc["_id"], "locked_by": None},
                                {
                                    "$set": {
                                        "status": JobStatus.LOCKED,
                                        "locked_by": worker_id,
                                        "locked_at": now,
                                        "updated_at": now,
                                        "watermark_state": WatermarkState.PROCESSING,
                                    }
                                },
                                return_document=ReturnDocument.AFTER,
                                session=session
                            )
                            if res:
                                claimed.append(res)
                                claimed_ids.append(res["_id"])
        else:
            # Standalone MongoDB: Use atomic find_one_and_update (no multi-document atomicity for albums)
            for _ in range(batch_size):
                res = await self._queue.find_one_and_update(
                    {
                        "status": JobStatus.WATERMARKING,
                        "watermark_required": True,
                        "watermark_state": WatermarkState.PENDING,
                        "locked_by": None,
                        "_id": {"$nin": claimed_ids}
                    },
                    {
                        "$set": {
                            "status": JobStatus.LOCKED,
                            "locked_by": worker_id,
                            "locked_at": now,
                            "updated_at": now,
                            "watermark_state": WatermarkState.PROCESSING,
                        }
                    },
                    sort=[("priority", -1), ("created_at", 1)],
                    return_document=ReturnDocument.AFTER
                )
                if res:
                    claimed.append(res)
                    claimed_ids.append(res["_id"])
                else:
                    break

        return claimed

    async def claim_next(self, worker_id: str, batch_size: int = 1) -> List[dict]:
        """
        Atomically claim the next N pending jobs.
        If a claimed job is part of a media group, atomically claims the ENTIRE group.
        """
        from app.core.database import DatabaseManager
        use_transactions = DatabaseManager.transactions_supported()

        now = datetime.now(timezone.utc)
        claimed = []
        claimed_ids = []

        if use_transactions:
            async with await self._db.client.start_session() as session:
                for _ in range(batch_size):
                    # 1. Find candidate
                    doc = await self._queue.find_one(
                        {
                            "status": JobStatus.PENDING,
                            "_id": {"$nin": claimed_ids},
                            "$or": [
                                {"execute_after": None},
                                {"execute_after": {"$lte": now}},
                            ],
                            "locked_by": None,
                        },
                        sort=[("priority", -1), ("execute_after", 1), ("_id", 1)],
                        session=session
                    )
                    if not doc:
                        break

                    group_id = doc.get("media_group_id")
                    async with session.start_transaction():
                        if group_id:
                            # 2. Claim group
                            result = await self._queue.update_many(
                                {
                                    "media_group_id": group_id,
                                    "status": JobStatus.PENDING,
                                    "locked_by": None,
                                },
                                {
                                    "$set": {
                                        "status": JobStatus.LOCKED,
                                        "locked_by": worker_id,
                                        "locked_at": now,
                                        "updated_at": now,
                                    }
                                },
                                session=session
                            )
                            if result.modified_count > 0:
                                cursor = self._queue.find({
                                    "media_group_id": group_id,
                                    "locked_by": worker_id,
                                }).sort("album_sequence_index", 1)
                                async for g_doc in cursor:
                                    if g_doc["_id"] not in claimed_ids:
                                        claimed.append(g_doc)
                                        claimed_ids.append(g_doc["_id"])
                        else:
                            # 2. Claim single
                            res = await self._queue.find_one_and_update(
                                {"_id": doc["_id"], "locked_by": None},
                                {
                                    "$set": {
                                        "status": JobStatus.LOCKED,
                                        "locked_by": worker_id,
                                        "locked_at": now,
                                        "updated_at": now,
                                    }
                                },
                                return_document=ReturnDocument.AFTER,
                                session=session
                            )
                            if res:
                                claimed.append(res)
                                claimed_ids.append(res["_id"])
        else:
            # Standalone MongoDB fallback
            for _ in range(batch_size):
                res = await self._queue.find_one_and_update(
                    {
                        "status": JobStatus.PENDING,
                        "locked_by": None,
                        "$or": [
                            {"execute_after": None},
                            {"execute_after": {"$lte": now}},
                        ],
                        "_id": {"$nin": claimed_ids}
                    },
                    {
                        "$set": {
                            "status": JobStatus.LOCKED,
                            "locked_by": worker_id,
                            "locked_at": now,
                            "updated_at": now,
                        }
                    },
                    sort=[("priority", -1), ("execute_after", 1), ("_id", 1)],
                    return_document=ReturnDocument.AFTER
                )
                if res:
                    claimed.append(res)
                    claimed_ids.append(res["_id"])
                else:
                    break

        return claimed

        return claimed

    # ─── State Transitions ────────────────────────────────────────────────────

    async def mark_processing(self, job_id: str, worker_id: str) -> bool:
        """Transition from LOCKED to PROCESSING."""
        result = await self._queue.update_one(
            {"_id": ObjectId(job_id), "locked_by": worker_id, "status": JobStatus.LOCKED},
            {
                "$set": {
                    "status": JobStatus.PROCESSING,
                    "updated_at": datetime.now(timezone.utc),
                }
            },
        )
        return result.modified_count > 0

    async def mark_ready(self, job_id: str, worker_id: str) -> bool:
        """Transition to READY state after watermarking or preparation."""
        result = await self._queue.update_one(
            {"_id": ObjectId(job_id), "locked_by": worker_id},
            {
                "$set": {
                    "status": JobStatus.READY,
                    "locked_by": None,
                    "locked_at": None,
                    "updated_at": datetime.now(timezone.utc),
                }
            },
        )
        return result.modified_count > 0

    async def mark_delivering(self, job_id: str, worker_id: str) -> bool:
        """Transition to DELIVERING state."""
        result = await self._queue.update_one(
            {"_id": ObjectId(job_id), "locked_by": worker_id, "status": {"$in": [JobStatus.LOCKED, JobStatus.PROCESSING]}},
            {
                "$set": {
                    "status": JobStatus.DELIVERING,
                    "updated_at": datetime.now(timezone.utc),
                }
            },
        )
        return result.modified_count > 0

    async def release_claim(self, job_id: str) -> None:
        """Release lock and return to PENDING or WATERMARKING."""
        doc = await self._queue.find_one({"_id": ObjectId(job_id)})
        if not doc:
            return

        next_status = JobStatus.WATERMARKING if (
            doc.get("watermark_required") and doc.get("watermark_state") == WatermarkState.PENDING
        ) else JobStatus.PENDING

        await self._queue.update_one(
            {"_id": ObjectId(job_id)},
            {
                "$set": {
                    "status": next_status,
                    "locked_by": None,
                    "locked_at": None,
                    "updated_at": datetime.now(timezone.utc),
                }
            },
        )

    # ─── Progress Updates ─────────────────────────────────────────────────────

    async def record_target_delivered(self, job_id: str, target_id: str) -> None:
        """Record a successful delivery to a target."""
        await self._queue.update_one(
            {"_id": ObjectId(job_id)},
            {
                "$addToSet": {"delivered_targets": target_id},
                "$set": {"updated_at": datetime.now(timezone.utc)}
            }
        )

    async def record_target_failed(self, job_id: str, target_id: str, error: str) -> None:
        """Record a failed delivery attempt to a target."""
        await self._queue.update_one(
            {"_id": ObjectId(job_id)},
            {
                "$set": {
                    f"failed_targets.{target_id}": error,
                    "updated_at": datetime.now(timezone.utc)
                }
            }
        )

    # ─── Idempotency Locks ────────────────────────────────────────────────────

    async def acquire_delivery_lock(self, job_id: str, target_id: str, ttl_seconds: int = 3600) -> bool:
        """Acquire a distributed delivery lock for a specific target."""
        lock_key = f"delivery:{job_id}:{target_id}"
        expires_at = datetime.now(timezone.utc) + timedelta(seconds=ttl_seconds)
        try:
            await self._locks.insert_one({
                "lock_key": lock_key,
                "expires_at": expires_at,
                "created_at": datetime.now(timezone.utc),
            })
            return True
        except DuplicateKeyError:
            # Check if it's expired (though Mongo TTL should handle it, we be safe)
            existing = await self._locks.find_one({"lock_key": lock_key})
            if existing and existing["expires_at"] < datetime.now(timezone.utc):
                await self._locks.delete_one({"lock_key": lock_key})
                return await self.acquire_delivery_lock(job_id, target_id, ttl_seconds)
            return False

    async def extend_delivery_lock(self, job_id: str, target_id: str, ttl_seconds: int = 3600) -> bool:
        """Extend an existing delivery lock (heartbeat)."""
        lock_key = f"delivery:{job_id}:{target_id}"
        expires_at = datetime.now(timezone.utc) + timedelta(seconds=ttl_seconds)
        result = await self._locks.update_one(
            {"lock_key": lock_key},
            {"$set": {"expires_at": expires_at}}
        )
        return result.modified_count > 1

    async def release_delivery_lock(self, job_id: str, target_id: str) -> None:
        """Release a delivery lock."""
        await self._locks.delete_one({"lock_key": f"delivery:{job_id}:{target_id}"})

    # ─── Completion & Failure ─────────────────────────────────────────────────

    async def mark_completed(self, job_id: str) -> None:
        """Mark job as completed and cleanup."""
        await self._queue.update_one(
            {"_id": ObjectId(job_id)},
            {
                "$set": {
                    "status": JobStatus.COMPLETED,
                    "completed_at": datetime.now(timezone.utc),
                    "updated_at": datetime.now(timezone.utc),
                    "locked_by": None,
                    "locked_at": None,
                }
            },
        )

    async def mark_failed(
        self,
        job_id: str,
        error: str,
        next_retry_delay_seconds: Optional[float] = None,
        increment_retry: bool = True,
    ) -> dict:
        """Mark job as failed and schedule retry."""
        now = datetime.now(timezone.utc)
        execute_after = (
            now + timedelta(seconds=next_retry_delay_seconds)
            if next_retry_delay_seconds
            else now
        )

        update_ops: dict = {
            "$set": {
                "status": JobStatus.PENDING,
                "last_error": error,
                "last_error_at": now,
                "execute_after": execute_after,
                "locked_by": None,
                "locked_at": None,
                "updated_at": now,
            }
        }
        if increment_retry:
            update_ops["$inc"] = {"retry_count": 1}

        return await self._queue.find_one_and_update(
            {"_id": ObjectId(job_id)},
            update_ops,
            return_document=ReturnDocument.AFTER,
        )

    async def move_to_dead_letter(self, job_id: str, final_error: str) -> str:
        """Move job to DLQ."""
        job_doc = await self._queue.find_one({"_id": ObjectId(job_id)})
        if not job_doc:
            raise JobNotFoundError(f"Job {job_id} not found")

        dlq_doc = {
            "original_job_id": job_id,
            "content_id": job_doc["content_id"],
            "failure_reason": "max_retries_exceeded",
            "final_error": final_error,
            "dead_at": datetime.now(timezone.utc),
            "metadata": job_doc.get("metadata", {}),
        }

        result = await self._dlq.update_one(
            {"original_job_id": job_id},
            {"$set": dlq_doc},
            upsert=True
        )
        
        await self._queue.update_one(
            {"_id": ObjectId(job_id)},
            {"$set": {"status": JobStatus.DEAD, "updated_at": datetime.now(timezone.utc)}}
        )

        return str(result.upserted_id or job_id)

    async def move_to_quarantine(self, job_id: str, reason: str) -> None:
        """Move unrecoverable job to quarantine."""
        await self._queue.update_one(
            {"_id": ObjectId(job_id)},
            {
                "$set": {
                    "status": JobStatus.QUARANTINE,
                    "quarantine_reason": reason,
                    "updated_at": datetime.now(timezone.utc),
                    "locked_by": None,
                    "locked_at": None,
                }
            }
        )
        logger.warning("Job moved to quarantine", extra={"ctx_job_id": job_id, "ctx_reason": reason})

    # ─── Atomic Album Operations ──────────────────────────────────────────────

    async def swap_album_vault_references(self, media_group_id: str, new_refs: List[dict]) -> None:
        """
        Atomically replace all vault references for an album.
        new_refs: list of {"album_sequence_index": int, "vault_message_id": int}
        """
        async with await self._db.client.start_session() as session:
            async with session.start_transaction():
                for ref in new_refs:
                    result = await self._queue.update_one(
                        {
                            "media_group_id": media_group_id,
                            "album_sequence_index": ref["album_sequence_index"],
                        },
                        {
                            "$set": {
                                "vault_message_id": ref["vault_message_id"],
                                "watermark_state": WatermarkState.COMPLETED,
                                "status": JobStatus.PENDING,
                                "updated_at": datetime.now(timezone.utc),
                            }
                        },
                        session=session
                    )
                    if result.modified_count == 0:
                        raise ConsistencyViolationError(f"Failed to swap reference for album {media_group_id} index {ref['album_sequence_index']}")

    # ─── Stale Lock Recovery ──────────────────────────────────────────────────

    async def recover_stale_jobs(self) -> int:
        """Recover jobs from crashed workers."""
        threshold = datetime.now(timezone.utc) - timedelta(
            seconds=settings.STALE_LOCK_THRESHOLD_SECONDS
        )
        now = datetime.now(timezone.utc)

        result = await self._queue.update_many(
            {
                "status": JobStatus.LOCKED,
                "locked_at": {"$lt": threshold},
            },
            {
                "$set": {
                    "status": JobStatus.PENDING,
                    "locked_by": None,
                    "locked_at": None,
                    "updated_at": now,
                }
            },
        )
        return result.modified_count

    # ─── Metrics ─────────────────────────────────────────────────────────────

    async def collect_metrics(self) -> QueueMetrics:
        pipeline = [{"$group": {"_id": "$status", "count": {"$sum": 1}}}]
        counts: dict[str, int] = {}
        async for doc in self._queue.aggregate(pipeline):
            counts[doc["_id"]] = doc["count"]

        metrics = QueueMetrics(
            pending_count=counts.get(JobStatus.PENDING, 0),
            processing_count=counts.get(JobStatus.PROCESSING, 0) + counts.get(JobStatus.LOCKED, 0) + counts.get(JobStatus.DELIVERING, 0),
            completed_count=counts.get(JobStatus.COMPLETED, 0),
            failed_count=counts.get(JobStatus.FAILED, 0),
            dead_count=counts.get(JobStatus.DEAD, 0),
        )
        return metrics

    async def get_job_by_id(self, job_id: str) -> Optional[dict]:
        return await self._queue.find_one({"_id": ObjectId(job_id)})

    async def get_user_queue(self, user_id: int, limit: int = 10) -> List[dict]:
        """Fetch pending/processing jobs for a specific user."""
        cursor = self._queue.find(
            {
                "metadata.submitter_user_id": user_id,
                "status": {"$in": [
                    JobStatus.PENDING, 
                    JobStatus.LOCKED, 
                    JobStatus.PROCESSING, 
                    JobStatus.WATERMARKING, 
                    JobStatus.READY, 
                    JobStatus.DELIVERING
                ]}
            },
            sort=[("created_at", -1)],
            limit=limit
        )
        return await cursor.to_list(length=limit)