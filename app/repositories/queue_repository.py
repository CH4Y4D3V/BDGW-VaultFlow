import uuid
from datetime import datetime, timedelta, timezone
from typing import Optional, List, Any
from motor.motor_asyncio import AsyncIOMotorDatabase
from bson import ObjectId
from app.config import settings
from app.core.models import QueueJob, DeadLetterJob, JobStatus, QueueMetrics
from app.core.exceptions import JobNotFoundError, DuplicateJobError
from app.utils.logger import get_logger

logger = get_logger(__name__)


class QueueRepository:
    def __init__(self, db: AsyncIOMotorDatabase):
        self._db = db
        self._queue = db[settings.QUEUE_COLLECTION]
        self._dlq = db[settings.DEAD_LETTER_COLLECTION]
        self._metrics = db[settings.METRICS_COLLECTION]

    # ─── Enqueue ─────────────────────────────────────────────────────────────

    async def enqueue(self, job: QueueJob) -> str:
        """
        Insert a new job. Raises DuplicateJobError if the same content_id
        is already pending/processing for any of the same target channels.
        """
        existing = await self._queue.find_one(
            {
                "content_id": job.content_id,
                "status": {"$in": [JobStatus.PENDING, JobStatus.PROCESSING, JobStatus.LOCKED]},
            }
        )
        if existing:
            raise DuplicateJobError(
                f"Active job already exists for content_id={job.content_id}"
            )

        doc = job.model_dump(by_alias=False, exclude={"id"})
        doc["created_at"] = datetime.now(timezone.utc)
        doc["updated_at"] = datetime.now(timezone.utc)

        result = await self._queue.insert_one(doc)
        job_id = str(result.inserted_id)
        logger.info(
            "Job enqueued",
            extra={
                "ctx_job_id": job_id,
                "ctx_content_id": job.content_id,
                "ctx_targets": len(job.target_channel_ids),
            },
        )
        return job_id

    # ─── Claim / Lock ─────────────────────────────────────────────────────────

    async def claim_next(self, worker_id: str, batch_size: int = 1) -> List[dict]:
        """
        Atomically claim the next N pending jobs using find_one_and_update.
        Returns raw dicts to avoid repeated serialization overhead.
        """
        now = datetime.now(timezone.utc)
        claimed = []

        for _ in range(batch_size):
            doc = await self._queue.find_one_and_update(
                {
                    "status": JobStatus.PENDING,
                    "$or": [
                        {"execute_after": None},
                        {"execute_after": {"$lte": now}},
                    ],
                },
                {
                    "$set": {
                        "status": JobStatus.LOCKED,
                        "locked_by": worker_id,
                        "locked_at": now,
                        "updated_at": now,
                    }
                },
                sort=[("priority", -1), ("scheduled_at", 1)],
                return_document=True,
            )
            if doc:
                claimed.append(doc)

        return claimed

    async def mark_processing(self, job_id: str, worker_id: str) -> bool:
        result = await self._queue.update_one(
            {"_id": ObjectId(job_id), "locked_by": worker_id},
            {
                "$set": {
                    "status": JobStatus.PROCESSING,
                    "processing_started_at": datetime.now(timezone.utc),
                    "updated_at": datetime.now(timezone.utc),
                }
            },
        )
        return result.modified_count > 0

    # ─── Progress Updates ─────────────────────────────────────────────────────

    async def record_target_delivered(self, job_id: str, target_id: str) -> None:
        await self._queue.update_one(
            {"_id": ObjectId(job_id)},
            {
                "$addToSet": {"delivered_targets": target_id},
                "$set": {"updated_at": datetime.now(timezone.utc)},
            },
        )

    async def record_target_failed(self, job_id: str, target_id: str, error: str) -> None:
        await self._queue.update_one(
            {"_id": ObjectId(job_id)},
            {
                "$set": {
                    f"failed_targets.{target_id}": error,
                    "updated_at": datetime.now(timezone.utc),
                }
            },
        )

    async def mark_watermark_applied(self, job_id: str, processed_path: str) -> None:
        await self._queue.update_one(
            {"_id": ObjectId(job_id)},
            {
                "$set": {
                    "watermark_applied": True,
                    "processed_media_path": processed_path,
                    "status": JobStatus.PENDING,
                    "updated_at": datetime.now(timezone.utc),
                }
            },
        )

    # ─── Completion ───────────────────────────────────────────────────────────

    async def mark_completed(self, job_id: str) -> None:
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
        logger.info("Job completed", extra={"ctx_job_id": job_id})

    async def mark_failed(
        self,
        job_id: str,
        error: str,
        next_retry_delay_seconds: Optional[float] = None,
    ) -> dict:
        now = datetime.now(timezone.utc)
        execute_after = (
            now + timedelta(seconds=next_retry_delay_seconds)
            if next_retry_delay_seconds
            else now
        )

        doc = await self._queue.find_one_and_update(
            {"_id": ObjectId(job_id)},
            {
                "$inc": {"retry_count": 1},
                "$set": {
                    "status": JobStatus.PENDING,
                    "last_error": error,
                    "last_error_at": now,
                    "execute_after": execute_after,
                    "locked_by": None,
                    "locked_at": None,
                    "updated_at": now,
                },
            },
            return_document=True,
        )
        return doc

    async def move_to_dead_letter(self, job_id: str, final_error: str) -> str:
        job_doc = await self._queue.find_one({"_id": ObjectId(job_id)})
        if not job_doc:
            raise JobNotFoundError(f"Job {job_id} not found")

        dlq_doc = {
            "original_job_id": job_id,
            "content_id": job_doc["content_id"],
            "source_channel_id": job_doc["source_channel_id"],
            "target_channel_ids": job_doc["target_channel_ids"],
            "failure_reason": "max_retries_exceeded",
            "retry_history": [],
            "final_error": final_error,
            "dead_at": datetime.now(timezone.utc),
            "metadata": job_doc.get("metadata", {}),
        }

        result = await self._dlq.insert_one(dlq_doc)
        await self._queue.update_one(
            {"_id": ObjectId(job_id)},
            {"$set": {"status": JobStatus.DEAD, "updated_at": datetime.now(timezone.utc)}},
        )

        logger.error(
            "Job moved to dead letter queue",
            extra={
                "ctx_job_id": job_id,
                "ctx_dlq_id": str(result.inserted_id),
                "ctx_error": final_error,
            },
        )
        return str(result.inserted_id)

    # ─── Stale Lock Recovery ──────────────────────────────────────────────────

    async def recover_stale_processing_jobs(self) -> int:
        """
        On worker startup or crash recovery sweep.
        Jobs that have been in LOCKED/PROCESSING for too long get reset.
        """
        threshold = datetime.now(timezone.utc) - timedelta(
            seconds=settings.STALE_LOCK_THRESHOLD_SECONDS
        )
        result = await self._queue.update_many(
            {
                "status": {"$in": [JobStatus.LOCKED, JobStatus.PROCESSING]},
                "locked_at": {"$lt": threshold},
            },
            {
                "$set": {
                    "status": JobStatus.PENDING,
                    "locked_by": None,
                    "locked_at": None,
                    "updated_at": datetime.now(timezone.utc),
                }
            },
        )
        if result.modified_count:
            logger.warning(
                f"Recovered {result.modified_count} stale jobs",
                extra={"ctx_count": result.modified_count},
            )
        return result.modified_count

    # ─── Scheduler Queries ────────────────────────────────────────────────────

    async def get_recently_posted_content_ids(
        self, source_channel_id: str, hours: int
    ) -> set[str]:
        since = datetime.now(timezone.utc) - timedelta(hours=hours)
        cursor = self._queue.find(
            {
                "source_channel_id": source_channel_id,
                "status": JobStatus.COMPLETED,
                "completed_at": {"$gte": since},
            },
            {"content_id": 1},
        )
        ids = set()
        async for doc in cursor:
            ids.add(doc["content_id"])
        return ids

    async def get_channel_pending_count(self, source_channel_id: str) -> int:
        return await self._queue.count_documents(
            {
                "source_channel_id": source_channel_id,
                "status": {"$in": [JobStatus.PENDING, JobStatus.PROCESSING, JobStatus.LOCKED]},
            }
        )

    # ─── Metrics ─────────────────────────────────────────────────────────────

    async def collect_metrics(self) -> QueueMetrics:
        pipeline = [
            {
                "$group": {
                    "_id": "$status",
                    "count": {"$sum": 1},
                }
            }
        ]
        counts: dict[str, int] = {}
        async for doc in self._queue.aggregate(pipeline):
            counts[doc["_id"]] = doc["count"]

        metrics = QueueMetrics(
            pending_count=counts.get(JobStatus.PENDING, 0),
            processing_count=counts.get(JobStatus.PROCESSING, 0)
            + counts.get(JobStatus.LOCKED, 0),
            completed_count=counts.get(JobStatus.COMPLETED, 0),
            failed_count=counts.get(JobStatus.FAILED, 0),
            dead_count=counts.get(JobStatus.DEAD, 0),
        )

        await self._metrics.insert_one(
            {**metrics.model_dump(), "collected_at": datetime.now(timezone.utc)}
        )
        return metrics

    async def get_job_by_id(self, job_id: str) -> Optional[dict]:
        return await self._queue.find_one({"_id": ObjectId(job_id)})
