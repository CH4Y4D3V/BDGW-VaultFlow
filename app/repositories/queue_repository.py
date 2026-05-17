from datetime import datetime, timedelta, timezone
from typing import Optional, List
from motor.motor_asyncio import AsyncIOMotorDatabase
from bson import ObjectId
from pymongo.errors import DuplicateKeyError
from app.config import settings
from app.core.models import QueueJob, JobStatus, QueueMetrics
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
                "ctx_targets": len(job.target_channel_ids),
            },
        )
        return job_id

    # ─── Claim / Lock ─────────────────────────────────────────────────────────

    async def claim_watermark_jobs(self, worker_id: str, batch_size: int = 1) -> List[dict]:
        now = datetime.now(timezone.utc)
        claimed = []
        for _ in range(batch_size):
            doc = await self._queue.find_one_and_update(
                {
                    "status": JobStatus.WATERMARKING,
                    "locked_by": None,
                },
                {
                    "$set": {
                        "locked_by": worker_id,
                        "locked_at": now,
                        "updated_at": now,
                    }
                },
                sort=[("priority", -1), ("created_at", 1)],
                return_document=True,
            )
            if doc:
                claimed.append(doc)
        return claimed

    async def claim_next(self, worker_id: str, batch_size: int = 1) -> List[dict]:
        """
        Atomically claim the next N pending jobs.
        If a claimed job is part of a media group, atomically claim the ENTIRE group
        so they are processed and dispatched as an atomic album.
        """
        now = datetime.now(timezone.utc)
        claimed = []
        claimed_ids = []

        for _ in range(batch_size):
            doc = await self._queue.find_one_and_update(
                {
                    "status": JobStatus.PENDING,
                    "_id": {"$nin": claimed_ids},
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
                sort=[("priority", -1), ("execute_after", 1), ("_id", 1)],
                return_document=True,
            )
            
            if not doc:
                break
                
            group_id = doc.get("metadata", {}).get("media_group_id")
            if group_id:
                # Ensure no other items in the group are still watermarking or processing
                unready_count = await self._queue.count_documents({
                    "metadata.media_group_id": group_id,
                    "_id": {"$ne": doc["_id"]},
                    "status": {"$in": [JobStatus.WATERMARKING, JobStatus.LOCKED, JobStatus.PROCESSING]}
                })
                
                if unready_count > 0:
                    # Rollback this claim, the album is not fully ready
                    await self._queue.update_one(
                        {"_id": doc["_id"]},
                        {
                            "$set": {
                                "status": JobStatus.PENDING,
                                "locked_by": None,
                                "locked_at": None,
                                "updated_at": now,
                            }
                        }
                    )
                    claimed_ids.append(doc["_id"])
                    continue
                
                # Atomically lock the rest of the media group
                result = await self._queue.update_many(
                    {
                        "status": JobStatus.PENDING,
                        "metadata.media_group_id": group_id,
                        "_id": {"$ne": doc["_id"]},
                    },
                    {
                        "$set": {
                            "status": JobStatus.LOCKED,
                            "locked_by": worker_id,
                            "locked_at": now,
                            "updated_at": now,
                        }
                    }
                )
                
                claimed.append(doc)
                claimed_ids.append(doc["_id"])

                if result.modified_count > 0:
                    cursor = self._queue.find({
                        "status": JobStatus.LOCKED,
                        "locked_by": worker_id,
                        "metadata.media_group_id": group_id,
                        "_id": {"$ne": doc["_id"]}
                    }).sort([("metadata.message_id", 1), ("_id", 1)])
                    
                    async for sibling in cursor:
                        claimed.append(sibling)
                        claimed_ids.append(sibling["_id"])
            else:
                claimed.append(doc)
                claimed_ids.append(doc["_id"])

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

    async def release_claim(self, job_id: str) -> None:
        """Revert a locked/processing job back to pending (e.g., lock contention, graceful shutdown)."""
        doc = await self._queue.find_one({"_id": ObjectId(job_id)})
        if not doc:
            return

        next_status = JobStatus.WATERMARKING if (
            doc.get("watermark_required") and not doc.get("watermark_applied")
        ) else JobStatus.PENDING

        await self._queue.update_one(
            {"_id": ObjectId(job_id)},
            {
                "$set": {
                    "status": next_status,
                    "locked_by": None,
                    "locked_at": None,
                    "processing_started_at": None,
                    "updated_at": datetime.now(timezone.utc),
                }
            },
        )

    # ─── Progress Updates ─────────────────────────────────────────────────────

    async def record_target_delivered(self, job_id: str, target_id: str) -> None:
        await self._queue.update_one(
            {"_id": ObjectId(job_id)},
            {
                "$addToSet": {"delivered_targets": target_id},
                "$set": {
                    "updated_at": datetime.now(timezone.utc),
                    "locked_at": datetime.now(timezone.utc),
                },
            },
        )

    async def record_target_failed(self, job_id: str, target_id: str, error: str) -> None:
        await self._queue.update_one(
            {"_id": ObjectId(job_id)},
            {
                "$set": {
                    f"failed_targets.{target_id}": error,
                    "updated_at": datetime.now(timezone.utc),
                    "locked_at": datetime.now(timezone.utc),
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
                    "locked_by": None,
                    "locked_at": None,
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
        increment_retry: bool = True,
    ) -> dict:
        job_doc = await self._queue.find_one({"_id": ObjectId(job_id)})
        if not job_doc:
            raise JobNotFoundError(f"Job {job_id} not found")

        now = datetime.now(timezone.utc)
        execute_after = (
            now + timedelta(seconds=next_retry_delay_seconds)
            if next_retry_delay_seconds
            else now
        )

        next_status = JobStatus.WATERMARKING if (
            job_doc.get("watermark_required") and not job_doc.get("watermark_applied")
        ) else JobStatus.PENDING

        update_ops = {
            "$set": {
                "status": next_status,
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

        doc = await self._queue.find_one_and_update(
            {"_id": ObjectId(job_id)},
            update_ops,
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

        try:
            result = await self._dlq.insert_one(dlq_doc)
            dlq_id = str(result.inserted_id)
        except DuplicateKeyError:
            existing_dlq = await self._dlq.find_one({"original_job_id": job_id})
            dlq_id = str(existing_dlq["_id"]) if existing_dlq else "unknown"
            logger.warning("Job already exists in dead letter queue", extra={"ctx_job_id": job_id})

        await self._queue.update_one(
            {"_id": ObjectId(job_id)},
            {"$set": {"status": JobStatus.DEAD, "updated_at": datetime.now(timezone.utc)}},
        )

        logger.error(
            "Job moved to dead letter queue",
            extra={
                "ctx_job_id": job_id,
                "ctx_dlq_id": dlq_id,
                "ctx_error": final_error,
            },
        )
        return dlq_id

    # ─── Stale Lock Recovery ──────────────────────────────────────────────────

    async def recover_stale_processing_jobs(self) -> int:
        """
        On worker startup or crash recovery sweep.
        Jobs that have been in LOCKED/PROCESSING for too long get reset.
        """
        threshold = datetime.now(timezone.utc) - timedelta(
            seconds=settings.STALE_LOCK_THRESHOLD_SECONDS
        )
        now = datetime.now(timezone.utc)
        
        result_dispatch = await self._queue.update_many(
            {
                "status": {"$in": [JobStatus.LOCKED, JobStatus.PROCESSING]},
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
        
        result_wm = await self._queue.update_many(
            {
                "status": JobStatus.WATERMARKING,
                "locked_at": {"$lt": threshold},
            },
            {
                "$set": {
                    "locked_by": None,
                    "locked_at": None,
                    "updated_at": now,
                }
            },
        )
        
        total = result_dispatch.modified_count + result_wm.modified_count
        if total:
            logger.warning(
                f"Recovered {total} stale jobs",
                extra={"ctx_count": total},
            )
        return total

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

        def _get_count(status_val) -> int:
            val = status_val.value if hasattr(status_val, "value") else status_val
            return counts.get(val, 0)

        metrics = QueueMetrics(
            pending_count=_get_count(JobStatus.PENDING),
            processing_count=_get_count(JobStatus.PROCESSING) + _get_count(JobStatus.LOCKED),
            completed_count=_get_count(JobStatus.COMPLETED),
            failed_count=_get_count(JobStatus.FAILED),
            dead_count=_get_count(JobStatus.DEAD),
        )

        await self._metrics.insert_one(
            {**metrics.model_dump(), "collected_at": datetime.now(timezone.utc)}
        )
        return metrics

    async def get_job_by_id(self, job_id: str) -> Optional[dict]:
        return await self._queue.find_one({"_id": ObjectId(job_id)})
