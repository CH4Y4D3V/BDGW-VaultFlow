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
        # Vault collection handle used by mark_completed() to release the
        # distribution_state lock and set cooldown_until after delivery so the
        # vault replay pool respects the configured fairness window.
        self._vault = db[getattr(settings, "VAULT_COLLECTION", "vault")]

    # ─── Enqueue ─────────────────────────────────────────────────────────────

    async def enqueue(self, job: QueueJob) -> str:
        """
        Strictly validate and enqueue a job.
        Raises InvalidQueueJobError if schema_version < 1.
        Raises VaultReferenceMissingError if vault references are absent.
        Raises DuplicateJobError if an active job already exists for content_id.
        Returns the inserted job_id as a string.
        """
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
        """
        Atomically claim jobs for watermarking, supporting media groups.
        If a claimed job belongs to a media group, the entire group is claimed atomically.
        Uses MongoDB multi-document transactions when the deployment supports them
        (replica set / mongos). Falls back to find_one_and_update otherwise.

        BUG FIX: In the transaction path, the find() used to retrieve claimed group
        docs now passes session=session so in-transaction uncommitted writes are visible.
        """
        from app.core.database import DatabaseManager
        use_transactions = DatabaseManager.transactions_supported()

        now = datetime.now(timezone.utc)
        claimed = []
        claimed_ids = []

        if use_transactions:
            async with await self._db.client.start_session() as session:
                for _ in range(batch_size):
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
                                # FIX: pass session so the find sees the in-transaction writes.
                                cursor = self._queue.find(
                                    {
                                        "media_group_id": group_id,
                                        "locked_by": worker_id,
                                    },
                                    session=session,
                                ).sort("album_sequence_index", 1)
                                async for g_doc in cursor:
                                    if g_doc["_id"] not in claimed_ids:
                                        claimed.append(g_doc)
                                        claimed_ids.append(g_doc["_id"])
                        else:
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
        Uses MongoDB multi-document transactions when supported; falls back to
        find_one_and_update on standalone instances.

        BUG FIX: In the transaction path, the find() used to retrieve claimed group
        docs now passes session=session so in-transaction uncommitted writes are visible.
        """
        from app.core.database import DatabaseManager
        use_transactions = DatabaseManager.transactions_supported()

        now = datetime.now(timezone.utc)
        claimed = []
        claimed_ids = []

        if use_transactions:
            async with await self._db.client.start_session() as session:
                for _ in range(batch_size):
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
                                # FIX: pass session so the find sees the in-transaction writes.
                                cursor = self._queue.find(
                                    {
                                        "media_group_id": group_id,
                                        "locked_by": worker_id,
                                    },
                                    session=session,
                                ).sort("album_sequence_index", 1)
                                async for g_doc in cursor:
                                    if g_doc["_id"] not in claimed_ids:
                                        claimed.append(g_doc)
                                        claimed_ids.append(g_doc["_id"])
                        else:
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

    # ─── State Transitions ────────────────────────────────────────────────────

    async def mark_processing(self, job_id: str, worker_id: str) -> bool:
        """
        Transition a job from LOCKED to PROCESSING.
        Returns True if the document was updated, False if the job was not found
        or was not in LOCKED state owned by this worker.
        """
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
        """
        Transition a job to READY state after watermarking or preparation.
        Clears the lock so the delivery dispatcher can claim the job.
        Returns True if the document was updated.
        """
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
        """
        Transition a job to DELIVERING state.
        Accepts jobs in LOCKED or PROCESSING state owned by this worker.
        Returns True if the document was updated.
        """
        result = await self._queue.update_one(
            {
                "_id": ObjectId(job_id),
                "locked_by": worker_id,
                "status": {"$in": [JobStatus.LOCKED, JobStatus.PROCESSING]},
            },
            {
                "$set": {
                    "status": JobStatus.DELIVERING,
                    "updated_at": datetime.now(timezone.utc),
                }
            },
        )
        return result.modified_count > 0

    async def release_claim(self, job_id: str) -> None:
        """
        Release the lock on a job and return it to its correct pre-lock status.
        Jobs whose watermarking never completed are returned to WATERMARKING;
        all others go back to PENDING.

        FIX: was checking watermark_state == WatermarkState.PENDING only.
        claim_watermark_jobs() sets watermark_state=PROCESSING immediately
        upon claim — so a worker that crashes or times out mid-processing
        leaves the job at watermark_state=PROCESSING, not PENDING. The old
        check missed this case entirely and routed crashed/stale watermark
        jobs to status=PENDING with their ORIGINAL un-watermarked
        vault_message_id still in place — the general dispatcher would then
        deliver unwatermarked content to the group, identical to the
        mark_watermark_failed bug this complements.
        Fixed: treat any watermark_state that is NOT COMPLETED as
        "watermarking incomplete, needs re-claim", covering both PENDING
        (never started) and PROCESSING (started but never finished) cases.
        """
        doc = await self._queue.find_one({"_id": ObjectId(job_id)})
        if not doc:
            return

        watermark_incomplete = (
            doc.get("watermark_required")
            and doc.get("watermark_state") != WatermarkState.COMPLETED
        )
        next_status = JobStatus.WATERMARKING if watermark_incomplete else JobStatus.PENDING

        update_fields: dict = {
            "status": next_status,
            "locked_by": None,
            "locked_at": None,
            "updated_at": datetime.now(timezone.utc),
        }
        if watermark_incomplete:
            # Reset to PENDING so claim_watermark_jobs' query
            # (watermark_state == WatermarkState.PENDING) matches again.
            update_fields["watermark_state"] = WatermarkState.PENDING

        await self._queue.update_one(
            {"_id": ObjectId(job_id)},
            {"$set": update_fields},
        )

    # ─── Progress Updates ─────────────────────────────────────────────────────

    async def record_target_delivered(self, job_id: str, target_id: str) -> None:
        """Record a successful delivery to a target using $addToSet for idempotency."""
        await self._queue.update_one(
            {"_id": ObjectId(job_id)},
            {
                "$addToSet": {"delivered_targets": target_id},
                "$set": {"updated_at": datetime.now(timezone.utc)}
            }
        )

    async def record_target_failed(self, job_id: str, target_id: str, error: str) -> None:
        """Record a failed delivery attempt to a target, keyed by target_id."""
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
        """
        Acquire a distributed delivery lock for a specific (job_id, target_id) pair.
        Uses MongoDB unique index on lock_key to guarantee exactly-once acquisition.

        If an existing lock has expired (based on expires_at), it is deleted and
        acquisition is retried up to 3 times total. Returns False if a live lock
        exists held by another worker.

        BUG FIX: Original used self-recursion which could stack indefinitely under
        tight race conditions. Replaced with an iterative loop with a max-attempts guard.

        BUG FIX: expires_at comparison now normalizes tz-naive datetimes from MongoDB
        to UTC-aware before comparing against datetime.now(timezone.utc).
        """
        lock_key = f"delivery:{job_id}:{target_id}"
        max_attempts = 3

        for attempt in range(max_attempts):
            expires_at = datetime.now(timezone.utc) + timedelta(seconds=ttl_seconds)
            try:
                await self._locks.insert_one({
                    "lock_key": lock_key,
                    "expires_at": expires_at,
                    "created_at": datetime.now(timezone.utc),
                })
                return True
            except DuplicateKeyError:
                existing = await self._locks.find_one({"lock_key": lock_key})
                if existing:
                    existing_expires = existing["expires_at"]
                    # Normalize tz-naive datetime from MongoDB to tz-aware for comparison.
                    if existing_expires.tzinfo is None:
                        existing_expires = existing_expires.replace(tzinfo=timezone.utc)

                    if existing_expires < datetime.now(timezone.utc):
                        # Expired lock — delete it and retry on next iteration.
                        await self._locks.delete_one({"lock_key": lock_key})
                        continue
                # Live lock held by another worker.
                return False

        logger.error(
            "acquire_delivery_lock exhausted %d attempts for key %s",
            max_attempts, lock_key,
        )
        return False

    async def extend_delivery_lock(self, job_id: str, target_id: str, ttl_seconds: int = 3600) -> bool:
        """
        Extend an existing delivery lock TTL (heartbeat from a long-running delivery).
        Returns True if the lock was found and updated, False if it does not exist.

        BUG FIX: Original used `result.modified_count > 1`. update_one can only
        return 0 or 1, so this was always False, silently failing every heartbeat
        and causing all delivery locks to expire mid-job. Changed to `> 0`.
        """
        lock_key = f"delivery:{job_id}:{target_id}"
        expires_at = datetime.now(timezone.utc) + timedelta(seconds=ttl_seconds)
        result = await self._locks.update_one(
            {"lock_key": lock_key},
            {"$set": {"expires_at": expires_at}}
        )
        return result.modified_count > 0  # FIX: was > 1, always False

    async def release_delivery_lock(self, job_id: str, target_id: str) -> None:
        """Release (delete) the delivery lock for a specific (job_id, target_id) pair."""
        await self._locks.delete_one({"lock_key": f"delivery:{job_id}:{target_id}"})

    # ─── Completion & Failure ─────────────────────────────────────────────────

    async def mark_completed(self, job_id: str) -> None:
        """
        Mark a job as COMPLETED, clear its lock fields, and release the vault doc.

        Two actions are performed atomically in sequence (the vault update is
        non-fatal — a failure there does not prevent the job from being marked
        COMPLETED):

        1. Queue job:  status → COMPLETED, lock fields cleared, completed_at set.
        2. Vault doc:  distribution_state → None (unlocked), cooldown_until set
                       to now + VAULT_FILL_COOLDOWN_HOURS, last_posted_at and
                       post_count updated for fairness tracking.

        The vault release is what allows this content to re-enter the vault
        replay pool after the cooldown window expires.  Without this step the
        vault doc keeps distribution_state="pending_delivery" forever, the
        provider never returns it again, and vault replay silently dies.
        """
        now = datetime.now(timezone.utc)

        # 1. Mark the queue job completed.
        await self._queue.update_one(
            {"_id": ObjectId(job_id)},
            {
                "$set": {
                    "status": JobStatus.COMPLETED,
                    "completed_at": now,
                    "updated_at": now,
                    "locked_by": None,
                    "locked_at": None,
                }
            },
        )

        # 2. Release the vault doc and set cooldown.
        # Read the job to get content_id (needed to find the vault doc).
        # Non-fatal: if this fails the job is still COMPLETED; the vault doc
        # just remains locked until the next restart recovery or manual fix.
        try:
            job_doc = await self._queue.find_one({"_id": ObjectId(job_id)})

            # FIX: enqueue_for_distribution writes content_id under
            # metadata.content_id (nested), but this method was reading
            # job_doc.get("content_id") (top-level only), which always
            # returned None. This caused:
            #   - vault doc distribution_state="pending_delivery" never cleared
            #   - cooldown_until never set
            #   - provider returned same vault doc on every scheduler cycle
            #   - new job created, watermarked again, delivered again — the
            #     "same content multiple times" loop reported in production.
            # Fix: check both locations.
            content_id = None
            if job_doc:
                content_id = (
                    job_doc.get("content_id")
                    or (job_doc.get("metadata") or {}).get("content_id")
                )

            if content_id:
                cooldown_hours = getattr(settings, "VAULT_FILL_COOLDOWN_HOURS", 24)
                cooldown_until = now + timedelta(hours=int(cooldown_hours))
                await self._vault.update_one(
                    {"content_id": content_id},
                    {
                        "$set": {
                            "distribution_state": None,
                            "cooldown_until": cooldown_until,
                            "last_posted_at": now,
                            "updated_at": now,
                        },
                        "$inc": {"post_count": 1},
                    },
                )
            else:
                logger.warning(
                    "mark_completed: job has no content_id (checked top-level and metadata) "
                    "— vault doc distribution_state will remain pending_delivery. "
                    "Fix: ensure enqueue_for_distribution sets content_id as a top-level field.",
                    extra={"ctx_job_id": job_id},
                )
        except Exception as vault_err:
            logger.error(
                "mark_completed: vault doc release failed (non-fatal)",
                extra={"ctx_job_id": job_id, "ctx_error": str(vault_err)},
            )

    async def mark_failed(
        self,
        job_id: str,
        error: str,
        next_retry_delay_seconds: Optional[float] = None,
        increment_retry: bool = True,
    ) -> dict:
        """
        Mark a job as failed, schedule retry, and clear its lock.
        If next_retry_delay_seconds is provided, the job will not be visible
        to workers until that delay has elapsed (execute_after field).
        Returns the updated job document, or None if the job was not found.

        WARNING: this unconditionally sets status=PENDING, which makes the
        job immediately claimable by the general dispatcher (claim_next).
        claim_next has NO awareness of watermark_required/watermark_state —
        it will deliver the job's CURRENT vault_message_id regardless of
        whether watermarking ever completed. Callers processing a
        watermark_required job whose watermarking failed MUST use
        mark_watermark_failed() instead, or unwatermarked content will be
        delivered straight to the distribution group.
        """
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

    async def mark_watermark_failed(
        self,
        job_id: str,
        error: str,
        next_retry_delay_seconds: Optional[float] = None,
        increment_retry: bool = True,
    ) -> dict:
        """
        Mark a WATERMARK job as failed and route it back for re-watermarking
        instead of into the general PENDING/dispatcher pool.

        ROOT CAUSE THIS FIXES: _process_group's exception handler previously
        called mark_failed() on watermark-job failures (e.g. when
        _swap_with_fallback/_swap_references_no_txn could not match the
        target document). mark_failed() unconditionally sets status=PENDING.
        The general dispatcher's claim_next() has zero awareness of
        watermark_required/watermark_state and will deliver ANY PENDING job
        using its current vault_message_id — which, since the swap never
        completed, still points at the ORIGINAL UN-WATERMARKED vault item.
        Result: content reached the distribution group with no watermark
        whatsoever, while the watermark worker silently uploaded an orphaned
        watermarked duplicate to the vault on every retry cycle.

        This method instead:
          - status -> WATERMARKING (re-claimable only by claim_watermark_jobs,
            never by the general dispatcher)
          - watermark_state -> PENDING (so claim_watermark_jobs' query matches)
          - clears locked_by/locked_at
          - increments retry_count / sets execute_after exactly like mark_failed

        If retries are exhausted, callers should route to move_to_dead_letter
        instead of calling this method (same pattern as mark_failed).
        """
        now = datetime.now(timezone.utc)
        execute_after = (
            now + timedelta(seconds=next_retry_delay_seconds)
            if next_retry_delay_seconds
            else now
        )

        update_ops: dict = {
            "$set": {
                "status": JobStatus.WATERMARKING,
                "watermark_state": WatermarkState.PENDING,
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
        """
        Move a job to the Dead Letter Queue after max retries or deadline exceeded.
        Uses a MongoDB transaction (when supported) to atomically write to DLQ
        and update the queue record in a single operation, preventing inconsistent
        dual-state if a crash occurs between the two writes.

        BUG FIX: Original had no transaction wrapping. A crash between DLQ upsert
        and queue status update left the job in its previous status while also
        appearing in DLQ — dual-state corruption. Now wrapped in a transaction
        when the deployment supports it.
        """
        job_doc = await self._queue.find_one({"_id": ObjectId(job_id)})
        if not job_doc:
            raise JobNotFoundError(f"Job {job_id} not found")

        dlq_doc = {
            "original_job_id": job_id,
            "content_id": job_doc["content_id"],
            "failure_reason": (
                "deadline_exceeded"
                if final_error == "deadline_exceeded"
                else "max_retries_exceeded"
            ),
            "final_error": final_error,
            "dead_at": datetime.now(timezone.utc),
            "metadata": job_doc.get("metadata", {}),
        }

        queue_update = {
            "$set": {
                "status": JobStatus.DEAD,
                "error": final_error,
                "updated_at": datetime.now(timezone.utc),
            }
        }

        from app.core.database import DatabaseManager
        use_transactions = DatabaseManager.transactions_supported()

        if use_transactions:
            async with await self._db.client.start_session() as session:
                async with session.start_transaction():
                    await self._dlq.update_one(
                        {"original_job_id": job_id},
                        {"$set": dlq_doc},
                        upsert=True,
                        session=session,
                    )
                    await self._queue.update_one(
                        {"_id": ObjectId(job_id)},
                        queue_update,
                        session=session,
                    )
        else:
            # Non-transactional path: DLQ upsert first so the job has a death
            # record even if the second write fails.
            await self._dlq.update_one(
                {"original_job_id": job_id},
                {"$set": dlq_doc},
                upsert=True,
            )
            await self._queue.update_one(
                {"_id": ObjectId(job_id)},
                queue_update,
            )

        return job_id

    async def move_to_quarantine(self, job_id: str, reason: str) -> None:
        """Move an unrecoverable job to QUARANTINE status with a recorded reason."""
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

    async def swap_album_vault_references(self, identifier: str, new_refs: List[dict]) -> None:
        """
        Atomically replace all vault references for an album or a single job.

        identifier: either a media_group_id (string) or a job_id (string).
        new_refs: list of {"album_sequence_index": int, "vault_message_id": int}.

        Uses a MongoDB transaction when supported. Falls back to sequential updates
        on standalone instances (same trade-off as claim_next non-tx path).

        BUG FIX: Original unconditionally started a MongoDB session and transaction.
        On standalone MongoDB instances (no replica set), this raises OperationFailure.
        All other transaction-using methods guard with DatabaseManager.transactions_supported().
        This now does the same.
        """
        from app.core.database import DatabaseManager
        use_transactions = DatabaseManager.transactions_supported()

        async def _do_swap(session=None):
            for ref in new_refs:
                query = {
                    "media_group_id": identifier,
                    "album_sequence_index": ref["album_sequence_index"],
                }

                if ref["album_sequence_index"] is None:
                    query = {
                        "$or": [
                            {"_id": ObjectId(identifier)},
                            {"media_group_id": identifier}
                        ]
                    }

                kwargs = {}
                if session is not None:
                    kwargs["session"] = session

                result = await self._queue.update_one(
                    query,
                    {
                        "$set": {
                            "vault_message_id": ref["vault_message_id"],
                            "watermark_state": WatermarkState.COMPLETED,
                            "status": JobStatus.PENDING,
                            "updated_at": datetime.now(timezone.utc),
                        }
                    },
                    **kwargs
                )
                if result.modified_count == 0:
                    raise ConsistencyViolationError(
                        f"Failed to swap reference for {identifier} "
                        f"index {ref['album_sequence_index']}"
                    )

        if use_transactions:
            async with await self._db.client.start_session() as session:
                async with session.start_transaction():
                    await _do_swap(session=session)
        else:
            await _do_swap(session=None)

    # ─── Stale Lock Recovery ──────────────────────────────────────────────────

    async def get_channel_pending_count(self, source_channel_id: str) -> int:
        """
        Return the count of in-flight jobs for a given source_channel_id.

        Used by the scheduler to compute available slots before creating new
        queue jobs for a distribution cycle.

        FIX: Previously only counted PENDING jobs. WATERMARKING jobs are
        invisible to this count because they have status=WATERMARKING, not
        PENDING. If 8 of 10 slots were occupied by WATERMARKING jobs, the
        scheduler would see 0 PENDING, compute 10 available slots, and attempt
        to enqueue 10 more items — 8 of which would immediately fail with
        DuplicateJobError (vault_ref_unique partial index) or, worse, create
        genuine duplicate jobs in deployments where the index is missing.

        Now counts all statuses that represent genuinely in-flight work:
        PENDING, WATERMARKING, LOCKED, PROCESSING, and DELIVERING.
        COMPLETED and FAILED jobs are not counted — they do not occupy slots.
        """
        return await self._queue.count_documents(
            {
                "source_channel_id": source_channel_id,
                "status": {
                    "$in": [
                        JobStatus.PENDING,
                        JobStatus.WATERMARKING,
                        JobStatus.LOCKED,
                        JobStatus.PROCESSING,
                        JobStatus.DELIVERING,
                    ]
                },
            }
        )

    async def get_deadline_exceeded_jobs(self, cutoff: datetime) -> list[dict]:
        """
        Return jobs whose deadline has passed and are still in PENDING status.

        BUG FIX: Original used hardcoded string "pending" instead of JobStatus.PENDING.
        Uses the correct model field 'queue_deadline' (GAP 6 FIX preserved).
        """
        cursor = self._queue.find(
            {"status": JobStatus.PENDING, "queue_deadline": {"$lte": cutoff}}
        )
        result = await cursor.to_list(length=None)
        return result if result is not None else []

    async def recover_stale_jobs(self) -> int:
        """
        Recover jobs from crashed workers, returning them to their correct pre-lock status.

        Three-phase approach:
          Phase 0 — Jobs stuck in PROCESSING (worker crashed between mark_processing and
                     dispatch completion, or mark_failed itself raised a DB error leaving
                     the job in PROCESSING). Reset to PENDING and increment retry_count so
                     max-retry enforcement eventually fires and moves them to dead_letters.
          Phase 1 — Jobs that were WATERMARKING (identified by watermark_state=PROCESSING)
                     are returned to WATERMARKING + watermark_state=PENDING.
          Phase 2 — All remaining stale LOCKED jobs (non-watermark) are returned to PENDING.

        Phases 1 and 2 are sequenced deliberately: Phase 1 updates a subset and changes
        their status away from LOCKED, so Phase 2 finds only the remaining non-watermark
        jobs.
        Returns the total count of recovered jobs.
        """
        threshold = datetime.now(timezone.utc) - timedelta(
            seconds=settings.STALE_LOCK_THRESHOLD_SECONDS
        )
        now = datetime.now(timezone.utc)

        # Phase 0: Recover stuck PROCESSING jobs.
        # updated_at is set by mark_processing(); if it's older than the stale threshold
        # the worker that owned this job is gone and will never complete it.
        # retry_count is incremented so that repeated recoveries eventually exhaust
        # max_retries and the dispatcher moves the job to dead_letters on the next attempt.
        processing_result = await self._queue.update_many(
            {
                "status": JobStatus.PROCESSING,
                "updated_at": {"$lt": threshold},
            },
            {
                "$set": {
                    "status": JobStatus.PENDING,
                    "locked_by": None,
                    "locked_at": None,
                    "updated_at": now,
                },
                "$inc": {"retry_count": 1},
            },
        )
        if processing_result.modified_count:
            logger.warning(
                "recover_stale_jobs: Phase 0 recovered %d stuck PROCESSING jobs → PENDING",
                processing_result.modified_count,
                extra={"ctx_phase": 0, "ctx_recovered": processing_result.modified_count},
            )

        # Phase 1: Recover watermarking jobs
        # FIX: missing $inc retry_count. Without it, a watermark job that keeps
        # timing out (stale lock) loops forever: worker claims it, times out,
        # stale recovery resets it, worker claims it again — uploading another
        # copy to the vault channel on every iteration. Adding $inc here ensures
        # that after max_retries stale recoveries the job eventually reaches
        # dead letter and the loop stops.
        wm_result = await self._queue.update_many(
            {
                "status": JobStatus.LOCKED,
                "locked_at": {"$lt": threshold},
                "watermark_required": True,
                "watermark_state": WatermarkState.PROCESSING
            },
            {
                "$set": {
                    "status": JobStatus.WATERMARKING,
                    "watermark_state": WatermarkState.PENDING,
                    "locked_by": None,
                    "locked_at": None,
                    "updated_at": now,
                },
                "$inc": {"retry_count": 1},
            },
        )

        # Phase 2: Recover all remaining stale LOCKED jobs to PENDING
        other_result = await self._queue.update_many(
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

        # Phase 3: Recover stale DELIVERING jobs.
        # The dispatcher transitions PROCESSING → DELIVERING per target in
        # mark_delivering(), then calls record_target_delivered() on success,
        # and finally mark_completed() when all targets are done.
        # If the worker crashes between mark_delivering() and mark_completed(),
        # the job is stuck in DELIVERING forever — the previous recover_stale_jobs
        # only handled LOCKED and PROCESSING, leaving DELIVERING jobs permanently
        # unreachable by claim_next() (which only claims PENDING) and by stale
        # recovery (which never looked at DELIVERING).
        #
        # Recovery is safe because dispatch() checks `delivered_targets` and
        # skips already-delivered targets, so resetting to PENDING does not
        # cause duplicate delivery of targets that were already recorded.
        # It WILL re-attempt any target that was not yet in `delivered_targets`
        # — which is exactly correct for a restart recovery.
        delivering_result = await self._queue.update_many(
            {
                "status": JobStatus.DELIVERING,
                "updated_at": {"$lt": threshold},
            },
            {
                "$set": {
                    "status": JobStatus.PENDING,
                    "locked_by": None,
                    "locked_at": None,
                    "updated_at": now,
                },
                "$inc": {"retry_count": 1},
            },
        )
        if delivering_result.modified_count:
            logger.warning(
                "recover_stale_jobs: Phase 3 recovered %d stuck DELIVERING jobs → PENDING",
                delivering_result.modified_count,
                extra={"ctx_phase": 3, "ctx_recovered": delivering_result.modified_count},
            )

        return (
            processing_result.modified_count
            + wm_result.modified_count
            + other_result.modified_count
            + delivering_result.modified_count
        )

    # ─── Fairness / Repost Prevention ────────────────────────────────────────

    async def get_recently_posted_content_ids(
        self, channel_id: str, hours: int = 168, limit: int = 500
    ) -> list[str]:
        """
        Return content_ids posted to this channel within the given time window.
        Used to enforce repost-prevention fairness rules.
        """
        cutoff = datetime.now(timezone.utc) - timedelta(hours=hours)
        docs = await self._queue.find(
            {
                "source_channel_id": channel_id,
                "status": JobStatus.COMPLETED,
                "completed_at": {"$gte": cutoff},
            },
            {"content_id": 1}
        ).sort("completed_at", -1).limit(limit).to_list(length=limit)
        return [d["content_id"] for d in docs if "content_id" in d]

    # ─── Metrics ─────────────────────────────────────────────────────────────

    async def collect_metrics(self) -> QueueMetrics:
        """
        Aggregate job counts by status and return a QueueMetrics snapshot.
        processing_count includes PROCESSING, LOCKED, DELIVERING, WATERMARKING,
        and READY — all statuses representing in-flight work.

        BUG FIX: Original excluded WATERMARKING, READY, and QUARANTINE from all
        metric buckets, causing in-flight counts to be understated.
        """
        pipeline = [{"$group": {"_id": "$status", "count": {"$sum": 1}}}]
        counts: dict[str, int] = {}
        async for doc in self._queue.aggregate(pipeline):
            counts[doc["_id"]] = doc["count"]

        metrics = QueueMetrics(
            pending_count=counts.get(JobStatus.PENDING, 0),
            processing_count=(
                counts.get(JobStatus.PROCESSING, 0)
                + counts.get(JobStatus.LOCKED, 0)
                + counts.get(JobStatus.DELIVERING, 0)
                + counts.get(JobStatus.WATERMARKING, 0)  # FIX: was missing
                + counts.get(JobStatus.READY, 0)          # FIX: was missing
            ),
            completed_count=counts.get(JobStatus.COMPLETED, 0),
            failed_count=counts.get(JobStatus.FAILED, 0),
            dead_count=(
                counts.get(JobStatus.DEAD, 0)
                + counts.get(JobStatus.QUARANTINE, 0)      # FIX: was missing
            ),
        )
        return metrics

    async def get_job_by_id(self, job_id: str) -> Optional[dict]:
        """Fetch a single job document by its MongoDB ObjectId string."""
        return await self._queue.find_one({"_id": ObjectId(job_id)})

    async def get_user_queue(self, user_id: int, limit: int = 10) -> List[dict]:
        """
        Fetch pending/in-flight jobs for a specific user (by submitter_user_id).
        Returns up to `limit` jobs sorted by most-recently created first.
        """
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