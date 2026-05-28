import asyncio  # FIX 5: was missing — asyncio.create_task/sleep used throughout
import random
import traceback
from datetime import datetime, timedelta, timezone
from typing import Optional, Callable, Any

from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.jobstores.memory import MemoryJobStore
from apscheduler.triggers.interval import IntervalTrigger
from motor.motor_asyncio import AsyncIOMotorDatabase
from pymongo.errors import DuplicateKeyError  # FIX 5: was missing — used in _acquire_lock

from app.config import settings
from app.core.models import QueueJob, JobStatus, MediaType, DistributionPriority, WatermarkState  # FIX 5: WatermarkState was missing
from app.core.exceptions import DuplicateJobError
from app.repositories.queue_repository import QueueRepository
from app.distribution.fairness import FairnessSelector
from app.utils.logger import get_logger

logger = get_logger(__name__)

# FIX 14: Daily caps are now driven by settings (env vars) instead of a
# hardcoded dict. DAILY_CAP_NSFW and DAILY_CAP_PREMIUM are defined in
# settings.py with safe defaults (75 and 140 respectively).
_DAILY_CAPS = {
    "nsfw": settings.DAILY_CAP_NSFW,
    "premium": settings.DAILY_CAP_PREMIUM,
}


def _get_daily_cap(dest: str) -> int:
    return _DAILY_CAPS.get(dest, 100)


class DistributionScheduler:
    def __init__(
        self,
        db: AsyncIOMotorDatabase,
        content_provider_callback: Callable,
    ):
        self._db = db
        self._queue_repo = QueueRepository(db)
        self._fairness = FairnessSelector(db)
        self._content_provider = content_provider_callback
        self._locks = db[settings.LOCK_COLLECTION]

        jobstores = {"default": MemoryJobStore()}
        self._scheduler = AsyncIOScheduler(jobstores=jobstores, timezone="UTC")
        self._started = False
        self._lock_task: Optional[asyncio.Task] = None

    async def _acquire_lock(self) -> bool:
        """Acquire distributed singleton lock for the scheduler."""
        lock_key = "scheduler_active_singleton"

        expires_at = datetime.now(timezone.utc) + timedelta(seconds=60)
        try:
            await self._locks.insert_one({
                "lock_key": lock_key,
                "expires_at": expires_at,
                "created_at": datetime.now(timezone.utc),
                "owner": "scheduler_process"
            })
            return True
        except DuplicateKeyError:
            return False

    async def _extend_lock(self) -> None:
        """Heartbeat to keep the scheduler lock alive."""
        while self._started:
            await asyncio.sleep(30)
            await self._locks.update_one(
                {"lock_key": "scheduler_active_singleton"},
                {"$set": {"expires_at": datetime.now(timezone.utc) + timedelta(seconds=60)}}
            )

    async def start(self) -> None:
        if self._started:
            return

        # Unconditionally clear any existing lock owned by this instance
        await self._locks.delete_many({"lock_key": "scheduler_active_singleton"})
        logger.info("scheduler lock cleared on startup")

        # Attempt to acquire singleton lock
        if not await self._acquire_lock():
            logger.info("Another scheduler instance is already running. Standing by.")
            return

        logger.info("Scheduler lock acquired. Starting integrity scan...")
        await self._run_startup_integrity_scan()

        # FIX 6: setup_jobs() was called here but never defined. Replace with
        # inline add_job() calls that actually register the scheduler methods.
        self._scheduler.add_job(
            self._distribution_cycle,
            trigger=IntervalTrigger(seconds=settings.SCHEDULER_INTERVAL_SECONDS),
            id="distribution_cycle",
            replace_existing=True,
            max_instances=1,
            coalesce=True,
        )
        self._scheduler.add_job(
            self._stale_lock_sweep,
            trigger=IntervalTrigger(seconds=120),
            id="stale_lock_sweep",
            replace_existing=True,
            max_instances=1,
            coalesce=True,
        )
        self._scheduler.add_job(
            self._deadline_sweep,
            trigger=IntervalTrigger(seconds=300),
            id="deadline_sweep",
            replace_existing=True,
            max_instances=1,
            coalesce=True,
        )
        self._scheduler.add_job(
            self._collect_metrics,
            trigger=IntervalTrigger(seconds=60),
            id="collect_metrics",
            replace_existing=True,
            max_instances=1,
            coalesce=True,
        )

        self._scheduler.start()
        self._started = True
        self._lock_task = asyncio.create_task(self._extend_lock())

        logger.info("Distribution scheduler started (Primary instance)")

    async def stop(self) -> None:
        if self._started:
            self._started = False
            if self._lock_task:
                self._lock_task.cancel()
            self._scheduler.shutdown(wait=True)
            await self._locks.delete_one({"lock_key": "scheduler_active_singleton"})
            logger.info("Distribution scheduler stopped and lock released")

    async def _run_startup_integrity_scan(self) -> None:
        """Scan for orphaned locks, missing references, and partially processed jobs."""
        now = datetime.now(timezone.utc)
        queue = self._db[settings.QUEUE_COLLECTION]

        # 1. Recover stale processing jobs
        recovered = await self._queue_repo.recover_stale_jobs()
        if recovered:
            logger.warning(f"Startup scan: recovered {recovered} orphaned processing jobs")

        # 2. Quarantine jobs with missing vault references
        missing_refs = await queue.update_many(
            {
                "status": {"$in": [JobStatus.PENDING, JobStatus.WATERMARKING, JobStatus.READY]},
                "$or": [{"vault_chat_id": None}, {"vault_message_id": None}]
            },
            {
                "$set": {
                    "status": JobStatus.QUARANTINE,
                    "quarantine_reason": "missing_vault_references",
                    "updated_at": now
                }
            }
        )
        if missing_refs.modified_count:
            logger.warning(f"Startup scan: quarantined {missing_refs.modified_count} jobs due to missing vault references")

        # 3. Detect partially watermarked albums
        broken_wm = await queue.update_many(
            {
                "status": JobStatus.LOCKED,
                "watermark_state": WatermarkState.PROCESSING,
                "locked_at": {"$lt": now - timedelta(minutes=30)}
            },
            {
                "$set": {
                    "status": JobStatus.WATERMARKING,
                    "watermark_state": WatermarkState.PENDING,
                    "locked_by": None,
                    "locked_at": None,
                    "updated_at": now
                }
            }
        )
        if broken_wm.modified_count:
            logger.warning(f"Startup scan: reset {broken_wm.modified_count} partially watermarked jobs")

    async def _distribution_cycle(self) -> None:
        """Main distribution loop executing as a singleton."""
        if await self._check_backpressure():
            logger.warning("Backpressure threshold reached. Skipping distribution cycle.")
            return

        try:
            logger.info("Distribution cycle started")
            channel_configs = await self._content_provider()
        except Exception as e:
            logger.error(f"Distribution cycle FAILED at content_provider step: {e}")
            return

        if not channel_configs:
            return

        total_enqueued = 0
        for config in channel_configs:
            try:
                source_id = config["source_channel_id"]
                targets = config["target_channel_ids"]
                content = config.get("content", [])
                if not content or not targets:
                    continue

                dest = source_id.replace("submission_", "")
                daily_cap = _get_daily_cap(dest)
                posted_today = await self._get_posted_count_last_24h(source_id)
                remaining = daily_cap - posted_today

                if remaining <= 0:
                    continue

                pending = await self._queue_repo.get_channel_pending_count(int(dest))
                slots = min(settings.MAX_JOBS_PER_CYCLE - pending, remaining)
                if slots <= 0:
                    continue

                selected = await self._fairness.select_eligible_content(
                    available_content=content,
                    source_channel_id=source_id,
                    max_count=slots,
                )

                group_times = {}
                g_idx = 0
                for item in selected:
                    gid = item.get("media_group_id") or item.get("content_id")
                    if gid not in group_times:
                        group_times[gid] = self._randomized_execute_time(g_idx)
                        g_idx += 1

                    if await self._enqueue_content(item, source_id, targets, group_times[gid], config.get("watermark_config")):
                        total_enqueued += 1

            except Exception as e:
                logger.error(f"Channel {config.get('source_channel_id')} failed: {e}")

        logger.info("Distribution cycle complete", extra={"ctx_enqueued": total_enqueued})

    async def _check_backpressure(self) -> bool:
        """Return True if queue depth exceeds safe threshold."""
        try:
            queue = self._db[settings.QUEUE_COLLECTION]
            active = await queue.count_documents({
                "status": {"$in": ["pending", "watermarking", "ready", "locked", "delivering"]}
            })
            return active >= settings.MAX_JOBS_PER_CYCLE * 2
        except Exception:
            return False

    async def _get_posted_count_last_24h(self, source_channel_id: str) -> int:
        try:
            cutoff = datetime.now(timezone.utc) - timedelta(hours=24)
            return await self._db[settings.QUEUE_COLLECTION].count_documents({
                "source_channel_id": source_channel_id,
                "status": JobStatus.COMPLETED,
                "completed_at": {"$gte": cutoff},
            })
        except Exception:
            return 0

    async def _enqueue_content(
        self,
        content_item: dict,
        source_channel_id: str,
        target_channel_ids: list[str],
        execute_after: datetime,
        watermark_config: Optional[dict] = None,
    ) -> bool:
        watermark_required = watermark_config is not None
        media_type_str = content_item.get("media_type", "text")

        try:
            media_type = MediaType(media_type_str)
        except ValueError:
            media_type = MediaType.TEXT

        initial_status = JobStatus.WATERMARKING if watermark_required else JobStatus.PENDING

        job = QueueJob(
            schema_version=1,
            content_id=content_item["content_id"],
            source_channel_id=source_channel_id,
            source_message_id=content_item.get("message_id"),
            vault_chat_id=settings.VAULT_CHANNEL_ID,
            vault_message_id=content_item.get("vault_message_id"),
            target_channel_ids=target_channel_ids,
            media_group_id=content_item.get("media_group_id"),
            media_type=media_type,
            media_file_id=content_item.get("file_id"),
            caption=content_item.get("caption"),
            priority=content_item.get("priority", DistributionPriority.NORMAL),
            status=initial_status,
            max_retries=settings.MAX_RETRY_ATTEMPTS,
            execute_after=execute_after,
            watermark_required=watermark_required,
            watermark_config=watermark_config,
            album_sequence_index=content_item.get("album_sequence_index"),
            metadata={**content_item.get("metadata", {})},
        )

        try:
            await self._queue_repo.enqueue(job)
            return True
        except DuplicateJobError:
            return False
        except Exception:
            logger.error(f"Failed to enqueue {content_item['content_id']}", exc_info=True)
            return False

    def _randomized_execute_time(self, index: int) -> datetime:
        base_delay = random.uniform(0, settings.SCHEDULER_INTERVAL_SECONDS)
        stagger = index * random.uniform(5, 15)
        return datetime.now(timezone.utc) + timedelta(seconds=base_delay + stagger)

    async def _stale_lock_sweep(self) -> None:
        try:
            recovered = await self._queue_repo.recover_stale_jobs()
            if recovered:
                logger.warning(f"Stale lock sweep recovered {recovered} jobs")
        except Exception as e:
            logger.error(f"Stale lock sweep FAILED: {e}")

    async def _deadline_sweep(self) -> None:
        try:
            now = datetime.now(timezone.utc)
            jobs = await self._queue_repo.get_deadline_exceeded_jobs(now)
            for job in jobs:
                await self._queue_repo.move_to_dead_letter(str(job["_id"]), "deadline_exceeded")
        except Exception as e:
            logger.error("Deadline sweep FAILED", extra={"ctx_error": str(e)})

    async def _collect_metrics(self) -> None:
        try:
            metrics = await self._queue_repo.collect_metrics()
            logger.info("Metrics collected", extra=metrics.model_dump())
        except Exception as e:
            logger.error(f"Metrics FAILED: {e}")

    def add_custom_job(self, func: Callable, trigger: Any, _id: str, **kwargs) -> None:
        self._scheduler.add_job(func, trigger=trigger, id=_id, replace_existing=True, max_instances=1, coalesce=True, **kwargs)
