import random
import traceback
from datetime import datetime, timedelta, timezone
from typing import Optional, Callable, Any

from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.jobstores.mongodb import MongoDBJobStore
from apscheduler.triggers.interval import IntervalTrigger
from motor.motor_asyncio import AsyncIOMotorDatabase
from pymongo import MongoClient

from app.config import settings
from app.core.models import QueueJob, JobStatus, MediaType, DistributionPriority
from app.core.exceptions import DuplicateJobError
from app.repositories.queue_repository import QueueRepository
from app.distribution.fairness import FairnessSelector
from app.utils.logger import get_logger

logger = get_logger(__name__)

_DEFAULT_DAILY_CAPS: dict[str, int] = {
    "nsfw": 75,
    "premium": 140,
}


def _get_daily_cap(dest: str) -> int:
    env_key = f"DAILY_CAP_{dest.upper()}"
    cap = getattr(settings, env_key, None)
    if cap and isinstance(cap, int) and cap > 0:
        return cap
    return _DEFAULT_DAILY_CAPS.get(dest, 100)


def _build_jobstore() -> MongoDBJobStore:
    """
    Build a persistent MongoDB job store for APScheduler.

    APScheduler's MongoDBJobStore requires a synchronous PyMongo client —
    it does not support Motor (async). We create a dedicated sync client
    here solely for the job store. This client is separate from the Motor
    client used everywhere else and does not interfere with it.

    Jobs persisted here survive process restarts, so the scheduler recovers
    its schedule automatically on boot without any re-registration dance.
    """
    # APScheduler needs a sync PyMongo client — Motor is async-only
    sync_client = MongoClient(settings.MONGO_URI)
    return MongoDBJobStore(
        database=settings.MONGO_DB_NAME,
        collection=settings.SCHEDULER_JOBS_COLLECTION,
        client=sync_client,
    )


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

        # Persistent MongoDB job store — scheduler state survives restarts
        jobstores = {"default": _build_jobstore()}
        self._scheduler = AsyncIOScheduler(
            jobstores=jobstores,
            timezone="UTC",
        )
        self._started = False

    def setup_jobs(self) -> None:
        self._scheduler.add_job(
            self._distribution_cycle,
            trigger=IntervalTrigger(seconds=settings.SCHEDULER_INTERVAL_SECONDS),
            id="distribution_cycle",
            name="Distribution Cycle",
            replace_existing=True,
            max_instances=1,
            coalesce=True,
            misfire_grace_time=60,
        )
        self._scheduler.add_job(
            self._stale_lock_sweep,
            trigger=IntervalTrigger(seconds=120),
            id="stale_lock_sweep",
            name="Stale Lock Sweep",
            replace_existing=True,
            max_instances=1,
            coalesce=True,
            misfire_grace_time=60,
        )
        self._scheduler.add_job(
            self._collect_metrics,
            trigger=IntervalTrigger(seconds=300),
            id="metrics_collection",
            name="Metrics Collection",
            replace_existing=True,
            max_instances=1,
            coalesce=True,
            misfire_grace_time=60,
        )
        self._scheduler.add_job(
            self._deadline_sweep,
            trigger=IntervalTrigger(seconds=60),
            id="deadline_sweep",
            name="Queue Deadline Sweep",
            replace_existing=True,
            max_instances=1,
            coalesce=True,
            misfire_grace_time=30,
        )
        logger.info("Scheduler jobs configured")

    async def start(self) -> None:
        if self._started:
            return
        self.setup_jobs()
        self._scheduler.start()
        self._started = True
        logger.info(
            "Distribution scheduler started (persistent MongoDB job store)",
            extra={
                "ctx_interval": settings.SCHEDULER_INTERVAL_SECONDS,
                "ctx_daily_caps": _DEFAULT_DAILY_CAPS,
                "ctx_jobstore_collection": settings.SCHEDULER_JOBS_COLLECTION,
            },
        )

    async def stop(self) -> None:
        if self._started:
            self._scheduler.shutdown(wait=True)
            self._started = False
            logger.info("Distribution scheduler stopped gracefully")

    async def _get_posted_count_last_24h(self, source_channel_id: str) -> int:
        queue_col = self._db[settings.QUEUE_COLLECTION]
        since = datetime.now(timezone.utc) - timedelta(hours=24)
        return await queue_col.count_documents({
            "source_channel_id": source_channel_id,
            "status": JobStatus.COMPLETED,
            "completed_at": {"$gte": since},
        })

    async def _distribution_cycle(self) -> None:
        try:
            logger.info("Distribution cycle started")
            channel_configs = await self._content_provider()
        except Exception as e:
            logger.error(
                "Distribution cycle FAILED at content_provider step: "
                f"{type(e).__name__}: {e}\n{traceback.format_exc()}"
            )
            return

        if not channel_configs:
            logger.info("No active channels returned by content provider")
            return

        total_enqueued = 0

        for config in channel_configs:
            try:
                source_channel_id = config["source_channel_id"]
                target_channel_ids = config["target_channel_ids"]
                available_content = config.get("content", [])
            except Exception as e:
                logger.error(
                    f"Distribution cycle: malformed config — {type(e).__name__}: {e}"
                )
                continue

            if not available_content or not target_channel_ids:
                continue

            try:
                dest = source_channel_id.replace("submission_", "")
                daily_cap = _get_daily_cap(dest)
                posted_today = await self._get_posted_count_last_24h(source_channel_id)
                remaining_cap = daily_cap - posted_today

                if remaining_cap <= 0:
                    logger.info(
                        "Daily cap reached — skipping",
                        extra={
                            "ctx_dest": dest,
                            "ctx_posted_today": posted_today,
                            "ctx_daily_cap": daily_cap,
                        },
                    )
                    continue

                logger.info(
                    "Daily cap status",
                    extra={
                        "ctx_dest": dest,
                        "ctx_posted_today": posted_today,
                        "ctx_daily_cap": daily_cap,
                        "ctx_remaining": remaining_cap,
                    },
                )
            except Exception as e:
                logger.error(
                    f"Distribution cycle: daily cap check FAILED — "
                    f"{type(e).__name__}: {e}\n{traceback.format_exc()}"
                )
                continue

            try:
                pending_count = await self._queue_repo.get_channel_pending_count(
                    source_channel_id
                )
                slots_available = min(
                    settings.MAX_JOBS_PER_CYCLE - pending_count,
                    remaining_cap,
                )
                if slots_available <= 0:
                    logger.info(
                        "No slots available, skipping",
                        extra={
                            "ctx_channel": source_channel_id,
                            "ctx_pending": pending_count,
                        },
                    )
                    continue
            except Exception as e:
                logger.error(
                    f"Distribution cycle: pending count check FAILED — "
                    f"{type(e).__name__}: {e}\n{traceback.format_exc()}"
                )
                continue

            try:
                selected_content = await self._fairness.select_eligible_content(
                    available_content=available_content,
                    source_channel_id=source_channel_id,
                    max_count=slots_available,
                )
            except Exception as e:
                logger.error(
                    f"Distribution cycle: fairness selector FAILED — "
                    f"{type(e).__name__}: {e}\n{traceback.format_exc()}"
                )
                continue

            group_execute_times: dict = {}
            group_index = 0

            for content_item in selected_content:
                try:
                    group_id = (
                        content_item.get("media_group_id")
                        or content_item.get("content_id")
                    )
                    if group_id not in group_execute_times:
                        group_execute_times[group_id] = self._randomized_execute_time(
                            group_index
                        )
                        group_index += 1

                    enqueued = await self._enqueue_content(
                        content_item=content_item,
                        source_channel_id=source_channel_id,
                        target_channel_ids=target_channel_ids,
                        execute_after=group_execute_times[group_id],
                        watermark_config=config.get("watermark_config"),
                    )
                    if enqueued:
                        total_enqueued += 1
                except Exception as e:
                    logger.error(
                        f"Distribution cycle: enqueue item FAILED — "
                        f"{type(e).__name__}: {e}\n{traceback.format_exc()}"
                    )
                    continue

        logger.info(
            "Distribution cycle completed",
            extra={"ctx_enqueued": total_enqueued},
        )

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
            content_id=content_item["content_id"],
            source_channel_id=source_channel_id,
            target_channel_ids=target_channel_ids,
            media_type=media_type,
            media_file_id=content_item.get("file_id"),
            media_path=content_item.get("file_path"),
            caption=content_item.get("caption"),
            priority=content_item.get("priority", DistributionPriority.NORMAL),
            status=initial_status,
            max_retries=settings.MAX_RETRY_ATTEMPTS,
            execute_after=execute_after,
            watermark_required=watermark_required,
            watermark_config=watermark_config,
            metadata={
                **content_item.get("metadata", {}),
                "media_group_id": content_item.get("media_group_id"),
                "message_id": content_item.get("message_id"),
            },
        )

        try:
            job_id = await self._queue_repo.enqueue(job)
            logger.debug(
                "Content enqueued",
                extra={
                    "ctx_job_id": job_id,
                    "ctx_content": content_item["content_id"],
                    "ctx_execute_after": execute_after.isoformat(),
                    "ctx_watermark": watermark_required,
                },
            )
            return True
        except DuplicateJobError:
            logger.debug(
                "Skipping duplicate content",
                extra={"ctx_content_id": content_item.get("content_id")},
            )
            return False
        except Exception as e:
            logger.error(
                f"Failed to enqueue {content_item.get('content_id')} — "
                f"{type(e).__name__}: {e}\n{traceback.format_exc()}"
            )
            return False

    def _randomized_execute_time(self, index: int) -> datetime:
        base_delay = random.uniform(0, settings.RANDOMIZE_POSTING_WINDOW)
        stagger = index * random.uniform(5, 15)
        return datetime.now(timezone.utc) + timedelta(seconds=base_delay + stagger)

    async def _stale_lock_sweep(self) -> None:
        try:
            recovered = await self._queue_repo.recover_stale_processing_jobs()
            if recovered:
                logger.warning(
                    "Stale lock sweep recovered jobs",
                    extra={"ctx_count": recovered},
                )
        except Exception as e:
            logger.error(
                f"Stale lock sweep FAILED — {type(e).__name__}: {e}\n{traceback.format_exc()}"
            )

    async def _deadline_sweep(self) -> None:
        try:
            overdue_jobs = await self._queue_repo.get_deadline_exceeded_jobs()
            if not overdue_jobs:
                return
            logger.warning(
                "Deadline sweep found overdue jobs",
                extra={"ctx_count": len(overdue_jobs)},
            )
            for job in overdue_jobs:
                job_id = str(job["_id"])
                try:
                    await self._queue_repo.move_to_dead_letter(
                        job_id, "queue_deadline_exceeded"
                    )
                    logger.warning(
                        "Job dead-lettered — deadline exceeded",
                        extra={"ctx_job_id": job_id},
                    )
                except Exception as e:
                    logger.error(
                        f"Failed to dead-letter job {job_id} — {type(e).__name__}: {e}"
                    )
        except Exception as e:
            logger.error(
                f"Deadline sweep FAILED — {type(e).__name__}: {e}\n{traceback.format_exc()}"
            )

    async def _collect_metrics(self) -> None:
        try:
            metrics = await self._queue_repo.collect_metrics()
            logger.info(
                "Queue metrics collected",
                extra={
                    "ctx_pending": metrics.pending_count,
                    "ctx_processing": metrics.processing_count,
                    "ctx_completed": metrics.completed_count,
                    "ctx_dead": metrics.dead_count,
                },
            )
        except Exception as e:
            logger.error(
                f"Metrics collection FAILED — {type(e).__name__}: {e}\n{traceback.format_exc()}"
            )

    def add_custom_job(
        self,
        func: Callable,
        trigger: Any,
        _id: str,
        **kwargs,
    ) -> None:
        self._scheduler.add_job(
            func,
            trigger=trigger,
            id=_id,
            replace_existing=True,
            max_instances=1,
            coalesce=True,
            misfire_grace_time=60,
            **kwargs,
        )