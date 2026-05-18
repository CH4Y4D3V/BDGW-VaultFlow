import random
from datetime import datetime, timedelta, timezone
from typing import Optional, Callable, Any
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.interval import IntervalTrigger
from motor.motor_asyncio import AsyncIOMotorDatabase
from app.config import settings
from app.core.models import QueueJob, JobStatus, MediaType, DistributionPriority
from app.core.exceptions import DuplicateJobError
from app.repositories.queue_repository import QueueRepository
from app.distribution.fairness import FairnessSelector
from app.utils.logger import get_logger

logger = get_logger(__name__)


class DistributionScheduler:
    """
    APScheduler wrapper that ONLY inserts jobs into the queue.
    Never directly delivers content. Dispatcher workers handle delivery.
    """

    def __init__(
        self,
        db: AsyncIOMotorDatabase,
        content_provider_callback: Callable,
    ):
        self._db = db
        self._queue_repo = QueueRepository(db)
        self._fairness = FairnessSelector(db)
        self._content_provider = content_provider_callback
        self._scheduler = AsyncIOScheduler(timezone="UTC")
        self._started = False

    def setup_jobs(self) -> None:
        """Register all recurring scheduler jobs."""

        # Main distribution cycle
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

        # Stale lock recovery sweep
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

        # Metrics collection
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

        # P1-C: Queue deadline enforcement — runs every 60 seconds
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
            "Distribution scheduler started",
            extra={"ctx_interval": settings.SCHEDULER_INTERVAL_SECONDS},
        )

    async def stop(self) -> None:
        if self._started:
            self._scheduler.shutdown(wait=True)
            self._started = False
            logger.info("Distribution scheduler stopped gracefully")

    async def _distribution_cycle(self) -> None:
        try:
            logger.info("Distribution cycle started")

            channel_configs = await self._content_provider()
            if not channel_configs:
                logger.info("No active channels returned by content provider")
                return

            total_enqueued = 0

            for config in channel_configs:
                source_channel_id = config["source_channel_id"]
                target_channel_ids = config["target_channel_ids"]
                available_content = config.get("content", [])

                if not available_content or not target_channel_ids:
                    continue

                pending_count = await self._queue_repo.get_channel_pending_count(
                    source_channel_id
                )
                slots_available = settings.MAX_JOBS_PER_CYCLE - pending_count
                if slots_available <= 0:
                    logger.info(
                        "Channel queue is full, skipping",
                        extra={
                            "ctx_channel": source_channel_id,
                            "ctx_pending": pending_count,
                        },
                    )
                    continue

                selected_content = await self._fairness.select_eligible_content(
                    available_content=available_content,
                    source_channel_id=source_channel_id,
                    max_count=slots_available,
                )

                group_execute_times: dict = {}
                group_index = 0

                for content_item in selected_content:
                    group_id = content_item.get("media_group_id") or content_item.get("content_id")
                    if group_id not in group_execute_times:
                        group_execute_times[group_id] = self._randomized_execute_time(group_index)
                        group_index += 1

                    execute_after = group_execute_times[group_id]
                    enqueued = await self._enqueue_content(
                        content_item=content_item,
                        source_channel_id=source_channel_id,
                        target_channel_ids=target_channel_ids,
                        execute_after=execute_after,
                        watermark_config=config.get("watermark_config"),
                    )
                    if enqueued:
                        total_enqueued += 1

            logger.info(
                "Distribution cycle completed",
                extra={"ctx_enqueued": total_enqueued},
            )

        except Exception as e:
            logger.error(
                "Distribution cycle failed",
                extra={"ctx_error": str(e)},
                exc_info=True,
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
                "Failed to enqueue content",
                extra={
                    "ctx_content_id": content_item.get("content_id"),
                    "ctx_error": str(e),
                },
                exc_info=True,
            )
            return False

    def _randomized_execute_time(self, index: int) -> datetime:
        base_delay = random.uniform(0, settings.RANDOMIZE_POSTING_WINDOW)
        stagger = index * random.uniform(5, 15)
        total_delay = base_delay + stagger
        return datetime.now(timezone.utc) + timedelta(seconds=total_delay)

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
                "Stale lock sweep failed",
                extra={"ctx_error": str(e)},
                exc_info=True,
            )

    async def _deadline_sweep(self) -> None:
        """
        P1-C: Enforce queue_deadline on jobs.
        Any job still PENDING or WATERMARKING past its deadline is moved to dead letter.
        """
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
                        "Job moved to dead letter — deadline exceeded",
                        extra={
                            "ctx_job_id": job_id,
                            "ctx_content_id": job.get("content_id"),
                            "ctx_deadline": str(job.get("queue_deadline")),
                        },
                    )
                except Exception as e:
                    logger.error(
                        "Failed to dead-letter deadline-exceeded job",
                        extra={"ctx_job_id": job_id, "ctx_error": str(e)},
                    )

        except Exception as e:
            logger.error(
                "Deadline sweep failed",
                extra={"ctx_error": str(e)},
                exc_info=True,
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
                "Metrics collection failed",
                extra={"ctx_error": str(e)},
                exc_info=True,
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