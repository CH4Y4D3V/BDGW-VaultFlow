import asyncio
import uuid
from typing import Optional, Callable
from motor.motor_asyncio import AsyncIOMotorDatabase
from app.config import settings
from app.core.models import JobStatus
from app.core.exceptions import MaxRetriesExceededError, DispatcherError
from app.distribution.dispatcher import DistributionDispatcher
from app.distribution.flood_wait import FloodWaitHandler, calculate_retry_delay
from app.distribution.target_balancer import TargetBalancer
from app.repositories.queue_repository import QueueRepository
from app.services.lock_service import DistributedLockService
from app.services.rate_limiter import RateLimiterService
from app.utils.logger import get_logger

logger = get_logger(__name__)


class DispatcherWorker:
    """
    Independent async worker that:
    1. Claims jobs from the queue
    2. Passes them to DistributionDispatcher
    3. Handles retry/dead-letter logic on failure
    4. Runs in its own asyncio Task
    """

    def __init__(
        self,
        worker_id: str,
        db: AsyncIOMotorDatabase,
        delivery_callback: Callable,
        rate_limiter: RateLimiterService,
        shared_flood_handler: FloodWaitHandler,
        shared_balancer: TargetBalancer,
    ):
        self._worker_id = worker_id
        self._db = db
        self._queue_repo = QueueRepository(db)
        self._lock_service = DistributedLockService(db, worker_id)
        self._rate_limiter = rate_limiter
        self._flood_handler = shared_flood_handler
        self._balancer = shared_balancer
        self._delivery_callback = delivery_callback
        self._dispatcher = DistributionDispatcher(
            queue_repo=self._queue_repo,
            rate_limiter=self._rate_limiter,
            flood_handler=self._flood_handler,
            target_balancer=self._balancer,
            delivery_callback=self._delivery_callback,
        )
        self._running = False
        self._task: Optional[asyncio.Task] = None

    async def start(self) -> None:
        if self._running:
            return

        # Recover any stale jobs from crashed workers on startup
        recovered = await self._queue_repo.recover_stale_processing_jobs()
        if recovered:
            logger.warning(
                f"Worker startup: recovered {recovered} stale jobs",
                extra={"ctx_worker": self._worker_id, "ctx_recovered": recovered},
            )

        await self._lock_service.recover_stale_locks()

        self._running = True
        self._task = asyncio.create_task(
            self._run_loop(),
            name=f"dispatcher-{self._worker_id}",
        )
        logger.info("Dispatcher worker started", extra={"ctx_worker": self._worker_id})

    async def stop(self) -> None:
        self._running = False
        if self._task:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
        logger.info("Dispatcher worker stopped", extra={"ctx_worker": self._worker_id})

    async def _run_loop(self) -> None:
        while self._running:
            try:
                jobs = await self._queue_repo.claim_next(
                    worker_id=self._worker_id,
                    batch_size=settings.WORKER_BATCH_SIZE,
                )

                if not jobs:
                    await asyncio.sleep(settings.WORKER_POLL_INTERVAL)
                    continue

                tasks = [self._handle_job(job) for job in jobs]
                await asyncio.gather(*tasks, return_exceptions=True)

            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(
                    "Dispatcher worker loop error",
                    extra={"ctx_worker": self._worker_id, "ctx_error": str(e)},
                    exc_info=True,
                )
                await asyncio.sleep(5)

    async def _handle_job(self, job_doc: dict) -> None:
        job_id = str(job_doc["_id"])

        lock_key = f"job:{job_id}"
        acquired = await self._lock_service.acquire(
            lock_key,
            ttl_seconds=settings.LOCK_TTL_SECONDS,
        )
        if not acquired:
            logger.warning(
                "Could not acquire job lock — another worker is processing it",
                extra={"ctx_job_id": job_id, "ctx_worker": self._worker_id},
            )
            return

        try:
            marked = await self._queue_repo.mark_processing(job_id, self._worker_id)
            if not marked:
                logger.warning(
                    "Job already taken by another worker after claim",
                    extra={"ctx_job_id": job_id},
                )
                return

            success = await self._dispatcher.dispatch(job_doc, self._worker_id)

            if not success:
                await self._handle_retry(job_doc, job_id, "Partial delivery failure")

        except MaxRetriesExceededError as e:
            await self._queue_repo.move_to_dead_letter(job_id, str(e))

        except Exception as e:
            logger.error(
                "Unexpected error handling job",
                extra={"ctx_job_id": job_id, "ctx_error": str(e)},
                exc_info=True,
            )
            await self._handle_retry(job_doc, job_id, str(e))

        finally:
            await self._lock_service.release(lock_key)

    async def _handle_retry(self, job_doc: dict, job_id: str, error: str) -> None:
        retry_count = job_doc.get("retry_count", 0)
        max_retries = job_doc.get("max_retries", settings.MAX_RETRY_ATTEMPTS)

        if retry_count >= max_retries:
            logger.error(
                "Max retries exceeded, sending to dead letter",
                extra={
                    "ctx_job_id": job_id,
                    "ctx_retries": retry_count,
                    "ctx_error": error,
                },
            )
            await self._queue_repo.move_to_dead_letter(job_id, error)
            return

        delay = calculate_retry_delay(retry_count)
        logger.warning(
            "Job failed, scheduling retry",
            extra={
                "ctx_job_id": job_id,
                "ctx_attempt": retry_count + 1,
                "ctx_max": max_retries,
                "ctx_retry_in": delay,
                "ctx_error": error,
            },
        )
        await self._queue_repo.mark_failed(job_id, error, next_retry_delay_seconds=delay)
