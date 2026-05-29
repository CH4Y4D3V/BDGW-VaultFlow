import asyncio
from typing import Optional, Callable
from motor.motor_asyncio import AsyncIOMotorDatabase
from app.config import settings
from app.core.logger import set_correlation_id, reset_correlation_id
from app.core.exceptions import MaxRetriesExceededError, FloodWaitError
from app.distribution.dispatcher import DistributionDispatcher
from app.distribution.flood_wait import FloodWaitHandler, calculate_retry_delay
from app.distribution.target_balancer import TargetBalancer
from app.repositories.queue_repository import QueueRepository
from app.distribution.lock_service import DistributedLockService
from app.distribution.rate_limiter import RateLimiterService
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
        recovered = await self._queue_repo.recover_stale_jobs()
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
        if self._task and not self._task.done():
            logger.info("Draining dispatcher worker...", extra={"ctx_worker": self._worker_id})
            try:
                await asyncio.wait_for(self._task, timeout=30.0)
            except asyncio.TimeoutError:
                logger.warning("Dispatcher worker drain timeout, force cancelling", extra={"ctx_worker": self._worker_id})
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

                groups = {}
                for job in jobs:
                    meta = job.get("metadata") or {}
                    gid = job.get("media_group_id") or meta.get("media_group_id") or str(job["_id"])
                    if gid not in groups:
                        groups[gid] = []
                    groups[gid].append(job)

                tasks = [self._handle_job_group(group) for group in groups.values()]
                results = await asyncio.gather(*tasks, return_exceptions=True)
                for res in results:
                    if isinstance(res, asyncio.CancelledError):
                        raise res
                    elif isinstance(res, Exception):
                        logger.error("Unhandled exception in job handler", exc_info=res)

            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(
                    "Dispatcher worker loop error",
                    extra={"ctx_worker": self._worker_id, "ctx_error": str(e)},
                    exc_info=True,
                )
                await asyncio.sleep(5)

    async def _handle_job_group(self, job_docs: list[dict]) -> None:
        primary_job = job_docs[0]
        primary_id = str(primary_job["_id"])
        meta = primary_job.get("metadata") or {}
        group_id = primary_job.get("media_group_id") or meta.get("media_group_id") or primary_id

        corr_token = set_correlation_id(f"grp_{group_id}")
        try:
            lock_key = f"group:{group_id}"
            acquired = await self._lock_service.acquire(
                lock_key,
                ttl_seconds=settings.LOCK_TTL_SECONDS,
            )
            if not acquired:
                logger.warning(
                    "Could not acquire group lock — another worker is processing it",
                    extra={"ctx_group_id": group_id, "ctx_worker": self._worker_id},
                )
                for job in job_docs:
                    await self._queue_repo.release_claim(str(job["_id"]))
                return

            try:
                for job in job_docs:
                    marked = await self._queue_repo.mark_processing(str(job["_id"]), self._worker_id)
                    if not marked:
                        logger.warning(
                            "Job already taken by another worker after claim",
                            extra={"ctx_job_id": str(job["_id"])},
                        )

                success = await self._dispatcher.dispatch(job_docs, self._worker_id)

                if not success:
                    remaining = set(primary_job.get("target_channel_ids", [])) - set(primary_job.get("delivered_targets", []))
                    max_wait = 0.0
                    for tid in remaining:
                        wait = self._flood_handler.seconds_until_available(tid)
                        if wait > max_wait:
                            max_wait = wait
                    
                    if max_wait > 0:
                        logger.warning("Group deferred due to FloodWait", extra={"ctx_group_id": group_id, "ctx_wait": max_wait})
                        for job in job_docs:
                            await self._queue_repo.mark_failed(
                                str(job["_id"]), 
                                "Deferred due to FloodWait", 
                                next_retry_delay_seconds=max_wait, 
                                increment_retry=False
                            )
                    else:
                        for job in job_docs:
                            await self._handle_retry(job, str(job["_id"]), "Partial delivery failure")

            except asyncio.CancelledError:
                logger.warning("Group cancelled during processing, releasing claim", extra={"ctx_group_id": group_id})
                for job in job_docs:
                    await self._queue_repo.release_claim(str(job["_id"]))
                raise

            except MaxRetriesExceededError as e:
                for job in job_docs:
                    await self._queue_repo.move_to_dead_letter(str(job["_id"]), str(e))

            except FloodWaitError as e:
                logger.warning(
                    "FloodWait encountered during dispatch",
                    extra={"ctx_group_id": group_id, "ctx_wait": e.seconds},
                )
                for job in job_docs:
                    await self._handle_retry(job, str(job["_id"]), str(e), override_delay=e.seconds)

            except Exception as e:
                logger.error(
                    "Unexpected error handling group",
                    extra={"ctx_group_id": group_id, "ctx_error": str(e)},
                    exc_info=True,
                )
                for job in job_docs:
                    await self._handle_retry(job, str(job["_id"]), str(e))

            finally:
                await self._lock_service.release(lock_key)
        finally:
            reset_correlation_id(corr_token)

    async def _handle_retry(
        self, job_doc: dict, job_id: str, error: str, override_delay: Optional[int] = None
    ) -> None:
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

        delay = override_delay if override_delay is not None else calculate_retry_delay(retry_count)
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


class DispatcherWorkerPool:
    """Manages N dispatcher worker tasks concurrently."""

    def __init__(
        self,
        db: AsyncIOMotorDatabase,
        delivery_callback: Callable,
        rate_limiter: RateLimiterService,
        flood_handler: FloodWaitHandler,
        target_balancer: TargetBalancer,
        worker_count: Optional[int] = None,
    ):
        self._worker_count = worker_count or settings.DISPATCHER_WORKER_COUNT
        self._workers: list[DispatcherWorker] = []
        self._db = db
        self._delivery_callback = delivery_callback
        self._rate_limiter = rate_limiter
        self._flood_handler = flood_handler
        self._balancer = target_balancer

    async def start(self) -> None:
        for i in range(self._worker_count):
            worker = DispatcherWorker(
                worker_id=f"dispatcher-{i}",
                db=self._db,
                delivery_callback=self._delivery_callback,
                rate_limiter=self._rate_limiter,
                shared_flood_handler=self._flood_handler,
                shared_balancer=self._balancer,
            )
            self._workers.append(worker)
            await worker.start()

        logger.info(
            f"Dispatcher pool started with {self._worker_count} workers",
            extra={"ctx_count": self._worker_count},
        )

    async def stop(self) -> None:
        if self._workers:
            # Drain concurrently to avoid blocking timeout delays
            await asyncio.gather(*(worker.stop() for worker in self._workers), return_exceptions=True)
        self._workers.clear()
        logger.info("Dispatcher pool stopped")
