import asyncio  # FIX 10: was missing — asyncio.Event() and asyncio.create_task() used below
from datetime import datetime, timezone
from typing import List, Callable, Awaitable
from app.core.models import DistributionResult
from app.core.exceptions import (
    FloodWaitError,
)
from app.distribution.flood_wait import FloodWaitHandler
from app.distribution.target_balancer import TargetBalancer
from app.repositories.queue_repository import QueueRepository
from app.distribution.rate_limiter import RateLimiterService
from app.utils.logger import get_logger

logger = get_logger(__name__)


class DistributionDispatcher:
    """
    Executes delivery of a single job to all its target channels.
    - Respects rate limits per target and globally
    - Handles FloodWait gracefully
    - Updates job state in DB as delivery progresses
    - Does NOT directly touch Telegram — calls the registered delivery callback
    """

    def __init__(
        self,
        queue_repo: QueueRepository,
        rate_limiter: RateLimiterService,
        flood_handler: FloodWaitHandler,
        target_balancer: TargetBalancer,
        delivery_callback: Callable[[List[dict], str], Awaitable[None]],
    ):
        self._queue = queue_repo
        self._rate_limiter = rate_limiter
        self._flood_handler = flood_handler
        self._balancer = target_balancer
        self._deliver = delivery_callback  # async def deliver(job, target_id) -> None

    async def dispatch(self, job_docs: List[dict], worker_id: str) -> bool:
        """
        Main dispatch entry point.
        Returns True if job fully completed (all targets), False if partial/failed.
        """
        primary_job = job_docs[0]
        primary_id = str(primary_job["_id"])
        target_ids: List[str] = primary_job.get("target_channel_ids", [])
        delivered: List[str] = primary_job.get("delivered_targets", [])
        remaining = [t for t in target_ids if t not in delivered]

        if not remaining:
            for job in job_docs:
                await self._queue.mark_completed(str(job["_id"]))
            return True

        sorted_targets = await self._balancer.sort_targets_by_load(remaining)
        all_succeeded = True

        for target_id in sorted_targets:
            # ── Distributed Idempotency Lock ──────────────────────────────────
            # Acquire lock to prevent duplicate delivery to this target
            lock_acquired = await self._queue.acquire_delivery_lock(primary_id, target_id)
            if not lock_acquired:
                logger.warning(
                    "Duplicate delivery prevented: lock already held for target",
                    extra={"ctx_job_id": primary_id, "ctx_target": target_id}
                )
                continue

            try:
                if self._flood_handler.is_blocked(target_id):
                    all_succeeded = False
                    continue

                # ── SYSTEM 16: DAILY CAP CHECK ──
                cap_allowed, current_count = await self._rate_limiter.check_daily_cap(target_id)
                if not cap_allowed:
                    logger.warning(
                        "Daily posting cap reached for target",
                        extra={"ctx_target": target_id, "ctx_count": current_count}
                    )
                    all_succeeded = False
                    continue

                allowed, reason = await self._rate_limiter.check_and_consume(target_id)
                if not allowed:
                    all_succeeded = False
                    continue

                # Transition jobs to DELIVERING state
                for job in job_docs:
                    await self._queue.mark_delivering(str(job["_id"]), worker_id)

                # ── Heartbeat Task ────────────────────────────────────────────
                heartbeat_stop = asyncio.Event()

                async def _heartbeat():
                    while not heartbeat_stop.is_set():
                        await asyncio.sleep(30)
                        await self._queue.extend_delivery_lock(primary_id, target_id)

                heartbeat_task = asyncio.create_task(_heartbeat())

                try:
                    result = await self._dispatch_to_target(job_docs, primary_id, target_id)
                finally:
                    heartbeat_stop.set()
                    await heartbeat_task

                if result.success:
                    # ── SYSTEM 16: INCREMENT DAILY COUNT ──
                    await self._rate_limiter.increment_daily_count(target_id)

                    for job in job_docs:
                        await self._queue.record_target_delivered(str(job["_id"]), target_id)
                    await self._balancer.record_delivery(target_id, success=True)
                else:
                    for job in job_docs:
                        await self._queue.record_target_failed(str(job["_id"]), target_id, result.error or "unknown")
                    await self._balancer.record_delivery(target_id, success=False)
                    all_succeeded = False

            finally:
                # Release lock after delivery attempt (success or fail)
                await self._queue.release_delivery_lock(primary_id, target_id)

        # Re-fetch to get current delivered state
        updated = await self._queue.get_job_by_id(primary_id)
        if not updated:
            return False

        current_delivered = set(updated.get("delivered_targets", []))
        all_targets = set(target_ids)

        if all_targets == current_delivered:
            for job in job_docs:
                await self._queue.mark_completed(str(job["_id"]))
            return True

        return all_succeeded

    async def _dispatch_to_target(
        self, job_docs: List[dict], primary_id: str, target_id: str
    ) -> DistributionResult:
        try:
            await self._deliver(job_docs, target_id)
            return DistributionResult(
                job_id=primary_id,
                target_id=target_id,
                success=True,
                delivered_at=datetime.now(timezone.utc),
            )

        except FloodWaitError as e:
            self._flood_handler.register_flood_wait(target_id, e.seconds)
            return DistributionResult(
                job_id=primary_id,
                target_id=target_id,
                success=False,
                error=f"floodwait:{e.seconds}",
                floodwait_seconds=e.seconds,
            )

        except Exception as e:
            logger.error(
                "Delivery failed",
                extra={
                    "ctx_group_id": primary_id,
                    "ctx_target": target_id,
                    "ctx_error": str(e),
                },
                exc_info=True,
            )
            return DistributionResult(
                job_id=primary_id,
                target_id=target_id,
                success=False,
                error=str(e),
            )
