import asyncio
from datetime import datetime, timezone
from typing import Optional, List
from motor.motor_asyncio import AsyncIOMotorDatabase
from app.config import settings
from app.core.models import QueueJob, DistributionResult, JobStatus, MediaType
from app.core.exceptions import (
    FloodWaitError,
    MaxRetriesExceededError,
    RateLimitExceededError,
    DispatcherError,
)
from app.distribution.flood_wait import FloodWaitHandler, calculate_retry_delay
from app.distribution.target_balancer import TargetBalancer
from app.repositories.queue_repository import QueueRepository
from app.services.rate_limiter import RateLimiterService
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
        delivery_callback,  # Injected by worker; avoids circular dep with Telegram layer
    ):
        self._queue = queue_repo
        self._rate_limiter = rate_limiter
        self._flood_handler = flood_handler
        self._balancer = target_balancer
        self._deliver = delivery_callback  # async def deliver(job, target_id) -> None

    async def dispatch(self, job_doc: dict, worker_id: str) -> bool:
        """
        Main dispatch entry point.
        Returns True if job fully completed (all targets), False if partial/failed.
        """
        job_id = str(job_doc["_id"])
        target_ids: List[str] = job_doc.get("target_channel_ids", [])
        delivered: List[str] = job_doc.get("delivered_targets", [])
        remaining = [t for t in target_ids if t not in delivered]

        if not remaining:
            logger.info(
                "All targets already delivered",
                extra={"ctx_job_id": job_id},
            )
            await self._queue.mark_completed(job_id)
            return True

        sorted_targets = await self._balancer.sort_targets_by_load(remaining)
        all_succeeded = True

        for target_id in sorted_targets:
            if self._flood_handler.is_blocked(target_id):
                wait = self._flood_handler.seconds_until_available(target_id)
                logger.warning(
                    "Target is flood-waited, skipping for now",
                    extra={
                        "ctx_job_id": job_id,
                        "ctx_target": target_id,
                        "ctx_wait": wait,
                    },
                )
                all_succeeded = False
                continue

            allowed, reason = await self._rate_limiter.check_and_consume(target_id)
            if not allowed:
                logger.warning(
                    "Rate limit hit, deferring target",
                    extra={
                        "ctx_job_id": job_id,
                        "ctx_target": target_id,
                        "ctx_reason": reason,
                    },
                )
                all_succeeded = False
                continue

            result = await self._dispatch_to_target(job_doc, job_id, target_id)

            if result.success:
                await self._queue.record_target_delivered(job_id, target_id)
                await self._balancer.record_delivery(target_id, success=True)
                logger.info(
                    "Target delivered",
                    extra={"ctx_job_id": job_id, "ctx_target": target_id},
                )
            else:
                await self._queue.record_target_failed(job_id, target_id, result.error or "unknown")
                await self._balancer.record_delivery(target_id, success=False)
                all_succeeded = False

        # Re-fetch to get current delivered state
        updated = await self._queue.get_job_by_id(job_id)
        if not updated:
            return False

        current_delivered = set(updated.get("delivered_targets", []))
        all_targets = set(target_ids)

        if all_targets == current_delivered:
            await self._queue.mark_completed(job_id)
            return True

        return all_succeeded

    async def _dispatch_to_target(
        self, job_doc: dict, job_id: str, target_id: str
    ) -> DistributionResult:
        try:
            await self._deliver(job_doc, target_id)
            return DistributionResult(
                job_id=job_id,
                target_id=target_id,
                success=True,
                delivered_at=datetime.now(timezone.utc),
            )

        except FloodWaitError as e:
            self._flood_handler.register_flood_wait(target_id, e.seconds)
            return DistributionResult(
                job_id=job_id,
                target_id=target_id,
                success=False,
                error=f"floodwait:{e.seconds}",
                floodwait_seconds=e.seconds,
            )

        except Exception as e:
            logger.error(
                "Delivery failed",
                extra={
                    "ctx_job_id": job_id,
                    "ctx_target": target_id,
                    "ctx_error": str(e),
                },
                exc_info=True,
            )
            return DistributionResult(
                job_id=job_id,
                target_id=target_id,
                success=False,
                error=str(e),
            )
