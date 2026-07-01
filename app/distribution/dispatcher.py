"""
app/distribution/dispatcher.py

Executes delivery of a single job group to all its target channels.

Responsibilities:
  - Enforces the correct job state machine: LOCKED → PROCESSING → DELIVERING
    → SENT / FAILED. Skipping PROCESSING was a critical bug that caused stale
    lock recovery to reset actively-delivering jobs, producing duplicate sends.
  - Respects per-target and global rate limits.
  - Handles FloodWait gracefully via ``FloodWaitHandler``.
  - Maintains a per-target heartbeat to extend the delivery lock while delivery
    is in progress, preventing premature lock expiry on slow uploads.
  - Does NOT touch the Telegram API directly — all sends go through the
    registered ``delivery_callback``.
  - Updates job state in MongoDB at every state transition for restart safety
    (Section 25 of the master reference).

State machine enforced here:
  claim_next()  → LOCKED      (done by the worker before calling dispatch)
  mark_processing()  → PROCESSING  (done once per job at start of dispatch)
  mark_delivering()  → DELIVERING  (done per target, inside the target loop)
  record_target_delivered / mark_completed → SENT
  record_target_failed   → FAILED (partial); worker handles retry/DLQ
"""

from __future__ import annotations

import asyncio
import logging
from collections.abc import Callable, Awaitable
from datetime import datetime, timezone

from app.core.models import DistributionResult
from app.core.exceptions import FloodWaitError
from app.distribution.flood_wait import FloodWaitHandler
from app.distribution.target_balancer import TargetBalancer
from app.repositories.queue_repository import QueueRepository
from app.distribution.rate_limiter import RateLimiterService

logger = logging.getLogger(__name__)


class DistributionDispatcher:
    """Delivers a single job (or media-group job batch) to all target channels.

    A "job group" (``job_docs``) is a list of queue documents that share the
    same ``media_group_id`` and must be delivered as a single atomic unit
    (albums). For single-media jobs the list has exactly one element.

    Args:
        queue_repo:        Repository for ``queue_jobs`` collection ops.
        rate_limiter:      Service for per-target rate and daily-cap checks.
        flood_handler:     Tracks and enforces FloodWait blocks per target.
        target_balancer:   Sorts targets by current delivery load.
        delivery_callback: Async callable that performs the actual Telegram
                           send. Signature:
                               ``async (job_docs: list[dict], target_id: str) -> None``
                           Must raise ``FloodWaitError`` on Telegram flood,
                           or any other exception on delivery failure.
    """

    def __init__(
        self,
        queue_repo: QueueRepository,
        rate_limiter: RateLimiterService,
        flood_handler: FloodWaitHandler,
        target_balancer: TargetBalancer,
        delivery_callback: Callable[[list[dict], str], Awaitable[None]],
    ) -> None:
        self._queue = queue_repo
        self._rate_limiter = rate_limiter
        self._flood_handler = flood_handler
        self._balancer = target_balancer
        self._deliver = delivery_callback

    # ── Public entry point ────────────────────────────────────────────────────

    async def dispatch(self, job_docs: list[dict], worker_id: str) -> bool:
        """Deliver *job_docs* to all pending target channels.

        The primary job (``job_docs[0]``) drives target selection and state.
        All jobs in the group share the same targets and are transitioned
        together at each state boundary.

        State transitions made here:
          1. ``mark_processing()`` — once, before the target loop.
          2. ``mark_delivering()`` — once per target, inside the target loop.
          3. ``record_target_delivered`` / ``record_target_failed`` — per result.
          4. ``mark_completed()`` — when all targets are confirmed delivered.

        Args:
            job_docs:  List of MongoDB job documents forming one logical unit.
            worker_id: Identifier of the calling worker (for DELIVERING state).

        Returns:
            ``True`` if all targets are confirmed delivered after this run.
            ``False`` if delivery was partial or any target failed.
        """
        primary_job = job_docs[0]
        primary_id = str(primary_job["_id"])
        target_ids: list[str] = primary_job.get("target_channel_ids", [])
        delivered: list[str] = primary_job.get("delivered_targets", [])
        remaining = [t for t in target_ids if t not in delivered]

        if not remaining:
            # All targets already delivered (idempotent re-run).
            for job in job_docs:
                await self._queue.mark_completed(str(job["_id"]))
            return True

        # ── PROCESSING transition ─────────────────────────────────────────────
        # Move jobs from LOCKED → PROCESSING before we begin target iteration.
        # This is required so stale-lock recovery (which scans for LOCKED jobs
        # past a time threshold) does not mistake an actively-processing job
        # for a stale lock and reset it to PENDING, causing a duplicate send.
        for job in job_docs:
            await self._queue.mark_processing(str(job["_id"]), worker_id)

        sorted_targets = await self._balancer.sort_targets_by_load(remaining)
        all_succeeded = True

        for target_id in sorted_targets:
            # ── Distributed idempotency lock ──────────────────────────────────
            # Prevents two workers from delivering the same job to the same
            # target concurrently (e.g. after a worker restart).
            try:
                lock_acquired = await self._queue.acquire_delivery_lock(primary_id, target_id)
            except Exception as exc:  # noqa: BLE001
                logger.error(
                    "Failed to acquire delivery lock — skipping target",
                    extra={
                        "ctx_job_id": primary_id,
                        "ctx_target": target_id,
                        "ctx_error": str(exc),
                    },
                    exc_info=True,
                )
                all_succeeded = False
                continue

            if not lock_acquired:
                logger.warning(
                    "Duplicate delivery prevented: lock already held for target",
                    extra={"ctx_job_id": primary_id, "ctx_target": target_id},
                )
                continue

            try:
                await self._deliver_to_target_with_lock(
                    job_docs=job_docs,
                    primary_id=primary_id,
                    target_id=target_id,
                    worker_id=worker_id,
                )
            except _TargetSkipped:
                all_succeeded = False
            except _TargetFailed:
                all_succeeded = False
            finally:
                # Always release the lock, whether delivery succeeded or failed.
                try:
                    await self._queue.release_delivery_lock(primary_id, target_id)
                except Exception as exc:  # noqa: BLE001
                    logger.error(
                        "Failed to release delivery lock",
                        extra={
                            "ctx_job_id": primary_id,
                            "ctx_target": target_id,
                            "ctx_error": str(exc),
                        },
                        exc_info=True,
                    )

        # ── Completion check ──────────────────────────────────────────────────
        # Re-fetch the primary job to get the authoritative delivered-targets
        # list, which may have been updated by other workers concurrently.
        updated = await self._queue.get_job_by_id(primary_id)
        if not updated:
            logger.error(
                "Primary job disappeared after delivery loop",
                extra={"ctx_job_id": primary_id},
            )
            return False

        current_delivered = set(updated.get("delivered_targets", []))
        all_targets = set(target_ids)

        if all_targets == current_delivered:
            for job in job_docs:
                await self._queue.mark_completed(str(job["_id"]))
            return True

        return all_succeeded

    # ── Internal helpers ──────────────────────────────────────────────────────

    async def _deliver_to_target_with_lock(
        self,
        job_docs: list[dict],
        primary_id: str,
        target_id: str,
        worker_id: str,
    ) -> None:
        """Run all pre-delivery checks, transition state, deliver, and record.

        Raises:
            _TargetSkipped: If delivery should be skipped (flood block, cap,
                            rate limit). The lock is still released by the
                            caller's ``finally`` block.
            _TargetFailed:  If delivery was attempted but failed. The job is
                            recorded as failed for this target.
        """
        # ── Pre-delivery guards ───────────────────────────────────────────────

        if self._flood_handler.is_blocked(target_id):
            logger.debug(
                "Target is flood-blocked — skipping",
                extra={"ctx_job_id": primary_id, "ctx_target": target_id},
            )
            raise _TargetSkipped

        cap_allowed, current_count = await self._rate_limiter.check_daily_cap(target_id)
        if not cap_allowed:
            logger.warning(
                "Daily posting cap reached for target",
                extra={"ctx_target": target_id, "ctx_count": current_count},
            )
            raise _TargetSkipped

        allowed, reason = await self._rate_limiter.check_and_consume(target_id)
        if not allowed:
            logger.debug(
                "Rate limit not satisfied — skipping target",
                extra={
                    "ctx_job_id": primary_id,
                    "ctx_target": target_id,
                    "ctx_reason": reason,
                },
            )
            raise _TargetSkipped

        # ── DELIVERING transition ─────────────────────────────────────────────
        # Move jobs from PROCESSING → DELIVERING for this specific target.
        # Must happen AFTER rate-limit checks so we don't mark a job as
        # DELIVERING if we're going to skip it anyway.
        for job in job_docs:
            await self._queue.mark_delivering(str(job["_id"]), worker_id)

        # ── Heartbeat ─────────────────────────────────────────────────────────
        # Extends the delivery lock every 30 s to prevent it from expiring
        # during a slow upload (large video, media group, etc.).
        heartbeat_stop = asyncio.Event()

        async def _heartbeat() -> None:
            while not heartbeat_stop.is_set():
                await asyncio.sleep(30)
                if heartbeat_stop.is_set():
                    break
                try:
                    await self._queue.extend_delivery_lock(primary_id, target_id)
                except Exception as exc:  # noqa: BLE001
                    logger.warning(
                        "Failed to extend delivery lock in heartbeat",
                        extra={
                            "ctx_job_id": primary_id,
                            "ctx_target": target_id,
                            "ctx_error": str(exc),
                        },
                    )

        heartbeat_task = asyncio.create_task(_heartbeat())

        try:
            result = await self._dispatch_to_target(job_docs, primary_id, target_id)
        finally:
            heartbeat_stop.set()
            # Await the task so it exits cleanly. Wrap in try/except so a
            # heartbeat error does not suppress the delivery result.
            try:
                await heartbeat_task
            except Exception as exc:  # noqa: BLE001
                logger.warning(
                    "Heartbeat task raised on shutdown",
                    extra={
                        "ctx_job_id": primary_id,
                        "ctx_target": target_id,
                        "ctx_error": str(exc),
                    },
                )

        # ── Record outcome ────────────────────────────────────────────────────
        if result.success:
            await self._rate_limiter.increment_daily_count(target_id)
            for job in job_docs:
                await self._queue.record_target_delivered(str(job["_id"]), target_id)
            await self._balancer.record_delivery(target_id, success=True)
        else:
            for job in job_docs:
                await self._queue.record_target_failed(
                    str(job["_id"]), target_id, result.error or "unknown"
                )
            await self._balancer.record_delivery(target_id, success=False)
            raise _TargetFailed

    async def _dispatch_to_target(
        self,
        job_docs: list[dict],
        primary_id: str,
        target_id: str,
    ) -> DistributionResult:
        """Invoke the delivery callback and return a ``DistributionResult``.

        This method never raises — all exceptions are caught and returned as
        failed ``DistributionResult`` objects so the caller can record them
        uniformly.

        Args:
            job_docs:   The job document list to deliver.
            primary_id: String ID of the primary job (for logging).
            target_id:  The target channel ID to deliver to.

        Returns:
            A ``DistributionResult`` indicating success or failure.
        """
        try:
            await self._deliver(job_docs, target_id)
            return DistributionResult(
                job_id=primary_id,
                target_id=target_id,
                success=True,
                delivered_at=datetime.now(timezone.utc),
            )

        except FloodWaitError as exc:
            self._flood_handler.register_flood_wait(target_id, exc.seconds)
            logger.warning(
                "FloodWait received — target blocked",
                extra={
                    "ctx_job_id": primary_id,
                    "ctx_target": target_id,
                    "ctx_flood_seconds": exc.seconds,
                },
            )
            return DistributionResult(
                job_id=primary_id,
                target_id=target_id,
                success=False,
                error=f"floodwait:{exc.seconds}",
                floodwait_seconds=exc.seconds,
            )

        except Exception as exc:  # noqa: BLE001
            logger.error(
                "Delivery failed",
                extra={
                    "ctx_job_id": primary_id,
                    "ctx_target": target_id,
                    "ctx_error": str(exc),
                },
                exc_info=True,
            )
            return DistributionResult(
                job_id=primary_id,
                target_id=target_id,
                success=False,
                error=str(exc),
            )


# ---------------------------------------------------------------------------
# Private sentinel exceptions — used only within this module to signal
# control flow from _deliver_to_target_with_lock back to the dispatch loop.
# These are not part of the public API and should not be caught externally.
# ---------------------------------------------------------------------------


class _TargetSkipped(Exception):
    """Raised when a target is skipped due to flood block, cap, or rate limit."""


class _TargetFailed(Exception):
    """Raised when a delivery attempt was made but the result was failure."""
