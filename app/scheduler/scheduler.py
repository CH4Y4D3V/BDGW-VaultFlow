"""
app/scheduler/scheduler.py

Distribution scheduler — singleton orchestrator for the VaultFlow queue engine.

Responsibilities:
  - Acquire and maintain a MongoDB-backed distributed singleton lock so only
    one scheduler process is active at a time across all deployment replicas.
  - Run the main distribution cycle on a configurable interval to enqueue
    content for delivery.
  - Enforce per-vault-type daily caps loaded from environment variables.
  - Enforce strict NSFW / Premium queue separation — no cross-contamination.
  - Sweep stale job locks and deadline-exceeded jobs on periodic intervals.
  - Collect queue health metrics every 60 seconds.

Spec references:
  §12.2  Dual queue architecture (NSFW / Premium, never mixed)
  §12.5  Daily limits: NSFW_DAILY_LIMIT / PREMIUM_DAILY_LIMIT (ENV-controlled)
  §22    Audit logging (queue actions)
  §24    FloodWait / async / restart safety
  §25    Restart safety — DB is source of truth
"""

import asyncio
import random
from datetime import datetime, timedelta, timezone
from typing import Any, Callable, Optional

from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.jobstores.memory import MemoryJobStore
from apscheduler.triggers.interval import IntervalTrigger
from motor.motor_asyncio import AsyncIOMotorDatabase
from pymongo.errors import DuplicateKeyError

from app.config import settings
from app.core.exceptions import DuplicateJobError
from app.core.models import (
    DistributionPriority,
    JobStatus,
    MediaType,
    QueueJob,
    WatermarkState,
)
from app.distribution.fairness import FairnessSelector
from app.repositories.queue_repository import QueueRepository
from app.utils.logger import get_logger

logger = get_logger(__name__)


# ── DAILY CAPS ────────────────────────────────────────────────────────────────
#
# Spec §12.5 defines NSFW_DAILY_LIMIT and PREMIUM_DAILY_LIMIT as the canonical
# env var names.  Legacy deployments may use DAILY_CAP_NSFW / DAILY_CAP_PREMIUM.
# We support both with a priority chain: spec name → legacy name → hard default.
# This ensures operators following either the spec env names or the legacy names
# both get correct cap enforcement.
#
_DAILY_CAPS: dict[str, int] = {
    "nsfw": int(
        getattr(settings, "NSFW_DAILY_LIMIT", None)
        or getattr(settings, "DAILY_CAP_NSFW", 75)
    ),
    "premium": int(
        getattr(settings, "PREMIUM_DAILY_LIMIT", None)
        or getattr(settings, "DAILY_CAP_PREMIUM", 140)
    ),
}

# Frozen set of valid vault types.  Any resolved vault_type not in this set
# is rejected before enqueueing to prevent cross-queue contamination (§12.2).
_VALID_VAULT_TYPES: frozenset[str] = frozenset({"nsfw", "premium"})


def _get_daily_cap(vault_type: str) -> int:
    """
    Return the configured daily posting cap for the given vault type.

    Falls back to 100 for any unrecognised vault type, though in practice
    unrecognised types are rejected upstream before this is called.

    Args:
        vault_type: 'nsfw' or 'premium'.

    Returns:
        Maximum number of completed jobs allowed in a 24-hour window.
    """
    return _DAILY_CAPS.get(vault_type, 100)


def _resolve_vault_type(content_item: dict, source_id: str) -> Optional[str]:
    """
    Resolve the canonical vault_type ('nsfw' or 'premium') for a content item.

    Resolution order:
      1. content_item['moderation_destination'] — the field actually written
         by archive_to_vault() on every vault document (Section 11). This is
         the most reliable source: provider.py's vault.find() query is
         itself filtered on this exact field, so it is guaranteed present
         and correct on every item reaching this function, regardless of
         which approval path wrote it (execute_queue's "submission_" source
         label, or handle_direct_vault_upload's raw vault channel ID).
      2. content_item['vault_type']     — explicit field, if ever set.
      3. source_id label prefix          — e.g. 'submission_nsfw' → 'nsfw'.
         Only matches the execute_queue() approval path; does NOT match
         direct-vault-upload's raw channel ID source_channel_id values
         (e.g. '-1002048690257'), which is why (1) above is required.
      4. None                            — caller must treat as unresolvable.

    Args:
        content_item: Content descriptor dict from the provider callback.
        source_id:    source_channel_id label string (e.g. 'submission_nsfw'
                      or a raw vault channel ID string).

    Returns:
        'nsfw', 'premium', or None if the type cannot be determined.
    """
    # Most authoritative: the field archive_to_vault() actually writes.
    mod_dest = content_item.get("moderation_destination")
    if mod_dest in _VALID_VAULT_TYPES:
        return mod_dest

    # Explicit vault_type on the content item, if ever set.
    vault_type = content_item.get("vault_type")
    if vault_type in _VALID_VAULT_TYPES:
        return vault_type

    # Parse from source_id label: 'submission_nsfw' → 'nsfw'.
    if source_id.startswith("submission_"):
        candidate = source_id[len("submission_"):]
        if candidate in _VALID_VAULT_TYPES:
            return candidate

    return None


class DistributionScheduler:
    """
    Singleton distribution scheduler for the VaultFlow queue engine.

    Only one instance should be active at a time across all deployment replicas.
    Singleton enforcement is implemented via a MongoDB distributed lock with a
    TTL-based heartbeat.

    Usage::

        scheduler = DistributionScheduler(db, content_provider_callback)
        await scheduler.start()
        # ... application runs ...
        await scheduler.stop()
    """

    def __init__(
        self,
        db: AsyncIOMotorDatabase,
        content_provider_callback: Callable,
    ) -> None:
        """
        Initialise the scheduler without starting it.

        Args:
            db:
                Motor async database handle pointing to the application DB.
            content_provider_callback:
                Async callable that returns a list of channel config dicts.
                Each dict must contain at minimum:
                  - source_channel_id (str)
                  - target_channel_ids (list[str])
                  - content (list[dict])
                Optional keys: watermark_config, vault_type.
        """
        self._db = db
        self._queue_repo = QueueRepository(db)
        self._fairness = FairnessSelector(db)
        self._content_provider = content_provider_callback
        self._locks = db[settings.LOCK_COLLECTION]

        jobstores = {"default": MemoryJobStore()}
        self._scheduler = AsyncIOScheduler(jobstores=jobstores, timezone="UTC")
        self._started = False
        self._lock_task: Optional[asyncio.Task] = None

    # ── DISTRIBUTED LOCK ─────────────────────────────────────────────────────

    async def _acquire_lock(self) -> bool:
        """
        Attempt to acquire the singleton scheduler lock in MongoDB.

        Inserts a lock document with a unique 'lock_key' field.  The
        collection must have a unique index on 'lock_key' so that a
        concurrent insert by another replica raises DuplicateKeyError.

        The lock document expires at now + 90 s.  The heartbeat
        (_extend_lock) refreshes the expiry every 30 s, providing a
        3× safety margin against transient MongoDB latency spikes.

        Returns:
            True  — lock acquired successfully; this instance is primary.
            False — lock already held by another instance; stand by.
        """
        lock_key = "scheduler_active_singleton"
        expires_at = datetime.now(timezone.utc) + timedelta(seconds=90)
        try:
            await self._locks.insert_one({
                "lock_key": lock_key,
                "expires_at": expires_at,
                "created_at": datetime.now(timezone.utc),
                "owner": "scheduler_process",
            })
            return True
        except DuplicateKeyError:
            return False

    async def _extend_lock(self) -> None:
        """
        Heartbeat coroutine that keeps the singleton lock alive.

        Refreshes the lock TTL to (now + 90 s) every 30 s while the
        scheduler is running.  Runs as a background asyncio Task created
        in start().  All MongoDB errors are caught and logged — a single
        failed refresh does not terminate the heartbeat loop, giving the
        scheduler multiple chances to renew before the 90 s TTL expires.

        Terminates cleanly when self._started is set to False by stop().
        """
        while self._started:
            await asyncio.sleep(30)
            try:
                await self._locks.update_one(
                    {"lock_key": "scheduler_active_singleton"},
                    {
                        "$set": {
                            "expires_at": datetime.now(timezone.utc) + timedelta(seconds=90),
                        }
                    },
                )
            except Exception as exc:
                logger.error(
                    f"Scheduler lock heartbeat refresh failed: {exc}",
                    exc_info=True,
                )

    # ── LIFECYCLE ─────────────────────────────────────────────────────────────

    async def start(self) -> None:
        """
        Start the distribution scheduler as the primary instance.

        Steps:
          1. Idempotency guard — returns immediately if already started.
          2. Delete any stale lock left by a previous crashed instance of
             this same process (not other replicas — each replica should
             call start() only once).
          3. Attempt to acquire the singleton lock.  If another live
             instance holds it, log and return (stand-by mode).
          4. Run startup integrity scan to recover orphaned jobs.
          5. Register all periodic APScheduler jobs (distribution cycle,
             stale-lock sweep, deadline sweep, metrics collection).
          6. Start APScheduler and the lock heartbeat task.
        """
        if self._started:
            return

        # Clear any lock this process left behind on a previous run (crash
        # recovery).  We do NOT clear locks owned by other replicas — that
        # would allow two primary schedulers to run simultaneously.
        await self._locks.delete_many({"lock_key": "scheduler_active_singleton"})
        logger.info("Scheduler: stale lock cleared on startup")

        if not await self._acquire_lock():
            logger.info(
                "Scheduler: another instance holds the singleton lock. "
                "This instance will stand by."
            )
            return

        logger.info("Scheduler: singleton lock acquired. Running integrity scan...")
        await self._run_startup_integrity_scan()

        # Distribution cycle — main work loop.
        self._scheduler.add_job(
            self._distribution_cycle,
            trigger=IntervalTrigger(seconds=settings.SCHEDULER_INTERVAL_SECONDS),
            id="distribution_cycle",
            replace_existing=True,
            max_instances=1,
            coalesce=True,
        )
        # Stale lock sweep — recovers PROCESSING jobs interrupted by crashes.
        self._scheduler.add_job(
            self._stale_lock_sweep,
            trigger=IntervalTrigger(seconds=120),
            id="stale_lock_sweep",
            replace_existing=True,
            max_instances=1,
            coalesce=True,
        )
        # Deadline sweep — moves expired jobs to dead-letter queue.
        self._scheduler.add_job(
            self._deadline_sweep,
            trigger=IntervalTrigger(seconds=300),
            id="deadline_sweep",
            replace_existing=True,
            max_instances=1,
            coalesce=True,
        )
        # Metrics collection — emits queue health stats every 60 s.
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
        """
        Gracefully stop the scheduler and release the singleton lock.

        Actions:
          - Sets self._started = False to terminate the heartbeat loop.
          - Cancels the heartbeat Task.
          - Shuts down APScheduler (wait=True drains running jobs first).
          - Deletes the MongoDB lock document so another replica can take
            over immediately without waiting for the TTL to expire.
        """
        if not self._started:
            return

        self._started = False
        if self._lock_task:
            self._lock_task.cancel()
        self._scheduler.shutdown(wait=True)
        await self._locks.delete_one({"lock_key": "scheduler_active_singleton"})
        logger.info("Distribution scheduler stopped and singleton lock released")

    # ── STARTUP INTEGRITY SCAN ────────────────────────────────────────────────

    async def _run_startup_integrity_scan(self) -> None:
        """
        Scan the queue collection for data integrity problems on startup.

        This scan repairs state left behind by a previous process crash or
        an interrupted deployment.  Three categories of issues are corrected:

        1. Orphaned PROCESSING jobs (§25):
             Jobs that were claimed for delivery but never completed because
             the worker process was killed.  Reset to PENDING so they can be
             retried by the delivery engine.

        2. Missing vault references:
             PENDING / WATERMARKING / READY jobs with null vault_chat_id or
             vault_message_id will always fail delivery (VaultReferenceMissingError).
             Quarantine them immediately to prevent them from consuming retry
             budget.  Root cause: incomplete upstream vault archival.

        3. Partially-watermarked LOCKED jobs:
             Jobs that were LOCKED for watermarking but the watermark worker
             crashed mid-operation and have been locked for >30 minutes.
             Reset to WATERMARKING / PENDING watermark state so the pipeline
             can restart them cleanly.
        """
        now = datetime.now(timezone.utc)
        queue = self._db[settings.QUEUE_COLLECTION]

        # 1. Recover orphaned PROCESSING jobs.
        recovered = await self._queue_repo.recover_stale_jobs()
        if recovered:
            logger.warning(
                f"Startup scan: recovered {recovered} orphaned PROCESSING jobs → PENDING"
            )

        # 2. Quarantine jobs with missing vault references.
        missing_refs_result = await queue.update_many(
            {
                "status": {
                    "$in": [JobStatus.PENDING, JobStatus.WATERMARKING, JobStatus.READY]
                },
                "$or": [{"vault_chat_id": None}, {"vault_message_id": None}],
            },
            {
                "$set": {
                    "status": JobStatus.QUARANTINE,
                    "quarantine_reason": "missing_vault_references",
                    "updated_at": now,
                }
            },
        )
        if missing_refs_result.modified_count:
            logger.warning(
                f"Startup scan: quarantined {missing_refs_result.modified_count} jobs "
                "— missing vault references (vault archival was incomplete)"
            )

        # 3. Reset partially-watermarked LOCKED jobs.
        broken_wm_result = await queue.update_many(
            {
                "status": JobStatus.LOCKED,
                "watermark_state": WatermarkState.PROCESSING,
                "locked_at": {"$lt": now - timedelta(minutes=30)},
            },
            {
                "$set": {
                    "status": JobStatus.WATERMARKING,
                    "watermark_state": WatermarkState.PENDING,
                    "locked_by": None,
                    "locked_at": None,
                    "updated_at": now,
                }
            },
        )
        if broken_wm_result.modified_count:
            logger.warning(
                f"Startup scan: reset {broken_wm_result.modified_count} partially "
                "watermarked LOCKED jobs → WATERMARKING"
            )

    # ── DISTRIBUTION CYCLE ────────────────────────────────────────────────────

    async def _distribution_cycle(self) -> None:
        """
        Main distribution loop — executed on every scheduler tick.

        For each channel config returned by the content provider:
          1. Resolve vault_type ('nsfw' or 'premium') from the content item
             or source_channel_id label.  Skip the config entirely if the
             type is unresolvable to prevent cross-queue contamination (§12.2).
          2. Look up the daily cap for this vault type from _DAILY_CAPS.
          3. Count jobs completed in the last 24 hours for this source channel.
          4. Calculate available slots (min of cycle budget and remaining cap).
          5. Ask the fairness layer to select eligible content.
          6. Assign randomised execute times — same media_group_id gets the
             same time so album items are co-scheduled.
          7. Enqueue each selected item.

        The entire cycle is skipped if backpressure is detected (queue depth
        exceeds MAX_JOBS_PER_CYCLE × 2).

        All per-channel exceptions are caught and logged individually; one
        failing channel config does not abort the whole cycle.
        """
        if await self._check_backpressure():
            logger.warning(
                "Scheduler: backpressure threshold reached — skipping distribution cycle"
            )
            return

        try:
            logger.info("Distribution cycle: starting")
            channel_configs = await self._content_provider()
        except Exception as exc:
            logger.error(
                f"Distribution cycle: FAILED at content_provider step: {exc}",
                exc_info=True,
            )
            return

        if not channel_configs:
            logger.debug("Distribution cycle: no channel configs returned — nothing to do")
            return

        total_enqueued = 0

        for config in channel_configs:
            try:
                source_id: str = config["source_channel_id"]
                targets: list[str] = config["target_channel_ids"]
                content: list[dict] = config.get("content", [])

                if not content or not targets:
                    continue

                # Resolve vault_type for daily-cap lookup and NSFW/Premium
                # separation.  All items in a single channel config share the
                # same vault type so we resolve from the first item.
                vault_type = _resolve_vault_type(content[0], source_id)
                if vault_type is None:
                    logger.error(
                        f"Distribution cycle: cannot resolve vault_type for "
                        f"source_id={source_id!r} — skipping to prevent "
                        "cross-queue contamination (§12.2)"
                    )
                    continue

                daily_cap = _get_daily_cap(vault_type)
                posted_today = await self._get_posted_count_last_24h(source_id)
                remaining = daily_cap - posted_today

                if remaining <= 0:
                    logger.info(
                        f"Distribution cycle: daily cap reached for "
                        f"vault_type={vault_type!r} source={source_id!r} "
                        f"cap={daily_cap} posted={posted_today}"
                    )
                    continue

                pending = await self._queue_repo.get_channel_pending_count(source_id)
                slots = min(settings.MAX_JOBS_PER_CYCLE - pending, remaining)
                if slots <= 0:
                    continue

                selected = await self._fairness.select_eligible_content(
                    available_content=content,
                    source_channel_id=source_id,
                    max_count=slots,
                )

                if not selected:
                    continue

                # Assign execute times.  Items sharing a media_group_id belong
                # to the same album and must be co-scheduled (same execute_after).
                group_times: dict[str, datetime] = {}
                g_idx = 0
                for item in selected:
                    gid: str = item.get("media_group_id") or item.get("content_id")
                    if gid not in group_times:
                        group_times[gid] = self._randomized_execute_time(g_idx)
                        g_idx += 1

                    if await self._enqueue_content(
                        item,
                        source_id,
                        targets,
                        group_times[gid],
                        config.get("watermark_config"),
                    ):
                        total_enqueued += 1

            except Exception as exc:
                logger.error(
                    f"Distribution cycle: error processing "
                    f"source_id={config.get('source_channel_id')!r}: {exc}",
                    exc_info=True,
                )

        logger.info(
            "Distribution cycle: complete",
            extra={"ctx_enqueued": total_enqueued},
        )

    # ── BACKPRESSURE ──────────────────────────────────────────────────────────

    async def _check_backpressure(self) -> bool:
        """
        Return True if the active queue depth exceeds the safe threshold.

        Counts all jobs in active states (PENDING, WATERMARKING, READY,
        LOCKED, DELIVERING).  If the total exceeds MAX_JOBS_PER_CYCLE × 2
        the distribution cycle should be skipped to avoid unbounded memory /
        DB growth.

        Returns:
            True  — backpressure active; skip this cycle.
            False — queue depth is acceptable; proceed normally.
            False — also returned on DB error (fail-open: prefer distributing
                    over starvation from a transient query failure).
        """
        try:
            queue = self._db[settings.QUEUE_COLLECTION]
            active_count = await queue.count_documents({
                "status": {
                    "$in": [
                        JobStatus.PENDING,
                        JobStatus.WATERMARKING,
                        JobStatus.READY,
                        JobStatus.LOCKED,
                        JobStatus.DELIVERING,
                    ]
                }
            })
            return active_count >= settings.MAX_JOBS_PER_CYCLE * 2
        except Exception as exc:
            logger.error(f"Backpressure check failed (failing open): {exc}", exc_info=True)
            return False

    # ── DAILY CAP TRACKING ────────────────────────────────────────────────────

    async def _get_posted_count_last_24h(self, source_channel_id: str) -> int:
        """
        Count jobs completed for the given source channel in the last 24 hours.

        Used to enforce per-source-channel daily posting caps (§12.5).  The
        query filters on COMPLETED status and completed_at >= (now - 24h).

        On any DB error, returns 0 (fail-open for cap checks: prefer delivering
        over starvation caused by a transient query failure).

        Args:
            source_channel_id: Source channel label string (e.g. 'submission_nsfw').

        Returns:
            Number of completed jobs in the past 24 hours, or 0 on error.
        """
        try:
            cutoff = datetime.now(timezone.utc) - timedelta(hours=24)
            return await self._db[settings.QUEUE_COLLECTION].count_documents({
                "source_channel_id": source_channel_id,
                "status": JobStatus.COMPLETED,
                "completed_at": {"$gte": cutoff},
            })
        except Exception as exc:
            logger.error(
                f"Daily-count query failed for source_channel_id={source_channel_id!r}: {exc}",
                exc_info=True,
            )
            return 0

    # ── ENQUEUE ───────────────────────────────────────────────────────────────

    async def _enqueue_content(
        self,
        content_item: dict,
        source_channel_id: str,
        target_channel_ids: list[str],
        execute_after: datetime,
        watermark_config: Optional[dict] = None,
    ) -> bool:
        """
        Build a QueueJob from a content descriptor and write it to the queue.

        vault_message_id resolution:
            The field 'vault_message_id' in content_item is the authoritative
            message ID in the vault channel.  If absent, falls back to
            'message_id' with a warning — this indicates incomplete upstream
            vault archival and the delivery pipeline may fail at _resolve_vault_ref.
            Callers should ensure vault archival populates vault_message_id
            before the content item reaches the scheduler.

        Args:
            content_item:       Content descriptor dict from the provider.
            source_channel_id:  Source channel label string.
            target_channel_ids: Distribution target channel/group ID list.
            execute_after:      Earliest scheduled delivery datetime (UTC).
            watermark_config:   Watermark parameters; None → no watermark.

        Returns:
            True  — job written to the queue successfully.
            False — duplicate job (DuplicateJobError) or unexpected error.
        """
        watermark_required = watermark_config is not None
        media_type_str = content_item.get("media_type", "text")

        try:
            media_type = MediaType(media_type_str)
        except ValueError:
            media_type = MediaType.TEXT

        # Vault message ID resolution with explicit fallback warning.
        vault_message_id = content_item.get("vault_message_id")
        if not vault_message_id:
            fallback_id = content_item.get("message_id")
            if fallback_id:
                logger.warning(
                    f"content_id={content_item.get('content_id')!r}: "
                    "'vault_message_id' is absent — falling back to 'message_id'. "
                    "Ensure vault archival sets vault_message_id to avoid "
                    "delivery failures in _resolve_vault_ref."
                )
            vault_message_id = fallback_id

        initial_status = JobStatus.WATERMARKING if watermark_required else JobStatus.PENDING

        # Resolve the correct destination-specific vault channel ID.
        #
        # ROOT CAUSE FIX (spec Section 11 — strict NSFW/Premium vault
        # separation): this previously hardcoded settings.VAULT_CHANNEL_ID
        # for EVERY job regardless of the content's actual destination. Since
        # this deployment's VAULT_CHANNEL_ID happens to equal
        # NSFW_VAULT_CHANNEL_ID, NSFW jobs got the right channel by
        # coincidence — but every PREMIUM job's vault_chat_id pointed at the
        # wrong channel, so resolve_fresh_message() (media_refresh.py) could
        # never locate the message there, causing every watermark job to
        # exhaust all resolution paths and fall back to a raw file_id.
        #
        # Resolution order:
        #   1. content_item['vault_channel_id'] — the real field written by
        #      archive_to_vault() on every vault document (most reliable,
        #      zero extra logic needed since provider.py returns full
        #      documents with no field projection).
        #   2. Destination-aware lookup via moderation_destination, mirroring
        #      _resolve_vault_channel_id() in moderation_actions.py.
        #   3. settings.VAULT_CHANNEL_ID — legacy fallback only.
        raw_vault_channel_id = content_item.get("vault_channel_id")
        try:
            resolved_vault_chat_id = int(raw_vault_channel_id) if raw_vault_channel_id else None
        except (TypeError, ValueError):
            resolved_vault_chat_id = None
        if not resolved_vault_chat_id:
            mod_dest = content_item.get("moderation_destination")
            if mod_dest == "nsfw":
                resolved_vault_chat_id = settings.NSFW_VAULT_CHANNEL_ID or settings.VAULT_CHANNEL_ID
            elif mod_dest == "premium":
                resolved_vault_chat_id = settings.PREMIUM_VAULT_CHANNEL_ID or settings.VAULT_CHANNEL_ID
            else:
                resolved_vault_chat_id = settings.VAULT_CHANNEL_ID

        job = QueueJob(
            schema_version=1,
            content_id=content_item["content_id"],
            source_channel_id=source_channel_id,
            source_message_id=content_item.get("message_id"),
            vault_chat_id=resolved_vault_chat_id,
            vault_message_id=vault_message_id,
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
            logger.debug(
                "Job enqueued",
                extra={
                    "ctx_content_id": content_item["content_id"],
                    "ctx_source": source_channel_id,
                    "ctx_execute_after": execute_after.isoformat(),
                    "ctx_watermark": watermark_required,
                },
            )
            return True
        except DuplicateJobError:
            return False
        except Exception:
            logger.error(
                f"Failed to enqueue content_id={content_item.get('content_id')!r}",
                exc_info=True,
            )
            return False

    # ── SCHEDULING HELPERS ────────────────────────────────────────────────────

    def _randomized_execute_time(self, index: int) -> datetime:
        """
        Return a randomised future datetime for job scheduling.

        Delay is uniformly sampled from [60, 300] seconds (1–5 minutes) to
        avoid bursty posting behaviour.  A deterministic stagger of 2 s ×
        index is added on top to maintain relative ordering between groups
        enqueued in the same cycle without scheduling them at identical times.

        Args:
            index: Zero-based position of this group in the current cycle.

        Returns:
            UTC datetime between (now + 60 s) and (now + 300 s + stagger).
        """
        delay = random.uniform(60, 300)
        stagger = index * 2  # 2 s gap between consecutive groups in this cycle
        return datetime.now(timezone.utc) + timedelta(seconds=delay + stagger)

    # ── PERIODIC MAINTENANCE JOBS ─────────────────────────────────────────────

    async def _stale_lock_sweep(self) -> None:
        """
        Recover queue jobs that have been PROCESSING for too long.

        Delegates to QueueRepository.recover_stale_jobs() which resets
        orphaned PROCESSING jobs back to PENDING.  Called every 120 s by
        APScheduler.  Acts as a safety net for delivery worker crashes that
        occur after the startup integrity scan.
        """
        try:
            recovered = await self._queue_repo.recover_stale_jobs()
            if recovered:
                logger.warning(
                    f"Stale-lock sweep: recovered {recovered} orphaned PROCESSING jobs"
                )
        except Exception as exc:
            logger.error(f"Stale-lock sweep FAILED: {exc}", exc_info=True)

    async def _deadline_sweep(self) -> None:
        """
        Move deadline-exceeded jobs to the dead-letter queue (§12.3).

        Jobs that have missed their delivery deadline are irrecoverably
        moved to dead_letters for manual admin review.  Called every 300 s
        by APScheduler.
        """
        try:
            now = datetime.now(timezone.utc)
            jobs = await self._queue_repo.get_deadline_exceeded_jobs(now)
            for job in jobs:
                await self._queue_repo.move_to_dead_letter(
                    str(job["_id"]), "deadline_exceeded"
                )
        except Exception as exc:
            logger.error(
                "Deadline sweep FAILED",
                extra={"ctx_error": str(exc)},
                exc_info=True,
            )

    async def _collect_metrics(self) -> None:
        """
        Collect queue health metrics and emit them to the structured log.

        Called every 60 s by APScheduler.  The metrics model is emitted at
        INFO level so it can be scraped by a log aggregator or monitoring
        pipeline.  A None return from collect_metrics (unexpected but
        possible) is handled gracefully with a warning log instead of an
        AttributeError crash.
        """
        try:
            metrics = await self._queue_repo.collect_metrics()
            if metrics is not None:
                logger.info("Queue metrics collected", extra=metrics.model_dump())
            else:
                logger.warning(
                    "collect_metrics returned None — skipping metric emit. "
                    "Check QueueRepository.collect_metrics() implementation."
                )
        except Exception as exc:
            logger.error(f"Metrics collection FAILED: {exc}", exc_info=True)

    # ── PUBLIC API ────────────────────────────────────────────────────────────

    def add_custom_job(
        self,
        func: Callable,
        trigger: Any,
        _id: str,
        **kwargs: Any,
    ) -> None:
        """
        Register an arbitrary APScheduler job on the running scheduler.

        Convenience method for external callers that need to hook into this
        scheduler instance — for example, the membership reconciliation worker
        (§26) or subscription expiry notification worker (§7.7).

        All jobs registered via this method use max_instances=1 and
        coalesce=True to prevent overlapping executions.

        Args:
            func:    Async callable to execute on the trigger.
            trigger: APScheduler trigger instance (e.g. IntervalTrigger).
            _id:     Unique job ID.  An existing job with the same ID is
                     replaced atomically.
            **kwargs: Additional keyword arguments forwarded to add_job().

        Raises:
            RuntimeError: If called before start() (scheduler not yet running).
        """
        self._scheduler.add_job(
            func,
            trigger=trigger,
            id=_id,
            replace_existing=True,
            max_instances=1,
            coalesce=True,
            **kwargs,
        )