"""
app/services/vault_pool_service.py

PURPOSE
-------
Per Spec Section 11 & 12 (Steps 63, 66):
When the distribution queue has no fresh PENDING jobs for a given vault_type,
this service selects an eligible item from the corresponding vault archive and
creates a new PENDING queue_jobs record so the distribution worker can process
it normally.

SEPARATION CONTRACT (NEVER VIOLATE)
-------------------------------------
  nsfw vault_type   → nsfw  queue_jobs only → NSFW Group  distribution only
  premium vault_type → premium queue_jobs only → Premium Group distribution only

No cross-contamination is possible because vault_type is validated at entry
and embedded in every query filter and every written document.

SELECTION ALGORITHM
--------------------
  1. Filter vault_items: vault_type matches AND cooldown elapsed
     (last_posted_at is None  OR  last_posted_at < now - VAULT_COOLDOWN_HOURS)
  2. Sort for fair rotation:
       a. Items never posted (last_posted_at=None) sorted first
       b. Oldest last_posted_at next
       c. Lowest post_count as tiebreaker
  3. Take top VAULT_FAIRNESS_WINDOW candidates
  4. Pick one randomly within that window (prevents deterministic re-selection)
  5. Write queue_jobs record to MongoDB FIRST (restart-safe)
  6. Update vault_item tracking (last_posted_at, post_count) — non-critical
  7. Write audit_logs entry — non-critical

DISTRIBUTED LOCKING
--------------------
  Redis lock per vault_type prevents two concurrent scheduler ticks from
  selecting the same item or creating duplicate jobs.
  Lock key  : vault_pool:lock:{vault_type}
  Lock TTL  : LOCK_TTL_SECONDS (default 30s)

RESTART SAFETY
--------------
  queue_jobs document is inserted into MongoDB BEFORE any Telegram action.
  On restart, the distribution worker finds the PENDING job and processes it.
  No state is lost.
"""

import asyncio
import logging
import os
import random
from datetime import datetime, timedelta, timezone
from typing import Any, Optional

from bson import ObjectId
from motor.motor_asyncio import AsyncIOMotorDatabase

from app.config import settings
from app.core.database import DatabaseManager
from app.core.redis_client import get_redis

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Module-level constants — all configurable via environment variables
# ---------------------------------------------------------------------------

# Hours a vault item must sit idle before becoming eligible for replay.
# Prevents the same item from being reposted in back-to-back scheduler ticks.
VAULT_COOLDOWN_HOURS: int = int(os.getenv("VAULT_COOLDOWN_HOURS", "24"))

# How many top-sorted candidates to randomly choose from.
# Higher = more variety; Lower = stricter fairness.
VAULT_FAIRNESS_WINDOW: int = int(os.getenv("VAULT_FAIRNESS_WINDOW", "10"))

# Redis distributed lock TTL in seconds.
# Must be longer than the longest expected _select_and_enqueue() execution.
LOCK_TTL_SECONDS: int = int(os.getenv("VAULT_POOL_LOCK_TTL", "30"))

# Sentinel datetime used as a sort key for items with no last_posted_at.
_EPOCH = datetime.min.replace(tzinfo=timezone.utc)

# Allowed vault_type values — enforced at every entry point.
VALID_VAULT_TYPES: frozenset[str] = frozenset({"nsfw", "premium"})


# ---------------------------------------------------------------------------
# Exceptions
# ---------------------------------------------------------------------------


class VaultPoolError(Exception):
    """
    Raised when the vault pool service encounters an unrecoverable failure
    that the caller must handle (e.g., DB insertion failure after lock was held).
    """


# ---------------------------------------------------------------------------
# Service
# ---------------------------------------------------------------------------


class VaultPoolService:
    """
    Vault replay pool service.

    Responsibilities:
      - Determine when the distribution queue for a vault_type is empty.
      - Select an eligible vault_items record using cooldown filter and
        fair rotation.
      - Create a PENDING queue_jobs record so the distribution worker
        can process it without any changes.
      - Enforce strict NSFW/Premium separation at the DB query level.
      - Operate under a Redis distributed lock to prevent duplicate selection.
      - Write all state to MongoDB before any Telegram action (restart safety).

    This service does NOT forward content to groups — that is the distribution
    worker's responsibility.  This service only creates the queue job.

    Usage (from DistributionScheduler):
        vault_pool = create_vault_pool_service()
        job_id = await vault_pool.ensure_queue_has_jobs("nsfw")
        # If job_id is not None, the distribution worker will pick it up.
    """

    def __init__(self, db: AsyncIOMotorDatabase) -> None:
        """
        Initialise the service with a Motor database handle.

        Args:
            db: AsyncIOMotorDatabase from DatabaseManager.get_db().
                Must be acquired AFTER DatabaseManager.connect() has completed.
        """
        self._db = db

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    async def ensure_queue_has_jobs(
        self,
        vault_type: str,
        triggered_by: Optional[int] = None,
    ) -> Optional[ObjectId]:
        """
        Convenience method for the DistributionScheduler.

        Checks whether PENDING jobs exist for vault_type.  If the queue is
        empty, selects an eligible vault item and creates a queue_jobs record.

        Args:
            vault_type: "nsfw" or "premium".
            triggered_by: admin user_id if manually triggered; None if
                          called from the automated scheduler.

        Returns:
            ObjectId of the newly created queue_jobs record if one was
            created, or None if the queue already had jobs or the vault
            had no eligible items.

        Raises:
            ValueError: if vault_type is not "nsfw" or "premium".
        """
        if vault_type not in VALID_VAULT_TYPES:
            raise ValueError(
                f"Invalid vault_type='{vault_type}'. "
                f"Allowed values: {sorted(VALID_VAULT_TYPES)}"
            )

        is_empty = await self.is_queue_empty(vault_type)
        if not is_empty:
            logger.debug(
                "[VaultPool] Queue has PENDING jobs for vault_type=%s. "
                "No replay needed.",
                vault_type,
            )
            return None

        logger.info(
            "[VaultPool] Queue is empty for vault_type=%s. "
            "Initiating vault replay selection.",
            vault_type,
        )
        return await self.create_replay_job(vault_type, triggered_by=triggered_by)

    async def create_replay_job(
        self,
        vault_type: str,
        triggered_by: Optional[int] = None,
    ) -> Optional[ObjectId]:
        """
        Acquire Redis lock, select an eligible vault item, and create
        a PENDING queue_jobs record.

        This is the main entry point when you already know the queue is empty
        and want to force a replay cycle.

        Args:
            vault_type: "nsfw" or "premium". Enforced strictly.
            triggered_by: admin user_id if triggered manually; None if system.

        Returns:
            ObjectId of the created queue_jobs record, or None if:
              - Redis lock could not be acquired (another worker active)
              - Redis is unreachable
              - No eligible vault items exist (all on cooldown or vault empty)

        Raises:
            ValueError: if vault_type is invalid.
            VaultPoolError: if MongoDB insertion fails after lock was held.
        """
        if vault_type not in VALID_VAULT_TYPES:
            raise ValueError(
                f"Invalid vault_type='{vault_type}'. "
                f"Allowed values: {sorted(VALID_VAULT_TYPES)}"
            )

        lock_key = f"vault_pool:lock:{vault_type}"

        # --- Acquire Redis distributed lock ---
        try:
            redis = await get_redis()
            acquired = await redis.set(lock_key, "1", nx=True, ex=LOCK_TTL_SECONDS)
        except Exception as exc:
            logger.error(
                "[VaultPool] Redis unavailable — cannot acquire lock. "
                "Skipping vault replay to prevent duplicate selection. "
                "vault_type=%s error=%s",
                vault_type,
                exc,
            )
            # Fail safe: never proceed without the lock.
            return None

        if not acquired:
            logger.warning(
                "[VaultPool] Lock already held for vault_type=%s. "
                "Another scheduler worker is active. Skipping this cycle.",
                vault_type,
            )
            return None

        try:
            return await self._select_and_enqueue(vault_type, triggered_by)
        except VaultPoolError:
            # Propagate: caller must know the insertion failed.
            raise
        except Exception as exc:
            logger.exception(
                "[VaultPool] Unexpected exception in _select_and_enqueue. "
                "vault_type=%s error=%s",
                vault_type,
                exc,
            )
            # Do not propagate unknown exceptions to scheduler; log and return None.
            return None
        finally:
            # Always release the lock, even on exceptions.
            try:
                await redis.delete(lock_key)
            except Exception as exc:
                logger.error(
                    "[VaultPool] CRITICAL: Failed to release Redis lock. "
                    "Lock key=%s error=%s. "
                    "Lock will expire automatically after %ds.",
                    lock_key,
                    exc,
                    LOCK_TTL_SECONDS,
                )

    async def is_queue_empty(self, vault_type: str) -> bool:
        """
        Check whether the queue_jobs collection has any PENDING jobs for
        the given vault_type.

        Used by DistributionScheduler to decide whether to call
        create_replay_job().

        Args:
            vault_type: "nsfw" or "premium".

        Returns:
            True  — no PENDING jobs exist for this vault_type (queue is empty).
            False — at least one PENDING job exists, or DB check failed.
                    Returns False on failure to avoid triggering spurious
                    replay cycles when the DB is temporarily unreachable.

        Raises:
            ValueError: if vault_type is invalid.
        """
        if vault_type not in VALID_VAULT_TYPES:
            raise ValueError(
                f"Invalid vault_type='{vault_type}'. "
                f"Allowed values: {sorted(VALID_VAULT_TYPES)}"
            )

        try:
            count = await self._db[settings.QUEUE_COLLECTION].count_documents(
                {"vault_type": vault_type, "status": "PENDING"},
                limit=1,
            )
            return count == 0
        except Exception as exc:
            logger.exception(
                "[VaultPool] DB error checking queue empty status. "
                "vault_type=%s error=%s. "
                "Returning False (fail-safe: assume not empty).",
                vault_type,
                exc,
            )
            # Fail safe: assume not empty to avoid spurious replay cycles.
            return False

    async def get_vault_stats(self, vault_type: str) -> dict[str, Any]:
        """
        Return diagnostic statistics for the given vault type.

        Useful for monitoring dashboards, health checks, and admin commands.

        Args:
            vault_type: "nsfw" or "premium".

        Returns:
            dict with keys:
                vault_type        (str)
                total_items       (int) — all items in this vault
                never_posted      (int) — items with last_posted_at = None
                eligible          (int) — items past cooldown threshold
                on_cooldown       (int) — items still within cooldown window
                avg_post_count    (float)
                max_post_count    (int)
                cooldown_hours    (int) — current configured cooldown
                error             (str, only present on failure)

        Raises:
            ValueError: if vault_type is invalid.
        """
        if vault_type not in VALID_VAULT_TYPES:
            raise ValueError(
                f"Invalid vault_type='{vault_type}'. "
                f"Allowed values: {sorted(VALID_VAULT_TYPES)}"
            )

        cutoff_time = datetime.now(tz=timezone.utc) - timedelta(hours=VAULT_COOLDOWN_HOURS)

        pipeline = [
            {"$match": {"vault_type": vault_type}},
            {
                "$group": {
                    "_id": None,
                    "total_items": {"$sum": 1},
                    "never_posted": {
                        "$sum": {
                            "$cond": [{"$eq": ["$last_posted_at", None]}, 1, 0]
                        }
                    },
                    "eligible": {
                        "$sum": {
                            "$cond": [
                                {
                                    "$or": [
                                        {"$eq": ["$last_posted_at", None]},
                                        {"$lt": ["$last_posted_at", cutoff_time]},
                                    ]
                                },
                                1,
                                0,
                            ]
                        }
                    },
                    "avg_post_count": {"$avg": "$post_count"},
                    "max_post_count": {"$max": "$post_count"},
                }
            },
        ]

        try:
            result = await self._db[settings.VAULT_COLLECTION].aggregate(pipeline).to_list(
                length=1
            )
        except Exception as exc:
            logger.exception(
                "[VaultPool] Aggregation failed for vault stats. "
                "vault_type=%s error=%s",
                vault_type,
                exc,
            )
            return {"vault_type": vault_type, "error": str(exc)}

        if not result:
            return {
                "vault_type": vault_type,
                "total_items": 0,
                "never_posted": 0,
                "eligible": 0,
                "on_cooldown": 0,
                "avg_post_count": 0.0,
                "max_post_count": 0,
                "cooldown_hours": VAULT_COOLDOWN_HOURS,
            }

        row = result[0]
        total = row.get("total_items", 0) or 0
        eligible = row.get("eligible", 0) or 0

        return {
            "vault_type": vault_type,
            "total_items": total,
            "never_posted": row.get("never_posted", 0) or 0,
            "eligible": eligible,
            "on_cooldown": max(0, total - eligible),
            "avg_post_count": round(float(row.get("avg_post_count") or 0.0), 2),
            "max_post_count": int(row.get("max_post_count") or 0),
            "cooldown_hours": VAULT_COOLDOWN_HOURS,
        }

    # ------------------------------------------------------------------
    # Private Implementation
    # ------------------------------------------------------------------

    async def _select_and_enqueue(
        self,
        vault_type: str,
        triggered_by: Optional[int],
    ) -> Optional[ObjectId]:
        """
        Core selection and enqueue logic.  Called exclusively while holding
        the Redis distributed lock.

        Steps:
          1. Query vault_items with vault_type + cooldown filter
          2. Sort for fair rotation (never-posted first, then oldest, then
             lowest post_count)
          3. Take top VAULT_FAIRNESS_WINDOW candidates
          4. Pick one randomly from the window
          5. Insert queue_jobs record (RESTART-SAFE: DB FIRST)
          6. Update vault_item last_posted_at + post_count (non-critical)
          7. Write audit_logs entry (non-critical)

        Args:
            vault_type: Validated vault type string ("nsfw" or "premium").
            triggered_by: admin user_id or None.

        Returns:
            ObjectId of the created queue_jobs record, or None if vault
            has no eligible items.

        Raises:
            VaultPoolError: if queue_jobs MongoDB insertion fails.
        """
        cutoff_time = datetime.now(tz=timezone.utc) - timedelta(
            hours=VAULT_COOLDOWN_HOURS
        )

        # -------------------------------------------------------------------
        # Step 1: Query eligible vault items
        # vault_type is in the filter — separation is enforced at DB level.
        # -------------------------------------------------------------------
        eligible_query: dict[str, Any] = {
            "vault_type": vault_type,  # STRICT SEPARATION: never query across types
            "$or": [
                {"last_posted_at": None},
                {"last_posted_at": {"$lt": cutoff_time}},
            ],
        }

        try:
            cursor = self._db[settings.VAULT_COLLECTION].find(
                eligible_query,
                projection={
                    "_id": 1,
                    "user_id": 1,
                    "vault_type": 1,
                    "content_type": 1,
                    "vault_channel_id": 1,
                    "telegram_message_id": 1,
                    "media_hash": 1,
                    "file_id": 1,
                    "last_posted_at": 1,
                    "post_count": 1,
                    "submission_id": 1,
                },
            )
            eligible_items: list[dict[str, Any]] = await cursor.to_list(length=None)
        except Exception as exc:
            logger.exception(
                "[VaultPool] DB query failed for eligible vault items. "
                "vault_type=%s cutoff=%s error=%s",
                vault_type,
                cutoff_time.isoformat(),
                exc,
            )
            raise VaultPoolError(
                f"vault_items query failed for vault_type={vault_type}"
            ) from exc

        if not eligible_items:
            logger.info(
                "[VaultPool] No eligible items for replay. "
                "vault_type=%s cooldown_hours=%d. "
                "All items are on cooldown or vault is empty.",
                vault_type,
                VAULT_COOLDOWN_HOURS,
            )
            return None

        # -------------------------------------------------------------------
        # Step 2: Sort for fair rotation
        # Primary   : items never posted (last_posted_at=None) first
        # Secondary : oldest last_posted_at first (most stale → highest priority)
        # Tertiary  : lowest post_count first
        # -------------------------------------------------------------------
        eligible_items.sort(
            key=lambda item: (
                # never-posted items get sort key 0; posted items get 1
                0 if item.get("last_posted_at") is None else 1,
                # fallback to epoch for never-posted items (stable sort)
                item.get("last_posted_at") or _EPOCH,
                # lowest post_count wins tiebreaker
                item.get("post_count") or 0,
            )
        )

        # -------------------------------------------------------------------
        # Step 3: Take top N candidates for randomised selection
        # -------------------------------------------------------------------
        candidates = eligible_items[:VAULT_FAIRNESS_WINDOW]

        logger.info(
            "[VaultPool] Eligible=%d  Fairness window=%d  vault_type=%s",
            len(eligible_items),
            len(candidates),
            vault_type,
        )

        # -------------------------------------------------------------------
        # Step 4: Random selection within the fairness window
        # noqa: S311 — not used for cryptographic purposes
        # -------------------------------------------------------------------
        selected: dict[str, Any] = random.choice(candidates)  # noqa: S311

        vault_id: ObjectId = selected["_id"]
        now: datetime = datetime.now(tz=timezone.utc)

        # -------------------------------------------------------------------
        # Step 5: Write queue_jobs record to MongoDB FIRST
        # RESTART SAFETY: If the bot crashes after this line, the distribution
        # worker will find this PENDING job on restart and process it.
        # -------------------------------------------------------------------
        job_doc: dict[str, Any] = {
            "vault_id": vault_id,
            "vault_type": vault_type,      # embedded for index + no-cross-contamination
            "status": "PENDING",
            "created_at": now,
            "scheduled_at": now,
            "processed_at": None,
            "retry_count": 0,
            "error_message": None,
        }

        try:
            result = await self._db[settings.QUEUE_COLLECTION].insert_one(job_doc)
            job_id: ObjectId = result.inserted_id
        except Exception as exc:
            logger.exception(
                "[VaultPool] CRITICAL: Failed to insert queue_jobs record. "
                "vault_id=%s vault_type=%s error=%s",
                vault_id,
                vault_type,
                exc,
            )
            raise VaultPoolError(
                f"queue_jobs insert failed for vault_id={vault_id} "
                f"vault_type={vault_type}"
            ) from exc

        logger.info(
            "[VaultPool] Replay job created. "
            "job_id=%s vault_id=%s vault_type=%s "
            "post_count_was=%d last_posted_was=%s",
            job_id,
            vault_id,
            vault_type,
            selected.get("post_count") or 0,
            selected.get("last_posted_at"),
        )

        # -------------------------------------------------------------------
        # Step 6: Update vault_item tracking (non-critical)
        # If this fails, the job is still valid.  The item's cooldown will not
        # be recorded, meaning it might be selected again on the next cycle.
        # This is acceptable — it will eventually self-correct.
        # -------------------------------------------------------------------
        try:
            update_result = await self._db[settings.VAULT_COLLECTION].update_one(
                {"_id": vault_id},
                {
                    "$set": {"last_posted_at": now},
                    "$inc": {"post_count": 1},
                },
            )
            if update_result.matched_count == 0:
                logger.warning(
                    "[VaultPool] vault_items update matched 0 documents. "
                    "vault_id=%s may have been deleted concurrently.",
                    vault_id,
                )
        except Exception as exc:
            logger.error(
                "[VaultPool] Non-critical: vault_item tracking update failed. "
                "vault_id=%s job_id=%s error=%s. "
                "Job is still valid and will be processed.",
                vault_id,
                job_id,
                exc,
            )

        # -------------------------------------------------------------------
        # Step 7: Write audit log (non-critical)
        # -------------------------------------------------------------------
        await self._write_audit_log(
            action="VAULT_REPLAY_JOB_CREATED",
            admin_user_id=triggered_by,
            target_user_id=selected.get("user_id"),
            detail={
                "job_id": str(job_id),
                "vault_id": str(vault_id),
                "vault_type": vault_type,
                "content_type": selected.get("content_type", "unknown"),
                "media_hash": selected.get("media_hash", ""),
                "vault_channel_id": selected.get("vault_channel_id"),
                "post_count_before": selected.get("post_count") or 0,
                "last_posted_at_before": (
                    selected["last_posted_at"].isoformat()
                    if selected.get("last_posted_at")
                    else None
                ),
                "cooldown_hours": VAULT_COOLDOWN_HOURS,
                "eligible_count": len(eligible_items),
                "fairness_window": len(candidates),
                "triggered_by": triggered_by or "scheduler",
            },
        )

        return job_id

    async def _write_audit_log(
        self,
        action: str,
        admin_user_id: Optional[int],
        target_user_id: Optional[int],
        detail: dict[str, Any],
    ) -> None:
        """
        Write a single entry to the audit_logs collection.

        This method is intentionally non-raising — an audit log failure
        must never block or abort distribution operations.

        Args:
            action: Action type string (e.g., "VAULT_REPLAY_JOB_CREATED").
            admin_user_id: admin user_id or None for system-triggered events.
            target_user_id: Telegram user_id of the content submitter, or None.
            detail: Arbitrary dict of action-specific data (stored as JSON blob).
        """
        audit_doc: dict[str, Any] = {
            "action": action,
            "admin_user_id": admin_user_id,
            "target_user_id": target_user_id,
            "detail": detail,
            "timestamp": datetime.now(tz=timezone.utc),
        }
        try:
            await self._db["audit_logs"].insert_one(audit_doc)
        except Exception as exc:
            logger.error(
                "[VaultPool] Audit log write failed — non-critical. "
                "action=%s error=%s",
                action,
                exc,
            )


# ---------------------------------------------------------------------------
# Factory
# ---------------------------------------------------------------------------


def create_vault_pool_service() -> VaultPoolService:
    """
    Factory function for VaultPoolService.

    Retrieves the Motor database from the application's DatabaseManager
    singleton and returns a configured VaultPoolService instance.

    Must be called AFTER ``await DatabaseManager.connect()`` has completed
    during application startup.

    Returns:
        Configured VaultPoolService instance.

    Example:
        # In application startup or dependency injection:
        vault_pool = create_vault_pool_service()

        # In DistributionScheduler tick:
        job_id = await vault_pool.ensure_queue_has_jobs("nsfw")
    """
    db: AsyncIOMotorDatabase = DatabaseManager.get_db()
    return VaultPoolService(db=db)