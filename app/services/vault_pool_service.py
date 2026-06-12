"""
app/services/vault_pool_service.py

FIX B-08: All queries now use `moderation_destination` instead of `vault_type`.
The vault documents inserted by moderation_actions.archive_to_vault() write
`moderation_destination: "nsfw"/"premium"` — not `vault_type`.
Queue jobs also reference vault_chat_id + vault_message_id (integers), not
a vault_id ObjectId. Both inconsistencies are corrected here.
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

VAULT_COOLDOWN_HOURS: int = int(os.getenv("VAULT_COOLDOWN_HOURS", "24"))
VAULT_FAIRNESS_WINDOW: int = int(os.getenv("VAULT_FAIRNESS_WINDOW", "10"))
LOCK_TTL_SECONDS: int = int(os.getenv("VAULT_POOL_LOCK_TTL", "30"))

_EPOCH = datetime.min.replace(tzinfo=timezone.utc)
VALID_VAULT_TYPES: frozenset[str] = frozenset({"nsfw", "premium"})


class VaultPoolError(Exception):
    pass


class VaultPoolService:
    """Vault replay pool service. See module docstring for full design notes."""

    def __init__(self, db: AsyncIOMotorDatabase) -> None:
        self._db = db

    async def ensure_queue_has_jobs(
        self,
        vault_type: str,
        triggered_by: Optional[int] = None,
    ) -> Optional[ObjectId]:
        if vault_type not in VALID_VAULT_TYPES:
            raise ValueError(f"Invalid vault_type='{vault_type}'")

        is_empty = await self.is_queue_empty(vault_type)
        if not is_empty:
            return None

        logger.info("[VaultPool] Queue empty for vault_type=%s — initiating replay.", vault_type)
        return await self.create_replay_job(vault_type, triggered_by=triggered_by)

    async def create_replay_job(
        self,
        vault_type: str,
        triggered_by: Optional[int] = None,
    ) -> Optional[ObjectId]:
        if vault_type not in VALID_VAULT_TYPES:
            raise ValueError(f"Invalid vault_type='{vault_type}'")

        lock_key = f"vault_pool:lock:{vault_type}"

        try:
            redis = await get_redis()
            acquired = await redis.set(lock_key, "1", nx=True, ex=LOCK_TTL_SECONDS)
        except Exception as exc:
            logger.error("[VaultPool] Redis unavailable — skipping replay. error=%s", exc)
            return None

        if not acquired:
            logger.warning("[VaultPool] Lock held for vault_type=%s — skipping.", vault_type)
            return None

        try:
            return await self._select_and_enqueue(vault_type, triggered_by)
        except VaultPoolError:
            raise
        except Exception as exc:
            logger.exception("[VaultPool] Unexpected error in _select_and_enqueue. error=%s", exc)
            return None
        finally:
            try:
                await redis.delete(lock_key)
            except Exception as exc:
                logger.error("[VaultPool] Failed to release Redis lock. key=%s error=%s", lock_key, exc)

    async def is_queue_empty(self, vault_type: str) -> bool:
        if vault_type not in VALID_VAULT_TYPES:
            raise ValueError(f"Invalid vault_type='{vault_type}'")
        try:
            # FIX B-08: queue jobs use `moderation_destination` not `vault_type`
            count = await self._db[settings.QUEUE_COLLECTION].count_documents(
                {"moderation_destination": vault_type, "status": "PENDING"},
                limit=1,
            )
            return count == 0
        except Exception as exc:
            logger.exception("[VaultPool] DB error checking queue. error=%s", exc)
            return False  # fail-safe: assume not empty

    async def get_vault_stats(self, vault_type: str) -> dict[str, Any]:
        if vault_type not in VALID_VAULT_TYPES:
            raise ValueError(f"Invalid vault_type='{vault_type}'")

        cutoff_time = datetime.now(tz=timezone.utc) - timedelta(hours=VAULT_COOLDOWN_HOURS)

        # FIX B-08: use `moderation_destination` not `vault_type`
        pipeline = [
            {"$match": {"moderation_destination": vault_type}},
            {
                "$group": {
                    "_id": None,
                    "total_items": {"$sum": 1},
                    "never_posted": {
                        "$sum": {"$cond": [{"$eq": ["$last_posted_at", None]}, 1, 0]}
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
            result = await self._db[settings.VAULT_COLLECTION].aggregate(pipeline).to_list(length=1)
        except Exception as exc:
            logger.exception("[VaultPool] Aggregation failed. error=%s", exc)
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

    async def _select_and_enqueue(
        self,
        vault_type: str,
        triggered_by: Optional[int],
    ) -> Optional[ObjectId]:
        cutoff_time = datetime.now(tz=timezone.utc) - timedelta(hours=VAULT_COOLDOWN_HOURS)

        # FIX B-08: filter by `moderation_destination`, not `vault_type`
        eligible_query: dict[str, Any] = {
            "moderation_destination": vault_type,
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
                    "submitter_user_id": 1,
                    "moderation_destination": 1,
                    "media_type": 1,
                    "vault_channel_id": 1,
                    "vault_message_id": 1,
                    "file_unique_id": 1,
                    "last_posted_at": 1,
                    "post_count": 1,
                    "content_id": 1,
                },
            )
            eligible_items: list[dict[str, Any]] = await cursor.to_list(length=None)
        except Exception as exc:
            logger.exception("[VaultPool] DB query failed. error=%s", exc)
            raise VaultPoolError(f"vault query failed for vault_type={vault_type}") from exc

        if not eligible_items:
            logger.info(
                "[VaultPool] No eligible items for replay. vault_type=%s cooldown_hours=%d",
                vault_type,
                VAULT_COOLDOWN_HOURS,
            )
            return None

        # Sort: never-posted first, then oldest, then lowest post_count
        eligible_items.sort(
            key=lambda item: (
                0 if item.get("last_posted_at") is None else 1,
                item.get("last_posted_at") or _EPOCH,
                item.get("post_count") or 0,
            )
        )

        candidates = eligible_items[:VAULT_FAIRNESS_WINDOW]
        selected: dict[str, Any] = random.choice(candidates)  # noqa: S311
        vault_id: ObjectId = selected["_id"]
        now: datetime = datetime.now(tz=timezone.utc)

        # FIX B-08: queue job uses moderation_destination (not vault_type),
        # vault_chat_id + vault_message_id (not vault_id ObjectId)
        job_doc: dict[str, Any] = {
            "content_id": selected.get("content_id", str(vault_id)),
            "vault_chat_id": int(selected.get("vault_channel_id") or settings.VAULT_CHANNEL_ID),
            "vault_message_id": int(selected.get("vault_message_id", 0)),
            "moderation_destination": vault_type,
            "status": "PENDING",
            "created_at": now,
            "scheduled_at": now,
            "processed_at": None,
            "retry_count": 0,
            "error_message": None,
            "source": "vault_replay",
        }

        try:
            result = await self._db[settings.QUEUE_COLLECTION].insert_one(job_doc)
            job_id: ObjectId = result.inserted_id
        except Exception as exc:
            logger.exception("[VaultPool] Failed to insert queue job. error=%s", exc)
            raise VaultPoolError(f"queue insert failed for vault_type={vault_type}") from exc

        logger.info(
            "[VaultPool] Replay job created. job_id=%s vault_id=%s vault_type=%s",
            job_id,
            vault_id,
            vault_type,
        )

        # Update vault item tracking (non-critical)
        try:
            await self._db[settings.VAULT_COLLECTION].update_one(
                {"_id": vault_id},
                {"$set": {"last_posted_at": now}, "$inc": {"post_count": 1}},
            )
        except Exception as exc:
            logger.error("[VaultPool] vault_item tracking update failed (non-fatal). error=%s", exc)

        # Audit log (non-critical)
        await self._write_audit_log(
            action="VAULT_REPLAY_JOB_CREATED",
            admin_user_id=triggered_by,
            target_user_id=selected.get("submitter_user_id"),
            detail={
                "job_id": str(job_id),
                "vault_id": str(vault_id),
                "vault_type": vault_type,
                "cooldown_hours": VAULT_COOLDOWN_HOURS,
                "eligible_count": len(eligible_items),
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
        try:
            await self._db["audit_logs"].insert_one({
                "action": action,
                "admin_user_id": admin_user_id,
                "target_user_id": target_user_id,
                "detail": detail,
                "timestamp": datetime.now(tz=timezone.utc),
            })
        except Exception as exc:
            logger.error("[VaultPool] Audit log write failed. error=%s", exc)


def create_vault_pool_service() -> VaultPoolService:
    """Factory: returns a configured VaultPoolService. Call after DB connect."""
    db: AsyncIOMotorDatabase = DatabaseManager.get_db()
    return VaultPoolService(db=db)
