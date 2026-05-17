import asyncio
import uuid
from contextlib import asynccontextmanager
from datetime import datetime, timedelta, timezone
from typing import Optional, AsyncGenerator
from motor.motor_asyncio import AsyncIOMotorDatabase
from pymongo.errors import DuplicateKeyError
from app.config import settings
from app.core.exceptions import QueueLockError, StaleLockError
from app.utils.logger import get_logger

logger = get_logger(__name__)


class DistributedLockService:
    def __init__(self, db: AsyncIOMotorDatabase, worker_id: str):
        self._db = db
        self._worker_id = worker_id
        self._collection = db[settings.LOCK_COLLECTION]

    async def acquire(
        self,
        lock_key: str,
        ttl_seconds: Optional[int] = None,
        retry_attempts: Optional[int] = None,
        retry_delay: Optional[float] = None,
    ) -> bool:
        ttl = ttl_seconds or settings.LOCK_TTL_SECONDS
        attempts = retry_attempts or settings.LOCK_RETRY_ATTEMPTS
        delay = retry_delay or settings.LOCK_RETRY_DELAY

        expires_at = datetime.now(timezone.utc) + timedelta(seconds=ttl)

        for attempt in range(attempts):
            try:
                await self._collection.insert_one(
                    {
                        "lock_key": lock_key,
                        "held_by": self._worker_id,
                        "acquired_at": datetime.now(timezone.utc),
                        "expires_at": expires_at,
                    }
                )
                logger.debug(
                    "Lock acquired",
                    extra={
                        "ctx_lock_key": lock_key,
                        "ctx_worker": self._worker_id,
                        "ctx_attempt": attempt + 1,
                    },
                )
                return True

            except DuplicateKeyError:
                # Lock exists — check if stale
                existing = await self._collection.find_one({"lock_key": lock_key})
                if existing and existing["expires_at"] < datetime.now(timezone.utc):
                    recovered = await self._recover_stale_lock(lock_key, expires_at, existing)
                    if recovered:
                        return True

                if attempt < attempts - 1:
                    await asyncio.sleep(delay * (attempt + 1))
                    continue

        return False

    async def release(self, lock_key: str) -> bool:
        result = await self._collection.delete_one(
            {"lock_key": lock_key, "held_by": self._worker_id}
        )
        released = result.deleted_count > 0
        if released:
            logger.debug(
                "Lock released",
                extra={"ctx_lock_key": lock_key, "ctx_worker": self._worker_id},
            )
        else:
            logger.warning(
                "Lock release failed — not owned by this worker or already expired",
                extra={"ctx_lock_key": lock_key, "ctx_worker": self._worker_id},
            )
        return released

    async def extend(self, lock_key: str, additional_seconds: int) -> bool:
        new_expiry = datetime.now(timezone.utc) + timedelta(seconds=additional_seconds)
        result = await self._collection.update_one(
            {"lock_key": lock_key, "held_by": self._worker_id},
            {"$set": {"expires_at": new_expiry}},
        )
        return result.modified_count > 0

    async def _recover_stale_lock(
        self, lock_key: str, new_expiry: datetime, existing_doc: dict
    ) -> bool:
        result = await self._collection.find_one_and_replace(
            {
                "lock_key": lock_key,
                "_id": existing_doc["_id"],
                "expires_at": {"$lt": datetime.now(timezone.utc)},
            },
            {
                "lock_key": lock_key,
                "held_by": self._worker_id,
                "acquired_at": datetime.now(timezone.utc),
                "expires_at": new_expiry,
                "recovered_from": existing_doc.get("held_by"),
            },
        )
        if result:
            logger.warning(
                "Stale lock recovered",
                extra={
                    "ctx_lock_key": lock_key,
                    "ctx_previous_holder": existing_doc.get("held_by"),
                    "ctx_new_holder": self._worker_id,
                },
            )
            return True
        return False

    async def recover_stale_locks(self) -> int:
        """
        Sweep for locks held by crashed workers.
        Called on startup and periodically.
        """
        threshold = datetime.now(timezone.utc) - timedelta(
            seconds=settings.STALE_LOCK_THRESHOLD_SECONDS
        )
        result = await self._collection.delete_many({"expires_at": {"$lt": threshold}})
        if result.deleted_count:
            logger.warning(
                f"Recovered {result.deleted_count} stale lock(s)",
                extra={"ctx_count": result.deleted_count},
            )
        return result.deleted_count

    @asynccontextmanager
    async def lock(
        self, lock_key: str, ttl_seconds: Optional[int] = None
    ) -> AsyncGenerator[bool, None]:
        acquired = await self.acquire(lock_key, ttl_seconds=ttl_seconds)
        if not acquired:
            raise QueueLockError(f"Could not acquire lock: {lock_key}")
        try:
            yield acquired
        finally:
            await self.release(lock_key)
