from __future__ import annotations
from contextlib import asynccontextmanager
from typing import AsyncGenerator
from app.utils.logger import get_logger

logger = get_logger(__name__)


@asynccontextmanager
async def acquire_lock(key: str, timeout: int = 30) -> AsyncGenerator[bool, None]:
    """
    Async context manager wrapping Redis SET NX for distributed locking.
    Yields True if acquired, False if not (callers should check).
    Always releases on exit.
    """
    from app.core.redis_client import RedisClient
    redis = await RedisClient.get_client()
    lock_key = f"lock:{key}"
    acquired = False
    try:
        acquired = bool(await redis.set(lock_key, "1", nx=True, ex=timeout))
        if not acquired:
            logger.warning("acquire_lock: could not acquire %s", lock_key)
        yield acquired
    finally:
        if acquired:
            try:
                await redis.delete(lock_key)
            except Exception as exc:
                logger.warning("acquire_lock: release failed for %s: %s", lock_key, exc)