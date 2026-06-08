"""
app/core/redis.py
-----------------
Consolidated Redis client and distributed lock manager.

This module replaces the previous split implementation in:
  - app/core/redis_client.py
  - app/core/redis_lock.py

Rationale:
  - The lock is tightly coupled to the client.
  - Simplifies imports for all consumers.
  - Provides a single point of configuration.

Key components:
  - RedisClient: Singleton manager for the aioredis.Redis connection pool.
  - get_redis():   Async accessor for the singleton client.
  - RedisLock:   Async context manager for distributed locks.
"""
from __future__ import annotations

import asyncio
from typing import Optional, Any

import redis.asyncio as aioredis
from redis.asyncio.client import Redis
from redis.asyncio.connection import ConnectionPool

from app.config import settings
from app.utils.logger import get_logger

logger = get_logger(__name__)


# ── Client Singleton ──────────────────────────────────────────────────────────

class RedisClient:
    """Singleton wrapper for the redis.asyncio (aioredis) client pool."""

    _pool: Optional[ConnectionPool] = None
    _client: Optional[Redis] = None

    @classmethod
    async def get_client(cls) -> Redis:
        """Return the singleton Redis client, creating it if necessary."""
        if cls._client is None:
            if cls._pool is None:
                try:
                    cls._pool = ConnectionPool.from_url(
                        settings.REDIS_URL,
                        decode_responses=True,
                        max_connections=settings.REDIS_MAX_CONNECTIONS,
                    )
                    logger.info("Redis connection pool created")
                except Exception as e:
                    logger.exception("Failed to create Redis connection pool")
                    raise  # Propagate error to abort startup if Redis is critical

            cls._client = aioredis.Redis(connection_pool=cls._pool)
            logger.info("Redis client initialized")
        return cls._client

    @classmethod
    async def disconnect(cls) -> None:
        """Gracefully disconnect the client and close the connection pool."""
        if cls._client:
            try:
                await cls._client.close()
                logger.info("Redis client closed")
            except Exception:
                logger.error("Error closing Redis client", exc_info=True)
            finally:
                cls._client = None

        if cls._pool:
            try:
                await cls._pool.disconnect()
                logger.info("Redis connection pool disconnected")
            except Exception:
                logger.error("Error disconnecting Redis pool", exc_info=True)
            finally:
                cls._pool = None


async def get_redis() -> Redis:
    """Convenience accessor for the singleton Redis client."""
    return await RedisClient.get_client()


# ── Distributed Lock ──────────────────────────────────────────────────────────

class RedisLock:
    """
    Fail-closed async context manager for distributed Redis locks.

    Ensures that a critical section of code is executed by only one
    worker at a time across multiple replicas.

    Features:
      - Atomic acquisition via `SET key value NX PX ttl`.
      - Automatic release on exiting the `async with` block.
      - Fail-closed: If Redis is unavailable, the lock is NOT acquired,
        and the critical section is skipped.
      - Graceful handling of lock contention (logs and skips).

    Usage:
        lock_key = f"lock:my_resource:{resource_id}"
        async with RedisLock(lock_key, ttl=30) as lock:
            if lock:
                # Lock acquired, proceed with critical section
                ...
            else:
                # Lock not acquired (contention or Redis error)
                ...
    """

    def __init__(self, key: str, ttl: int = 60) -> None:
        """
        Args:
            key: Unique key for the lock.
            ttl: Time-to-live in seconds for the lock to auto-expire.
        """
        if not key.startswith("lock:"):
            raise ValueError("Redis lock keys must be prefixed with 'lock:'")
        self._key = key
        self._ttl = ttl
        self._acquired = False

    async def __aenter__(self) -> "RedisLock":
        """Attempt to acquire the lock atomically."""
        try:
            redis = await get_redis()
            # SET key value NX PX ttl_ms — returns True if acquired
            self._acquired = await redis.set(
                self._key,
                "1",
                nx=True,
                px=self._ttl * 1000,
            )
            if not self._acquired:
                logger.debug(
                    "Could not acquire distributed lock (contention)",
                    extra={"ctx_lock_key": self._key},
                )
        except Exception as exc:
            logger.error(
                "Redis lock acquisition failed (fail-closed)",
                extra={"ctx_lock_key": self._key, "ctx_error": str(exc)},
                exc_info=True,
            )
            self._acquired = False  # Ensure fail-closed
        return self

    async def __aexit__(
        self,
        exc_type: Optional[type] = None,
        exc_val: Optional[Exception] = None,
        exc_tb: Optional[Any] = None,
    ) -> bool:
        """Release the lock if it was acquired."""
        if self._acquired:
            try:
                redis = await get_redis()
                await redis.delete(self._key)
            except Exception as exc:
                logger.warning(
                    "Failed to release distributed lock (will TTL)",
                    extra={"ctx_lock_key": self._key, "ctx_error": str(exc)},
                )
        # Do not suppress exceptions from the 'with' block
        return False

    def __bool__(self) -> bool:
        """Allows checking the lock's status: `if lock:`"""
        return self._acquired
