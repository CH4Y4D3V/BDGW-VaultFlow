from __future__ import annotations

"""
redis_client.py
───────────────
Async Redis client manager for BDGW VaultFlow.

Provides a singleton async Redis connection with automatic health checks
and reconnection. The ``get_redis()`` function is kept for backward
compatibility but is clearly documented as deprecated — all new code should
use ``await RedisClient.get_client()``.
"""

import asyncio
from typing import Optional

import redis.asyncio as aioredis
from redis.asyncio.client import Redis
from redis.exceptions import ConnectionError, TimeoutError

from app.config import settings
from app.utils.logger import get_logger

logger = get_logger(__name__)


# ── Deprecated legacy holder (defined before get_redis to avoid forward ref) ─

class _LegacyRedisHolder:
    """
    Internal namespace for the deprecated synchronous-style Redis accessor.

    Kept as a class purely to mirror the original module structure during
    the deprecation window. Do not add behaviour here.
    """
    instance: Optional[Redis] = None


# ── Primary async client ──────────────────────────────────────────────────────

class RedisClient:
    """
    Resilient async Redis client manager.

    Uses a double-checked locking pattern for initialisation and performs
    a PING health check on every ``get_client()`` call. Stale connections
    are detected and replaced automatically.

    Usage:
        redis = await RedisClient.get_client()
        await redis.set("key", "value")
    """

    _instance: Optional[Redis] = None
    _lock = asyncio.Lock()

    @classmethod
    async def get_client(cls) -> Redis:
        """
        Return the singleton async Redis client.

        Initialises the client on first call. On every subsequent call a PING
        health check is performed; if it fails the connection is closed and a
        new one is created before returning.

        Raises ``redis.exceptions.ConnectionError`` if reconnection also fails,
        so callers should handle this for non-critical paths.
        """
        if cls._instance is None:
            async with cls._lock:
                # Double-check after acquiring the lock
                if cls._instance is None:
                    cls._instance = aioredis.Redis.from_url(
                        settings.REDIS_URL,
                        decode_responses=True,
                        socket_connect_timeout=2,
                        socket_timeout=2,
                        retry_on_timeout=True,
                        health_check_interval=30,
                    )
                    logger.info(
                        "Async Redis client initialised",
                        extra={"ctx_url": settings.REDIS_URL.split("@")[-1]},
                    )

        # Health check on every call
        try:
            await cls._instance.ping()
        except (ConnectionError, TimeoutError) as exc:
            logger.warning(
                "Redis health check failed — attempting reconnect",
                extra={"ctx_error": str(exc)},
            )
            async with cls._lock:
                # Another coroutine may have already reconnected while we
                # were waiting for the lock — check before replacing.
                if cls._instance is not None:
                    try:
                        await cls._instance.aclose()
                    except Exception:
                        pass
                    cls._instance = None

                cls._instance = aioredis.Redis.from_url(
                    settings.REDIS_URL,
                    decode_responses=True,
                    socket_connect_timeout=5,
                    socket_timeout=5,
                    retry_on_timeout=True,
                    health_check_interval=30,
                )
                # Raises if still unreachable — let callers decide how to handle
                await cls._instance.ping()
                logger.info("Redis connection recovered")

        return cls._instance

    @classmethod
    async def close(cls) -> None:
        """
        Close the Redis connection gracefully.

        Call this during bot shutdown to release the connection cleanly.
        Safe to call even if the client was never initialised.
        """
        if cls._instance is not None:
            try:
                await cls._instance.aclose()
            except Exception as exc:
                logger.warning("Error closing Redis client: %s", exc)
            finally:
                cls._instance = None
            logger.info("Redis client closed")


# ── Deprecated accessor ───────────────────────────────────────────────────────

def get_redis() -> Redis:
    """
    .. deprecated::
        Use ``await RedisClient.get_client()`` instead.

    Returns the cached async Redis client instance without performing a
    health check. Intended only for callers that cannot be easily converted
    to async — new code must not use this function.

    IMPORTANT: The returned client is async. Any method call on it must be
    awaited. Using it in a synchronous context without an event loop will
    raise a RuntimeError.
    """
    if _LegacyRedisHolder.instance is None:
        _LegacyRedisHolder.instance = aioredis.Redis.from_url(
            settings.REDIS_URL,
            decode_responses=True,
            socket_connect_timeout=2,
            socket_timeout=2,
        )
    return _LegacyRedisHolder.instance
