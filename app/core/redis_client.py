from __future__ import annotations

import asyncio
from typing import Optional

import redis.asyncio as aioredis
from redis.asyncio.client import Redis
from redis.exceptions import ConnectionError, TimeoutError

from app.config import settings
from app.utils.logger import get_logger

logger = get_logger(__name__)

class RedisClient:
    """
    Resilient Async Redis client manager.
    Handles automatic reconnection and health checks.
    """
    _instance: Optional[Redis] = None
    _lock = asyncio.Lock()

    @classmethod
    async def get_client(cls) -> Redis:
        """
        Get or create the Redis client. 
        Verifies connection health before returning.
        """
        if cls._instance is None:
            async with cls._lock:
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

        # Health check
        try:
            await cls._instance.ping()
        except (ConnectionError, TimeoutError) as e:
            logger.warning(
                "Redis health check failed, attempting to recover...",
                extra={"ctx_error": str(e)}
            )
            async with cls._lock:
                try:
                    # Close old connection
                    await cls._instance.aclose()
                except Exception:
                    pass
                
                # Re-create
                cls._instance = aioredis.Redis.from_url(
                    settings.REDIS_URL,
                    decode_responses=True,
                    socket_connect_timeout=5,
                    socket_timeout=5,
                    retry_on_timeout=True,
                    health_check_interval=30,
                )
                await cls._instance.ping()
                logger.info("Redis connection recovered")

        return cls._instance

def get_redis() -> Redis:
    """
    Legacy sync wrapper for backward compatibility.
    NOTE: In async contexts, prefer `await RedisClient.get_client()`.
    Since this is a CLI agent, we can't easily change all callers to async
    if they weren't already. But most are in async handlers.
    """
    # This is slightly problematic if called from a sync context,
    # but the project is async-first.
    if _redis_deprecated._instance is None:
         _redis_deprecated._instance = aioredis.Redis.from_url(
            settings.REDIS_URL,
            decode_responses=True,
            socket_connect_timeout=2,
            socket_timeout=2,
        )
    return _redis_deprecated._instance

class _redis_deprecated:
    _instance: Optional[Redis] = None
