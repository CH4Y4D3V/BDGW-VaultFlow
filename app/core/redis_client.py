from __future__ import annotations

"""
Shared async Redis client singleton.

Used by:
  - payment_handler.py  : payment session key-value cache (FIX 18)
  - submission_handler.py : fast payment-state gate (FIX 18)

Key schema:
  pay_session:{user_id}  →  "1"  (TTL 3600s, set on payment session open)

Import pattern:
    from app.core.redis_client import get_redis
    redis = get_redis()
    await redis.exists("pay_session:123456")
"""

from typing import Optional

import redis.asyncio as aioredis

from app.config import settings
from app.utils.logger import get_logger

logger = get_logger(__name__)

_redis: Optional[aioredis.Redis] = None


def get_redis() -> aioredis.Redis:
    """
    Return the module-level async Redis client.
    Connection is lazy — no network call until the first command is issued.
    Thread/task safe: redis-py async client handles connection pooling internally.
    """
    global _redis
    if _redis is None:
        _redis = aioredis.Redis.from_url(
            settings.REDIS_URL,
            decode_responses=True,
            socket_connect_timeout=2,
            socket_timeout=2,
        )
        logger.info(
            "Async Redis client initialised",
            extra={"ctx_url": settings.REDIS_URL.split("@")[-1]},  # strip credentials if any
        )
    return _redis
