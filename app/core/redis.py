"""
app/core/redis.py
-----------------
Thin re-export shim so callers that import from app.core.redis
get the same async client as app.core.redis_client.

invite_service.py and other modules import:
    from app.core.redis import get_redis_client
"""
from app.core.redis_client import RedisClient


async def get_redis_client():
    """Return the singleton async Redis client (aioredis.Redis)."""
    return await RedisClient.get_client()
