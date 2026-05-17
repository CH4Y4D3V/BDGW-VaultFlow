import asyncio
import time
from typing import Optional
from app.config import settings
from app.core.exceptions import RateLimitExceededError
from app.utils.logger import get_logger

logger = get_logger(__name__)


class TokenBucket:
    """Thread-safe async token bucket for rate limiting."""

    def __init__(self, rate: float, capacity: int):
        self._rate = rate  # tokens per second
        self._capacity = capacity
        self._tokens = float(capacity)
        self._last_refill = time.monotonic()
        self._lock = asyncio.Lock()

    async def consume(self, tokens: int = 1) -> bool:
        async with self._lock:
            now = time.monotonic()
            elapsed = now - self._last_refill
            self._tokens = min(
                self._capacity,
                self._tokens + elapsed * self._rate,
            )
            self._last_refill = now

            if self._tokens >= tokens:
                self._tokens -= tokens
                return True
            return False

    async def wait_and_consume(self, tokens: int = 1, timeout: float = 30.0) -> None:
        deadline = time.monotonic() + timeout
        while True:
            if await self.consume(tokens):
                return
            if time.monotonic() >= deadline:
                raise RateLimitExceededError("Rate limit timeout exceeded")
            await asyncio.sleep(0.1)


class RateLimiterService:
    """
    Manages global and per-target rate limits.
    Per-target buckets are lazily created and not persisted (reset on restart).
    That's acceptable — they refill naturally within a minute.
    """

    def __init__(self):
        self._global_bucket = TokenBucket(
            rate=settings.GLOBAL_RATE_LIMIT_PER_MIN / 60.0,
            capacity=settings.GLOBAL_RATE_LIMIT_PER_MIN,
        )
        self._target_buckets: dict[str, TokenBucket] = {}
        self._bucket_lock = asyncio.Lock()

    async def _get_target_bucket(self, target_id: str) -> TokenBucket:
        async with self._bucket_lock:
            if target_id not in self._target_buckets:
                self._target_buckets[target_id] = TokenBucket(
                    rate=settings.PER_TARGET_RATE_LIMIT_PER_MIN / 60.0,
                    capacity=settings.PER_TARGET_RATE_LIMIT_PER_MIN,
                )
            return self._target_buckets[target_id]

    async def check_global(self) -> bool:
        return await self._global_bucket.consume()

    async def check_target(self, target_id: str) -> bool:
        bucket = await self._get_target_bucket(target_id)
        return await bucket.consume()

    async def wait_global(self, timeout: float = 30.0) -> None:
        await self._global_bucket.wait_and_consume(timeout=timeout)

    async def wait_target(self, target_id: str, timeout: float = 30.0) -> None:
        bucket = await self._get_target_bucket(target_id)
        await bucket.wait_and_consume(timeout=timeout)

    async def check_and_consume(self, target_id: str) -> tuple[bool, Optional[str]]:
        """Returns (allowed, reason_if_denied)."""
        global_ok = await self._global_bucket.consume()
        if not global_ok:
            return False, "global_rate_limit"

        target_bucket = await self._get_target_bucket(target_id)
        target_ok = await target_bucket.consume()
        if not target_ok:
            return False, f"per_target_rate_limit:{target_id}"

        return True, None
