import asyncio
import random
import time
from typing import Callable, Any, Optional
from app.config import settings
from app.core.exceptions import FloodWaitError, MaxRetriesExceededError
from app.utils.logger import get_logger

logger = get_logger(__name__)


class FloodWaitHandler:
    """
    Centralized FloodWait state per (worker, target) pair.
    Tracks when each target is available again.

    FIX: register_flood_wait() is a sync method but needs to schedule
    an async Redis persist. Uses asyncio.get_running_loop() with a
    try/except RuntimeError for cases where it's called from a sync
    context (e.g. startup). In-memory state is always updated regardless.

    Redis key schema:  fw:{target_id}  → str(wall_clock_expiry_timestamp)
    TTL = remaining wait seconds so keys auto-expire.
    """

    def __init__(self):
        self._blocked_until: dict[str, float] = {}  # monotonic timestamps

        import redis.asyncio as aioredis
        self._redis = aioredis.Redis.from_url(
            settings.REDIS_URL,
            socket_connect_timeout=2,
            socket_timeout=2,
            decode_responses=True,
        )

    def register_flood_wait(self, target_id: str, wait_seconds: int) -> None:
        total_wait = wait_seconds + settings.FLOODWAIT_EXTRA_BUFFER
        capped_wait = min(total_wait, settings.FLOODWAIT_MAX_WAIT)

        # Always update in-memory state synchronously — this is the fast path
        self._blocked_until[target_id] = time.monotonic() + capped_wait
        blocked_until_wall = time.time() + capped_wait

        # Persist to Redis asynchronously — best effort, non-blocking
        async def _save() -> None:
            try:
                await self._redis.setex(
                    f"fw:{target_id}",
                    int(capped_wait) + 1,
                    str(blocked_until_wall),
                )
            except Exception as e:
                logger.warning(
                    "FloodWait: Redis persist failed — in-memory state still valid",
                    extra={"ctx_target": target_id, "ctx_error": str(e)},
                )

        # Schedule the async save only if an event loop is running
        try:
            loop = asyncio.get_running_loop()
            loop.create_task(_save())
        except RuntimeError:
            # No running event loop (called from sync context at startup)
            # In-memory state was already set above — safe to skip Redis persist here
            logger.debug(
                "FloodWait: no running loop for Redis persist — in-memory only",
                extra={"ctx_target": target_id},
            )

        logger.warning(
            "FloodWait registered",
            extra={
                "ctx_target": target_id,
                "ctx_wait_seconds": capped_wait,
                "ctx_original_seconds": wait_seconds,
            },
        )

    def is_blocked(self, target_id: str) -> bool:
        until = self._blocked_until.get(target_id)
        if until is None:
            return False
        if time.monotonic() >= until:
            del self._blocked_until[target_id]
            return False
        return True

    def seconds_until_available(self, target_id: str) -> float:
        until = self._blocked_until.get(target_id)
        if until is None:
            return 0.0
        remaining = until - time.monotonic()
        return max(0.0, remaining)

    def get_blocked_targets(self) -> dict[str, float]:
        now = time.monotonic()
        return {
            tid: max(0.0, until - now)
            for tid, until in self._blocked_until.items()
            if until > now
        }

    async def wait(self, target_id: str) -> None:
        """If a target is blocked, wait until it is available."""
        remaining = self.seconds_until_available(target_id)
        if remaining > 0:
            await asyncio.sleep(remaining)


    async def load_from_redis(self) -> None:
        """
        Pre-populate in-memory state from Redis on worker startup.
        Call this once during startup before workers begin polling.
        """
        try:
            keys = await self._redis.keys("fw:*")
            if not keys:
                return

            now_wall = time.time()
            now_mono = time.monotonic()
            loaded = 0

            for key in keys:
                try:
                    val = await self._redis.get(key)
                    if not val:
                        continue
                    wall_expiry = float(val)
                    remaining = wall_expiry - now_wall
                    if remaining > 0:
                        target_id = key if isinstance(key, str) else key.decode()
                        target_id = target_id.removeprefix("fw:")
                        self._blocked_until[target_id] = now_mono + remaining
                        loaded += 1
                except (ValueError, AttributeError):
                    pass

            if loaded:
                logger.info(
                    "FloodWait: in-memory state restored from Redis",
                    extra={"ctx_count": loaded},
                )
        except Exception as e:
            logger.warning(
                "FloodWait: could not load state from Redis — starting with empty state",
                extra={"ctx_error": str(e)},
            )


def calculate_retry_delay(
    attempt: int,
    base_delay: Optional[float] = None,
    max_delay: Optional[float] = None,
    jitter: Optional[float] = None,
) -> float:
    base = base_delay or settings.RETRY_BASE_DELAY
    cap = max_delay or settings.RETRY_MAX_DELAY
    jitter_range = jitter if jitter is not None else settings.RETRY_JITTER_RANGE

    exponential = base * (2 ** attempt)
    noise = random.uniform(0, jitter_range)
    delay = min(exponential + noise, cap)
    return delay


async def with_retry(
    coro_factory: Callable[[], Any],
    max_attempts: int,
    job_id: str,
    target_id: str,
    flood_handler: Optional[FloodWaitHandler] = None,
) -> Any:
    last_error: Optional[Exception] = None

    for attempt in range(max_attempts):
        try:
            return await coro_factory()

        except FloodWaitError as e:
            last_error = e
            if flood_handler:
                flood_handler.register_flood_wait(target_id, e.seconds)

            logger.warning(
                "FloodWait during dispatch",
                extra={
                    "ctx_job_id": job_id,
                    "ctx_target": target_id,
                    "ctx_wait": e.seconds,
                    "ctx_attempt": attempt + 1,
                },
            )

            wait_time = min(e.seconds + settings.FLOODWAIT_EXTRA_BUFFER, settings.FLOODWAIT_MAX_WAIT)
            await asyncio.sleep(wait_time)

        except Exception as e:
            last_error = e
            delay = calculate_retry_delay(attempt)
            logger.warning(
                "Dispatch attempt failed",
                extra={
                    "ctx_job_id": job_id,
                    "ctx_target": target_id,
                    "ctx_attempt": attempt + 1,
                    "ctx_max": max_attempts,
                    "ctx_error": str(e),
                    "ctx_retry_in": delay,
                },
            )
            if attempt < max_attempts - 1:
                await asyncio.sleep(delay)

    raise MaxRetriesExceededError(
        f"Job {job_id} → target {target_id} failed after {max_attempts} attempts. "
        f"Last error: {last_error}"
    ) from last_error
