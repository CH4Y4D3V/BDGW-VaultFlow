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
    Tracks when each target is available again to avoid hammering it.

    FIX 13: In-memory state is now backed by Redis so flood-wait periods
    survive process restarts. Wall-clock timestamps (time.time()) are stored
    in Redis; monotonic timestamps are used for in-process comparisons.

    Redis key schema:  fw:{target_id}  →  str(wall_clock_expiry_timestamp)
    TTL is set to the remaining wait seconds so keys auto-expire.

    Degrades gracefully if Redis is unavailable — falls back to in-memory only
    with a warning logged at the first failure.
    """

    def __init__(self):
        self._blocked_until: dict[str, float] = {}  # monotonic timestamps

        # Synchronous Redis client — FloodWaitHandler methods are all synchronous
        # and called from within async tasks, so a sync client avoids requiring
        # an event loop reference at construction time.
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
        self._blocked_until[target_id] = time.monotonic() + capped_wait

        # FIX 13: persist wall-clock expiry to Redis so restarts don't
        # immediately retry flood-waited targets.
        blocked_until_wall = time.time() + capped_wait
        
        async def _save():
            try:
                await self._redis.setex(
                    f"fw:{target_id}",
                    int(capped_wait) + 1,  # TTL slightly longer than the wait so the key
                                            # is still present for load_from_redis() on restart
                    str(blocked_until_wall),
                )
            except Exception as e:
                logger.warning(
                    "FloodWait: Redis persist failed — in-memory state still valid",
                    extra={"ctx_target": target_id, "ctx_error": str(e)},
                )
        
        import asyncio
        asyncio.create_task(_save())

        logger.warning(
            "FloodWait registered",
            extra={
                "ctx_target": target_id,
                "ctx_wait_seconds": capped_wait,
                "ctx_original_seconds": wait_seconds,
            },
        )

    def is_blocked(self, target_id: str) -> bool:
        """
        Check if a target is currently blocked by a FloodWait.
        Relies on in-memory state which is pre-populated from Redis at startup.
        """
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

    async def load_from_redis(self) -> None:
        """
        FIX 13: Pre-populate in-memory state from Redis on worker startup.
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
    """
    Exponential backoff with jitter.
    delay = min(base * 2^attempt + jitter, max_delay)
    """
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
    """
    Wraps a coroutine factory with retry logic.
    Handles FloodWaitError specially — waits the specified time before retrying.
    All other exceptions trigger exponential backoff.
    """
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
