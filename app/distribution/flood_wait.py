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
    """

    def __init__(self):
        self._blocked_until: dict[str, float] = {}

    def register_flood_wait(self, target_id: str, wait_seconds: int) -> None:
        total_wait = wait_seconds + settings.FLOODWAIT_EXTRA_BUFFER
        capped_wait = min(total_wait, settings.FLOODWAIT_MAX_WAIT)
        self._blocked_until[target_id] = time.monotonic() + capped_wait
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
