import asyncio
import random
from collections import defaultdict
from datetime import datetime, timezone
from typing import List
from app.utils.logger import get_logger

logger = get_logger(__name__)


class TargetBalancer:
    """
    Tracks delivery counts per target channel.
    Ensures no single target gets disproportionate traffic
    when multiple jobs compete for the same targets.

    State is in-memory per worker — intentional.
    Cross-worker fairness is enforced at the scheduler level (job distribution).
    """

    def __init__(self):
        self._delivery_counts: dict[str, int] = defaultdict(int)
        self._failure_counts: dict[str, int] = defaultdict(int)
        self._last_delivery: dict[str, datetime] = {}
        self._lock = asyncio.Lock()

    async def record_delivery(self, target_id: str, success: bool) -> None:
        async with self._lock:
            if success:
                self._delivery_counts[target_id] += 1
                self._last_delivery[target_id] = datetime.now(timezone.utc)
            else:
                self._failure_counts[target_id] += 1

    async def sort_targets_by_load(self, target_ids: List[str]) -> List[str]:
        """
        Sort targets ascending by delivery count to prefer under-served targets.
        Shuffle equal-count targets for randomness.
        """
        async with self._lock:
            def sort_key(tid: str) -> tuple:
                return (
                    self._delivery_counts[tid],
                    self._failure_counts[tid],
                    random.random(),
                )

            return sorted(target_ids, key=sort_key)

    async def get_least_loaded_targets(
        self, target_ids: List[str], n: int
    ) -> List[str]:
        sorted_targets = await self.sort_targets_by_load(target_ids)
        return sorted_targets[:n]

    async def get_stats(self) -> dict:
        async with self._lock:
            return {
                "delivery_counts": dict(self._delivery_counts),
                "failure_counts": dict(self._failure_counts),
                "tracked_targets": len(self._delivery_counts),
            }

    async def reset_target(self, target_id: str) -> None:
        async with self._lock:
            self._delivery_counts.pop(target_id, None)
            self._failure_counts.pop(target_id, None)
            self._last_delivery.pop(target_id, None)
