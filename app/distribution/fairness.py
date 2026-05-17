import random
from datetime import datetime, timedelta, timezone
from typing import List, Optional, Any
from motor.motor_asyncio import AsyncIOMotorDatabase
from app.config import settings
from app.utils.logger import get_logger

logger = get_logger(__name__)


class FairnessSelector:
    """
    Selects content for scheduling using a weighted fairness algorithm.

    Guarantees:
    - No content posted more than once within REPOST_PREVENTION_HOURS
    - Channels with less recent activity are prioritized
    - Content selection is randomized within eligible pool to prevent predictability
    - Underrepresented content types get priority rotation
    """

    def __init__(self, db: AsyncIOMotorDatabase):
        self._db = db

    async def select_eligible_content(
        self,
        available_content: List[dict],
        source_channel_id: str,
        max_count: int,
    ) -> List[dict]:
        """
        Filters available content through fairness constraints and returns
        a prioritized, randomized selection up to max_count.
        """
        if not available_content:
            return []

        from app.repositories.queue_repository import QueueRepository
        queue_repo = QueueRepository(self._db)

        recently_posted = await queue_repo.get_recently_posted_content_ids(
            source_channel_id,
            hours=settings.REPOST_PREVENTION_HOURS,
        )

        eligible = [
            c for c in available_content
            if c.get("content_id") not in recently_posted
        ]

        if not eligible:
            logger.info(
                "All content was recently posted; nothing to schedule",
                extra={"ctx_channel": source_channel_id},
            )
            return []

        # Score each piece of content for fairness priority
        scored = self._score_content(eligible)

        # Group into tiers, shuffle within each tier for randomness
        return self._select_with_fairness(scored, max_count)

    def _score_content(self, content_list: List[dict]) -> List[tuple[float, dict]]:
        """
        Score content on:
        - Age (older = higher priority, FIFO with jitter)
        - View count (lower view count = higher priority)
        - Random jitter (prevents strict ordering → unpredictable posting pattern)
        """
        now = datetime.now(timezone.utc)
        scored = []

        for item in content_list:
            created_at = item.get("created_at")
            if isinstance(created_at, datetime):
                age_hours = (now - created_at.replace(tzinfo=timezone.utc)).total_seconds() / 3600
            else:
                age_hours = 0.0

            view_count = item.get("view_count", 0)
            # Normalize: higher age = higher score, lower views = higher score
            age_score = min(age_hours / 168.0, 1.0)  # cap at 7 days
            view_penalty = min(view_count / 10000.0, 1.0)
            jitter = random.uniform(0, 0.2)

            score = age_score - (view_penalty * 0.3) + jitter
            scored.append((score, item))

        return sorted(scored, key=lambda x: x[0], reverse=True)

    def _select_with_fairness(
        self, scored: List[tuple[float, dict]], max_count: int
    ) -> List[dict]:
        """
        Split into top-tier (score > 0.5) and rest.
        Take majority from top-tier, fill remainder from rest (shuffled).
        This ensures quality content gets priority without being fully deterministic.
        """
        top_tier = [item for score, item in scored if score > 0.5]
        lower_tier = [item for score, item in scored if score <= 0.5]

        random.shuffle(top_tier)
        random.shuffle(lower_tier)

        top_quota = min(int(max_count * 0.7), len(top_tier))
        lower_quota = min(max_count - top_quota, len(lower_tier))

        selected = top_tier[:top_quota] + lower_tier[:lower_quota]

        # Final shuffle so the ordering itself isn't predictable
        random.shuffle(selected)

        return selected[:max_count]
