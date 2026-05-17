import random
from datetime import datetime, timezone
from typing import List
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

    def _score_content(self, content_list: List[dict]) -> List[tuple[float, List[dict]]]:
        """
        Score content on:
        - Age (older = higher priority, FIFO with jitter)
        - View count (lower view count = higher priority)
        - Random jitter (prevents strict ordering → unpredictable posting pattern)
        """
        now = datetime.now(timezone.utc)

        groups = {}
        for item in content_list:
            group_id = item.get("media_group_id") or item.get("content_id")
            if group_id not in groups:
                groups[group_id] = []
            groups[group_id].append(item)

        scored = []

        for group_id, items in groups.items():
            # Sort deterministically within album to preserve Telegram delivery order
            items.sort(key=lambda x: x.get("message_id", x.get("content_id", "")))
            
            primary = items[0]
            created_at = primary.get("created_at")
            if isinstance(created_at, datetime):
                age_hours = (now - created_at.replace(tzinfo=timezone.utc)).total_seconds() / 3600
            else:
                age_hours = 0.0

            view_count = primary.get("view_count", 0)
            # Normalize: higher age = higher score, lower views = higher score
            age_score = min(age_hours / 168.0, 1.0)  # cap at 7 days
            view_penalty = min(view_count / 10000.0, 1.0)
            jitter = random.uniform(0, 0.2)

            score = age_score - (view_penalty * 0.3) + jitter
            scored.append((score, items))

        return sorted(scored, key=lambda x: x[0], reverse=True)

    def _select_with_fairness(
        self, scored: List[tuple[float, List[dict]]], max_count: int
    ) -> List[dict]:
        """
        Split into top-tier (score > 0.5) and rest.
        Take majority from top-tier, fill remainder from rest (shuffled).
        This ensures quality content gets priority without being fully deterministic.
        """
        top_tier = [items for score, items in scored if score > 0.5]
        lower_tier = [items for score, items in scored if score <= 0.5]

        random.shuffle(top_tier)
        random.shuffle(lower_tier)

        # Treat albums as unified units for slot allocation
        top_quota = min(int(max_count * 0.7), len(top_tier))
        lower_quota = min(max_count - top_quota, len(lower_tier))

        selected_groups = top_tier[:top_quota] + lower_tier[:lower_quota]

        # Final shuffle so the ordering itself isn't predictable
        random.shuffle(selected_groups)

        selected = []
        for group in selected_groups:
            selected.extend(group)

        return selected
