from __future__ import annotations

from app.config import settings
from app.core.models import ModerationDestination
from app.repositories.channel_repository import ChannelRepository
from app.moderation.moderation_actions import _get_watermark_config
from app.utils.logger import get_logger

logger = get_logger(__name__)


class ChannelService:
    def __init__(self) -> None:
        self._repo = ChannelRepository()

    async def seed_channels(self) -> None:
        """Seed default distribution channels from environment variables."""
        # Seed NSFW
        if settings.NSFW_GROUP_ID:
            await self._repo.upsert_channel(
                destination=ModerationDestination.NSFW.value,
                doc={
                    "destination": ModerationDestination.NSFW.value,
                    "source_channel_id": f"submission_{ModerationDestination.NSFW.value}",
                    "target_channel_ids": [str(settings.NSFW_GROUP_ID)],
                    "is_active": True,
                    "watermark_config": _get_watermark_config(ModerationDestination.NSFW),
                },
            )

        # Seed Premium
        if settings.PREMIUM_GROUP_ID:
            await self._repo.upsert_channel(
                destination=ModerationDestination.PREMIUM.value,
                doc={
                    "destination": ModerationDestination.PREMIUM.value,
                    "source_channel_id": f"submission_{ModerationDestination.PREMIUM.value}",
                    "target_channel_ids": [str(settings.PREMIUM_GROUP_ID)],
                    "is_active": True,
                    "watermark_config": _get_watermark_config(ModerationDestination.PREMIUM),
                },
            )
        logger.info("Distribution channels seeded successfully")