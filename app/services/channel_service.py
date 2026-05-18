# app/services/channel_service.py
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
        """
        Seed distribution channels from env vars.
        Fails loudly if neither destination is configured — a misconfigured
        channel silently kills the entire distribution pipeline.
        """
        seeded: list[str] = []
        warnings: list[str] = []

        if settings.NSFW_GROUP_ID and settings.NSFW_GROUP_ID != 0:
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
            seeded.append(f"NSFW → {settings.NSFW_GROUP_ID}")
        else:
            warnings.append("NSFW_GROUP_ID not set or zero — NSFW channel NOT seeded")

        if settings.PREMIUM_GROUP_ID and settings.PREMIUM_GROUP_ID != 0:
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
            seeded.append(f"PREMIUM → {settings.PREMIUM_GROUP_ID}")
        else:
            warnings.append("PREMIUM_GROUP_ID not set or zero — PREMIUM channel NOT seeded")

        for w in warnings:
            logger.warning(w)

        if not seeded:
            logger.error(
                "CRITICAL: No distribution channels seeded. "
                "Bot will start but distribution pipeline is dead. "
                "Set NSFW_GROUP_ID and/or PREMIUM_GROUP_ID in your environment."
            )
            return

        logger.info(
            "Distribution channels seeded",
            extra={"ctx_channels": seeded},
        )