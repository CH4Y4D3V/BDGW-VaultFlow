from __future__ import annotations
from app.config import settings
from app.core.models import ModerationDestination
from app.repositories.channel_repository import ChannelRepository
from app.utils.logger import get_logger

logger = get_logger(__name__)


class ChannelService:
    def __init__(self) -> None:
        self._repo = ChannelRepository()

    async def seed_channels(self) -> None:
        """
        Seed distribution channels from env vars into channel_config collection.
        CRITICAL: Both NSFW_GROUP_ID and PREMIUM_GROUP_ID default to 0.
        A falsy guard (if settings.NSFW_GROUP_ID) silently skips seeding,
        leaving channel_config empty and killing the entire distribution pipeline.
        This method always runs at boot. If neither group is configured it logs
        a hard error so the operator knows immediately why distribution is dead.
        """
        # Lazy import to break circular dependency:
        # channel_service → moderation_actions → audit_service →
        # services/__init__ → channel_service (circular)
        from app.moderation.moderation_actions import _get_watermark_config

        seeded: list[str] = []

        if settings.NSFW_GROUP_ID and settings.NSFW_GROUP_ID != 0:
            await self._repo.upsert_channel(
                destination=ModerationDestination.NSFW.value,
                doc={
                    "destination": ModerationDestination.NSFW.value,
                    "source_channel_id": str(settings.VAULT_CHANNEL_ID),
                    "target_channel_ids": [str(settings.NSFW_GROUP_ID)],
                    "is_active": True,
                    "watermark_config": _get_watermark_config(ModerationDestination.NSFW),
                },
            )
            seeded.append(f"NSFW → {settings.NSFW_GROUP_ID}")
            logger.info(
                "NSFW channel seeded",
                extra={"ctx_group_id": settings.NSFW_GROUP_ID},
            )
        else:
            logger.warning(
                "NSFW_GROUP_ID is 0 or not set — NSFW distribution channel NOT seeded. "
                "Set NSFW_GROUP_ID in your environment to enable NSFW distribution."
            )

        if settings.PREMIUM_GROUP_ID and settings.PREMIUM_GROUP_ID != 0:
            await self._repo.upsert_channel(
                destination=ModerationDestination.PREMIUM.value,
                doc={
                    "destination": ModerationDestination.PREMIUM.value,
                    "source_channel_id": str(settings.PREMIUM_VAULT_CHANNEL_ID),
                    "target_channel_ids": [str(settings.PREMIUM_GROUP_ID)],
                    "is_active": True,
                    "watermark_config": _get_watermark_config(ModerationDestination.PREMIUM),
                },
            )
            seeded.append(f"PREMIUM → {settings.PREMIUM_GROUP_ID}")
            logger.info(
                "PREMIUM channel seeded",
                extra={"ctx_group_id": settings.PREMIUM_GROUP_ID},
            )
        else:
            logger.warning(
                "PREMIUM_GROUP_ID is 0 or not set — PREMIUM distribution channel NOT seeded. "
                "Set PREMIUM_GROUP_ID in your environment to enable PREMIUM distribution."
            )

        if not seeded:
            logger.error(
                "CRITICAL: No distribution channels were seeded. "
                "The distribution pipeline is completely dead. "
                "Set NSFW_GROUP_ID and/or PREMIUM_GROUP_ID in your environment and restart."
            )
        else:
            logger.info(
                "Distribution channels seeded successfully",
                extra={"ctx_seeded": seeded},
            )