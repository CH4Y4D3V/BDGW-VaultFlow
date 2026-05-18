from datetime import datetime, timezone
from typing import List, Dict

from app.config import settings
from app.core.database import DatabaseManager
from app.core.models import ModerationState
from app.utils.logger import get_logger

logger = get_logger(__name__)


async def fetch_distribution_content() -> List[Dict]:
    """
    Called by DistributionScheduler to fetch valid vault content.

    Provides the data boundary ensuring only fully ingested, non-duplicate,
    non-locked, non-cooldown content is released to the fairness selector.

    Includes diagnostic logging so operators can see exactly why content
    is or isn't flowing — previously this returned [] silently with no
    indication of whether the channel_config was empty or vault was empty.
    """
    db = DatabaseManager.get_db()
    vault = db[getattr(settings, "VAULT_COLLECTION", "vault")]
    channels = db[getattr(settings, "CHANNEL_CONFIG_COLLECTION", "channel_config")]

    # Diagnostic: detect missing channel config immediately
    total_active_channels = await channels.count_documents({"is_active": True})
    if total_active_channels == 0:
        logger.error(
            "fetch_distribution_content: channel_config has NO active channels. "
            "Seed is missing — check NSFW_GROUP_ID / PREMIUM_GROUP_ID env vars "
            "and ensure ChannelService.seed_channels() ran at boot."
        )
        return []

    active_configs = []
    now = datetime.now(timezone.utc)

    async for config in channels.find({"is_active": True}):
        dest = config.get("destination")
        source_id = config.get("source_channel_id")

        if not dest or not source_id:
            logger.warning(
                "Skipping malformed channel config — missing destination or source_channel_id",
                extra={"ctx_config_id": str(config.get("_id"))},
            )
            continue

        # Diagnostic: count total queued vault items for this destination
        total_queued = await vault.count_documents({
            "moderation_destination": dest,
            "status": ModerationState.QUEUED.value,
        })

        # Diagnostic: count items blocked by lock/cooldown
        total_locked = await vault.count_documents({
            "moderation_destination": dest,
            "status": ModerationState.QUEUED.value,
            "distribution_state": {"$in": ["locked", "removed"]},
        })

        # Main query: eligible content only
        cursor = vault.find({
            "moderation_destination": dest,
            "status": ModerationState.QUEUED.value,
            "distribution_state": {"$nin": ["locked", "removed"]},
            "$or": [
                {"cooldown_until": None},
                {"cooldown_until": {"$exists": False}},
                {"cooldown_until": {"$lte": now}},
            ],
        }).sort("message_id", 1).limit(getattr(settings, "MAX_JOBS_PER_CYCLE", 100))

        content = await cursor.to_list(length=None)

        logger.info(
            "Channel provider query result",
            extra={
                "ctx_dest": dest,
                "ctx_total_queued": total_queued,
                "ctx_locked_or_removed": total_locked,
                "ctx_eligible": len(content),
            },
        )

        if content:
            active_configs.append({
                "source_channel_id": source_id,
                "target_channel_ids": config.get("target_channel_ids", []),
                "content": content,
                "watermark_config": config.get("watermark_config"),
            })
        else:
            if total_queued == 0:
                logger.info(
                    "No queued vault content for destination — vault is empty for this dest",
                    extra={"ctx_dest": dest},
                )
            else:
                logger.warning(
                    "Vault has queued content but none eligible — all locked or on cooldown",
                    extra={
                        "ctx_dest": dest,
                        "ctx_total_queued": total_queued,
                        "ctx_locked": total_locked,
                    },
                )

    return active_configs
