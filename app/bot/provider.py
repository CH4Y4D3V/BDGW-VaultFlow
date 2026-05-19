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

    LOG CLARITY FIX: the previous implementation emitted
    "No active channels returned by content provider" even when channels
    existed and were queried but their vault was simply empty. This was
    misleading — operators interpreted it as a seeding failure when in
    reality the system was working correctly and just had no content to post.
    The final log now distinguishes three cases clearly:
      A) No channels configured at all (seeding failure)
      B) Channels configured, some have eligible content
      C) Channels configured, ALL vaults empty / all locked
    """
    db = DatabaseManager.get_db()
    vault = db[getattr(settings, "VAULT_COLLECTION", "vault")]
    channels = db[getattr(settings, "CHANNEL_CONFIG_COLLECTION", "channel_config")]

    # Case A: no channels at all — seeding failure
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

        # Diagnostic counts so operators can see exactly why content is or isn't flowing
        total_queued = await vault.count_documents({
            "moderation_destination": dest,
            "status": ModerationState.QUEUED.value,
        })

        total_locked = await vault.count_documents({
            "moderation_destination": dest,
            "status": ModerationState.QUEUED.value,
            "distribution_state": {"$in": ["locked", "removed"]},
        })

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
                    "Vault is empty for destination — no content has been approved yet. "
                    "Submit and approve content through the moderation pipeline to begin distribution.",
                    extra={"ctx_dest": dest},
                )
            else:
                logger.warning(
                    "Vault has queued content but none is eligible — "
                    "all items are locked, removed, or on cooldown.",
                    extra={
                        "ctx_dest": dest,
                        "ctx_total_queued": total_queued,
                        "ctx_locked": total_locked,
                    },
                )

    # LOG CLARITY FIX: distinguish empty vault (normal on fresh deploy) from
    # missing channel config (seeding failure). The old message fired for both.
    if not active_configs:
        logger.info(
            "fetch_distribution_content: returning no eligible content this cycle. "
            "Channels are configured (%d active) but all destination vaults are "
            "empty or fully locked. This is normal on a fresh deployment — "
            "submit content via the bot and approve it through moderation.",
            total_active_channels,
            extra={"ctx_active_channels": total_active_channels},
        )

    return active_configs