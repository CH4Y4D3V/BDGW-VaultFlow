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

    FIX: Previously only queried status=QUEUED. Content approved via
    execute_approve() has status=POSTED and was invisible forever, meaning
    "Approve Immediately" content never re-entered the distribution pool.
    Now also includes status=POSTED items where cooldown_until has expired
    (or is absent). execute_approve() sets cooldown_until on the vault doc
    so these items are suppressed until their cooldown passes.
    """
    db = DatabaseManager.get_db()
    vault = db[getattr(settings, "VAULT_COLLECTION", "vault")]
    channels = db[getattr(settings, "CHANNEL_CONFIG_COLLECTION", "channel_config")]

    # Case A: no channels at all — seeding failure
    total_active_channels = await channels.count_documents({"is_active": True})
    if total_active_channels == 0:
        logger.error(
            "no_active_channels_configured",
            extra={"ctx_hint": "Check NSFW_GROUP_ID/PREMIUM_GROUP_ID and seeding"}
        )
        return []

    active_configs = []
    now = datetime.now(timezone.utc)

    async for config in channels.find({"is_active": True}):
        dest = config.get("destination")
        source_id = config.get("source_channel_id")

        if not dest or not source_id:
            logger.warning(
                "malformed_channel_config_skipped",
                extra={"ctx_config_id": str(config.get("_id"))},
            )
            continue

        # Eligible statuses: QUEUED (queued for first delivery) and POSTED
        # (already posted once via execute_approve — eligible for vault replay
        # after cooldown expires).
        eligible_statuses = [
            ModerationState.QUEUED.value,
            ModerationState.POSTED.value,
        ]

        # Diagnostic counts so operators can see exactly why content is or isn't flowing
        total_queued = await vault.count_documents({
            "moderation_destination": dest,
            "status": {"$in": eligible_statuses},
        })

        total_locked = await vault.count_documents({
            "moderation_destination": dest,
            "status": {"$in": eligible_statuses},
            "distribution_state": {"$in": ["locked", "removed"]},
        })

        cursor = vault.find({
            "moderation_destination": dest,
            "status": {"$in": eligible_statuses},
            "distribution_state": {"$nin": ["locked", "removed"]},
            "$or": [
                {"cooldown_until": None},
                {"cooldown_until": {"$exists": False}},
                {"cooldown_until": {"$lte": now}},
            ],
        }).sort("vault_message_id", 1).limit(getattr(settings, "MAX_JOBS_PER_CYCLE", 100))

        content = await cursor.to_list(length=None)

        logger.info(
            "channel_provider_query_result",
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
                    "vault_empty_for_destination",
                    extra={"ctx_dest": dest},
                )
            else:
                logger.warning(
                    "vault_has_content_none_eligible",
                    extra={
                        "ctx_dest": dest,
                        "ctx_total_queued": total_queued,
                        "ctx_locked": total_locked,
                    },
                )

    if not active_configs:
        logger.info(
            "no_eligible_content_this_cycle",
            extra={"ctx_active_channels": total_active_channels},
        )

    return active_configs
