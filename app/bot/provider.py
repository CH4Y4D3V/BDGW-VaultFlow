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
    """
    db = DatabaseManager.get_db()
    vault = db[getattr(settings, "VAULT_COLLECTION", "vault")]
    channels = db[getattr(settings, "CHANNEL_CONFIG_COLLECTION", "channel_config")]

    active_configs = []
    now = datetime.now(timezone.utc)

    async for config in channels.find({"is_active": True}):
        dest = config.get("destination")
        source_id = config.get("source_channel_id")
        
        if not dest or not source_id:
            continue

        # M5: exclude locked/removed items and items still within cooldown window
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

        if content:
            active_configs.append({
                "source_channel_id": source_id,
                "target_channel_ids": config.get("target_channel_ids", []),
                "content": content,
                "watermark_config": config.get("watermark_config"),
            })

    return active_configs