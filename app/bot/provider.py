from typing import List, Dict


from app.config import settings
from app.core.database import DatabaseManager
from app.utils.logger import get_logger

logger = get_logger(__name__)


async def fetch_distribution_content() -> List[Dict]:
    """
    Called by DistributionScheduler to fetch valid vault content.
    Provides the data boundary ensuring only fully ingested, non-duplicate content
    is released to the fairness selector.
    """
    db = DatabaseManager.get_db()
    vault = db[getattr(settings, "VAULT_COLLECTION", "vault")]
    channels = db[getattr(settings, "CHANNEL_CONFIG_COLLECTION", "channel_config")]

    active_configs = []
    
    async for config in channels.find({"is_active": True}):
        source_id = config["source_channel_id"]
        
        cursor = vault.find({
            "source_channel_id": source_id,
            "status": "pending_distribution"
        }).sort("message_id", 1).limit(getattr(settings, "MAX_JOBS_PER_CYCLE", 100))
        
        content = await cursor.to_list(length=None)
        
        if content:
            active_configs.append({
                "source_channel_id": source_id,
                "target_channel_ids": config.get("target_channel_ids", []),
                "content": content,
                "watermark_config": config.get("watermark_config")
            })
            
    return active_configs