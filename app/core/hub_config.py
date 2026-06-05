from __future__ import annotations
from typing import Optional
from app.utils.logger import get_logger

logger = get_logger(__name__)


class HubConfig:
    """
    Supports both attribute access (cfg.hub_supergroup_id)
    and dict-style access (cfg.get("hub_supergroup_id")).
    Loaded lazily from MongoDB; falls back to settings on miss.
    """

    def __init__(self) -> None:
        self._cache: Optional[dict] = None

    def _fallback(self) -> dict:
        from app.config import settings
        return {
            "hub_supergroup_id": getattr(settings, "VERIFICATION_GROUP_ID", 0),
            "admin_logs_topic_id": getattr(settings, "HUB_TOPIC_ADMIN_LOGS", 0),
            "nsfw_group_id": getattr(settings, "NSFW_GROUP_ID", 0),
            "premium_group_id": getattr(settings, "PREMIUM_GROUP_ID", 0),
            "main_channel_id": getattr(settings, "MAIN_CHANNEL_ID", 0),
        }

    async def load_from_db(self) -> None:
        try:
            from app.core.database import DatabaseManager
            db = DatabaseManager.get_db()
            docs = await db["hub_config"].find({}).to_list(length=None)
            self._cache = {doc["key"]: doc["value"] for doc in docs}
        except Exception as exc:
            logger.warning("hub_config DB load failed — using settings fallback: %s", exc)
            self._cache = self._fallback()

    def _data(self) -> dict:
        if self._cache is None:
            return self._fallback()
        return self._cache

    def get(self, key: str, default=None):
        return self._data().get(key, default)

    @property
    def hub_supergroup_id(self) -> int:
        return int(self.get("hub_supergroup_id") or 0)

    @property
    def admin_logs_topic_id(self) -> int:
        return int(self.get("admin_logs_topic_id") or 0)

    @property
    def nsfw_group_id(self) -> int:
        return int(self.get("nsfw_group_id") or 0)

    @property
    def premium_group_id(self) -> int:
        return int(self.get("premium_group_id") or 0)

    def __getitem__(self, key: str):
        return self._data()[key]


hub_config = HubConfig()


def get_hub_config() -> HubConfig:
    return hub_config