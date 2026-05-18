from __future__ import annotations

from app.config import settings
from app.repositories.base import BaseRepository


class ChannelRepository(BaseRepository):
    collection_name = getattr(settings, "CHANNEL_CONFIG_COLLECTION", "channel_config")

    async def upsert_channel(self, destination: str, doc: dict) -> None:
        await self.update_one(
            {"destination": destination},
            {"$set": doc},
            upsert=True,
        )

    async def get_active_channels(self) -> list[dict]:
        return await self.find_many({"is_active": True})