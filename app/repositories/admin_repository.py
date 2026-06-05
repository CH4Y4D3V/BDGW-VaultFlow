from __future__ import annotations

from typing import Optional
from app.repositories.base import BaseRepository


class AdminRepository(BaseRepository):
    collection_name = "admins"

    async def get_active_by_user_id(self, user_id: int) -> Optional[dict]:
        """
        Fetch an active admin record by Telegram user_id.
        """
        return await self.find_one({
            "user_id": user_id,
            "is_active": True,
        })

    async def create_indexes(self) -> None:
        """
        Create unique index on user_id for the admins collection.
        """
        from pymongo import ASCENDING
        await self.collection.create_index(
            [("user_id", ASCENDING)],
            unique=True,
            name="admins_user_id_unique"
        )
