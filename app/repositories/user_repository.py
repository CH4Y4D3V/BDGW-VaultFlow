from __future__ import annotations

from datetime import datetime, timezone
from typing import Optional

from motor.motor_asyncio import AsyncIOMotorDatabase
from pymongo import ASCENDING

from app.repositories.base import BaseRepository

class UserRepository(BaseRepository):
    collection_name = "users"

    async def upsert_user(
        self,
        user_id: int,
        first_name: str,
        last_name: Optional[str] = None,
        username: Optional[str] = None,
    ) -> None:
        """Create or update user record with core metadata."""
        now = datetime.now(timezone.utc)
        name = first_name + (f" {last_name}" if last_name else "")
        
        await self.collection.update_one(
            {"_id": user_id},
            {
                "$set": {
                    "user_id": user_id,
                    "name": name,
                    "username": username,
                    "updated_at": now,
                },
                "$setOnInsert": {
                    "join_date": now,
                    "is_banned": False,
                    "metadata": {},
                }
            },
            upsert=True
        )

    async def get_user(self, user_id: int) -> Optional[dict]:
        return await self.collection.find_one({"_id": user_id})

    async def create_indexes(self) -> None:
        await self.collection.create_index([("user_id", ASCENDING)], unique=True)
        await self.collection.create_index([("username", ASCENDING)])
        await self.collection.create_index([("join_date", ASCENDING)])
