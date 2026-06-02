from __future__ import annotations

from datetime import datetime, timezone
from typing import Optional

from motor.motor_asyncio import AsyncIOMotorDatabase
from pymongo import ASCENDING

from app.repositories.base import BaseRepository

import uuid
from app.models.user import User

class UserRepository(BaseRepository):
    collection_name = "users"

    async def upsert_user(
        self,
        user_id: int,
        full_name: str,
        username: Optional[str] = None,
        referred_by: Optional[int] = None,
    ) -> User:
        """Create or update user record with full metadata."""
        now = datetime.now(timezone.utc)
        
        # Check if user exists
        existing = await self.get_user(user_id)
        if existing:
            update_data = {
                "full_name": full_name,
                "username": username,
                "updated_at": now,
            }
            await self.collection.update_one({"_id": user_id}, {"$set": update_data})
            return User.from_dict({**existing, **update_data})

        # Create new user
        new_user = User(
            _id=user_id,
            username=username,
            full_name=full_name,
            join_date=now,
            referral_code=str(uuid.uuid4())[:8],
            referred_by=referred_by,
            created_at=now,
            updated_at=now
        )
        await self.collection.insert_one(new_user.to_dict())
        return new_user

    async def get_user(self, user_id: int) -> Optional[dict]:
        return await self.collection.find_one({"_id": user_id})

    async def get_user_model(self, user_id: int) -> Optional[User]:
        doc = await self.get_user(user_id)
        return User.from_dict(doc) if doc else None

    async def get_by_referral_code(self, code: str) -> Optional[User]:
        doc = await self.collection.find_one({"referral_code": code})
        return User.from_dict(doc) if doc else None

    async def ban_user(self, user_id: int, reason: str = "No reason provided") -> bool:
        """Permanently ban a user."""
        result = await self.collection.update_one(
            {"_id": user_id},
            {
                "$set": {
                    "is_banned": True, 
                    "ban_reason": reason, 
                    "updated_at": datetime.now(timezone.utc)
                }
            }
        )
        return result.modified_count > 0

    async def set_onboarded(self, user_id: int, status: bool = True) -> bool:
        result = await self.collection.update_one(
            {"_id": user_id},
            {"$set": {"onboarded": status, "updated_at": datetime.now(timezone.utc)}}
        )
        return result.modified_count > 0

    async def update_scores(self, user_id: int, trust_delta: int = 0, fraud_delta: int = 0) -> bool:
        result = await self.collection.update_one(
            {"_id": user_id},
            {
                "$inc": {"trust_score": trust_delta, "fraud_score": fraud_delta},
                "$set": {"updated_at": datetime.now(timezone.utc)}
            }
        )
        return result.modified_count > 0

    async def create_indexes(self) -> None:
        # _id IS the user_id — MongoDB enforces uniqueness automatically
        # Only create indexes on secondary lookup fields
        await self.collection.create_index(
            [("username", ASCENDING)],
            sparse=True,
            name="user_username_lookup"
        )
        await self.collection.create_index(
            [("referral_code", ASCENDING)],
            unique=True,
            sparse=True,
            name="user_referral_code_unique"
        )
        await self.collection.create_index(
            [("referred_by", ASCENDING)],
            sparse=True,
            name="user_referred_by_lookup"
        )
        await self.collection.create_index(
            [("join_date", ASCENDING)],
            name="user_join_date"
        )
