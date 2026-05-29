from __future__ import annotations
from datetime import datetime, timedelta, timezone
from typing import List, Optional
from motor.motor_asyncio import AsyncIOMotorDatabase
from pymongo import IndexModel, ASCENDING, DESCENDING, ReturnDocument
from pymongo.errors import DuplicateKeyError
from app.referral.models import ReferralStatus

class ReferralRepository:
    def __init__(self, db: AsyncIOMotorDatabase):
        self._referrals = db['referrals']
        self._wallets = db['referral_wallets']

    async def create_indexes(self) -> None:
        # Index 1: unique index on referred_user_id (prevents double referral)
        await self._referrals.create_index(
            [("referred_user_id", ASCENDING)],
            unique=True,
            name="unique_referral_user"
        )
        
        # Index 2: compound index on (referrer_user_id, status) for wallet sync
        await self._referrals.create_index(
            [("referrer_user_id", ASCENDING), ("status", ASCENDING)],
            name="referrer_status_lookup"
        )
        
        # Index 3: index on (status, created_at) for background job and manual purging
        await self._referrals.create_index(
            [("status", ASCENDING), ("created_at", ASCENDING)],
            name="qualification_job_lookup"
        )

        # Wallet unique index
        await self._wallets.create_index(
            [("user_id", ASCENDING)],
            unique=True,
            name="unique_wallet_user"
        )

    async def purge_stale_pending(self, hours: int = 48) -> int:
        """Manually purge PENDING referrals older than N hours."""
        threshold = datetime.now(timezone.utc) - timedelta(hours=hours)
        result = await self._referrals.delete_many({
            "status": ReferralStatus.PENDING,
            "created_at": {"$lt": threshold}
        })
        return result.deleted_count

    async def create_pending(self, referrer_id: int, referred_id: int) -> bool:
        doc = {
            "referrer_user_id": referrer_id,
            "referred_user_id": referred_id,
            "status": ReferralStatus.PENDING,
            "qualified": False,
            "channel_member": True,
            "bot_active": True,
            "created_at": datetime.now(timezone.utc),
            "qualified_at": None,
            "invalidated_at": None
        }
        try:
            await self._referrals.insert_one(doc)
            return True
        except DuplicateKeyError:
            return False

    async def get_referral_by_referred(self, referred_id: int) -> Optional[dict]:
        return await self._referrals.find_one({"referred_user_id": referred_id})

    async def get_pending_older_than(self, hours: int) -> List[dict]:
        threshold = datetime.now(timezone.utc) - timedelta(hours=hours)
        cursor = self._referrals.find({
            "status": ReferralStatus.PENDING,
            "created_at": {"$lte": threshold}
        })
        return await cursor.to_list(length=None)

    async def qualify_referral(self, referred_id: int) -> bool:
        now = datetime.now(timezone.utc)
        res = await self._referrals.find_one_and_update(
            {"referred_user_id": referred_id, "status": ReferralStatus.PENDING},
            {
                "$set": {
                    "status": ReferralStatus.QUALIFIED,
                    "qualified": True,
                    "qualified_at": now
                }
            },
            return_document=ReturnDocument.AFTER
        )
        return res is not None

    async def invalidate_referral(self, referred_id: int) -> bool:
        now = datetime.now(timezone.utc)
        res = await self._referrals.find_one_and_update(
            {"referred_user_id": referred_id, "status": ReferralStatus.QUALIFIED},
            {
                "$set": {
                    "status": ReferralStatus.INVALIDATED,
                    "invalidated_at": now
                }
            },
            return_document=ReturnDocument.AFTER
        )
        return res is not None

    async def reactivate_referral(self, referred_id: int) -> bool:
        res = await self._referrals.find_one_and_update(
            {"referred_user_id": referred_id, "status": ReferralStatus.INVALIDATED},
            {
                "$set": {
                    "status": ReferralStatus.QUALIFIED,
                    "invalidated_at": None,
                    "channel_member": True
                }
            },
            return_document=ReturnDocument.AFTER
        )
        return res is not None

    async def get_wallet(self, user_id: int) -> Optional[dict]:
        return await self._wallets.find_one({"user_id": user_id})

    async def upsert_wallet(self, user_id: int) -> None:
        await self._wallets.update_one(
            {"user_id": user_id},
            {
                "$setOnInsert": {
                    "points_balance": 0,
                    "total_earned": 0,
                    "total_spent": 0,
                    "active_referrals": 0
                }
            },
            upsert=True
        )

    async def increment_balance(self, user_id: int, amount: int) -> None:
        update = {"$inc": {"points_balance": amount}}
        if amount > 0:
            update["$inc"]["total_earned"] = amount
            update["$inc"]["active_referrals"] = 1
        else:
            # Note: The decrement case for active_referrals
            update["$inc"]["active_referrals"] = -1
            
        await self._wallets.update_one({"user_id": user_id}, update, upsert=True)

    async def decrement_balance(self, user_id: int) -> None:
        # Atomic points_balance = max(0, points_balance - 1), active_referrals -= 1
        await self._wallets.update_one(
            {"user_id": user_id},
            {"$inc": {"points_balance": -1, "active_referrals": -1}}
        )
        # Safeguard: clamp negative balance
        await self._wallets.update_one(
            {"user_id": user_id, "points_balance": {"$lt": 0}},
            {"$set": {"points_balance": 0}}
        )

    async def deduct_points(self, user_id: int, amount: int) -> bool:
        res = await self._wallets.find_one_and_update(
            {"user_id": user_id, "points_balance": {"$gte": amount}},
            {"$inc": {"points_balance": -amount, "total_spent": amount}},
            return_document=ReturnDocument.AFTER
        )
        return res is not None
