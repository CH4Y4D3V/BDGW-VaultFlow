from __future__ import annotations
from datetime import datetime, timezone
from typing import Optional
from pymongo import IndexModel, ASCENDING
from app.core.database import DatabaseManager
from app.config import settings

class ReferralRepository:
    def __init__(self):
        self.db = DatabaseManager.get_db()
        self.referrals = self.db["referrals"]
        self.wallets = self.db["referral_wallets"]

    async def create_indexes(self):
        # referred_user_id is UNIQUE: One referral per account
        await self.referrals.create_indexes([
            IndexModel([("referred_user_id", ASCENDING)], unique=True, name="unique_referral"),
            IndexModel([("referrer_user_id", ASCENDING)], name="referrer_lookup"),
            IndexModel([("status", ASCENDING)], name="status_lookup")
        ])
        # user_id is UNIQUE: One wallet per user
        await self.wallets.create_indexes([
            IndexModel([("user_id", ASCENDING)], unique=True, name="unique_wallet")
        ])

    async def upsert_wallet(self, user_id: int):
        await self.wallets.update_one(
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

    async def get_wallet(self, user_id: int) -> Optional[dict]:
        return await self.wallets.find_one({"user_id": user_id})

    async def create_pending_referral(self, referrer_id: int, referred_id: int):
        await self.referrals.insert_one({
            "referrer_user_id": referrer_id,
            "referred_user_id": referred_id,
            "status": "pending",
            "qualified": False,
            "channel_member": True,
            "bot_active": True,
            "created_at": datetime.now(timezone.utc),
            "qualified_at": None,
            "invalidated_at": None
        })

    async def get_referral_by_referred(self, referred_user_id: int) -> Optional[dict]:
        return await self.referrals.find_one({"referred_user_id": referred_user_id})

    async def activate_referral(self, referred_user_id: int):
        await self.referrals.update_one(
            {"referred_user_id": referred_user_id},
            {
                "$set": {
                    "status": "active",
                    "qualified": True,
                    "qualified_at": datetime.now(timezone.utc)
                }
            }
        )

    async def invalidate_referral(self, referred_user_id: int):
        await self.referrals.update_one(
            {"referred_user_id": referred_user_id},
            {
                "$set": {
                    "status": "inactive",
                    "invalidated_at": datetime.now(timezone.utc)
                }
            }
        )

    async def restore_referral(self, referred_user_id: int):
        await self.referrals.update_one(
            {"referred_user_id": referred_user_id},
            {
                "$set": {
                    "status": "active",
                    "invalidated_at": None
                }
            }
        )

    async def increment_wallet_points(self, user_id: int, amount: int):
        # amount can be negative for deduction
        await self.wallets.update_one(
            {"user_id": user_id},
            {
                "$inc": {
                    "points_balance": amount,
                    "total_earned": amount if amount > 0 else 0,
                    "active_referrals": 1 if amount > 0 else -1
                }
            },
            upsert=True
        )

    async def spend_wallet_points(self, user_id: int, amount: int) -> bool:
        res = await self.wallets.update_one(
            {"user_id": user_id, "points_balance": {"$gte": amount}},
            {
                "$inc": {
                    "points_balance": -amount,
                    "total_spent": amount
                }
            }
        )
        return res.modified_count > 0
