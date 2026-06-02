from __future__ import annotations
from datetime import datetime, timedelta, timezone
from typing import List, Optional
from motor.motor_asyncio import AsyncIOMotorDatabase
from pymongo import IndexModel, ASCENDING, DESCENDING, ReturnDocument
from pymongo.errors import DuplicateKeyError, OperationFailure
from app.referral.models import ReferralStatus


class ReferralRepository:
    def __init__(self, db: AsyncIOMotorDatabase):
        self._referrals = db['referrals']
        self._wallets = db['referral_wallets']

    async def create_indexes(self) -> None:
        """
        Create or reconcile indexes for referral collections.

        Uses drop-and-recreate only for conflicting indexes (name mismatch on
        the same key pattern). All indexes are idempotent on re-run.
        """
        # ── referrals collection ──────────────────────────────────────────────

        referral_indexes = [
            # Unique: one referral record per referred user
            IndexModel(
                [("referred_user_id", ASCENDING)],
                unique=True,
                name="unique_referral_user",
            ),
            # Compound: wallet sync queries
            IndexModel(
                [("referrer_user_id", ASCENDING), ("status", ASCENDING)],
                name="referrer_status_lookup",
            ),
            # Partial TTL: auto-expire PENDING records after 48 h
            IndexModel(
                [("created_at", ASCENDING)],
                name="pending_ttl",
                expireAfterSeconds=172800,
                partialFilterExpression={"status": ReferralStatus.PENDING},
            ),
            # Compound: background qualification job
            IndexModel(
                [("status", ASCENDING), ("created_at", ASCENDING)],
                name="qualification_job_lookup",
            ),
        ]

        await self._safe_create_indexes(self._referrals, referral_indexes, "referrals")

        # ── referral_wallets collection ───────────────────────────────────────

        wallet_indexes = [
            IndexModel(
                [("user_id", ASCENDING)],
                unique=True,
                name="unique_wallet_user",
            ),
        ]

        await self._safe_create_indexes(self._referrals.database['referral_wallets'], wallet_indexes, "referral_wallets")

    @staticmethod
    async def _safe_create_indexes(collection, indexes: list, label: str) -> None:
        """
        Attempt to create indexes one by one. On IndexOptionsConflict (name already exists
        with different options, or same options under a different name), drop
        conflicting index and retry.
        """
        for index in indexes:
            idx_name = index.document.get("name")
            try:
                await collection.create_indexes([index])
            except OperationFailure as e:
                if e.code == 85:  # IndexOptionsConflict
                    try:
                        from app.utils.logger import get_logger
                        get_logger(__name__).warning(
                            f"IndexOptionsConflict for {idx_name} on {label}, dropping and recreating."
                        )
                        # We try to drop by name
                        await collection.drop_index(idx_name)
                        await collection.create_indexes([index])
                    except Exception as retry_err:
                        from app.utils.logger import get_logger
                        get_logger(__name__).error(
                            f"Failed to reconcile index {idx_name} for {label}",
                            extra={"ctx_error": str(retry_err)},
                        )
                else:
                    from app.utils.logger import get_logger
                    get_logger(__name__).error(
                        f"Index creation failed for {idx_name} on {label}",
                        extra={"ctx_error": str(e), "ctx_code": e.code},
                    )
            except Exception as ex:
                from app.utils.logger import get_logger
                get_logger(__name__).error(
                    f"Unexpected error creating index {idx_name} on {label}",
                    extra={"ctx_error": str(ex)},
                )

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
        update: dict = {"$inc": {"points_balance": amount}}
        if amount > 0:
            update["$inc"]["total_earned"] = amount
            update["$inc"]["active_referrals"] = 1
        else:
            update["$inc"]["active_referrals"] = -1

        await self._wallets.update_one({"user_id": user_id}, update, upsert=True)

    async def decrement_balance(self, user_id: int) -> None:
        await self._wallets.update_one(
            {"user_id": user_id},
            {"$inc": {"points_balance": -1, "active_referrals": -1}}
        )
        # Clamp negative balance
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