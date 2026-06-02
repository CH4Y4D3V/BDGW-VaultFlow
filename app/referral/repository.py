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
        Attempt to create indexes. On IndexOptionsConflict (name already exists
        with different options, or same options under a different name), drop
        conflicting indexes by name and retry once.
        """
        try:
            await collection.create_indexes(indexes)
        except OperationFailure as e:
            if e.code == 85:  # IndexOptionsConflict
                # Drop all indexes that conflict with the ones we want to create
                try:
                    existing = await collection.list_indexes().to_list(length=100)
                    existing_names = {idx["name"] for idx in existing if idx["name"] != "_id_"}
                    desired_names = {idx.document["name"] for idx in indexes}

                    # Drop any index whose name is NOT in our desired set
                    # (old names left over from a previous schema)
                    to_drop = existing_names - desired_names
                    for name in to_drop:
                        try:
                            await collection.drop_index(name)
                        except Exception as drop_err:
                            from app.utils.logger import get_logger
                            get_logger(__name__).warning(
                                f"Could not drop index {name} on {label}",
                                extra={"ctx_error": str(drop_err)},
                            )

                    # Retry creation
                    await collection.create_indexes(indexes)
                    from app.utils.logger import get_logger
                    get_logger(__name__).info(
                        f"{label} indexes reconciled successfully after conflict resolution",
                    )
                except Exception as retry_err:
                    from app.utils.logger import get_logger
                    get_logger(__name__).error(
                        f"Failed to reconcile indexes for {label}",
                        extra={"ctx_error": str(retry_err)},
                        exc_info=True,
                    )
            else:
                from app.utils.logger import get_logger
                get_logger(__name__).error(
                    f"Index creation failed for {label}",
                    extra={"ctx_error": str(e), "ctx_code": e.code},
                    exc_info=True,
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