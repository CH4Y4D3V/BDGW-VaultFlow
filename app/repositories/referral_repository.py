from __future__ import annotations
from datetime import datetime, timedelta, timezone
from typing import List, Optional
from motor.motor_asyncio import AsyncIOMotorDatabase
from pymongo import IndexModel, ASCENDING, DESCENDING, ReturnDocument
from pymongo.errors import DuplicateKeyError, OperationFailure
from app.referral.models import ReferralStatus
from app.utils.logger import get_logger

logger = get_logger(__name__)


class ReferralRepository:
    def __init__(self, db: AsyncIOMotorDatabase):
        self._referrals = db['referrals']
        self._wallets = db['referral_wallets']

    async def create_indexes(self) -> None:
        """
        FIX: Each index created in its own try/except so a conflict on one
        index does not abort creation of the remaining indexes.
        If an index already exists with identical options, MongoDB is a no-op.
        If it exists with different options (OperationFailure), we drop and recreate.
        """
        indexes_to_create = [
            (
                self._referrals,
                IndexModel(
                    [("referred_user_id", ASCENDING)],
                    unique=True,
                    name="unique_referral_user",
                ),
                "unique_referral_user",
            ),
            (
                self._referrals,
                IndexModel(
                    [("referrer_user_id", ASCENDING), ("status", ASCENDING)],
                    name="referrer_status_lookup",
                ),
                "referrer_status_lookup",
            ),
            (
                self._referrals,
                IndexModel(
                    [("status", ASCENDING), ("created_at", ASCENDING)],
                    name="qualification_job_lookup",
                ),
                "qualification_job_lookup",
            ),
            (
                self._wallets,
                IndexModel(
                    [("user_id", ASCENDING)],
                    unique=True,
                    name="unique_wallet_user",
                ),
                "unique_wallet_user",
            ),
        ]

        for collection, index_model, name in indexes_to_create:
            try:
                await collection.create_indexes([index_model])
                logger.debug(
                    "referral_index_created",
                    extra={"ctx_index": name, "ctx_collection": collection.name},
                )
            except OperationFailure as e:
                # Index exists with different options — drop and recreate
                logger.warning(
                    "referral_index_conflict_dropping",
                    extra={"ctx_index": name, "ctx_error": str(e)},
                )
                try:
                    await collection.drop_index(name)
                    await collection.create_indexes([index_model])
                    logger.info(
                        "referral_index_recreated",
                        extra={"ctx_index": name},
                    )
                except Exception as retry_e:
                    logger.error(
                        "referral_index_recreate_failed",
                        extra={"ctx_index": name, "ctx_error": str(retry_e)},
                    )
            except Exception as e:
                logger.error(
                    "referral_index_create_failed",
                    extra={"ctx_index": name, "ctx_error": str(e)},
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
