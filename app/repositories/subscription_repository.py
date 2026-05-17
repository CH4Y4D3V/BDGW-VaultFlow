from __future__ import annotations

from datetime import datetime, timedelta
from typing import Optional

from pymongo import ASCENDING, DESCENDING

from app.models.subscription import Plan, Subscription, SubscriptionStatus
from app.repositories.base import BaseRepository


class SubscriptionRepository(BaseRepository):
    collection_name = "subscriptions"

    # ── Single-record ops ─────────────────────────────────────────────────────

    async def get_by_user_id(self, user_id: int) -> Optional[Subscription]:
        doc = await self.find_one({"user_id": user_id})
        return Subscription.from_dict(doc) if doc else None

    async def upsert(self, subscription: Subscription) -> None:
        await self.collection.update_one(
            {"user_id": subscription.user_id},
            {"$set": subscription.to_dict()},
            upsert=True,
        )

    async def update_status(
        self,
        user_id: int,
        status: SubscriptionStatus,
        updated_at: Optional[datetime] = None,
    ) -> None:
        await self.update_one(
            {"user_id": user_id},
            {
                "$set": {
                    "status": status.value,
                    "updated_at": updated_at or datetime.utcnow(),
                }
            },
        )

    async def delete_by_user_id(self, user_id: int) -> int:
        return await self.delete_one({"user_id": user_id})

    # ── Bulk expiry queries ───────────────────────────────────────────────────

    async def get_expiring_soon(self, within_hours: int = 24) -> list[Subscription]:
        """Active subscriptions expiring within the given window (for notifications)."""
        now = datetime.utcnow()
        cutoff = now + timedelta(hours=within_hours)
        docs = await self.find_many(
            {
                "status": SubscriptionStatus.ACTIVE.value,
                "expires_at": {"$lte": cutoff, "$gt": now},
                "plan": {
                    "$nin": [
                        Plan.FREE.value,
                        Plan.OWNER.value,
                        Plan.SUDO.value,
                    ]
                },
            },
            sort=[("expires_at", ASCENDING)],
        )
        return [Subscription.from_dict(d) for d in docs]

    async def get_newly_expired(self) -> list[Subscription]:
        """Active subs whose expires_at has passed — must be moved to grace."""
        now = datetime.utcnow()
        docs = await self.find_many(
            {
                "status": SubscriptionStatus.ACTIVE.value,
                "expires_at": {"$lte": now, "$ne": None},
                "plan": {
                    "$nin": [
                        Plan.FREE.value,
                        Plan.OWNER.value,
                        Plan.SUDO.value,
                    ]
                },
            }
        )
        return [Subscription.from_dict(d) for d in docs]

    async def get_grace_expired(self) -> list[Subscription]:
        """Grace subs whose grace_until has passed — must be fully expired."""
        now = datetime.utcnow()
        docs = await self.find_many(
            {
                "status": SubscriptionStatus.GRACE.value,
                "grace_until": {"$lte": now},
            }
        )
        return [Subscription.from_dict(d) for d in docs]

    # ── List / stats queries ──────────────────────────────────────────────────

    async def get_all_active(self, plan: Optional[Plan] = None) -> list[Subscription]:
        filter_: dict = {"status": SubscriptionStatus.ACTIVE.value}
        if plan:
            filter_["plan"] = plan.value
        docs = await self.find_many(filter_, sort=[("expires_at", ASCENDING)])
        return [Subscription.from_dict(d) for d in docs]

    async def get_all_by_status(self, status: SubscriptionStatus) -> list[Subscription]:
        docs = await self.find_many({"status": status.value})
        return [Subscription.from_dict(d) for d in docs]

    async def get_paginated(
        self,
        status: Optional[SubscriptionStatus] = None,
        plan: Optional[Plan] = None,
        limit: int = 50,
        skip: int = 0,
    ) -> tuple[list[Subscription], int]:
        filter_: dict = {}
        if status:
            filter_["status"] = status.value
        if plan:
            filter_["plan"] = plan.value
        total = await self.count(filter_)
        docs = await self.find_many(
            filter_,
            sort=[("updated_at", DESCENDING)],
            limit=limit,
            skip=skip,
        )
        return [Subscription.from_dict(d) for d in docs], total

    async def get_stats(self) -> dict:
        """Aggregate counts grouped by status × plan."""
        pipeline = [
            {
                "$group": {
                    "_id": {"status": "$status", "plan": "$plan"},
                    "count": {"$sum": 1},
                }
            }
        ]
        result: dict = {}
        async for doc in self.collection.aggregate(pipeline):
            key = f"{doc['_id']['status']}:{doc['_id']['plan']}"
            result[key] = doc["count"]
        return result