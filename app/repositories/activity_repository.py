from __future__ import annotations

from datetime import datetime, timezone
from typing import Optional

from pymongo import DESCENDING

from app.models.activity import Activity, ActivityAction
from app.repositories.base import BaseRepository


class ActivityRepository(BaseRepository):
    collection_name = "activity"

    # ── Write ─────────────────────────────────────────────────────────────────

    async def log(self, activity: Activity) -> None:
        await self.insert_one(activity.to_dict())

    async def log_activity(
        self,
        user_id: int,
        action: ActivityAction,
        chat_id: Optional[int] = None,
        performed_by: Optional[int] = None,
        metadata: Optional[dict] = None,
    ) -> None:
        """Convenience helper to log an activity record."""
        activity = Activity(
            user_id=user_id,
            action=action,
            timestamp=datetime.now(timezone.utc),
            chat_id=chat_id,
            performed_by=performed_by,
            metadata=metadata or {},
        )
        await self.log(activity)

    # ── User-scoped reads ─────────────────────────────────────────────────────

    async def get_user_activity(
        self,
        user_id: int,
        action: Optional[ActivityAction] = None,
        since: Optional[datetime] = None,
        limit: int = 100,
    ) -> list[Activity]:
        filter_: dict = {"user_id": user_id}
        if action:
            filter_["action"] = action.value
        if since:
            filter_["timestamp"] = {"$gte": since}
        docs = await self.find_many(
            filter_,
            sort=[("timestamp", DESCENDING)],
            limit=limit,
        )
        return [Activity.from_dict(d) for d in docs]

    async def get_user_last_action(
        self,
        user_id: int,
        action: ActivityAction,
    ) -> Optional[Activity]:
        docs = await (
            self.collection
            .find({"user_id": user_id, "action": action.value})
            .sort("timestamp", DESCENDING)
            .limit(1)
            .to_list(length=1)
        )
        return Activity.from_dict(docs[0]) if docs else None

    async def count_user_actions(
        self,
        user_id: int,
        action: ActivityAction,
        since: Optional[datetime] = None,
    ) -> int:
        filter_: dict = {"user_id": user_id, "action": action.value}
        if since:
            filter_["timestamp"] = {"$gte": since}
        return await self.count(filter_)

    # ── Chat-scoped reads ─────────────────────────────────────────────────────

    async def get_chat_activity(
        self,
        chat_id: int,
        action: Optional[ActivityAction] = None,
        since: Optional[datetime] = None,
        limit: int = 100,
    ) -> list[Activity]:
        filter_: dict = {"chat_id": chat_id}
        if action:
            filter_["action"] = action.value
        if since:
            filter_["timestamp"] = {"$gte": since}
        docs = await self.find_many(
            filter_,
            sort=[("timestamp", DESCENDING)],
            limit=limit,
        )
        return [Activity.from_dict(d) for d in docs]

    # ── Global reads ──────────────────────────────────────────────────────────

    async def get_recent(
        self,
        action: Optional[ActivityAction] = None,
        since: Optional[datetime] = None,
        limit: int = 200,
    ) -> list[Activity]:
        filter_: dict = {}
        if action:
            filter_["action"] = action.value
        if since:
            filter_["timestamp"] = {"$gte": since}
        docs = await self.find_many(
            filter_,
            sort=[("timestamp", DESCENDING)],
            limit=limit,
        )
        return [Activity.from_dict(d) for d in docs]

    # ── Aggregation ───────────────────────────────────────────────────────────

    async def get_audit_summary(self, since: datetime) -> dict:
        """Grouped counts with unique-user cardinality per action type."""
        pipeline = [
            {"$match": {"timestamp": {"$gte": since}}},
            {
                "$group": {
                    "_id": "$action",
                    "count": {"$sum": 1},
                    "unique_users": {"$addToSet": "$user_id"},
                }
            },
            {
                "$project": {
                    "_id": 1,
                    "count": 1,
                    "unique_user_count": {"$size": "$unique_users"},
                }
            },
        ]
        result: dict = {}
        async for doc in self.collection.aggregate(pipeline):
            result[doc["_id"]] = {
                "count": doc["count"],
                "unique_users": doc["unique_user_count"],
            }
        return result