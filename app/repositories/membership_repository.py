from __future__ import annotations

from datetime import datetime, timezone
from typing import Optional

from app.models.membership import ChatType, Membership, MembershipStatus
from app.repositories.base import BaseRepository


class MembershipRepository(BaseRepository):
    collection_name = "memberships"

    # ── Single-record ops ─────────────────────────────────────────────────────

    async def get(self, user_id: int, chat_id: int) -> Optional[Membership]:
        doc = await self.find_one({"user_id": user_id, "chat_id": chat_id})
        return Membership.from_dict(doc) if doc else None

    async def upsert(self, membership: Membership) -> None:
        await self.collection.update_one(
            {"user_id": membership.user_id, "chat_id": membership.chat_id},
            {"$set": membership.to_dict()},
            upsert=True,
        )

    async def update_status(
        self,
        user_id: int,
        chat_id: int,
        status: MembershipStatus,
        reason: Optional[str] = None,
    ) -> None:
        now = datetime.now(timezone.utc)
        patch: dict = {
            "status": status.value,
            "last_verified": now,
        }
        if status in (MembershipStatus.REMOVED, MembershipStatus.KICKED):
            patch["removed_at"] = now
            patch["removed_reason"] = reason
        await self.update_one(
            {"user_id": user_id, "chat_id": chat_id},
            {"$set": patch},
        )

    async def update_last_verified(self, user_id: int, chat_id: int) -> None:
        await self.update_one(
            {"user_id": user_id, "chat_id": chat_id},
            {"$set": {"last_verified": datetime.now(timezone.utc)}},
        )

    # ── List queries ──────────────────────────────────────────────────────────

    async def get_user_memberships(
        self,
        user_id: int,
        status: Optional[MembershipStatus] = None,
    ) -> list[Membership]:
        filter_: dict = {"user_id": user_id}
        if status:
            filter_["status"] = status.value
        docs = await self.find_many(filter_)
        return [Membership.from_dict(d) for d in docs]

    async def get_chat_members(
        self,
        chat_id: int,
        status: MembershipStatus = MembershipStatus.ACTIVE,
    ) -> list[Membership]:
        docs = await self.find_many({"chat_id": chat_id, "status": status.value})
        return [Membership.from_dict(d) for d in docs]

    async def get_by_chat_type(
        self,
        user_id: int,
        chat_type: ChatType,
    ) -> list[Membership]:
        docs = await self.find_many(
            {
                "user_id": user_id,
                "chat_type": chat_type.value,
                "status": MembershipStatus.ACTIVE.value,
            }
        )
        return [Membership.from_dict(d) for d in docs]

    async def get_stale_memberships(
        self,
        chat_id: int,
        before: datetime,
    ) -> list[Membership]:
        docs = await self.find_many(
            {
                "chat_id": chat_id,
                "status": MembershipStatus.ACTIVE.value,
                "last_verified": {"$lt": before},
            }
        )
        return [Membership.from_dict(d) for d in docs]

    async def remove_user_from_all_chats(
        self,
        user_id: int,
        reason: str,
        chat_ids: Optional[list[int]] = None,
    ) -> int:
        """Bulk-mark active memberships as kicked."""
        now = datetime.now(timezone.utc)
        filter_: dict = {
            "user_id": user_id,
            "status": MembershipStatus.ACTIVE.value,
        }
        if chat_ids:
            filter_["chat_id"] = {"$in": chat_ids}
        return await self.update_many(
            filter_,
            {
                "$set": {
                    "status": MembershipStatus.KICKED.value,
                    "removed_at": now,
                    "removed_reason": reason,
                    "last_verified": now,
                }
            },
        )