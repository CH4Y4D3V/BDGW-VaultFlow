from __future__ import annotations

from datetime import datetime
from typing import Optional

from app.models.invite import Invite, InviteStatus
from app.repositories.base import BaseRepository


class InviteRepository(BaseRepository):
    collection_name = "invites"

    # ── Single-record ops ─────────────────────────────────────────────────────

    async def get_by_token(self, token: str) -> Optional[Invite]:
        doc = await self.find_one({"token": token})
        return Invite.from_dict(doc) if doc else None

    async def create(self, invite: Invite) -> None:
        await self.insert_one(invite.to_dict())

    async def consume(self, token: str, user_id: int) -> Optional[Invite]:
        """Atomically decrement uses_remaining and record the user.

        Returns the post-update document, or None if the invite is no longer valid.
        After consuming the last use the status is flipped to EXHAUSTED in the same
        request so there is no race window.
        """
        now = datetime.utcnow()
        doc = await self.collection.find_one_and_update(
            {
                "token": token,
                "status": InviteStatus.ACTIVE.value,
                "uses_remaining": {"$gt": 0},
                "$or": [
                    {"expires_at": None},
                    {"expires_at": {"$gt": now}},
                ],
            },
            {
                "$inc": {"uses_remaining": -1},
                "$push": {"used_by": user_id},
            },
            return_document=True,
        )
        if doc is None:
            return None

        invite = Invite.from_dict(doc)

        # Seal the invite if all uses are exhausted
        if invite.uses_remaining <= 0:
            await self.update_one(
                {"token": token},
                {"$set": {"status": InviteStatus.EXHAUSTED.value}},
            )
            invite.status = InviteStatus.EXHAUSTED

        return invite

    async def revoke(self, token: str, revoked_by: int) -> bool:
        now = datetime.utcnow()
        count = await self.update_one(
            {"token": token, "status": InviteStatus.ACTIVE.value},
            {
                "$set": {
                    "status": InviteStatus.REVOKED.value,
                    "revoked_by": revoked_by,
                    "revoked_at": now,
                }
            },
        )
        return count > 0

    # ── Bulk ops ──────────────────────────────────────────────────────────────

    async def expire_stale(self) -> int:
        """Flip active invites whose expires_at has passed to EXPIRED."""
        now = datetime.utcnow()
        return await self.update_many(
            {
                "status": InviteStatus.ACTIVE.value,
                "expires_at": {"$lte": now, "$ne": None},
            },
            {"$set": {"status": InviteStatus.EXPIRED.value}},
        )

    # ── List queries ──────────────────────────────────────────────────────────

    async def get_by_creator(
        self,
        user_id: int,
        status: Optional[InviteStatus] = None,
    ) -> list[Invite]:
        filter_: dict = {"created_by": user_id}
        if status:
            filter_["status"] = status.value
        docs = await self.find_many(filter_, sort=[("created_at", -1)])
        return [Invite.from_dict(d) for d in docs]

    async def get_active_for_chat(self, chat_id: int) -> list[Invite]:
        docs = await self.find_many(
            {"chat_id": chat_id, "status": InviteStatus.ACTIVE.value}
        )
        return [Invite.from_dict(d) for d in docs]