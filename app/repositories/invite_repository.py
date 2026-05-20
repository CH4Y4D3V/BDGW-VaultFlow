from __future__ import annotations

from datetime import datetime, timezone
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
        now = datetime.now(timezone.utc)
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
        now = datetime.now(timezone.utc)
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
        now = datetime.now(timezone.utc)
        return await self.update_many(
            {
                "status": InviteStatus.ACTIVE.value,
                "expires_at": {"$lte": now, "$ne": None},
            },
            {"$set": {"status": InviteStatus.EXPIRED.value}},
        )

    async def revoke_all_active_for_user_chat(
        self, user_id: int, chat_id: int
    ) -> list[str]:
        """
        B-02 Step 3: Cancel all previously issued, unexpired ACTIVE invites
        for a given (user_id, chat_id) combination.

        FIX 10: Uses intended_user_id field instead of notes regex.
        """
        now = datetime.now(timezone.utc)

        # Find all qualifying documents first to collect their invite links
        cursor = self.collection.find(
            {
                "chat_id": chat_id,
                "status": InviteStatus.ACTIVE.value,
                "intended_user_id": user_id,
                "$or": [
                    {"expires_at": None},
                    {"expires_at": {"$gt": now}},
                ],
            }
        )
        docs = await cursor.to_list(length=None)
        if not docs:
            return []

        revoked_links: list[str] = [
            d["telegram_link"] for d in docs if d.get("telegram_link")
        ]
        token_list = [d["token"] for d in docs]

        # Bulk-revoke them atomically
        await self.collection.update_many(
            {
                "token": {"$in": token_list},
                "status": InviteStatus.ACTIVE.value,
            },
            {
                "$set": {
                    "status": InviteStatus.REVOKED.value,
                    "revoked_at": now,
                }
            },
        )

        return revoked_links

    async def get_active_invite_for_user_chat(
        self, user_id: int, chat_id: int
    ) -> Optional[Invite]:
        """
        B-02 Step 1 helper: find an ACTIVE invite intended for this specific
        user in this specific chat.

        FIX 10: Queries intended_user_id field (indexed) instead of notes regex.
        Returns the most recently created matching invite, or None.
        """
        now = datetime.now(timezone.utc)
        docs = await self.find_many(
            {
                "chat_id": chat_id,
                "status": InviteStatus.ACTIVE.value,
                "intended_user_id": user_id,
                "$or": [
                    {"expires_at": None},
                    {"expires_at": {"$gt": now}},
                ],
            },
            sort=[("created_at", -1)],
            limit=1,
        )
        return Invite.from_dict(docs[0]) if docs else None

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
