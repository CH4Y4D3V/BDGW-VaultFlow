from __future__ import annotations

from datetime import datetime, timezone
from typing import Optional

from pyrogram.client import Client
from pyrogram.errors import (
    FloodWait,
    UserNotParticipant,
    ChatAdminRequired,
    RPCError,
    PeerIdInvalid,
)
from pyrogram.enums import ChatMemberStatus

from app.config import settings
from app.models.membership import ChatType, Membership, MembershipStatus
from app.repositories.membership_repository import MembershipRepository
from app.repositories.activity_repository import ActivityRepository
from app.models.activity import Activity, ActivityAction
from app.utils.logger import get_logger

logger = get_logger(__name__)


class MembershipService:
    """
    Manages membership state across all tracked chats.

    Two layers:
      - Telegram layer  : ban/unban/kick via Pyrogram client
      - Database layer  : MembershipRepository for persistent state

    All Telegram calls accept an optional client parameter.
    Methods without a client parameter are pure DB operations.
    """

    def __init__(self) -> None:
        self._repo = MembershipRepository()
        self._activity = ActivityRepository()

    # ── Chat type resolution ──────────────────────────────────────────────────

    @staticmethod
    def resolve_chat_type(chat_id: int) -> ChatType:
        if settings.NSFW_GROUP_ID and chat_id == settings.NSFW_GROUP_ID:
            return ChatType.NSFW
        if settings.PREMIUM_GROUP_ID and chat_id == settings.PREMIUM_GROUP_ID:
            return ChatType.PREMIUM
        return ChatType.PUBLIC

    @staticmethod
    def get_managed_chat_ids() -> list[int]:
        chats = []
        if settings.NSFW_GROUP_ID:
            chats.append(settings.NSFW_GROUP_ID)
        if settings.PREMIUM_GROUP_ID:
            chats.append(settings.PREMIUM_GROUP_ID)
        return chats

    # ── Read ──────────────────────────────────────────────────────────────────

    async def get_membership(self, user_id: int, chat_id: int) -> Optional[Membership]:
        return await self._repo.get(user_id, chat_id)

    async def get_user_memberships(
        self,
        user_id: int,
        status: Optional[MembershipStatus] = None,
    ) -> list[Membership]:
        return await self._repo.get_user_memberships(user_id, status)

    async def get_active_chat_members(self, chat_id: int) -> list[Membership]:
        return await self._repo.get_chat_members(chat_id, MembershipStatus.ACTIVE)

    async def is_active_member(self, user_id: int, chat_id: int) -> bool:
        m = await self._repo.get(user_id, chat_id)
        return m is not None and m.is_active

    # ── Telegram membership verification ──────────────────────────────────────

    async def verify_membership(
        self,
        client: Client,
        user_id: int,
        chat_id: int,
    ) -> bool:
        """
        Check Telegram directly (not just the DB) whether the user is still in the chat.
        Updates the DB record to match reality.
        """
        try:
            member = await client.get_chat_member(chat_id=chat_id, user_id=user_id)
            active = member.status not in (
                ChatMemberStatus.LEFT,
                ChatMemberStatus.BANNED,
            )
            if active:
                await self._repo.update_last_verified(user_id, chat_id)
            else:
                await self._repo.update_status(user_id, chat_id, MembershipStatus.REMOVED)
            return active

        except UserNotParticipant:
            await self._repo.update_status(user_id, chat_id, MembershipStatus.REMOVED)
            return False
        except (PeerIdInvalid, RPCError) as e:
            logger.warning(
                "Could not verify membership via Telegram",
                extra={"ctx_user_id": user_id, "ctx_chat_id": chat_id, "ctx_error": str(e)},
            )
            return False

    # ── Record join / leave ───────────────────────────────────────────────────

    async def record_join(self, user_id: int, chat_id: int) -> None:
        """Called from membership_handler when a user joins."""
        now = datetime.now(timezone.utc)
        chat_type = self.resolve_chat_type(chat_id)
        membership = Membership(
            user_id=user_id,
            chat_id=chat_id,
            chat_type=chat_type,
            status=MembershipStatus.ACTIVE,
            joined_at=now,
            last_verified=now,
        )
        await self._repo.upsert(membership)
        await self._activity.log(Activity(
            user_id=user_id,
            action=ActivityAction.JOIN,
            timestamp=now,
            chat_id=chat_id,
        ))
        logger.info("Membership recorded: join", extra={"ctx_user_id": user_id, "ctx_chat": chat_id})

    async def record_leave(self, user_id: int, chat_id: int, reason: str = "left") -> None:
        """Called from membership_handler when a user leaves or is kicked."""
        now = datetime.now(timezone.utc)
        status = MembershipStatus.KICKED if reason == "kicked" else MembershipStatus.REMOVED
        await self._repo.update_status(user_id, chat_id, status, reason=reason)
        action = ActivityAction.KICK if reason == "kicked" else ActivityAction.LEAVE
        await self._activity.log(Activity(
            user_id=user_id,
            action=action,
            timestamp=now,
            chat_id=chat_id,
        ))

    # ── Admin moderation actions (require client) ─────────────────────────────

    async def kick_user(
        self,
        client: Client,
        user_id: int,
        chat_id: int,
        performed_by: int,
        reason: str = "admin action",
    ) -> bool:
        """
        Kick (ban + immediate unban) a user from a chat.
        Allows re-entry after the kick.
        """
        try:
            await client.ban_chat_member(chat_id=chat_id, user_id=user_id)
            await client.unban_chat_member(chat_id=chat_id, user_id=user_id)
            await self._repo.update_status(
                user_id, chat_id, MembershipStatus.REMOVED, reason=reason
            )
            await self._activity.log(Activity(
                user_id=user_id,
                action=ActivityAction.KICK,
                timestamp=datetime.now(timezone.utc),
                chat_id=chat_id,
                performed_by=performed_by,
                metadata={"reason": reason},
            ))
            logger.info(
                "User kicked",
                extra={
                    "ctx_user_id": user_id,
                    "ctx_chat_id": chat_id,
                    "ctx_performed_by": performed_by,
                },
            )
            return True
        except ChatAdminRequired:
            logger.warning("Bot not admin — cannot kick", extra={"ctx_chat_id": chat_id})
            return False
        except FloodWait as e:
            import asyncio
            await asyncio.sleep(int(e.value) + settings.FLOODWAIT_EXTRA_BUFFER)
            return False
        except Exception as e:
            logger.error("Kick failed", extra={"ctx_user_id": user_id, "ctx_error": str(e)})
            return False

    async def ban_user(
        self,
        client: Client,
        user_id: int,
        chat_id: int,
        performed_by: int,
        reason: str = "banned",
    ) -> bool:
        """Permanently ban a user. No unban — must be manually reversed."""
        try:
            await client.ban_chat_member(chat_id=chat_id, user_id=user_id)
            await self._repo.update_status(
                user_id, chat_id, MembershipStatus.KICKED, reason=reason
            )
            await self._activity.log(Activity(
                user_id=user_id,
                action=ActivityAction.BAN,
                timestamp=datetime.now(timezone.utc),
                chat_id=chat_id,
                performed_by=performed_by,
                metadata={"reason": reason},
            ))
            logger.info(
                "User banned",
                extra={
                    "ctx_user_id": user_id,
                    "ctx_chat_id": chat_id,
                    "ctx_performed_by": performed_by,
                },
            )
            return True
        except ChatAdminRequired:
            logger.warning("Bot not admin — cannot ban", extra={"ctx_chat_id": chat_id})
            return False
        except Exception as e:
            logger.error("Ban failed", extra={"ctx_user_id": user_id, "ctx_error": str(e)})
            return False

    async def unban_user(
        self,
        client: Client,
        user_id: int,
        chat_id: int,
        performed_by: int,
    ) -> bool:
        """Remove a ban so the user can rejoin."""
        try:
            await client.unban_chat_member(chat_id=chat_id, user_id=user_id)
            await self._repo.update_status(user_id, chat_id, MembershipStatus.REMOVED)
            await self._activity.log(Activity(
                user_id=user_id,
                action=ActivityAction.UNBAN,
                timestamp=datetime.now(timezone.utc),
                chat_id=chat_id,
                performed_by=performed_by,
            ))
            logger.info(
                "User unbanned",
                extra={"ctx_user_id": user_id, "ctx_chat_id": chat_id},
            )
            return True
        except Exception as e:
            logger.error("Unban failed", extra={"ctx_user_id": user_id, "ctx_error": str(e)})
            return False

    async def remove_from_all_managed_chats(
        self,
        client: Client,
        user_id: int,
        performed_by: int,
        reason: str = "subscription_expired",
    ) -> dict[int, bool]:
        """
        Kick the user from all managed destination chats.
        Returns {chat_id: success} mapping.
        """
        results: dict[int, bool] = {}
        for chat_id in self.get_managed_chat_ids():
            results[chat_id] = await self.kick_user(
                client, user_id, chat_id, performed_by, reason
            )
        return results

    # ── Bulk operations ───────────────────────────────────────────────────────

    async def get_stale_members(
        self,
        chat_id: int,
        before: datetime,
    ) -> list[Membership]:
        return await self._repo.get_stale_memberships(chat_id, before)

    async def count_active_members(self, chat_id: int) -> int:
        members = await self._repo.get_chat_members(chat_id, MembershipStatus.ACTIVE)
        return len(members)
