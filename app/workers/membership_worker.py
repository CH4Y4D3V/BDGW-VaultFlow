from __future__ import annotations

import asyncio
from datetime import datetime, timezone
from typing import Optional

from pyrogram.client import Client
from pyrogram.enums import ChatMemberStatus          # FIX D-03: was UserStatus (invalid)
from pyrogram.errors import FloodWait, UserNotParticipant

from app.config import settings
from app.core.database import DatabaseManager
from app.utils.logger import get_logger

logger = get_logger(__name__)

_RECONCILE_INTERVAL_SECONDS = 3600  # 1 hour


class MembershipReconciliationWorker:
    """
    Periodically checks that active premium subscribers are actually
    in the premium chats. Logs discrepancies.

    FIX D-03: Pyrogram uses ChatMemberStatus, not UserStatus.
              UserStatus.MEMBER does not exist and raises AttributeError.
    """

    def __init__(self) -> None:
        self._bot: Optional[Client] = None
        self._running = False
        self._task: Optional[asyncio.Task] = None

    async def start(self, bot: Client) -> None:
        if self._running:
            return
        self._bot = bot
        self._running = True
        self._task = asyncio.create_task(
            self._run_loop(), name="membership-reconciliation"
        )
        logger.info("Membership Reconciliation worker started")

    async def stop(self) -> None:
        self._running = False
        if self._task and not self._task.done():
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
        logger.info("Membership Reconciliation worker stopped")

    async def _run_loop(self) -> None:
        while self._running:
            try:
                await self.run_sweep()
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error("Membership reconciliation sweep error", exc_info=e)
            try:
                await asyncio.sleep(_RECONCILE_INTERVAL_SECONDS)
            except asyncio.CancelledError:
                break

    async def run_sweep(self) -> None:
        if not self._bot:
            return

        db = DatabaseManager.get_db()
        now = datetime.now(timezone.utc)
        logger.info(
            "Membership reconciliation sweep running",
            extra={"ctx_time": now.isoformat()},
        )

        active_subs = await db["subscriptions"].find({
            "status": "ACTIVE",
            "plan": {"$nin": ["free", "owner", "sudo"]},
        }).to_list(length=None)

        premium_chats = []
        if settings.PREMIUM_GROUP_ID:
            premium_chats.append(settings.PREMIUM_GROUP_ID)
        premium_channel = getattr(settings, "PREMIUM_CHANNEL_ID", None)
        if premium_channel and premium_channel != settings.PREMIUM_GROUP_ID:
            premium_chats.append(premium_channel)

        if not premium_chats:
            logger.warning("No premium chats configured for reconciliation.")
            return

        # Active member statuses in Pyrogram (FIX D-03)
        active_statuses = {
            ChatMemberStatus.MEMBER,
            ChatMemberStatus.OWNER,
            ChatMemberStatus.ADMINISTRATOR,
        }

        discrepancies = 0
        for sub in active_subs:
            user_id = sub["user_id"]
            for chat_id in premium_chats:
                try:
                    member = await self._bot.get_chat_member(chat_id, user_id)
                    # FIX D-03: compare against ChatMemberStatus, not UserStatus
                    if member.status not in active_statuses:
                        logger.warning(
                            "Membership discrepancy found!",
                            extra={
                                "ctx_user_id": user_id,
                                "ctx_chat_id": chat_id,
                                "ctx_expected_status": "member/admin/owner",
                                "ctx_actual_status": str(member.status),
                            },
                        )
                        discrepancies += 1
                except UserNotParticipant:
                    logger.warning(
                        "Membership discrepancy found!",
                        extra={
                            "ctx_user_id": user_id,
                            "ctx_chat_id": chat_id,
                            "ctx_expected_status": "member",
                            "ctx_actual_status": "not_in_chat",
                        },
                    )
                    discrepancies += 1
                except FloodWait as e:
                    await asyncio.sleep(e.value + 2)
                except Exception as e:
                    logger.error(
                        "Error checking chat member",
                        extra={
                            "ctx_user_id": user_id,
                            "ctx_chat_id": chat_id,
                            "ctx_error": str(e),
                        },
                    )

        if discrepancies > 0:
            logger.warning(
                "Membership reconciliation sweep finished with discrepancies.",
                extra={"ctx_discrepancy_count": discrepancies},
            )
        else:
            logger.info("Membership reconciliation sweep finished. No discrepancies found.")
