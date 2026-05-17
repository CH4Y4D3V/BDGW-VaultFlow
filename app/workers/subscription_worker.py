from __future__ import annotations

import asyncio
from datetime import datetime, timezone
from typing import Optional

from pyrogram.client import Client
from pyrogram.errors import (
    FloodWait,
    UserIsBlocked,
    InputUserDeactivated,
    PeerIdInvalid,
    RPCError,
    UserNotParticipant,
    ChatAdminRequired,
)

from app.config import settings
from app.services.subscription_service import SubscriptionService
from app.utils.logger import get_logger

logger = get_logger(__name__)

_SWEEP_INTERVAL_SECONDS = 300  # 5 minutes
_MAX_NOTIFY_RETRIES = 2


class SubscriptionWorker:
    """
    Background worker responsible for the subscription state machine.

    Sweep cycle (every 5 minutes):
      1. Active → Grace   : expires_at has passed
      2. Grace  → Expired : grace_until has passed → remove from chats
      3. Expiry DM notifications for both transitions

    Never raises out of the run loop — all errors are caught and logged.
    Restart-safe: if swept users were already transitioned, DB upsert is idempotent.
    """

    def __init__(self) -> None:
        self._service = SubscriptionService()
        self._bot: Optional[Client] = None
        self._running = False
        self._task: Optional[asyncio.Task] = None

    # ── Lifecycle ─────────────────────────────────────────────────────────────

    async def start(self, bot: Client) -> None:
        if self._running:
            return
        self._bot = bot
        self._running = True
        self._task = asyncio.create_task(self._run_loop(), name="subscription-worker")
        logger.info("Subscription worker started")

    async def stop(self) -> None:
        self._running = False
        if self._task and not self._task.done():
            self._task.cancel()
            try:
                await asyncio.wait_for(asyncio.shield(self._task), timeout=15.0)
            except (asyncio.TimeoutError, asyncio.CancelledError):
                pass
        logger.info("Subscription worker stopped")

    # ── Main loop ─────────────────────────────────────────────────────────────

    async def _run_loop(self) -> None:
        while self._running:
            try:
                await self._sweep()
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error("Subscription sweep unhandled error", exc_info=e)
            try:
                await asyncio.sleep(_SWEEP_INTERVAL_SECONDS)
            except asyncio.CancelledError:
                break

    # ── Sweep ─────────────────────────────────────────────────────────────────

    async def _sweep(self) -> None:
        now = datetime.now(timezone.utc)
        logger.debug("Subscription sweep running", extra={"ctx_time": now.isoformat()})

        # ── Step 1: Active → Grace ────────────────────────────────────────────
        newly_expired = await self._service.get_newly_expired()
        for sub in newly_expired:
            try:
                await self._service.set_grace(sub)
                await self._notify(
                    sub.user_id,
                    f"⚠️ <b>Your subscription has expired.</b>\n\n"
                    f"You have a grace period of <b>{settings.GRACE_PERIOD_DAYS} day(s)</b> "
                    f"to renew before your access is removed.\n\n"
                    f"Contact an admin to resubscribe.",
                )
                logger.info(
                    "Subscription → grace",
                    extra={"ctx_user_id": sub.user_id, "ctx_plan": sub.plan.value},
                )
            except Exception as e:
                logger.error(
                    "Failed to move sub to grace",
                    extra={"ctx_user_id": sub.user_id, "ctx_error": str(e)},
                )

        # ── Step 2: Grace → Fully Expired ────────────────────────────────────
        grace_expired = await self._service.get_grace_expired()
        for sub in grace_expired:
            try:
                await self._service.expire(sub)
                await self._notify(
                    sub.user_id,
                    "❌ <b>Your subscription has fully expired.</b>\n\n"
                    "Your access has been removed. To resubscribe, contact an admin.",
                )
                await self._remove_from_chats(sub.user_id)
                logger.info(
                    "Subscription fully expired",
                    extra={"ctx_user_id": sub.user_id, "ctx_plan": sub.plan.value},
                )
            except Exception as e:
                logger.error(
                    "Failed to process grace-expired sub",
                    extra={"ctx_user_id": sub.user_id, "ctx_error": str(e)},
                )

        if newly_expired or grace_expired:
            logger.info(
                "Subscription sweep complete",
                extra={
                    "ctx_moved_to_grace": len(newly_expired),
                    "ctx_fully_expired": len(grace_expired),
                },
            )

    # ── Telegram helpers ──────────────────────────────────────────────────────

    async def _notify(self, user_id: int, text: str) -> None:
        """Best-effort DM. Silently drops blocked/deactivated users."""
        if not self._bot:
            return
        for attempt in range(_MAX_NOTIFY_RETRIES):
            try:
                await self._bot.send_message(
                    chat_id=user_id,
                    text=text,
                    parse_mode="html",
                )
                return
            except FloodWait as e:
                await asyncio.sleep(int(e.value) + settings.FLOODWAIT_EXTRA_BUFFER)
            except (UserIsBlocked, InputUserDeactivated, PeerIdInvalid):
                return  # User unreachable — not an error
            except RPCError as e:
                if attempt == _MAX_NOTIFY_RETRIES - 1:
                    logger.debug(
                        "Could not notify user",
                        extra={"ctx_user_id": user_id, "ctx_error": str(e)},
                    )
                await asyncio.sleep(2 ** attempt)
            except Exception as e:
                logger.debug(
                    "Unexpected notify error",
                    extra={"ctx_user_id": user_id, "ctx_error": str(e)},
                )
                return

    async def _remove_from_chats(self, user_id: int) -> None:
        """
        Kick the user from managed destination chats.
        Immediately unbans so they can rejoin after resubscribing.
        Only acts if the bot is admin in the target chat.
        Silently ignores if user already left.
        """
        if not self._bot:
            return

        target_chats: list[int] = []
        if settings.NSFW_GROUP_ID:
            target_chats.append(settings.NSFW_GROUP_ID)
        if settings.PREMIUM_GROUP_ID:
            target_chats.append(settings.PREMIUM_GROUP_ID)

        for chat_id in target_chats:
            try:
                await self._bot.ban_chat_member(chat_id=chat_id, user_id=user_id)
                # Unban immediately so they can re-enter after resubscribing
                await self._bot.unban_chat_member(chat_id=chat_id, user_id=user_id)
                logger.info(
                    "Expired user removed from chat",
                    extra={"ctx_user_id": user_id, "ctx_chat_id": chat_id},
                )
            except ChatAdminRequired:
                logger.warning(
                    "Bot is not admin — cannot remove user",
                    extra={"ctx_user_id": user_id, "ctx_chat_id": chat_id},
                )
            except UserNotParticipant:
                pass  # Already not in chat — fine
            except FloodWait as e:
                await asyncio.sleep(int(e.value) + settings.FLOODWAIT_EXTRA_BUFFER)
            except Exception as e:
                logger.debug(
                    "Could not remove user from chat",
                    extra={"ctx_user_id": user_id, "ctx_chat_id": chat_id, "ctx_error": str(e)},
                )
