from __future__ import annotations

import asyncio
from datetime import datetime, timedelta, timezone
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
from app.core.database import DatabaseManager
from app.services.subscription_service import SubscriptionService
from app.utils.logger import get_logger

logger = get_logger(__name__)

_SWEEP_INTERVAL_SECONDS = 300   # Full sweep every 5 minutes
_REMINDER_INTERVAL_SECONDS = 3600  # Reminder check every hour
_MAX_NOTIFY_RETRIES = 2


class SubscriptionWorker:
    """
    Background worker for the subscription state machine.

    Sweep cycle (every 5 minutes):
      1. Active → Grace   : expires_at has passed
      2. Grace  → Expired : grace_until has passed → remove from chats

    Reminder cycle (every hour):
      3. Send 7-day expiry warning (once per subscription)
      4. Send 3-day expiry warning (once per subscription)
    """

    def __init__(self) -> None:
        self._service = SubscriptionService()
        self._bot: Optional[Client] = None
        self._running = False
        self._sweep_task: Optional[asyncio.Task] = None
        self._reminder_task: Optional[asyncio.Task] = None

    # ── Lifecycle ─────────────────────────────────────────────────────────────

    async def start(self, bot: Client) -> None:
        if self._running:
            return
        self._bot = bot
        self._running = True
        self._sweep_task = asyncio.create_task(
            self._run_sweep_loop(), name="subscription-sweep"
        )
        self._reminder_task = asyncio.create_task(
            self._run_reminder_loop(), name="subscription-reminders"
        )
        logger.info("Subscription worker started")

    async def stop(self) -> None:
        self._running = False
        for task in (self._sweep_task, self._reminder_task):
            if task and not task.done():
                task.cancel()
                try:
                    await task
                except asyncio.CancelledError:
                    pass
        logger.info("Subscription worker stopped")

    # ── Sweep loop (state machine) ────────────────────────────────────────────

    async def _run_sweep_loop(self) -> None:
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

    # ── Reminder loop ─────────────────────────────────────────────────────────

    async def _run_reminder_loop(self) -> None:
        while self._running:
            try:
                await self._sweep_reminders()
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error("Subscription reminder sweep error", exc_info=e)
            try:
                await asyncio.sleep(_REMINDER_INTERVAL_SECONDS)
            except asyncio.CancelledError:
                break

    async def _sweep_reminders(self) -> None:
        """
        Check for subscriptions expiring in ~7 days and ~3 days.
        Uses a ±12-hour window around each threshold to avoid missing subs.
        Tracks sent reminders directly on the subscription document via
        reminder_7d_sent / reminder_3d_sent boolean fields.
        These fields are NOT on the Subscription model — they are set directly
        in MongoDB so the model class doesn't need changing.
        """
        now = datetime.now(timezone.utc)
        db = DatabaseManager.get_db()
        col = db["subscriptions"]

        # ── 7-day reminder (window: now -> 7d) ─────────────────────────────
        min_7d = now
        max_7d = now + timedelta(days=7)

        subs_7d = await col.find({
            "status": "active",
            "expires_at": {"$gte": min_7d, "$lte": max_7d},
            "plan": {"$nin": ["free", "owner", "sudo"]},
            "reminder_7d_sent": {"$ne": True},
        }).to_list(length=None)

        for sub_doc in subs_7d:
            user_id = sub_doc["user_id"]
            expires_at = sub_doc.get("expires_at")
            days_left = (
                (expires_at - now).days
                if expires_at else 7
            )
            try:
                await self._notify(
                    user_id,
                    f"⏰ <b>Subscription expiring in {days_left} days</b>\n\n"
                    f"Your premium subscription expires on "
                    f"<b>{expires_at.strftime('%Y-%m-%d') if expires_at else 'soon'}</b>.\n\n"
                    "Renew early to avoid losing access. Contact an admin to resubscribe.",
                )
                await col.update_one(
                    {"user_id": user_id},
                    {"$set": {"reminder_7d_sent": True}},
                )
                logger.info(
                    "7-day expiry reminder sent",
                    extra={"ctx_user_id": user_id},
                )
            except Exception as e:
                logger.error(
                    "Failed to send 7-day reminder",
                    extra={"ctx_user_id": user_id, "ctx_error": str(e)},
                )

        # ── 3-day reminder (window: 2.5d → 3.5d) ─────────────────────────────
        min_3d = now + timedelta(days=2, hours=12)
        max_3d = now + timedelta(days=3, hours=12)

        subs_3d = await col.find({
            "status": "active",
            "expires_at": {"$gte": min_3d, "$lte": max_3d},
            "plan": {"$nin": ["free", "owner", "sudo"]},
            "reminder_3d_sent": {"$ne": True},
        }).to_list(length=None)

        # ── 3-day reminder #2 (window: -0.5d → +0.5d) ────────────────────────
        # RC-11 fix: second window for the 3-day reminder to catch late-entries.
        subs_3d_v2 = await col.find({
            "status": "active",
            "expires_at": {"$gte": now - timedelta(hours=12), "$lte": now + timedelta(hours=12)},
            "plan": {"$nin": ["free", "owner", "sudo"]},
            "reminder_3d_sent": {"$ne": True},
        }).to_list(length=None)
        
        subs_3d.extend(subs_3d_v2)

        for sub_doc in subs_3d:
            user_id = sub_doc["user_id"]
            expires_at = sub_doc.get("expires_at")
            days_left = (
                (expires_at - now).days
                if expires_at else 3
            )
            try:
                await self._notify(
                    user_id,
                    f"⚠️ <b>Subscription expiring in {days_left} days!</b>\n\n"
                    f"Your premium access expires on "
                    f"<b>{expires_at.strftime('%Y-%m-%d') if expires_at else 'soon'}</b>.\n\n"
                    "Renew NOW to avoid being removed from premium channels.",
                )
                await col.update_one(
                    {"user_id": user_id},
                    {"$set": {"reminder_3d_sent": True}},
                )
                logger.info(
                    "3-day expiry reminder sent",
                    extra={"ctx_user_id": user_id},
                )
            except Exception as e:
                logger.error(
                    "Failed to send 3-day reminder",
                    extra={"ctx_user_id": user_id, "ctx_error": str(e)},
                )

        total = len(subs_7d) + len(subs_3d)
        if total:
            logger.info(
                "Expiry reminders sent",
                extra={"ctx_7d": len(subs_7d), "ctx_3d": len(subs_3d)},
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
        Kick the user from all managed destination chats.
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
                pass  # Already not in chat
            except FloodWait as e:
                await asyncio.sleep(int(e.value) + settings.FLOODWAIT_EXTRA_BUFFER)
            except Exception as e:
                logger.debug(
                    "Could not remove user from chat",
                    extra={"ctx_user_id": user_id, "ctx_chat_id": chat_id, "ctx_error": str(e)},
                )
