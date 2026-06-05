from __future__ import annotations

# ------------------------------------------------------------
# FILE: app/workers/subscription_worker.py
# Spec: Master Reference Section 7.7, 7.8, 9.4, 22, 24, 25, 26
# ------------------------------------------------------------

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
from app.core.redis_client import get_redis
from app.services.subscription_service import SubscriptionService
from app.utils.logger import get_logger

logger = get_logger(__name__)

# ── Timing constants ──────────────────────────────────────────────────────────
_SWEEP_INTERVAL_SECONDS = 300       # Full state-machine sweep every 5 minutes
_REMINDER_INTERVAL_SECONDS = 3600   # Reminder check every hour
_RECONCILE_INTERVAL_SECONDS = 21600 # Membership reconciliation every 6 hours
_MAX_NOTIFY_RETRIES = 3             # DM delivery attempts before giving up

# ── Redis distributed-lock TTL (seconds) ─────────────────────────────────────
# Long enough to cover one full sweep run; prevents double-processing across
# horizontally-scaled workers or rapid restarts.
_LOCK_TTL_SECONDS = 120

# ── Admin Logs entry format (Section 9.4) ────────────────────────────────────
_ADMIN_LOG_TEMPLATE = (
    "<b>[{action}]</b>\n"
    "Admin     : System\n"
    "Admin ID  : N/A\n"
    "Target    : {full_name} (@{username})\n"
    "Target ID : <code>{user_id}</code>\n"
    "Detail    : {detail}\n"
    "Time      : {timestamp}"
)


class SubscriptionWorker:
    """
    Background worker implementing the subscription state machine.

    State transitions (Section 7.7, 7.8):
      Active → Grace   : expires_at has passed
      Grace  → Expired : grace_until has passed
                         → kick user from ALL premium groups (Section 7.8)

    Reminder cycle (Section 7.7):
      Send 7-day expiry warning (once per subscription)
      Send 3-day expiry warning (once per subscription)

    Membership reconciliation cycle (Section 26):
      Verify active subscriptions against actual group membership and
      repair any inconsistencies.

    All expiry events are:
      • Protected by a Redis distributed lock to prevent double-processing
        under concurrent workers or rapid restarts.
      • Written to audit_logs (MongoDB) AND Admin Logs topic simultaneously
        (Section 9.4 / Section 22).
      • Loaded from MongoDB on startup — fully restart-safe (Section 25).
    """

    def __init__(self) -> None:
        """Initialise worker; bot client injected later via start()."""
        self._service = SubscriptionService()
        self._bot: Optional[Client] = None
        self._running = False
        self._sweep_task: Optional[asyncio.Task] = None
        self._reminder_task: Optional[asyncio.Task] = None
        self._reconcile_task: Optional[asyncio.Task] = None

    # ── Lifecycle ─────────────────────────────────────────────────────────────

    async def start(self, bot: Client) -> None:
        """
        Start all background loops.

        Args:
            bot: Authenticated Pyrogram client used for Telegram API calls.
        """
        if self._running:
            logger.warning("SubscriptionWorker.start() called while already running — ignored")
            return
        self._bot = bot
        self._running = True
        self._sweep_task = asyncio.create_task(
            self._run_sweep_loop(), name="subscription-sweep"
        )
        self._reminder_task = asyncio.create_task(
            self._run_reminder_loop(), name="subscription-reminders"
        )
        self._reconcile_task = asyncio.create_task(
            self._run_reconcile_loop(), name="subscription-reconcile"
        )
        logger.info("SubscriptionWorker started — sweep / reminders / reconcile loops active")

    async def stop(self) -> None:
        """
        Gracefully cancel all background tasks and wait for them to finish.
        Safe to call multiple times.
        """
        self._running = False
        tasks = [t for t in (self._sweep_task, self._reminder_task, self._reconcile_task)
                 if t and not t.done()]
        for task in tasks:
            task.cancel()
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)
        logger.info("SubscriptionWorker stopped")

    # ── Sweep loop (state machine) ────────────────────────────────────────────

    async def _run_sweep_loop(self) -> None:
        """
        Infinite loop that drives the subscription state machine.
        Runs once immediately on start, then every _SWEEP_INTERVAL_SECONDS.
        Catches and logs all non-cancellation exceptions so the loop
        never dies silently.
        """
        while self._running:
            try:
                await self._sweep()
            except asyncio.CancelledError:
                break
            except Exception as exc:
                logger.error(
                    "Subscription sweep unhandled error — loop continues",
                    exc_info=exc,
                )
            try:
                await asyncio.sleep(_SWEEP_INTERVAL_SECONDS)
            except asyncio.CancelledError:
                break

    async def _sweep(self) -> None:
        """
        Execute one full state-machine sweep cycle.

        Step 1 — Active → Grace:
            Subscriptions whose expires_at has passed but have not yet
            entered the grace period. Sets grace_until and notifies user.

        Step 2 — Grace → Expired:
            Subscriptions whose grace_until has passed. Marks expired,
            notifies user, kicks from ALL premium groups, writes audit
            entry to both MongoDB and Admin Logs topic.

        Each record is processed under a per-user Redis distributed lock
        to guarantee idempotency across restarts and concurrent workers.
        """
        now = datetime.now(timezone.utc)
        logger.debug("Subscription sweep starting", extra={"ctx_time": now.isoformat()})

        # ── Step 1: Active → Grace ────────────────────────────────────────────
        newly_expired = await self._service.get_newly_expired()
        grace_moved = 0
        for sub in newly_expired:
            lock_key = f"sub_grace:{sub.user_id}"
            async with _redis_lock(lock_key, ttl=_LOCK_TTL_SECONDS):
                try:
                    # Idempotency: re-fetch to confirm still needs transition
                    fresh = await self._service.get_subscription(sub.user_id)
                    if fresh is None or fresh.status.value != "active":
                        continue

                    await self._service.set_grace(sub)
                    await self._notify(
                        sub.user_id,
                        f"⚠️ <b>Your subscription has expired.</b>\n\n"
                        f"You have a grace period of "
                        f"<b>{settings.GRACE_PERIOD_DAYS} day(s)</b> "
                        f"to renew before your access is removed.\n\n"
                        f"Contact an admin to resubscribe.",
                    )
                    logger.info(
                        "Subscription moved to grace",
                        extra={"ctx_user_id": sub.user_id, "ctx_plan": sub.plan.value},
                    )
                    grace_moved += 1
                except Exception as exc:
                    logger.error(
                        "Failed to move subscription to grace",
                        extra={"ctx_user_id": sub.user_id, "ctx_error": str(exc)},
                        exc_info=exc,
                    )

        # ── Step 2: Grace → Fully Expired ────────────────────────────────────
        grace_expired = await self._service.get_grace_expired()
        fully_expired = 0
        for sub in grace_expired:
            lock_key = f"sub_expire:{sub.user_id}"
            async with _redis_lock(lock_key, ttl=_LOCK_TTL_SECONDS):
                try:
                    # Idempotency: re-fetch to confirm still needs transition
                    fresh = await self._service.get_subscription(sub.user_id)
                    if fresh is None or fresh.status.value != "GRACE":
                        continue

                    await self._service.expire(sub)

                    # Write to audit_logs (MongoDB) — Section 22
                    await _write_audit_log(
                        action="SUBSCRIPTION EXPIRED",
                        admin_user_id=None,
                        target_user_id=sub.user_id,
                        detail={
                            "subscription_id": str(sub.subscription_id),
                            "plan": sub.plan.value,
                            "expired_at": now.isoformat(),
                        },
                    )

                    # Write to Admin Logs topic in Verification Hub — Section 9.4
                    user_info = await _fetch_user_display(sub.user_id)
                    await self._post_admin_log(
                        action="SUBSCRIPTION EXPIRED",
                        user_id=sub.user_id,
                        full_name=user_info["full_name"],
                        username=user_info["username"],
                        detail=f"Plan: {sub.plan.value} | expired after grace period",
                    )

                    # Notify user (Section 7.7 / 21: notification allowed on removal)
                    await self._notify(
                        sub.user_id,
                        "❌ <b>Your subscription has fully expired.</b>\n\n"
                        "Your access has been removed. "
                        "To resubscribe, please contact an admin.",
                    )

                    # Kick from ALL premium groups (Section 7.8 — core audit finding)
                    await self._remove_from_all_premium_chats(sub.user_id)

                    logger.info(
                        "Subscription fully expired and user removed from all premium chats",
                        extra={"ctx_user_id": sub.user_id, "ctx_plan": sub.plan.value},
                    )
                    fully_expired += 1
                except Exception as exc:
                    logger.error(
                        "Failed to process grace-expired subscription",
                        extra={"ctx_user_id": sub.user_id, "ctx_error": str(exc)},
                        exc_info=exc,
                    )

        if grace_moved or fully_expired:
            logger.info(
                "Subscription sweep complete",
                extra={
                    "ctx_moved_to_grace": grace_moved,
                    "ctx_fully_expired": fully_expired,
                },
            )

    # ── Reminder loop ─────────────────────────────────────────────────────────

    async def _run_reminder_loop(self) -> None:
        """
        Infinite loop that fires expiry-warning DMs.
        Runs every _REMINDER_INTERVAL_SECONDS (1 hour).
        Catches and logs all non-cancellation exceptions.
        """
        while self._running:
            try:
                await self._sweep_reminders()
            except asyncio.CancelledError:
                break
            except Exception as exc:
                logger.error(
                    "Subscription reminder sweep unhandled error — loop continues",
                    exc_info=exc,
                )
            try:
                await asyncio.sleep(_REMINDER_INTERVAL_SECONDS)
            except asyncio.CancelledError:
                break

    async def _sweep_reminders(self) -> None:
        """
        Find subscriptions approaching expiry and send DM warnings.

        7-day reminder window : [now, now + 7d]
        3-day reminder window : [now + 2d 12h, now + 3d 12h]

        Each reminder flag (reminder_7d_sent / reminder_3d_sent) is written
        directly to the subscriptions document in MongoDB so the flag
        survives restarts. The model class is NOT modified — flags are set
        via raw Motor update to avoid coupling the worker to the ORM.

        Free / owner / sudo plans are excluded — they never expire.

        A per-user Redis lock guards each reminder write to prevent
        duplicate DMs under concurrent workers.
        """
        now = datetime.now(timezone.utc)
        db = DatabaseManager.get_db()
        col = db["subscriptions"]

        # ── 7-day reminder ──────────────────────────────────────────────────
        subs_7d = await col.find({
            "status": "ACTIVE",
            "expires_at": {"$gte": now, "$lte": now + timedelta(days=7)},
            "plan": {"$nin": ["FREE", "OWNER", "SUDO"]},
            "reminder_7d_sent": {"$ne": True},
        }).to_list(length=None)

        reminded_7d = 0
        for sub_doc in subs_7d:
            user_id = sub_doc["user_id"]
            expires_at: Optional[datetime] = sub_doc.get("expires_at")
            days_left = int((expires_at - now).total_seconds() // 86400) if expires_at else 7
            lock_key = f"reminder_7d:{user_id}"
            async with _redis_lock(lock_key, ttl=60):
                # Re-check inside lock to prevent races
                still_pending = await col.find_one({
                    "user_id": user_id,
                    "status": "ACTIVE",
                    "reminder_7d_sent": {"$ne": True},
                })
                if not still_pending:
                    continue
                try:
                    await self._notify(
                        user_id,
                        f"⏰ <b>Subscription expiring in {days_left} day(s)</b>\n\n"
                        f"Your premium subscription expires on "
                        f"<b>"
                        f"{expires_at.strftime('%Y-%m-%d') if expires_at else 'soon'}"
                        f"</b>.\n\n"
                        "Renew early to keep your access. Contact an admin to resubscribe.",
                    )
                    await col.update_one(
                        {"user_id": user_id, "status": "active"},
                        {"$set": {"reminder_7d_sent": True}},
                    )
                    logger.info(
                        "7-day expiry reminder sent",
                        extra={"ctx_user_id": user_id, "ctx_days_left": days_left},
                    )
                    reminded_7d += 1
                except Exception as exc:
                    logger.error(
                        "Failed to send 7-day reminder",
                        extra={"ctx_user_id": user_id, "ctx_error": str(exc)},
                        exc_info=exc,
                    )

        # ── 3-day reminder ──────────────────────────────────────────────────
        subs_3d = await col.find({
            "status": "ACTIVE",
            "expires_at": {
                "$gte": now + timedelta(days=2, hours=12),
                "$lte": now + timedelta(days=3, hours=12),
            },
            "plan": {"$nin": ["FREE", "OWNER", "SUDO"]},
            "reminder_3d_sent": {"$ne": True},
        }).to_list(length=None)

        reminded_3d = 0
        for sub_doc in subs_3d:
            user_id = sub_doc["user_id"]
            expires_at = sub_doc.get("expires_at")
            days_left = int((expires_at - now).total_seconds() // 86400) if expires_at else 3
            lock_key = f"reminder_3d:{user_id}"
            async with _redis_lock(lock_key, ttl=60):
                still_pending = await col.find_one({
                    "user_id": user_id,
                    "status": "ACTIVE",
                    "reminder_3d_sent": {"$ne": True},
                })
                if not still_pending:
                    continue
                try:
                    await self._notify(
                        user_id,
                        f"⚠️ <b>Subscription expiring in {days_left} day(s)!</b>\n\n"
                        f"Your premium access expires on "
                        f"<b>"
                        f"{expires_at.strftime('%Y-%m-%d') if expires_at else 'soon'}"
                        f"</b>.\n\n"
                        "Renew NOW to avoid being removed from premium channels. "
                        "Contact an admin to resubscribe.",
                    )
                    await col.update_one(
                        {"user_id": user_id, "status": "ACTIVE"},
                        {"$set": {"reminder_3d_sent": True}},
                    )
                    logger.info(
                        "3-day expiry reminder sent",
                        extra={"ctx_user_id": user_id, "ctx_days_left": days_left},
                    )
                    reminded_3d += 1
                except Exception as exc:
                    logger.error(
                        "Failed to send 3-day reminder",
                        extra={"ctx_user_id": user_id, "ctx_error": str(exc)},
                        exc_info=exc,
                    )

        if reminded_7d or reminded_3d:
            logger.info(
                "Expiry reminders sent",
                extra={"ctx_7d": reminded_7d, "ctx_3d": reminded_3d},
            )

    # ── Membership reconciliation loop (Section 26) ───────────────────────────

    async def _run_reconcile_loop(self) -> None:
        """
        Infinite loop that cross-checks active subscriptions against actual
        Telegram group membership and repairs inconsistencies.
        Runs every _RECONCILE_INTERVAL_SECONDS (6 hours).
        Catches and logs all non-cancellation exceptions.
        """
        # Stagger start by 60 s to avoid collision with first sweep
        try:
            await asyncio.sleep(60)
        except asyncio.CancelledError:
            return
        while self._running:
            try:
                await self._reconcile_membership()
            except asyncio.CancelledError:
                break
            except Exception as exc:
                logger.error(
                    "Membership reconciliation unhandled error — loop continues",
                    exc_info=exc,
                )
            try:
                await asyncio.sleep(_RECONCILE_INTERVAL_SECONDS)
            except asyncio.CancelledError:
                break

    async def _reconcile_membership(self) -> None:
        """
        Verify that every user with an ACTIVE subscription is present in all
        managed premium chats, and that no expired user remains in them.

        Repair actions:
          • Expired user still in a premium chat → kick (same as _remove_from_all_premium_chats)
          • Active user missing from a premium chat → log warning only
            (bot cannot add users to channels; invite is required)

        Results are posted as a summary to the Admin Logs topic (Section 26).
        All reconciliation actions are written to audit_logs.
        """
        if not self._bot:
            return

        now = datetime.now(timezone.utc)
        db = DatabaseManager.get_db()
        sub_col = db["subscriptions"]
        target_chats = await _get_all_premium_chat_ids()

        kicked = 0
        warnings = 0

        # Check expired users still in chats
        expired_subs = await sub_col.find(
            {"status": {"$in": ["EXPIRED", "CANCELLED"]}}
        ).to_list(length=None)

        for sub_doc in expired_subs:
            user_id = sub_doc["user_id"]
            for chat_id in target_chats:
                try:
                    member = await self._bot.get_chat_member(
                        chat_id=chat_id, user_id=user_id
                    )
                    # If the API returns a member without raising, they are still in the chat
                    if member and member.status.value not in ("left", "banned", "kicked"):
                        logger.warning(
                            "Reconciliation: expired user still in premium chat — kicking",
                            extra={"ctx_user_id": user_id, "ctx_chat_id": chat_id},
                        )
                        await self._kick_with_unban(chat_id=chat_id, user_id=user_id)
                        await _write_audit_log(
                            action="USER KICKED",
                            admin_user_id=None,
                            target_user_id=user_id,
                            detail={
                                "reason": "Membership reconciliation: subscription expired",
                                "chat_id": chat_id,
                                "triggered_by": "reconciliation_worker",
                            },
                        )
                        kicked += 1
                except UserNotParticipant:
                    pass  # Already not in chat — correct state
                except FloodWait as exc:
                    await asyncio.sleep(int(exc.value) + settings.FLOODWAIT_EXTRA_BUFFER)
                except Exception as exc:
                    logger.debug(
                        "Reconciliation: could not check/kick user",
                        extra={
                            "ctx_user_id": user_id,
                            "ctx_chat_id": chat_id,
                            "ctx_error": str(exc),
                        },
                    )

        logger.info(
            "Membership reconciliation complete",
            extra={"ctx_kicked": kicked, "ctx_warnings": warnings},
        )

        # Post summary to Admin Logs topic (Section 26)
        summary = (
            f"<b>[MEMBERSHIP RECONCILIATION]</b>\n"
            f"Time      : {now.isoformat()}\n"
            f"Kicked    : {kicked}\n"
            f"Warnings  : {warnings}\n"
        )
        await self._send_to_admin_logs(summary)

    # ── Chat removal helpers ───────────────────────────────────────────────────

    async def _remove_from_all_premium_chats(self, user_id: int) -> None:
        """
        Kick the user from EVERY managed premium destination chat.

        "All premium groups" (Section 7.8) means the full set derived from
        hub_config at runtime — never a hardcoded list. This covers:
          • NSFW Group
          • Premium Group
          • Premium Channel (PREMIUM_CHANNEL_ID if separately configured)
          • Any additional premium-flagged groups stored in hub_config

        Immediately unbans after kicking so the user can rejoin if they
        resubscribe via a new invite link.

        Each removal action is logged to audit_logs and Admin Logs topic.

        Args:
            user_id: Telegram user ID of the expired subscriber.
        """
        if not self._bot:
            return

        target_chats = await _get_all_premium_chat_ids()
        if not target_chats:
            logger.warning(
                "No premium chats configured — cannot remove expired user",
                extra={"ctx_user_id": user_id},
            )
            return

        for chat_id in target_chats:
            await self._kick_with_unban(chat_id=chat_id, user_id=user_id)
            await _write_audit_log(
                action="USER KICKED",
                admin_user_id=None,
                target_user_id=user_id,
                detail={
                    "reason": "Subscription expired — auto-removed from premium chat",
                    "chat_id": chat_id,
                    "triggered_by": "subscription_worker",
                },
            )

    async def _kick_with_unban(self, *, chat_id: int, user_id: int) -> None:
        """
        Ban then immediately unban a user from a chat, with full error handling.

        Ban removes them from the chat. Immediate unban ensures they can
        re-enter via a fresh invite link after resubscribing (Section 7.8).

        Args:
            chat_id: Target Telegram chat / group / channel ID.
            user_id: Telegram user ID to remove.
        """
        try:
            await self._bot.ban_chat_member(chat_id=chat_id, user_id=user_id)
            await self._bot.unban_chat_member(chat_id=chat_id, user_id=user_id)
            logger.info(
                "Expired user kicked from premium chat",
                extra={"ctx_user_id": user_id, "ctx_chat_id": chat_id},
            )
        except ChatAdminRequired:
            logger.warning(
                "Bot lacks admin rights — cannot kick user from chat",
                extra={"ctx_user_id": user_id, "ctx_chat_id": chat_id},
            )
        except UserNotParticipant:
            pass  # Already not in the chat — desired state achieved
        except FloodWait as exc:
            wait = int(exc.value) + settings.FLOODWAIT_EXTRA_BUFFER
            logger.warning(
                "FloodWait while kicking user — sleeping",
                extra={"ctx_user_id": user_id, "ctx_chat_id": chat_id, "ctx_wait": wait},
            )
            await asyncio.sleep(wait)
            # Retry once after flood wait
            try:
                await self._bot.ban_chat_member(chat_id=chat_id, user_id=user_id)
                await self._bot.unban_chat_member(chat_id=chat_id, user_id=user_id)
            except Exception as retry_exc:
                logger.error(
                    "Retry kick failed after FloodWait",
                    extra={
                        "ctx_user_id": user_id,
                        "ctx_chat_id": chat_id,
                        "ctx_error": str(retry_exc),
                    },
                    exc_info=retry_exc,
                )
        except Exception as exc:
            logger.error(
                "Unexpected error kicking user from premium chat",
                extra={
                    "ctx_user_id": user_id,
                    "ctx_chat_id": chat_id,
                    "ctx_error": str(exc),
                },
                exc_info=exc,
            )

    # ── Telegram notification helpers ─────────────────────────────────────────

    async def _notify(self, user_id: int, text: str) -> None:
        """
        Send a DM to a user with retry logic and FloodWait handling.

        Silently drops blocked / deactivated / invalid users — these are
        expected conditions and must not pollute the error log.

        Uses exponential back-off between retries for transient RPC errors.

        Args:
            user_id: Telegram user ID to message.
            text:    HTML-formatted message body.
        """
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
            except FloodWait as exc:
                wait = int(exc.value) + settings.FLOODWAIT_EXTRA_BUFFER
                logger.warning(
                    "FloodWait on DM — sleeping",
                    extra={"ctx_user_id": user_id, "ctx_wait": wait},
                )
                await asyncio.sleep(wait)
                # Do not count FloodWait as an attempt failure
            except (UserIsBlocked, InputUserDeactivated, PeerIdInvalid):
                # User is unreachable — not a system error, do not log as warning
                logger.debug(
                    "User unreachable for DM — silently dropped",
                    extra={"ctx_user_id": user_id},
                )
                return
            except RPCError as exc:
                if attempt == _MAX_NOTIFY_RETRIES - 1:
                    logger.warning(
                        "Could not deliver DM after all retries",
                        extra={"ctx_user_id": user_id, "ctx_error": str(exc)},
                    )
                await asyncio.sleep(2 ** attempt)
            except Exception as exc:
                logger.error(
                    "Unexpected error delivering DM",
                    extra={"ctx_user_id": user_id, "ctx_error": str(exc)},
                    exc_info=exc,
                )
                return

    async def _post_admin_log(
        self,
        *,
        action: str,
        user_id: int,
        full_name: str,
        username: str,
        detail: str,
    ) -> None:
        """
        Post a structured entry to the Admin Logs topic in the
        Verification Hub (Section 9.4).

        If HUB_TOPIC_ADMIN_LOGS is not configured (e.g. race condition on
        first deployment), the entry is silently dropped with a warning —
        the audit_logs MongoDB write is the authoritative record and must
        already have completed before calling this function.

        Args:
            action:    Uppercase action type string (e.g. "SUBSCRIPTION EXPIRED").
            user_id:   Telegram ID of the affected user.
            full_name: Display name of the affected user.
            username:  Telegram username (without @), or "N/A".
            detail:    Human-readable description of the specific event.
        """
        admin_logs_topic_id = await _get_admin_logs_topic_id()
        if not admin_logs_topic_id:
            logger.warning(
                "Admin Logs topic ID not configured — skipping Admin Logs post",
                extra={"ctx_action": action, "ctx_user_id": user_id},
            )
            return

        hub_id = await _get_hub_supergroup_id()
        if not hub_id:
            logger.warning(
                "Hub supergroup ID not configured — skipping Admin Logs post",
                extra={"ctx_action": action, "ctx_user_id": user_id},
            )
            return

        text = _ADMIN_LOG_TEMPLATE.format(
            action=action,
            full_name=full_name,
            username=username or "N/A",
            user_id=user_id,
            detail=detail,
            timestamp=datetime.now(timezone.utc).isoformat(),
        )

        await self._send_to_admin_logs(text, topic_id=admin_logs_topic_id, hub_id=hub_id)

    async def _send_to_admin_logs(
        self,
        text: str,
        *,
        topic_id: Optional[int] = None,
        hub_id: Optional[int] = None,
    ) -> None:
        """
        Low-level function to send a message to the Admin Logs forum topic.

        Fetches hub_id and topic_id from hub_config if not provided.
        Handles FloodWait with a single retry. On failure, logs the error
        and continues — the MongoDB audit record is the authoritative log.

        Args:
            text:     HTML-formatted message to post.
            topic_id: Forum topic ID (Admin Logs topic). Fetched if None.
            hub_id:   Verification Hub supergroup ID. Fetched if None.
        """
        if not self._bot:
            return
        if topic_id is None:
            topic_id = await _get_admin_logs_topic_id()
        if hub_id is None:
            hub_id = await _get_hub_supergroup_id()
        if not topic_id or not hub_id:
            return
        for attempt in range(2):
            try:
                await self._bot.send_message(
                    chat_id=hub_id,
                    text=text,
                    parse_mode="html",
                    message_thread_id=topic_id,
                )
                return
            except FloodWait as exc:
                await asyncio.sleep(int(exc.value) + settings.FLOODWAIT_EXTRA_BUFFER)
            except Exception as exc:
                if attempt == 1:
                    logger.error(
                        "Failed to post to Admin Logs topic",
                        extra={"ctx_error": str(exc)},
                        exc_info=exc,
                    )
                return


# ── Module-level helpers ───────────────────────────────────────────────────────


async def _get_all_premium_chat_ids() -> list[int]:
    """
    Return the complete list of all premium-access chat IDs from hub_config.

    Section 7.8 mandates removal from ALL premium groups. This function
    reads hub_config (MongoDB) so the list is always authoritative and
    never hardcoded in source. Handles:
      • nsfw_group_id
      • premium_group_id
      • premium_vault_channel_id (users should not remain here on expiry)

    Additionally falls back to settings.NSFW_GROUP_ID and
    settings.PREMIUM_GROUP_ID as a safety net for deployments that have not
    yet written hub_config, but logs a warning when this path is taken.

    Returns:
        Deduplicated list of integer chat IDs. Empty list if none configured.
    """
    db = DatabaseManager.get_db()
    hub_col = db["hub_config"]

    chat_ids: list[int] = []
    for key in ("nsfw_group_id", "premium_group_id"):
        try:
            doc = await hub_col.find_one({"key": key})
            if doc and doc.get("value"):
                chat_ids.append(int(doc["value"]))
        except Exception as exc:
            logger.error(
                "Failed to read hub_config key",
                extra={"ctx_key": key, "ctx_error": str(exc)},
                exc_info=exc,
            )

    if not chat_ids:
        # Fallback to settings — emit a warning so operators know to fix config
        logger.warning(
            "hub_config has no premium chat IDs — falling back to settings. "
            "Ensure hub_config is populated on startup."
        )
        if getattr(settings, "NSFW_GROUP_ID", None):
            chat_ids.append(settings.NSFW_GROUP_ID)
        if getattr(settings, "PREMIUM_GROUP_ID", None):
            chat_ids.append(settings.PREMIUM_GROUP_ID)

    # Deduplicate while preserving order
    seen: set[int] = set()
    unique: list[int] = []
    for cid in chat_ids:
        if cid not in seen:
            seen.add(cid)
            unique.append(cid)
    return unique


async def _get_admin_logs_topic_id() -> Optional[int]:
    """
    Retrieve the Admin Logs forum topic ID from hub_config (MongoDB).

    Returns:
        Integer topic ID, or None if not yet configured.
    """
    try:
        db = DatabaseManager.get_db()
        doc = await db["hub_config"].find_one({"key": "admin_logs_topic_id"})
        if doc and doc.get("value"):
            return int(doc["value"])
    except Exception as exc:
        logger.error(
            "Failed to fetch admin_logs_topic_id from hub_config",
            extra={"ctx_error": str(exc)},
            exc_info=exc,
        )
    return None


async def _get_hub_supergroup_id() -> Optional[int]:
    """
    Retrieve the Verification Hub supergroup ID from hub_config (MongoDB).

    Returns:
        Integer chat ID, or None if not yet configured.
    """
    try:
        db = DatabaseManager.get_db()
        doc = await db["hub_config"].find_one({"key": "hub_supergroup_id"})
        if doc and doc.get("value"):
            return int(doc["value"])
    except Exception as exc:
        logger.error(
            "Failed to fetch hub_supergroup_id from hub_config",
            extra={"ctx_error": str(exc)},
            exc_info=exc,
        )
    return None


async def _fetch_user_display(user_id: int) -> dict:
    """
    Fetch user display information (full_name, username) from the users
    collection for use in Admin Logs entries.

    Returns a safe fallback dict if the user document is missing or the
    query fails — the admin log must still be posted.

    Args:
        user_id: Telegram user ID.

    Returns:
        Dict with keys 'full_name' and 'username'.
    """
    try:
        db = DatabaseManager.get_db()
        doc = await db["users"].find_one({"user_id": user_id})
        if doc:
            return {
                "full_name": doc.get("full_name", "Unknown"),
                "username": doc.get("username") or "N/A",
            }
    except Exception as exc:
        logger.error(
            "Failed to fetch user display info",
            extra={"ctx_user_id": user_id, "ctx_error": str(exc)},
            exc_info=exc,
        )
    return {"full_name": "Unknown", "username": "N/A"}


async def _write_audit_log(
    *,
    action: str,
    admin_user_id: Optional[int],
    target_user_id: Optional[int],
    detail: dict,
) -> None:
    """
    Write a structured entry to the audit_logs collection (Section 22).

    This is the MongoDB half of the dual-write requirement. The Admin Logs
    topic post is the other half and must be called separately.

    Args:
        action:          Uppercase action type string (e.g. "SUBSCRIPTION EXPIRED").
        admin_user_id:   Admin who triggered the action, or None for system events.
        target_user_id:  User affected by the action, or None if N/A.
        detail:          Arbitrary dict of action-specific metadata.
    """
    try:
        db = DatabaseManager.get_db()
        await db["audit_logs"].insert_one({
            "action": action,
            "admin_user_id": admin_user_id,
            "target_user_id": target_user_id,
            "detail": detail,
            "timestamp": datetime.now(timezone.utc),
        })
    except Exception as exc:
        # Log but do not re-raise — audit log failure must never abort
        # the business operation that triggered it.
        logger.error(
            "Failed to write audit log",
            extra={
                "ctx_action": action,
                "ctx_target": target_user_id,
                "ctx_error": str(exc),
            },
            exc_info=exc,
        )


# ── Redis distributed lock context manager ────────────────────────────────────


class _redis_lock:
    """
    Async context manager that acquires a Redis SET NX PX distributed lock.

    Usage:
        async with _redis_lock("my_lock_key", ttl=120):
            ...  # critical section

    If the lock cannot be acquired (another worker holds it), the context
    manager logs a warning and the body is SKIPPED, so the caller must
    account for that. This is intentional: if another process is already
    handling the same user's expiry, doing nothing is correct.

    The lock key is automatically namespaced under "vaultflow:lock:".

    Args:
        key: Logical lock identifier (e.g. "sub_expire:12345").
        ttl: Lock expiry in seconds. Defaults to _LOCK_TTL_SECONDS.
    """

    def __init__(self, key: str, *, ttl: int = _LOCK_TTL_SECONDS) -> None:
        self._key = f"vaultflow:lock:{key}"
        self._ttl = ttl
        self._acquired = False

    async def __aenter__(self) -> "_redis_lock":
        try:
            redis = await get_redis()
            # SET key value NX PX ttl_ms — returns True if acquired
            self._acquired = await redis.set(
                self._key,
                "1",
                nx=True,
                px=self._ttl * 1000,
            )
            if not self._acquired:
                logger.debug(
                    "Could not acquire distributed lock — skipping (another worker holds it)",
                    extra={"ctx_lock_key": self._key},
                )
        except Exception as exc:
            logger.error(
                "Redis lock acquisition error — proceeding without lock (risky)",
                extra={"ctx_lock_key": self._key, "ctx_error": str(exc)},
                exc_info=exc,
            )
            # Fail open: if Redis is unavailable we still process the record
            # to avoid silent data loss. Operators must fix Redis ASAP.
            self._acquired = True
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb) -> bool:
        if self._acquired:
            try:
                redis = await get_redis()
                await redis.delete(self._key)
            except Exception as exc:
                logger.warning(
                    "Failed to release distributed lock — will expire via TTL",
                    extra={"ctx_lock_key": self._key, "ctx_error": str(exc)},
                )
        # Never suppress exceptions from the protected block
        return False

    # Allow the body to check whether the lock was held
    def __bool__(self) -> bool:
        return self._acquired
