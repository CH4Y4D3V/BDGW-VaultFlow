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
from app.core.redis_client import get_redis
from app.services.subscription_service import SubscriptionService
from app.utils.logger import get_logger

logger = get_logger(__name__)

_SWEEP_INTERVAL_SECONDS = 300
_REMINDER_INTERVAL_SECONDS = 3600
_RECONCILE_INTERVAL_SECONDS = 21600
_MAX_NOTIFY_RETRIES = 3
_LOCK_TTL_SECONDS = 120

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
    def __init__(self) -> None:
        self._service = SubscriptionService()
        self._bot: Optional[Client] = None
        self._running = False
        self._sweep_task: Optional[asyncio.Task] = None
        self._reminder_task: Optional[asyncio.Task] = None
        self._reconcile_task: Optional[asyncio.Task] = None

    async def start(self, bot: Client) -> None:
        if self._running:
            return
        self._bot = bot
        self._running = True
        self._sweep_task = asyncio.create_task(self._run_sweep_loop(), name="subscription-sweep")
        self._reminder_task = asyncio.create_task(self._run_reminder_loop(), name="subscription-reminders")
        self._reconcile_task = asyncio.create_task(self._run_reconcile_loop(), name="subscription-reconcile")
        logger.info("SubscriptionWorker started")

    async def stop(self) -> None:
        self._running = False
        tasks = [t for t in (self._sweep_task, self._reminder_task, self._reconcile_task) if t and not t.done()]
        for task in tasks:
            task.cancel()
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)
        logger.info("SubscriptionWorker stopped")

    # ── Sweep loop ─────────────────────────────────────────────────────────

    async def _run_sweep_loop(self) -> None:
        while self._running:
            try:
                await self._sweep()
            except asyncio.CancelledError:
                break
            except Exception as exc:
                logger.error("Subscription sweep error", exc_info=exc)
            try:
                await asyncio.sleep(_SWEEP_INTERVAL_SECONDS)
            except asyncio.CancelledError:
                break

    async def _sweep(self) -> None:
        now = datetime.now(timezone.utc)

        # Active → Grace
        newly_expired = await self._service.get_newly_expired()
        for sub in newly_expired:
            lock_key = f"sub_grace:{sub.user_id}"
            async with _redis_lock(lock_key, ttl=_LOCK_TTL_SECONDS) as acquired:
                if not acquired:
                    continue
                try:
                    fresh = await self._service.get_subscription(sub.user_id)
                    if fresh is None or fresh.status.value != "ACTIVE":
                        continue
                    await self._service.set_grace(sub)
                    await self._notify(
                        sub.user_id,
                        f"⚠️ <b>Your subscription has expired.</b>\n\n"
                        f"You have a grace period of <b>{settings.GRACE_PERIOD_DAYS} day(s)</b> "
                        f"to renew before your access is removed.\n\nContact an admin to resubscribe.",
                    )
                    logger.info("Subscription moved to grace", extra={"ctx_user_id": sub.user_id})
                except Exception as exc:
                    logger.error("Failed to move subscription to grace", extra={"ctx_user_id": sub.user_id}, exc_info=exc)

        # Grace → Expired
        grace_expired = await self._service.get_grace_expired()
        for sub in grace_expired:
            lock_key = f"sub_expire:{sub.user_id}"
            async with _redis_lock(lock_key, ttl=_LOCK_TTL_SECONDS) as acquired:
                if not acquired:
                    continue
                try:
                    fresh = await self._service.get_subscription(sub.user_id)
                    if fresh is None or fresh.status.value != "GRACE":
                        continue
                    await self._service.expire(sub)
                    await _write_audit_log(
                        action="SUBSCRIPTION EXPIRED",
                        admin_user_id=None,
                        target_user_id=sub.user_id,
                        detail={"plan": sub.plan.value if sub.plan else "unknown", "expired_at": now.isoformat()},
                    )
                    user_info = await _fetch_user_display(sub.user_id)
                    await self._post_admin_log(
                        action="SUBSCRIPTION EXPIRED",
                        user_id=sub.user_id,
                        full_name=user_info["full_name"],
                        username=user_info["username"],
                        detail=f"Plan: {sub.plan.value if sub.plan else 'unknown'} | expired after grace",
                    )
                    await self._notify(
                        sub.user_id,
                        "❌ <b>Your subscription has fully expired.</b>\n\n"
                        "Your access has been removed. Contact an admin to resubscribe.",
                    )
                    await self._remove_from_all_premium_chats(sub.user_id)
                    logger.info("Subscription fully expired", extra={"ctx_user_id": sub.user_id})
                except Exception as exc:
                    logger.error("Failed to expire subscription", extra={"ctx_user_id": sub.user_id}, exc_info=exc)

    # ── Reminder loop ───────────────────────────────────────────────────────

    async def _run_reminder_loop(self) -> None:
        while self._running:
            try:
                await self._sweep_reminders()
            except asyncio.CancelledError:
                break
            except Exception as exc:
                logger.error("Subscription reminder error", exc_info=exc)
            try:
                await asyncio.sleep(_REMINDER_INTERVAL_SECONDS)
            except asyncio.CancelledError:
                break

    async def _sweep_reminders(self) -> None:
        now = datetime.now(timezone.utc)
        db = DatabaseManager.get_db()
        col = db["subscriptions"]

        # 7-day reminder
        subs_7d = await col.find({
            "status": {"$in": ["ACTIVE", "active"]},
            "expires_at": {"$gte": now, "$lte": now + timedelta(days=7)},
            "plan": {"$nin": ["FREE", "OWNER", "SUDO", "free", "owner", "sudo"]},
            "reminder_7d_sent": {"$ne": True},
        }).to_list(length=None)

        for sub_doc in subs_7d:
            user_id = sub_doc["user_id"]
            expires_at = sub_doc.get("expires_at")
            days_left = int((expires_at - now).total_seconds() // 86400) if expires_at else 7
            lock_key = f"reminder_7d:{user_id}"
            async with _redis_lock(lock_key, ttl=60) as acquired:
                if not acquired:
                    continue
                still_pending = await col.find_one({"_id": sub_doc["_id"], "reminder_7d_sent": {"$ne": True}})
                if not still_pending:
                    continue
                try:
                    await self._notify(
                        user_id,
                        f"⏰ <b>Subscription expiring in {days_left} day(s)</b>\n\n"
                        f"Expires on <b>{expires_at.strftime('%Y-%m-%d') if expires_at else 'soon'}</b>.\n\n"
                        "Renew early to keep your access.",
                    )
                    await col.update_one({"_id": sub_doc["_id"]}, {"$set": {"reminder_7d_sent": True}})
                except Exception as exc:
                    logger.error("Failed to send 7d reminder", extra={"ctx_user_id": user_id}, exc_info=exc)

        # 3-day reminder
        subs_3d = await col.find({
            "status": {"$in": ["ACTIVE", "active"]},
            "expires_at": {"$gte": now + timedelta(days=2, hours=12), "$lte": now + timedelta(days=3, hours=12)},
            "plan": {"$nin": ["FREE", "OWNER", "SUDO", "free", "owner", "sudo"]},
            "reminder_3d_sent": {"$ne": True},
        }).to_list(length=None)

        for sub_doc in subs_3d:
            user_id = sub_doc["user_id"]
            expires_at = sub_doc.get("expires_at")
            days_left = int((expires_at - now).total_seconds() // 86400) if expires_at else 3
            lock_key = f"reminder_3d:{user_id}"
            async with _redis_lock(lock_key, ttl=60) as acquired:
                if not acquired:
                    continue
                still_pending = await col.find_one({"_id": sub_doc["_id"], "reminder_3d_sent": {"$ne": True}})
                if not still_pending:
                    continue
                try:
                    await self._notify(
                        user_id,
                        f"⚠️ <b>Subscription expiring in {days_left} day(s)!</b>\n\n"
                        f"Expires on <b>{expires_at.strftime('%Y-%m-%d') if expires_at else 'soon'}</b>.\n\n"
                        "Renew NOW to avoid losing access.",
                    )
                    await col.update_one({"_id": sub_doc["_id"]}, {"$set": {"reminder_3d_sent": True}})
                except Exception as exc:
                    logger.error("Failed to send 3d reminder", extra={"ctx_user_id": user_id}, exc_info=exc)

    # ── Reconcile loop ──────────────────────────────────────────────────────

    async def _run_reconcile_loop(self) -> None:
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
                logger.error("Reconciliation error", exc_info=exc)
            try:
                await asyncio.sleep(_RECONCILE_INTERVAL_SECONDS)
            except asyncio.CancelledError:
                break

    async def _reconcile_membership(self) -> None:
        if not self._bot:
            return
        now = datetime.now(timezone.utc)
        db = DatabaseManager.get_db()
        target_chats = await _get_all_premium_chat_ids()
        kicked = 0

        expired_subs = await db["subscriptions"].find(
            {"status": {"$in": ["EXPIRED", "CANCELLED"]}}
        ).to_list(length=None)

        for sub_doc in expired_subs:
            user_id = sub_doc["user_id"]
            for chat_id in target_chats:
                try:
                    await self._bot.get_chat_member(chat_id=chat_id, user_id=user_id)
                    await self._kick_with_unban(chat_id=chat_id, user_id=user_id)
                    kicked += 1
                except UserNotParticipant:
                    pass
                except FloodWait as exc:
                    await asyncio.sleep(int(exc.value) + settings.FLOODWAIT_EXTRA_BUFFER)
                except Exception:
                    pass

        logger.info("Membership reconciliation complete", extra={"ctx_kicked": kicked})

    # ── Chat removal helpers ────────────────────────────────────────────────

    async def _remove_from_all_premium_chats(self, user_id: int) -> None:
        if not self._bot:
            return
        target_chats = await _get_all_premium_chat_ids()
        for chat_id in target_chats:
            await self._kick_with_unban(chat_id=chat_id, user_id=user_id)

    async def _kick_with_unban(self, *, chat_id: int, user_id: int) -> None:
        try:
            await self._bot.ban_chat_member(chat_id=chat_id, user_id=user_id)
            await self._bot.unban_chat_member(chat_id=chat_id, user_id=user_id)
            logger.info("Expired user kicked", extra={"ctx_user_id": user_id, "ctx_chat_id": chat_id})
        except ChatAdminRequired:
            logger.warning("Bot lacks admin rights to kick", extra={"ctx_chat_id": chat_id})
        except UserNotParticipant:
            pass
        except FloodWait as exc:
            await asyncio.sleep(int(exc.value) + settings.FLOODWAIT_EXTRA_BUFFER)
            try:
                await self._bot.ban_chat_member(chat_id=chat_id, user_id=user_id)
                await self._bot.unban_chat_member(chat_id=chat_id, user_id=user_id)
            except Exception as retry_exc:
                logger.error("Retry kick failed", extra={"ctx_user_id": user_id, "ctx_error": str(retry_exc)})
        except Exception as exc:
            logger.error("Kick error", extra={"ctx_user_id": user_id, "ctx_error": str(exc)})

    # ── Notifications ───────────────────────────────────────────────────────

    async def _notify(self, user_id: int, text: str) -> None:
        if not self._bot:
            return
        for attempt in range(_MAX_NOTIFY_RETRIES):
            try:
                await self._bot.send_message(chat_id=user_id, text=text, parse_mode="html")
                return
            except FloodWait as exc:
                await asyncio.sleep(int(exc.value) + settings.FLOODWAIT_EXTRA_BUFFER)
            except (UserIsBlocked, InputUserDeactivated, PeerIdInvalid):
                return
            except RPCError as exc:
                if attempt == _MAX_NOTIFY_RETRIES - 1:
                    logger.warning("Could not deliver DM", extra={"ctx_user_id": user_id, "ctx_error": str(exc)})
                await asyncio.sleep(2 ** attempt)
            except Exception as exc:
                logger.error("Unexpected DM error", extra={"ctx_user_id": user_id, "ctx_error": str(exc)})
                return

    async def _post_admin_log(self, *, action: str, user_id: int, full_name: str, username: str, detail: str) -> None:
        try:
            from app.services.hub_logger import write_admin_log
            await write_admin_log(
                action_type=action,
                performed_by=None,
                target_user_id=user_id,
                detail=detail,
            )
        except Exception as exc:
            logger.error("Failed to post admin log", extra={"ctx_error": str(exc)})

    async def _send_to_admin_logs(self, text: str, *, topic_id: Optional[int] = None, hub_id: Optional[int] = None) -> None:
        if not self._bot:
            return
        if not hub_id:
            hub_id = getattr(settings, "VERIFICATION_GROUP_ID", None)
        if not topic_id:
            topic_id = getattr(settings, "HUB_TOPIC_ADMIN_LOGS", None)
        if not topic_id or not hub_id:
            return
        for attempt in range(2):
            try:
                await self._bot.send_message(
                    chat_id=hub_id, text=text, parse_mode="html", message_thread_id=topic_id
                )
                return
            except FloodWait as exc:
                await asyncio.sleep(int(exc.value) + settings.FLOODWAIT_EXTRA_BUFFER)
            except Exception as exc:
                logger.error("Failed to post to Admin Logs", extra={"ctx_error": str(exc)})
                return


# ── Module-level helpers ─────────────────────────────────────────────────────

async def _get_all_premium_chat_ids() -> list[int]:
    db = DatabaseManager.get_db()
    chat_ids: list[int] = []
    for key in ("nsfw_group_id", "premium_group_id", "premium_vault_channel_id"):
        try:
            doc = await db["hub_config"].find_one({"key": key})
            if doc and doc.get("value"):
                chat_ids.append(int(doc["value"]))
        except Exception:
            pass

    if not chat_ids:
        logger.warning("hub_config has no premium chat IDs — falling back to settings")
        if getattr(settings, "NSFW_GROUP_ID", None):
            chat_ids.append(settings.NSFW_GROUP_ID)
        if getattr(settings, "PREMIUM_GROUP_ID", None):
            chat_ids.append(settings.PREMIUM_GROUP_ID)

    seen: set[int] = set()
    return [cid for cid in chat_ids if not (cid in seen or seen.add(cid))]


async def _fetch_user_display(user_id: int) -> dict:
    try:
        db = DatabaseManager.get_db()
        doc = await db["users"].find_one({"user_id": user_id})
        if doc:
            return {"full_name": doc.get("full_name", "Unknown"), "username": doc.get("username") or "N/A"}
    except Exception:
        pass
    return {"full_name": "Unknown", "username": "N/A"}


async def _write_audit_log(*, action: str, admin_user_id: Optional[int], target_user_id: Optional[int], detail: dict) -> None:
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
        logger.error("Failed to write audit log", extra={"ctx_action": action, "ctx_error": str(exc)})


class _redis_lock:
    """
    Async context manager for Redis SET NX distributed locking.

    FIX A-09: FAIL-CLOSED on Redis unavailability.
    Original code set _acquired=True when Redis was down (fail-open),
    allowing concurrent workers to all proceed, causing duplicate expiry
    notifications, duplicate kicks, and duplicate audit entries.

    Now: on Redis error we set _acquired=False and yield — the caller's
    `if not acquired: continue` guard skips the operation. One missed cycle
    is far safer than corrupting data across all concurrent workers.
    """

    def __init__(self, key: str, *, ttl: int = _LOCK_TTL_SECONDS) -> None:
        self._key = f"vaultflow:lock:{key}"
        self._ttl = ttl
        self._acquired = False

    async def __aenter__(self) -> "_redis_lock":
        try:
            redis = await get_redis()
            self._acquired = bool(
                await redis.set(self._key, "1", nx=True, px=self._ttl * 1000)
            )
            if not self._acquired:
                logger.debug("Lock not acquired — another worker holds it", extra={"ctx_key": self._key})
        except Exception as exc:
            # FIX A-09: FAIL-CLOSED — do NOT proceed without the lock.
            # Concurrent data corruption is worse than skipping one cycle.
            logger.error(
                "Redis lock unavailable — skipping to prevent data corruption",
                extra={"ctx_key": self._key, "ctx_error": str(exc)},
            )
            self._acquired = False  # explicit fail-closed
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb) -> bool:
        if self._acquired:
            try:
                redis = await get_redis()
                await redis.delete(self._key)
            except Exception as exc:
                logger.warning(
                    "Failed to release lock — will expire via TTL",
                    extra={"ctx_key": self._key, "ctx_error": str(exc)},
                )
        return False

    def __bool__(self) -> bool:
        return self._acquired
