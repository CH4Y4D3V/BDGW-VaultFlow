# FILE: app/core/lifecycle.py
"""
Manages the global application boot, health, and shutdown sequence for
BDGW VaultFlow.

Boot order (strict):
    Config validation
    → Health server (Railway liveness)
    → Database (MongoDB/Motor)
    → Channel seeding
    → Telegram client (Pyrogram)
    → Bot commands + topic cache
    → Distribution engine
    → Subscription worker
    → Referral system
    → Payment timeout monitor
    → Support inactivity monitor
    → Message cleanup worker
    → Watermark worker pool
    → Membership reconciliation worker    ← NEW (Section 26)

Shutdown order (reverse, each step isolated in try/except).
"""

from __future__ import annotations

import asyncio
import os
import sys
from datetime import datetime, timezone
from typing import Any, Optional

from pyrogram.enums import ParseMode
from pyrogram.errors import FloodWait, UserNotParticipant

from app.bot.client import get_bot, set_bot_id
from app.config import settings
from app.core.database import DatabaseManager
from app.core.logger import get_logger
from app.distribution.engine import DistributionEngine
from app.health import start_health_server
from app.referral.scheduler import ReferralScheduler
from app.services.channel_service import ChannelService
from app.workers.subscription_worker import SubscriptionWorker

logger = get_logger(__name__)


# ─────────────────────────────────────────────────────────────────────────────
# Section 26 — Membership Reconciliation Worker
# NOTE: Extract to app/workers/membership_reconciliation_worker.py in future.
# ─────────────────────────────────────────────────────────────────────────────

class MembershipReconciliationWorker:
    """
    Scheduled background worker that enforces the invariant:
      active subscribers  →  MUST be in their entitled group
      expired subscribers →  MUST NOT be in that group

    Per spec Section 26, this worker:
      1. Re-invites active subscribers who are missing from the group.
      2. Kicks (ban → immediate unban) expired members still in the group.
      3. Logs every repair action to the audit_logs MongoDB collection.
      4. Posts a run summary to the Admin Logs topic in the Verification Hub.

    A Redis distributed lock (key: lock:reconciliation:run) prevents
    concurrent runs when multiple Railway replicas are deployed.
    """

    LOCK_KEY = "lock:reconciliation:run"
    LOCK_TTL = 1800  # 30 minutes maximum hold

    def __init__(self, db, bot, redis_client) -> None:
        """
        Args:
            db:           Motor AsyncIOMotorDatabase instance.
            bot:          Authenticated Pyrogram Client.
            redis_client: aioredis Redis client (used for distributed lock).
        """
        self._db = db
        self._bot = bot
        self._redis = redis_client

    # ── Public entry point ────────────────────────────────────────────────────

    async def run_once(self) -> None:
        """
        Main entry point called by APScheduler every 6 hours.
        Acquires a distributed lock before running to prevent concurrent
        execution across replicas.
        """
        lock_acquired = await self._redis.set(
            self.LOCK_KEY, "1", ex=self.LOCK_TTL, nx=True
        )
        if not lock_acquired:
            logger.info("reconciliation_skipped_lock_held")
            return

        try:
            await self._run_reconciliation()
        except Exception:
            logger.exception("reconciliation_run_failed")
        finally:
            try:
                await self._redis.delete(self.LOCK_KEY)
            except Exception:
                pass  # Lock will TTL out naturally

    # ── Core reconciliation logic ─────────────────────────────────────────────

    async def _run_reconciliation(self) -> None:
        """
        Loads hub_config for group IDs, then runs the two repair phases:
          Phase 1 — re-invite active subscribers who left their group.
          Phase 2 — kick expired members who are still in the group.
        Emits a summary to the Admin Logs topic on completion.
        """
        now = datetime.now(timezone.utc)
        summary: dict[str, int] = {"re_invited": 0, "kicked": 0, "errors": 0}

        hub_config_docs = await self._db["hub_config"].find({}).to_list(length=None)
        hub_config: dict[str, Any] = {
            doc["key"]: doc["value"] for doc in hub_config_docs if "key" in doc
        }
        if not hub_config:
            logger.warning("reconciliation_hub_config_missing_using_settings_fallback")

        group_map: dict[str, Optional[int]] = {
            "nsfw":    hub_config.get("nsfw_group_id") or settings.NSFW_GROUP_ID,
            "premium": hub_config.get("premium_group_id") or settings.PREMIUM_GROUP_ID,
        }

        # Phase 1 — active subscribers missing from group
        async for sub in self._db["subscriptions"].find(
            {"status": "ACTIVE", "expires_at": {"$gt": now}}
        ):
            user_id: int = sub["user_id"]
            vault_type: str = sub.get("vault_type", "nsfw")
            group_id = group_map.get(vault_type)

            if not group_id:
                continue

            try:
                await self._bot.get_chat_member(group_id, user_id)
                # Member confirmed present — no action required.
            except UserNotParticipant:
                try:
                    await self._re_invite(user_id, int(group_id), sub)
                    summary["re_invited"] += 1
                except Exception as e:
                    logger.error(
                        "reconciliation_reinvite_failed",
                        extra={"ctx_user_id": user_id, "ctx_error": str(e)},
                    )
                    summary["errors"] += 1
            except Exception as e:
                logger.warning(
                    "reconciliation_member_check_failed",
                    extra={"ctx_user_id": user_id, "ctx_error": str(e)},
                )
                summary["errors"] += 1

        # Phase 2 — expired members still in group
        # Collect distinct (user_id, vault_type) pairs from expired subs
        expired_pairs: set[tuple[int, str]] = set()
        async for sub in self._db["subscriptions"].find(
            {"$or": [{"status": "EXPIRED"}, {"expires_at": {"$lt": now}}]}
        ):
            expired_pairs.add((int(sub["user_id"]), sub.get("vault_type", "nsfw")))

        for user_id, vault_type in expired_pairs:
            group_id = group_map.get(vault_type)
            if not group_id:
                continue

            try:
                await self._bot.get_chat_member(int(group_id), user_id)
                # Member is still present — kick them.
                try:
                    await self._kick_expired_member(user_id, int(group_id))
                    summary["kicked"] += 1
                except Exception as e:
                    logger.error(
                        "reconciliation_kick_failed",
                        extra={"ctx_user_id": user_id, "ctx_error": str(e)},
                    )
                    summary["errors"] += 1
            except UserNotParticipant:
                pass  # Already not a member — nothing to repair.
            except Exception as e:
                logger.warning(
                    "reconciliation_expire_check_failed",
                    extra={"ctx_user_id": user_id, "ctx_error": str(e)},
                )
                summary["errors"] += 1

        logger.info("reconciliation_complete", extra={"ctx_summary": summary})
        await self._post_summary(hub_config, summary, now)

    # ── Repair helpers ────────────────────────────────────────────────────────

    async def _re_invite(self, user_id: int, group_id: int, sub: dict) -> None:
        """
        Generates a single-use invite link for the target group and
        DMs it to the user.  FloodWait is handled with up to 3 retries.
        Action is written to audit_logs on success.
        """
        invite_link_obj = await self._bot.create_chat_invite_link(
            group_id,
            member_limit=1,
            name=f"reconcile_{user_id}",
        )
        invite_link: str = invite_link_obj.invite_link

        msg_text = (
            "🔔 Your premium subscription is active but you appear to have "
            "left the group. Click the link below to rejoin:\n\n"
            f"{invite_link}"
        )

        for attempt in range(3):
            try:
                await self._bot.send_message(chat_id=user_id, text=msg_text)
                break
            except FloodWait as fw:
                await asyncio.sleep(fw.value + 1)
            except Exception:
                if attempt == 2:
                    raise
                await asyncio.sleep(2 ** attempt)

        await self._write_audit(
            action="RECONCILE_REINVITE",
            user_id=user_id,
            group_id=group_id,
            details={"invite_link": invite_link, "sub_id": str(sub.get("_id"))},
        )

    async def _kick_expired_member(self, user_id: int, group_id: int) -> None:
        """
        Removes an expired subscriber from the group via ban + immediate
        unban (Telegram's standard kick pattern that preserves the ability
        to rejoin later on a new subscription).
        Sends a courtesy DM to the user.
        Writes to audit_logs on success.
        """
        for attempt in range(3):
            try:
                await self._bot.ban_chat_member(group_id, user_id)
                await asyncio.sleep(0.5)
                await self._bot.unban_chat_member(group_id, user_id)
                break
            except FloodWait as fw:
                await asyncio.sleep(fw.value + 1)
            except Exception:
                if attempt == 2:
                    raise
                await asyncio.sleep(2 ** attempt)

        try:
            await self._bot.send_message(
                chat_id=user_id,
                text=(
                    "ℹ️ Your premium subscription has expired and you have "
                    "been removed from the group. Renew at any time via /start."
                ),
            )
        except Exception:
            pass  # Non-critical — user may have blocked the bot.

        await self._write_audit(
            action="RECONCILE_KICK_EXPIRED",
            user_id=user_id,
            group_id=group_id,
            details={},
        )

    # ── Audit & reporting ─────────────────────────────────────────────────────

    async def _write_audit(
        self,
        action: str,
        user_id: int,
        group_id: int,
        details: dict,
    ) -> None:
        """
        Writes one reconciliation action record to the audit_logs collection.
        Failures are logged as errors but never re-raised — audit writes must
        never abort the repair loop.
        """
        doc = {
            "action": action,
            "user_id": user_id,
            "group_id": group_id,
            "details": details,
            "timestamp": datetime.now(timezone.utc),
            "source": "membership_reconciliation_worker",
        }
        try:
            await self._db["audit_logs"].insert_one(doc)
        except Exception as e:
            logger.error(
                "reconciliation_audit_write_failed",
                extra={"ctx_error": str(e)},
            )

    async def _post_summary(
        self,
        hub_config: dict,
        summary: dict,
        run_time: datetime,
    ) -> None:
        """
        Posts a human-readable reconciliation summary to the Admin Logs
        topic in the Verification Hub. If the topic ID is missing from
        hub_config the post is silently skipped.
        """
        admin_logs_topic_id = hub_config.get("admin_logs_topic_id") or getattr(
            settings, "HUB_TOPIC_ADMIN_LOGS", None
        )
        verification_group_id = hub_config.get("hub_supergroup_id") or getattr(
            settings, "VERIFICATION_GROUP_ID", None
        )

        if not admin_logs_topic_id or not verification_group_id:
            logger.warning(
                "reconciliation_summary_skipped_no_topic",
                extra={"ctx_hub_config_keys": list(hub_config.keys())},
            )
            return

        text = (
            "🔄 <b>Membership Reconciliation — Run Complete</b>\n\n"
            f"⏰ <b>Time:</b> {run_time.strftime('%Y-%m-%d %H:%M UTC')}\n"
            f"✅ <b>Re-invited (active, missing):</b> {summary['re_invited']}\n"
            f"🚫 <b>Kicked (expired, still present):</b> {summary['kicked']}\n"
            f"⚠️ <b>Errors:</b> {summary['errors']}"
        )

        for attempt in range(3):
            try:
                await self._bot.send_message(
                    chat_id=int(verification_group_id),
                    text=text,
                    message_thread_id=int(admin_logs_topic_id),
                    parse_mode=ParseMode.HTML,
                )
                return
            except FloodWait as fw:
                await asyncio.sleep(fw.value + 1)
            except Exception as e:
                logger.error(
                    "reconciliation_summary_post_failed",
                    extra={"ctx_error": str(e)},
                )
                return


# ─────────────────────────────────────────────────────────────────────────────
# Main lifecycle orchestrator
# ─────────────────────────────────────────────────────────────────────────────

class AppLifecycle:
    """
    Manages the global application boot and shutdown sequence.

    Each startup step is isolated in its own try/except block.  Critical
    failures (DB, bot client) call sys.exit(1).  Non-critical failures
    (monitors, schedulers) are logged as errors and skipped so that the
    rest of the platform can still serve users.
    """

    def __init__(self) -> None:
        """Declare all managed component references with explicit None defaults."""
        self._engine: Optional[DistributionEngine] = None
        self._subscription_worker: Optional[SubscriptionWorker] = None
        self._cleanup_worker: Optional[Any] = None
        self._referral_scheduler: Optional[ReferralScheduler] = None
        self._watermark_pool: Optional[Any] = None               # WatermarkWorkerPool
        self._reconciliation_worker: Optional[MembershipReconciliationWorker] = None
        self._payment_timeout_monitor: Optional[Any] = None
        self._support_monitor: Optional[Any] = None
        self._recovery_task: Optional[asyncio.Task] = None
        self._health_runner: Optional[Any] = None
        self._bot = get_bot()
        self._running = False

    # ── Boot sequence ──────────────────────────────────────────────────────────

    async def start(self) -> None:
        """
        Execute the full platform boot sequence in strict dependency order.
        Any step that fails catastrophically calls sys.exit(1) AFTER logging.
        Non-critical steps are isolated so a single failure cannot abort
        the entire startup.
        """
        if self._running:
            return

        logger.info("lifecycle_bootstrapping_start")

        # ── Step 0: Health server (Railway liveness probe) ─────────────────
        try:
            port = int(os.environ.get("PORT", 8080))
            self._health_runner = await start_health_server(port)
        except Exception:
            logger.error("lifecycle_health_server_failed", exc_info=True)

        # ── Step 1: Config validation ──────────────────────────────────────
        self._validate_config()

        # ── Step 2: Database ───────────────────────────────────────────────
        try:
            await DatabaseManager.connect()
        except Exception:
            logger.exception("lifecycle_db_init_failed")
            sys.exit(1)

        try:
            channel_service = ChannelService()
            await channel_service.seed_channels()
        except Exception:
            logger.exception("lifecycle_channel_seeding_failed")
            sys.exit(1)

        try:
            from app.services.topic_manager import seed_hub_config_defaults
            await seed_hub_config_defaults()
        except Exception:
            logger.exception("lifecycle_hub_config_seeding_failed")
            # Non-fatal: workers fall back to settings if hub_config is empty.

        # ── Step 3: Redis health check ─────────────────────────────────────
        redis_client = None
        try:
            from app.core.redis_client import RedisClient
            redis_client = await RedisClient.get_client()
            pong = await redis_client.ping()
            if not pong:
                raise ConnectionError("Redis ping returned falsy")
            logger.info("lifecycle_redis_connected")
        except Exception as e:
            logger.warning(
                "lifecycle_redis_unavailable_locks_disabled",
                extra={"ctx_error": str(e)},
            )
            redis_client = None  # Non-fatal; noted for operators

        # ── Step 4: Telegram client ────────────────────────────────────────
        try:
            logger.info("lifecycle_bot_start")
            await self._bot.start()
            me = await self._bot.get_me()
            set_bot_id(me.id)

            from pyrogram.types import (
                BotCommand,
                BotCommandScopeAllPrivateChats,
                BotCommandScopeChatAdministrators,
                BotCommandScopeChatMember,
            )

            # ── 1. Public commands — all private chats (Section 4.1) ─────────
            public_commands = [
                BotCommand("start",    "Main Menu"),
                BotCommand("takedown", "Remove Content"),
                BotCommand("help",     "Support"),
            ]

            # ── 2. Admin commands — shown in the hub (Section 9.5) ───────────
            hub_commands = [
                BotCommand("accept",      "Accept support session"),
                BotCommand("close",       "Close active support session"),
                BotCommand("ban",         "Ban user — /ban <id> <reason>"),
                BotCommand("unban",       "Remove ban — /unban <id>"),
                BotCommand("warn",        "Issue warning — /warn <id> <reason>"),
                BotCommand("mute",        "Mute user — /mute <id> <mins> <reason>"),
                BotCommand("unmute",      "Remove mute — /unmute <id>"),
                BotCommand("paymentdone", "Mark payment done — /paymentdone <id>"),
                BotCommand("profile",     "User profile card — /profile <id>"),
                BotCommand("history",     "Event history — /history <id>"),
                BotCommand("note",        "Add note — /note <id> <text>"),
                BotCommand("notes",       "List notes — /notes <id>"),
                BotCommand("grant",       "Grant premium — /grant <id> <days>"),
                BotCommand("revoke",      "Revoke premium — /revoke <id>"),
                BotCommand("broadcast",   "Broadcast to all users"),
            ]

            try:
                # Public scope
                await self._bot.set_bot_commands(
                    public_commands,
                    scope=BotCommandScopeAllPrivateChats(),
                )

                # Hub admin scope — all admins in the verification hub group
                # see these when they type / inside any hub topic
                hub_id = getattr(settings, "VERIFICATION_GROUP_ID", None)
                if hub_id:
                    try:
                        await self._bot.set_bot_commands(
                            hub_commands,
                            scope=BotCommandScopeChatAdministrators(chat_id=hub_id),
                        )
                    except Exception as hub_scope_err:
                        logger.warning(
                            "lifecycle_set_hub_admin_commands_failed",
                            extra={"ctx_error": str(hub_scope_err)},
                        )

                    # Also register per-user in private chat for each admin/owner
                    # so they see admin commands when DMing the bot directly
                    try:
                        from app.core.database import DatabaseManager
                        db = DatabaseManager.get_db()
                        admin_docs = await db["admins"].find(
                            {"is_active": True}
                        ).to_list(length=100)
                        for admin_doc in admin_docs:
                            try:
                                await self._bot.set_bot_commands(
                                    public_commands + hub_commands,
                                    scope=BotCommandScopeChatMember(
                                        chat_id=hub_id,
                                        user_id=admin_doc["user_id"],
                                    ),
                                )
                            except Exception:
                                pass  # Non-fatal — admin may have left hub
                    except Exception as per_user_err:
                        logger.warning(
                            "lifecycle_set_per_admin_commands_failed",
                            extra={"ctx_error": str(per_user_err)},
                        )

            except Exception as cmd_err:
                logger.warning(
                    "lifecycle_set_bot_commands_failed",
                    extra={"ctx_error": str(cmd_err)},
                )

            try:
                await self._verify_channel_access()
            except Exception as verify_err:
                logger.warning(
                    "lifecycle_channel_access_failed_degraded",
                    extra={"ctx_error": str(verify_err)},
                )

            total_handlers = self._audit_handler_registration()
            logger.info(
                "lifecycle_bot_connected",
                extra={
                    "ctx_bot_username": me.username,
                    "ctx_total_handlers": total_handlers,
                },
            )

            try:
                from app.services.topic_manager import get_topic_manager
                topic_manager = get_topic_manager()
                await topic_manager.restore_cache()
                await topic_manager.ensure_shared_topics(self._bot)
                logger.info("lifecycle_topic_cache_restored_and_shared_topics_ensured")
            except Exception as e:
                logger.warning(
                    "lifecycle_topic_initialization_failed",
                    extra={"ctx_error": str(e)},
                )

        except Exception as e:
            logger.exception(
                "lifecycle_bot_start_failed",
                extra={"ctx_error_type": type(e).__name__, "ctx_error": str(e)},
            )
            raise

        # ── Step 5: Distribution engine ────────────────────────────────────
        from app.bot.delivery import execute_telegram_delivery
        from app.bot.provider import fetch_distribution_content

        self._engine = DistributionEngine(
            delivery_callback=execute_telegram_delivery,
            content_provider_callback=fetch_distribution_content,
        )

        try:
            await self._engine.start()
        except Exception as e:
            logger.exception(
                "lifecycle_engine_start_failed",
                extra={"ctx_error_type": type(e).__name__, "ctx_error": str(e)},
            )
            raise

        # ── Step 6: Subscription worker ────────────────────────────────────
        self._subscription_worker = SubscriptionWorker()
        try:
            await self._subscription_worker.start(bot=self._bot)
        except Exception:
            logger.error("lifecycle_subscription_worker_failed", exc_info=True)
            self._subscription_worker = None

        # ── Step 7: Referral system ────────────────────────────────────────
        try:
            from app.referral.repository import ReferralRepository
            from app.referral.service import ReferralService
            from app.handlers.membership_handler import init_membership_handler

            ref_repo = ReferralRepository(DatabaseManager.get_db())

            try:
                from app.repositories.admin_repository import AdminRepository
                await AdminRepository().create_indexes()
            except Exception as e:
                logger.warning("lifecycle_admin_index_failed", extra={"ctx_error": str(e)})

            # ── BUG-1 FIX: Seed admins collection from ADMIN_IDS + OWNER_ID ──
            # The admins collection starts empty on every fresh deployment.
            # has_role() only bypasses DB for OWNER_ID; everyone else queries
            # the collection.  Without seeding, every user in ADMIN_IDS gets
            # "Unauthorized" on ALL button clicks.  We upsert here on every
            # boot so that adding/removing IDs from .env takes effect on restart.
            try:
                from datetime import datetime, timezone as _tz
                db = DatabaseManager.get_db()
                now = datetime.now(_tz.utc)
                owner_id = int(settings.OWNER_ID)
                admin_ids = list(settings.ADMIN_IDS) if hasattr(settings, "ADMIN_IDS") else []

                # Upsert OWNER record
                await db["admins"].update_one(
                    {"user_id": owner_id},
                    {
                        "$set": {
                            "user_id": owner_id,
                            "role": "owner",
                            "is_active": True,
                            "assigned_by": owner_id,
                        },
                        "$setOnInsert": {"assigned_at": now},
                    },
                    upsert=True,
                )

                # Upsert ADMIN records for every user in ADMIN_IDS
                for admin_uid in admin_ids:
                    uid = int(admin_uid)
                    if uid == owner_id:
                        continue  # already upserted as OWNER above
                    await db["admins"].update_one(
                        {"user_id": uid},
                        {
                            "$set": {
                                "user_id": uid,
                                "role": "admin",
                                "is_active": True,
                                "assigned_by": owner_id,
                            },
                            "$setOnInsert": {"assigned_at": now},
                        },
                        upsert=True,
                    )

                logger.info(
                    "lifecycle_admins_seeded",
                    extra={
                        "ctx_owner_id": owner_id,
                        "ctx_admin_count": len(admin_ids),
                    },
                )
            except Exception as seed_exc:
                logger.error(
                    "lifecycle_admin_seeding_failed",
                    extra={"ctx_error": str(seed_exc)},
                )
            # ── END BUG-1 FIX ────────────────────────────────────────────────

            try:
                await ref_repo.create_indexes()
            except Exception as idx_err:
                logger.error(
                    "lifecycle_referral_index_failed",
                    extra={"ctx_error": str(idx_err)},
                )

            ref_service = ReferralService(ref_repo, self._bot)

            if self._engine and self._engine.scheduler:
                raw_scheduler = self._engine.scheduler._scheduler
                self._referral_scheduler = ReferralScheduler(
                    service=ref_service,
                    scheduler=raw_scheduler,
                    channel_id=int(settings.MAIN_CHANNEL_ID),
                )
                self._referral_scheduler.register_jobs()
                logger.info("lifecycle_referral_jobs_registered")
            else:
                logger.warning("lifecycle_referral_scheduler_skipped")

            init_membership_handler(ref_service)

        except Exception as e:
            logger.error(
                "lifecycle_referral_startup_failed",
                extra={"ctx_error": str(e)},
            )

        # ── Step 8: Payment timeout monitor ───────────────────────────────
        try:
            from app.payments.repository import PaymentRepository
            from app.payments.timeouts import PaymentTimeoutMonitor
            from app.payments import get_payment_service

            payment_repo = PaymentRepository(DatabaseManager.get_db())
            try:
                await payment_repo.create_indexes()
            except Exception as idx_err:
                logger.error(
                    "lifecycle_payment_index_failed",
                    extra={"ctx_error": str(idx_err)},
                )

            self._payment_timeout_monitor = PaymentTimeoutMonitor(payment_repo)
            bot_ref = self._bot

            if self._engine and self._engine.scheduler:
                raw_scheduler = self._engine.scheduler._scheduler
                raw_scheduler.add_job(
                    self._payment_timeout_monitor.check_timeouts,
                    "interval",
                    minutes=1,
                    kwargs={"client": bot_ref},
                    id="payment_timeout_monitor",
                    replace_existing=True,
                    coalesce=True,
                )
                logger.info("lifecycle_payment_monitor_registered")
            else:
                logger.warning(
                    "lifecycle_payment_monitor_skipped",
                    extra={"ctx_reason": "engine_not_available"},
                )

            # Resume active sessions; store task reference for shutdown.
            payment_service = get_payment_service()
            self._recovery_task = asyncio.create_task(
                payment_service.resume_active_sessions(),
                name="payment_session_recovery",
            )
            logger.info("lifecycle_payment_recovery_initiated")

        except Exception as e:
            logger.error(
                "lifecycle_payment_monitor_failed",
                extra={"ctx_error": str(e)},
                exc_info=True,
            )

        # ── Step 9: Support inactivity monitor ────────────────────────────
        try:
            from app.services.support_monitor import SupportMonitor
            self._support_monitor = SupportMonitor(self._bot)

            if self._engine and self._engine.scheduler:
                raw_scheduler = self._engine.scheduler._scheduler
                raw_scheduler.add_job(
                    self._support_monitor.check_inactivity,
                    "interval",
                    minutes=1,
                    id="support_inactivity_monitor",
                    replace_existing=True,
                    coalesce=True,
                )
                logger.info("lifecycle_support_monitor_registered")
        except Exception as e:
            logger.error(
                "lifecycle_support_monitor_failed",
                extra={"ctx_error": str(e)},
            )

        # ── Step 10: Message cleanup worker ───────────────────────────────
        try:
            from app.utils.cleanup_worker import CleanupWorker
            self._cleanup_worker = CleanupWorker()
            await self._cleanup_worker.start(bot=self._bot)
            logger.info("lifecycle_cleanup_worker_started")
        except Exception as e:
            logger.error(
                "lifecycle_cleanup_monitor_failed",
                extra={"ctx_error": str(e)},
            )

        # ── Step 11: Watermark worker pool ─────────────────────────────────
        try:
            from app.watermark.worker_pool import WatermarkWorkerPool
            self._watermark_pool = WatermarkWorkerPool(db=DatabaseManager.get_db())
            await self._watermark_pool.start()
            logger.info("lifecycle_watermark_pool_started")
        except Exception as e:
            logger.error(
                "lifecycle_watermark_pool_failed",
                extra={"ctx_error": str(e)},
            )

        # ── Step 12: Membership reconciliation worker (Section 26) ─────────
        try:
            if redis_client is not None:
                self._reconciliation_worker = MembershipReconciliationWorker(
                    db=DatabaseManager.get_db(),
                    bot=self._bot,
                    redis_client=redis_client,
                )

                if self._engine and self._engine.scheduler:
                    raw_scheduler = self._engine.scheduler._scheduler
                    interval_hours = int(
                        getattr(settings, "RECONCILIATION_INTERVAL_HOURS", 6)
                    )
                    raw_scheduler.add_job(
                        self._reconciliation_worker.run_once,
                        "interval",
                        hours=interval_hours,
                        id="membership_reconciliation",
                        replace_existing=True,
                        coalesce=True,
                    )
                    logger.info(
                        "lifecycle_reconciliation_worker_registered",
                        extra={"ctx_interval_hours": interval_hours},
                    )
                    # Run immediately on boot to repair any drift since last restart.
                    asyncio.create_task(
                        self._reconciliation_worker.run_once(),
                        name="reconciliation_boot_run",
                    )
                else:
                    logger.warning("lifecycle_reconciliation_scheduler_unavailable")
            else:
                logger.warning(
                    "lifecycle_reconciliation_skipped_no_redis",
                    extra={"ctx_reason": "Redis client unavailable — lock cannot be acquired"},
                )
        except Exception as e:
            logger.error(
                "lifecycle_reconciliation_startup_failed",
                extra={"ctx_error": str(e)},
            )

        self._running = True
        logger.info("lifecycle_startup_complete")

    # ── Channel verification ───────────────────────────────────────────────────

    async def _verify_channel_access(self) -> None:
        """
        Confirms the bot has read access to all configured channels and groups.
        A failure on VAULT_CHANNEL_ID is fatal — the bot cannot operate
        without vault access.  All other failures are non-fatal warnings.
        """
        logger.info("lifecycle_channel_verification_start")
        channels_to_check = {
            "VAULT_CHANNEL_ID": getattr(settings, "VAULT_CHANNEL_ID", None),
            "PREMIUM_CHANNEL_ID": getattr(settings, "PREMIUM_CHANNEL_ID", None),
            "VERIFICATION_GROUP_ID": getattr(settings, "VERIFICATION_GROUP_ID", None),
            "NSFW_GROUP_ID": getattr(settings, "NSFW_GROUP_ID", None),
            "PREMIUM_GROUP_ID": getattr(settings, "PREMIUM_GROUP_ID", None),
        }
        critical_failure = False

        for name, raw_id in channels_to_check.items():
            is_critical = name == "VAULT_CHANNEL_ID"

            if not raw_id:
                if is_critical:
                    logger.error(
                        "lifecycle_critical_channel_unconfigured",
                        extra={"ctx_channel": name},
                    )
                    critical_failure = True
                continue

            try:
                channel_id = int(raw_id)
                chat = await self._bot.get_chat(channel_id)
                logger.info(
                    "lifecycle_channel_access_confirmed",
                    extra={"ctx_channel": name, "ctx_title": chat.title},
                )
            except Exception as e:
                if is_critical:
                    logger.error(
                        "lifecycle_critical_channel_access_failed",
                        extra={"ctx_channel": name, "ctx_error": str(e)},
                    )
                    critical_failure = True
                else:
                    logger.warning(
                        "lifecycle_channel_access_warning",
                        extra={"ctx_channel": name, "ctx_error": str(e)},
                    )

        if critical_failure:
            logger.error("lifecycle_boot_aborted_channel_failure")
            sys.exit(1)

    # ── Handler audit ──────────────────────────────────────────────────────────

    def _audit_handler_registration(self) -> int:
        """
        Inspects the Pyrogram dispatcher for registered handler groups and
        logs a breakdown.  Raises RuntimeError (instead of calling sys.exit)
        if zero handlers are found so that the boot sequence can still run
        the graceful shutdown path.
        """
        total_handlers = 0
        breakdown: dict[int, list[str]] = {}

        try:
            dispatcher = getattr(self._bot, "dispatcher", None)
            if dispatcher is None:
                logger.error("lifecycle_audit_dispatcher_missing")
                return 0

            groups = getattr(dispatcher, "groups", None)
            if groups is None:
                logger.error("lifecycle_audit_groups_missing")
                return 0

            for group_id, handlers in groups.items():
                handler_names: list[str] = []
                for h in handlers:
                    cb = getattr(h, "callback", None)
                    if cb is not None:
                        name = getattr(cb, "__name__", repr(cb))
                        module = getattr(cb, "__module__", "?")
                        handler_names.append(f"{module}.{name}")
                    else:
                        handler_names.append(repr(h))
                breakdown[group_id] = handler_names
                total_handlers += len(handler_names)

        except Exception as e:
            logger.error(
                "lifecycle_audit_inspection_failed",
                extra={"ctx_error": str(e)},
                exc_info=True,
            )
            return 0

        if total_handlers == 0:
            logger.error(
                "lifecycle_audit_no_handlers",
                extra={"ctx_groups": dict(breakdown)},
            )
            # Raise instead of sys.exit so the caller's try/except can
            # run the graceful shutdown sequence before the process exits.
            raise RuntimeError(
                "No Pyrogram handlers registered — bot would be deaf. "
                "Check that all handler modules imported correctly."
            )

        for group_id in sorted(breakdown.keys()):
            names = breakdown[group_id]
            logger.info(
                "lifecycle_audit_group_registered",
                extra={
                    "ctx_group_id": group_id,
                    "ctx_handler_count": len(names),
                    "ctx_handlers": names,
                },
            )

        logger.info(
            "lifecycle_audit_complete",
            extra={
                "ctx_total_handlers": total_handlers,
                "ctx_group_count": len(breakdown),
            },
        )

        if total_handlers < 5:
            logger.warning(
                "lifecycle_audit_few_handlers",
                extra={"ctx_total": total_handlers},
            )

        return total_handlers

    # ── Graceful shutdown ──────────────────────────────────────────────────────

    async def stop(self) -> None:
        """
        Shuts down all managed components in reverse boot order.
        Each step is individually guarded — one failure must not prevent
        subsequent cleanup steps from running.
        """
        logger.info("lifecycle_shutdown_start")

        # Cancel payment recovery task if it is still running.
        if self._recovery_task and not self._recovery_task.done():
            self._recovery_task.cancel()
            try:
                await self._recovery_task
            except (asyncio.CancelledError, Exception):
                pass

        # Reconciliation worker — no background loop to stop; APScheduler
        # job will be removed when the engine stops.  Release lock if held.
        if self._reconciliation_worker:
            try:
                await self._reconciliation_worker._redis.delete(
                    MembershipReconciliationWorker.LOCK_KEY
                )
            except Exception:
                pass

        # Watermark pool
        if self._watermark_pool:
            try:
                await self._watermark_pool.stop()
            except Exception:
                logger.error("lifecycle_shutdown_watermark_pool_failed", exc_info=True)

        # Referral scheduler
        if self._referral_scheduler:
            try:
                await self._referral_scheduler.stop()
            except Exception:
                logger.error("lifecycle_shutdown_referral_failed", exc_info=True)

        # Subscription worker
        if self._subscription_worker:
            try:
                await self._subscription_worker.stop()
            except Exception:
                logger.error("lifecycle_shutdown_sub_worker_failed", exc_info=True)

        # Cleanup worker
        if self._cleanup_worker:
            try:
                await self._cleanup_worker.stop()
            except Exception:
                logger.error("lifecycle_shutdown_cleanup_worker_failed", exc_info=True)

        # Distribution engine (owns the APScheduler — stops all registered jobs)
        if self._engine and self._engine.is_running:
            try:
                await asyncio.wait_for(self._engine.stop(), timeout=45.0)
            except asyncio.TimeoutError:
                logger.warning("lifecycle_shutdown_engine_timeout")
            except Exception:
                logger.error("lifecycle_shutdown_engine_failed", exc_info=True)

        # Telegram client
        try:
            if self._bot and getattr(self._bot, "is_connected", False):
                await self._bot.stop()
        except Exception:
            logger.error("lifecycle_shutdown_bot_failed", exc_info=True)

        # Database
        try:
            await DatabaseManager.disconnect()
        except Exception:
            logger.error("lifecycle_shutdown_db_failed", exc_info=True)

        # Health server
        if self._health_runner:
            try:
                await self._health_runner.cleanup()
            except Exception:
                logger.error("lifecycle_shutdown_health_failed", exc_info=True)

        self._running = False
        logger.info("lifecycle_shutdown_complete")

    # ── Config validation ──────────────────────────────────────────────────────

    def _validate_config(self) -> None:
        """
        Checks that all mandatory environment variables / settings are set.
        Calls sys.exit(1) immediately on any missing value so the container
        restart policy can surface the misconfiguration clearly in logs.
        """
        required = [
            ("MONGO_URI",             getattr(settings, "MONGO_URI", None)),
            ("MONGO_DB_NAME",         getattr(settings, "MONGO_DB_NAME", None)),
            ("BOT_TOKEN",             getattr(settings, "BOT_TOKEN", None)),
            ("API_ID",                getattr(settings, "API_ID", None)),
            ("API_HASH",              getattr(settings, "API_HASH", None)),
            ("VERIFICATION_GROUP_ID", getattr(settings, "VERIFICATION_GROUP_ID", None)),
            ("VAULT_CHANNEL_ID",      getattr(settings, "VAULT_CHANNEL_ID", None)),
            ("ADMIN_IDS",             getattr(settings, "ADMIN_IDS", None)),
            ("REDIS_URL",             getattr(settings, "REDIS_URL", None)),
        ]
        missing = [name for name, val in required if not val]
        if missing:
            logger.error(
                "lifecycle_config_validation_failed",
                extra={"ctx_missing": missing},
            )
            sys.exit(1)