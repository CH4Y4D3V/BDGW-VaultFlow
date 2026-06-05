import asyncio
import os
import sys
from typing import Optional, Any

from app.config import settings
from app.core.database import DatabaseManager
from app.core.logger import get_logger
from app.distribution.engine import DistributionEngine
from app.services.channel_service import ChannelService
from app.bot.client import get_bot, set_bot_id
from app.workers.subscription_worker import SubscriptionWorker
from app.health import start_health_server
from app.referral.scheduler import ReferralScheduler


logger = get_logger(__name__)


class AppLifecycle:
    """
    Manages the global application boot and shutdown sequence.

    Boot order (strict):
      Config validation → Logging → DB → Telegram → Engine/Workers → Subscription Worker

    Shutdown order (reverse):
      Subscription Worker → Engine → Telegram → DB
    """

    def __init__(self):
        self._engine: Optional[DistributionEngine] = None
        self._subscription_worker: Optional[SubscriptionWorker] = None
        self._cleanup_worker: Optional[Any] = None
        self._referral_scheduler: Optional[ReferralScheduler] = None
        self._bot = get_bot()
        self._health_runner: Optional[Any] = None
        self._running = False

    async def start(self) -> None:
        if self._running:
            return

        logger.info("lifecycle_bootstrapping_start")

        # 0. Start health server immediately for Railway liveness probe
        try:
            port = int(os.environ.get("PORT", 8080))
            self._health_runner = await start_health_server(port)
        except Exception:
            logger.error("lifecycle_health_server_failed", exc_info=True)

        # 1. Config Validation
        self._validate_config()

        # 2. Database
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

        # 3. Telegram Client
        try:
            logger.info("lifecycle_bot_start")
            await self._bot.start()
            me = await self._bot.get_me()
            set_bot_id(me.id)

            # FLOW 2: Register exactly 3 bot commands
            from pyrogram.types import BotCommand
            try:
                await self._bot.set_bot_commands([
                    BotCommand("start", "Main Menu"),
                    BotCommand("takedown", "Remove Content"),
                    BotCommand("help", "Support"),
                ])
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
                logger.info("lifecycle_topic_cache_restored")
            except Exception as e:
                logger.warning(
                    "lifecycle_topic_cache_restore_failed",
                    extra={"ctx_error": str(e)}
                )

        except Exception as e:
            logger.exception(
                "lifecycle_bot_start_failed",
                extra={"ctx_error_type": type(e).__name__, "ctx_error": str(e)},
            )
            raise

        # 4. Distribution Engine
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

        # 5. Subscription Worker
        self._subscription_worker = SubscriptionWorker()
        try:
            await self._subscription_worker.start(bot=self._bot)
        except Exception:
            logger.error("lifecycle_subscription_worker_failed", exc_info=True)
            self._subscription_worker = None

        # 6. Referral System Integration
        # FIX: Each sub-step isolated so one failure doesn't kill the rest.
        # Bot is fully started at this point — safe to pass to ReferralService.
        try:
            from app.referral.repository import ReferralRepository
            from app.referral.service import ReferralService
            from app.handlers.membership_handler import init_membership_handler

            ref_repo = ReferralRepository(DatabaseManager.get_db())

            # create_indexes is now per-index fault-tolerant
            try:
                await ref_repo.create_indexes()
            except Exception as idx_err:
                logger.error(
                    "lifecycle_referral_index_failed",
                    extra={"ctx_error": str(idx_err)},
                )

            # Bot is started — safe to instantiate ReferralService
            ref_service = ReferralService(ref_repo, self._bot)

            if self._engine and self._engine.scheduler:
                raw_scheduler = self._engine.scheduler._scheduler
                self._referral_scheduler = ReferralScheduler(
                    service=ref_service,
                    scheduler=raw_scheduler,
                    channel_id=int(settings.VAULT_CHANNEL_ID),
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

        # 7. Payment Timeout Monitor
        # FIX: Isolated block. Bot reference is passed at call time, not construction.
        try:
            from app.payments.repository import PaymentRepository
            from app.payments.timeouts import PaymentTimeoutMonitor
            from app.payments import get_payment_service

            payment_repo = PaymentRepository(DatabaseManager.get_db())
            timeout_monitor = PaymentTimeoutMonitor(payment_repo)
            bot_ref = self._bot  # capture for closure

            if self._engine and self._engine.scheduler:
                raw_scheduler = self._engine.scheduler._scheduler
                raw_scheduler.add_job(
                    timeout_monitor.check_timeouts,
                    "interval",
                    minutes=1,
                    # FIX: pass bot as keyword arg to match check_timeouts(client) signature
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

            # Resume active sessions — method now exists on PaymentService
            payment_service = get_payment_service()
            asyncio.create_task(payment_service.resume_active_sessions())
            logger.info("lifecycle_payment_recovery_initiated")

        except Exception as e:
            logger.error(
                "lifecycle_payment_monitor_failed",
                extra={"ctx_error": str(e)},
                exc_info=True,
            )

        # 8. Support Ticket Monitor
        try:
            from app.services.support_monitor import SupportMonitor
            support_monitor = SupportMonitor(self._bot)

            if self._engine and self._engine.scheduler:
                raw_scheduler = self._engine.scheduler._scheduler
                raw_scheduler.add_job(
                    support_monitor.check_inactivity,
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

        # 9. Message Cleanup Worker
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

        self._running = True
        logger.info("lifecycle_startup_complete")

    async def _verify_channel_access(self) -> None:
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
                        extra={"ctx_error": str(e)},
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

    def _audit_handler_registration(self) -> int:
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
                handler_names = []
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
            sys.exit(1)

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

    async def stop(self) -> None:
        logger.info("lifecycle_shutdown_start")

        if self._referral_scheduler:
            try:
                await self._referral_scheduler.stop()
            except Exception:
                logger.error("lifecycle_shutdown_referral_failed", exc_info=True)

        if self._subscription_worker:
            try:
                await self._subscription_worker.stop()
            except Exception:
                logger.error("lifecycle_shutdown_sub_worker_failed", exc_info=True)

        if self._cleanup_worker:
            try:
                await self._cleanup_worker.stop()
            except Exception:
                logger.error("lifecycle_shutdown_cleanup_worker_failed", exc_info=True)

        if self._engine and self._engine.is_running:
            try:
                await asyncio.wait_for(self._engine.stop(), timeout=45.0)
            except asyncio.TimeoutError:
                logger.warning("lifecycle_shutdown_engine_timeout")
            except Exception:
                logger.error("lifecycle_shutdown_engine_failed", exc_info=True)

        if self._bot and getattr(self._bot, "is_connected", False):
            try:
                await self._bot.stop()
            except Exception:
                logger.error("lifecycle_shutdown_bot_failed", exc_info=True)

        try:
            await DatabaseManager.disconnect()
        except Exception:
            logger.error("lifecycle_shutdown_db_failed", exc_info=True)

        if self._health_runner:
            try:
                await self._health_runner.cleanup()
            except Exception:
                logger.error("lifecycle_shutdown_health_failed", exc_info=True)

        self._running = False
        logger.info("lifecycle_shutdown_complete")

    def _validate_config(self) -> None:
        required = [
            ("MONGO_URI", getattr(settings, "MONGO_URI", None)),
            ("MONGO_DB_NAME", getattr(settings, "MONGO_DB_NAME", None)),
            ("BOT_TOKEN", getattr(settings, "BOT_TOKEN", None)),
            ("API_ID", getattr(settings, "API_ID", None)),
            ("API_HASH", getattr(settings, "API_HASH", None)),
            ("VERIFICATION_GROUP_ID", getattr(settings, "VERIFICATION_GROUP_ID", None)),
            ("VAULT_CHANNEL_ID", getattr(settings, "VAULT_CHANNEL_ID", None)),
        ]
        missing = [name for name, val in required if not val]
        if missing:
            logger.error(
                "lifecycle_config_validation_failed",
                extra={"ctx_missing": missing},
            )
            sys.exit(1)
