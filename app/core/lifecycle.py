import asyncio
import os
import sys
import pkgutil
import importlib
from typing import Optional, Any

from app.config import settings
from app.core.database import DatabaseManager
from app.core.logger import get_logger
from app.distribution.engine import DistributionEngine
from app.services.channel_service import ChannelService
from app.bot.client import get_bot, set_bot_id
from app.workers.subscription_worker import SubscriptionWorker
from app.health import start_health_server

# -----------------------------------------------------------------------------
# HANDLER REGISTRATION
# -----------------------------------------------------------------------------
# To ensure all handlers are registered, we dynamically import all modules
# from the `app.handlers` package. This is more robust than relying on the
# `plugins` dictionary in the Client constructor, which can fail silently.

def _load_all_handlers() -> None:
    """Dynamically discover and import all modules in the handlers package."""
    import pkgutil
    import importlib
    import traceback
    from app import handlers as handlers_package

    print(f"[HANDLER LOADER] Scanning: {handlers_package.__path__}")
    
    count = 0
    errors = 0

    for _, module_name, _ in pkgutil.walk_packages(
        handlers_package.__path__,
        prefix=f"{handlers_package.__name__}."
    ):
        try:
            importlib.import_module(module_name)
            print(f"[HANDLER LOADER] OK: {module_name}")
            count += 1
        except Exception as exc:
            print(f"[HANDLER LOADER] FAILED: {module_name}")
            print(f"[HANDLER LOADER] ERROR: {exc}")
            traceback.print_exc()
            errors += 1

    print(f"[HANDLER LOADER] Complete: {count} loaded, {errors} failed")
    
    if count == 0:
        print("[HANDLER LOADER] CRITICAL: Zero handlers loaded — aborting")
        import sys
        sys.exit(1)

_load_all_handlers()

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
        self._bot = get_bot()
        self._health_runner: Optional[Any] = None
        self._running = False

    async def start(self) -> None:
        if self._running:
            return

        logger.info("Bootstrapping VaultFlow runtime environment...")

        # 0. Start health server immediately for Railway liveness probe
        try:
            port = int(os.environ.get("PORT", 8080))
            self._health_runner = await start_health_server(port)
        except Exception:
            logger.error("Failed to start health server", exc_info=True)

        # 1. Config Validation
        self._validate_config()

        # 2. Database
        try:
            await DatabaseManager.connect()

            channel_service = ChannelService()
            await channel_service.seed_channels()
        except Exception:
            logger.error("Failed to connect to MongoDB", exc_info=True)
            sys.exit(1)

        # 3. Telegram Client
        try:
            logger.info("Starting Pyrogram client...")
            await self._bot.start()
            me = await self._bot.get_me()
            logger.info(
                "Telegram client connected",
                extra={"ctx_bot_username": me.username, "ctx_bot_id": me.id},
            )

            # FIX 8: Cache the bot's own user_id so group_handler can check
            # message.from_user.id == get_bot_id() without calling get_me() per message.
            set_bot_id(me.id)

            # Verify bot can access critical channels
            await self._verify_channel_access()

            # ── RC-7 / RC-1 FIX: Deep handler registration audit ─────────────
            self._audit_handler_registration()

        except (Exception, SystemExit):
            logger.error("Failed to start Pyrogram client", exc_info=True)
            await DatabaseManager.disconnect()
            sys.exit(1)

        # 4. Distribution Engine
        from app.bot.delivery import execute_telegram_delivery
        from app.bot.provider import fetch_distribution_content

        self._engine = DistributionEngine(
            delivery_callback=execute_telegram_delivery,
            content_provider_callback=fetch_distribution_content,
        )

        try:
            await self._engine.start()
        except Exception:
            logger.error("Failed to start Distribution Engine", exc_info=True)
            await self.stop()
            sys.exit(1)

        # 5. Subscription Worker
        self._subscription_worker = SubscriptionWorker()
        try:
            await self._subscription_worker.start(bot=self._bot)
        except Exception:
            logger.error("Failed to start Subscription Worker", exc_info=True)
            self._subscription_worker = None

        self._running = True
        logger.info("VaultFlow fully started — all systems operational.")

    async def _verify_channel_access(self) -> None:
        """
        Verify the bot can access critical channels at startup.
        This forces Pyrogram to cache the peer. Only the VAULT_CHANNEL_ID is
        considered a critical failure that aborts startup.
        """
        logger.info("Verifying access to critical channels...")
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
                # This is already checked in _validate_config for required ones,
                # but as a safeguard:
                if is_critical:
                    logger.critical(f"CRITICAL: {name} is not configured in environment. Aborting.")
                    critical_failure = True
                continue

            try:
                channel_id = int(raw_id)
                chat = await self._bot.get_chat(channel_id)
                logger.info(f"✅ Access confirmed for {name}: '{chat.title}' ({chat.id})")
            except Exception as e:
                log_msg = (
                    f"Failed to access channel {name} ({raw_id}). "
                    f"Ensure the bot is a member with appropriate permissions. Error: {e}"
                )
                if is_critical:
                    logger.critical(f"CRITICAL FAILURE: {log_msg}")
                    critical_failure = True
                else:
                    logger.warning(f"WARNING: {log_msg}")

        if critical_failure:
            logger.critical(
                "Aborting startup due to critical channel access failure. "
                "Please check bot membership and permissions in the VAULT_CHANNEL_ID."
            )
            sys.exit(1)

    def _audit_handler_registration(self) -> None:
        """
        Emit a detailed breakdown of all registered Pyrogram handlers.
        """
        total_handlers = 0
        breakdown: dict[int, list[str]] = {}

        try:
            dispatcher = getattr(self._bot, "dispatcher", None)
            if dispatcher is None:
                logger.error(
                    "STARTUP AUDIT: bot.dispatcher is None — "
                    "Pyrogram plugin system may not have initialised"
                )
                return

            groups = getattr(dispatcher, "groups", None)
            if groups is None:
                logger.error(
                    "STARTUP AUDIT: bot.dispatcher.groups is None — "
                    "cannot verify handler registration"
                )
                return

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
                "STARTUP AUDIT: handler inspection failed",
                extra={"ctx_error": str(e)},
                exc_info=True,
            )
            return

        if total_handlers == 0:
            logger.critical(
                "STARTUP AUDIT CRITICAL: ZERO handlers registered. "
                "The bot is connected but will not respond to ANY update. "
                "Check that app/handlers/ contains valid plugin files and that "
                "Pyrogram loaded them successfully (no import errors at startup). "
                "Aborting — a silent deaf bot is worse than not starting.",
                extra={"ctx_groups": dict(breakdown)},
            )
            sys.exit(1)

        for group_id in sorted(breakdown.keys()):
            names = breakdown[group_id]
            logger.info(
                "STARTUP AUDIT: handler group registered",
                extra={
                    "ctx_group_id": group_id,
                    "ctx_handler_count": len(names),
                    "ctx_handlers": names,
                },
            )

        logger.info(
            "STARTUP AUDIT: handler registration complete",
            extra={
                "ctx_total_handlers": total_handlers,
                "ctx_group_count": len(breakdown),
            },
        )

        if total_handlers < 5:
            logger.warning(
                "STARTUP AUDIT WARNING: very few handlers registered (%d). "
                "Plugin loading may have partially failed. "
                "Check for import errors in app/handlers/ files.",
                total_handlers,
                extra={"ctx_total": total_handlers},
            )

    async def stop(self) -> None:
        logger.info("Initiating graceful shutdown...")

        if self._subscription_worker:
            try:
                await self._subscription_worker.stop()
            except Exception:
                logger.error("Error stopping subscription worker", exc_info=True)

        if self._engine and self._engine.is_running:
            try:
                await asyncio.wait_for(self._engine.stop(), timeout=45.0)
            except asyncio.TimeoutError:
                logger.warning("Engine shutdown timed out after 45 seconds")
            except Exception:
                logger.error("Error during engine shutdown", exc_info=True)

        if self._bot and getattr(self._bot, "is_connected", False):
            try:
                await self._bot.stop()
            except Exception:
                logger.error("Error stopping Pyrogram client", exc_info=True)

        try:
            await DatabaseManager.disconnect()
        except Exception:
            logger.error("Error disconnecting MongoDB", exc_info=True)

        if self._health_runner:
            try:
                await self._health_runner.cleanup()
            except Exception:
                logger.error("Error stopping health server", exc_info=True)

        self._running = False
        logger.info("Shutdown complete.")

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
                "Missing required environment variables",
                extra={"ctx_missing": missing},
            )
            sys.exit(1)
