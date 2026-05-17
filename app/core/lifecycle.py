import asyncio
import sys
from typing import Optional

from app.config import settings
from app.core.database import DatabaseManager
from app.core.logger import get_logger
from app.distribution.engine import DistributionEngine
from app.bot.client import get_bot
from app.workers.subscription_worker import SubscriptionWorker

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
        self._running = False

    async def start(self) -> None:
        if self._running:
            return

        logger.info("Bootstrapping VaultFlow runtime environment...")

        # 1. Config Validation
        self._validate_config()

        # 2. Database
        try:
            await DatabaseManager.connect()
        except Exception:
            logger.error("Failed to connect to MongoDB", exc_info=True)
            sys.exit(1)

        # 3. Telegram Client
        try:
            logger.info("Starting Pyrogram client...")
            await self._bot.start()
            me = await self._bot.get_me()
            logger.info("Telegram client connected", extra={"ctx_bot_username": me.username})

            handlers_count = 0
            if hasattr(self._bot, "dispatcher") and hasattr(self._bot.dispatcher, "groups"):
                for group_id, handlers in self._bot.dispatcher.groups.items():
                    logger.info(
                        "Handler group registered",
                        extra={"ctx_group_id": group_id, "ctx_handlers_count": len(handlers)},
                    )
                    handlers_count += len(handlers)

            if handlers_count == 0:
                logger.warning("No Pyrogram handlers registered — update routing may fail.")
            else:
                logger.info("Telegram update routing active", extra={"ctx_total_handlers": handlers_count})

        except Exception:
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
            # Non-fatal — bot can run without subscription sweeper
            self._subscription_worker = None

        self._running = True
        logger.info("VaultFlow fully started — all systems operational.")

    async def stop(self) -> None:
        logger.info("Initiating graceful shutdown...")

        # 1. Subscription Worker
        if self._subscription_worker:
            try:
                await self._subscription_worker.stop()
            except Exception:
                logger.error("Error stopping subscription worker", exc_info=True)

        # 2. Distribution Engine
        if self._engine and self._engine.is_running:
            try:
                await asyncio.wait_for(self._engine.stop(), timeout=45.0)
            except asyncio.TimeoutError:
                logger.warning("Engine shutdown timed out after 45 seconds")
            except Exception:
                logger.error("Error during engine shutdown", exc_info=True)

        # 3. Telegram Client
        if self._bot and getattr(self._bot, "is_connected", False):
            try:
                await self._bot.stop()
            except Exception:
                logger.error("Error stopping Pyrogram client", exc_info=True)

        # 4. MongoDB
        try:
            await DatabaseManager.disconnect()
        except Exception:
            logger.error("Error disconnecting MongoDB", exc_info=True)

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
