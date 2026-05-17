import asyncio
import sys
from typing import Optional

from app.config import settings
from app.core.database import DatabaseManager
from app.core.logger import get_logger
from app.distribution.engine import DistributionEngine
from app.bot.client import get_bot

logger = get_logger(__name__)


class AppLifecycle:
    """
    Manages the global application boot and shutdown sequence.
    Ensures strict ordering of dependency initialization and teardown:
    Config -> Logging -> DB -> Telegram -> Engine/Workers -> Scheduler.
    """

    def __init__(self):
        self._engine: Optional[DistributionEngine] = None
        self._bot = get_bot()
        self._running = False

    async def start(self) -> None:
        if self._running:
            return

        logger.info("Bootstrapping VaultFlow runtime environment...")

        # 1. Runtime Config Validation
        self._validate_config()

        # 2. Database Initialization
        try:
            await DatabaseManager.connect()
        except Exception:
            logger.error("Failed to connect to MongoDB", exc_info=True)
            sys.exit(1)

        # 3. Telegram Client Boot
        try:
            logger.info("Starting Pyrogram client...")
            await self._bot.start()
            me = await self._bot.get_me()
            logger.info("Telegram client connected", extra={"ctx_bot_username": me.username})
        except Exception:
            logger.error("Failed to start Pyrogram client", exc_info=True)
            await DatabaseManager.disconnect()
            sys.exit(1)

        # 4. Engine / Worker Orchestration Boot
        # Lazy imports for business logic callbacks to avoid circular dependencies during initial boot
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

        self._running = True
        logger.info("VaultFlow application lifecycle fully started")

    async def stop(self) -> None:
        logger.info("Initiating global graceful shutdown...")

        # 1. Stop Distribution Engine (Drains workers, stops APScheduler)
        if self._engine and self._engine.is_running:
            try:
                await asyncio.wait_for(self._engine.stop(), timeout=45.0)
            except asyncio.TimeoutError:
                logger.warning("Engine shutdown timed out after 45 seconds")
            except Exception:
                logger.error("Error during engine shutdown", exc_info=True)

        # 2. Stop Telegram Client
        if self._bot and getattr(self._bot, "is_connected", False):
            logger.info("Disconnecting Pyrogram client...")
            try:
                await self._bot.stop()
            except Exception:
                logger.error("Error stopping Pyrogram client", exc_info=True)

        # 3. Disconnect MongoDB
        try:
            await DatabaseManager.disconnect()
        except Exception:
            logger.error("Error disconnecting MongoDB", exc_info=True)

        self._running = False
        logger.info("VaultFlow application lifecycle fully stopped")

    def _validate_config(self) -> None:
        """Ensure absolute minimum configuration is present for safe boot."""
        required = [
            ("MONGO_URI", getattr(settings, "MONGO_URI", None)),
            ("MONGO_DB_NAME", getattr(settings, "MONGO_DB_NAME", None)),
        ]
        missing = [name for name, val in required if not val]
        if missing:
            logger.error(
                "CRITICAL: Missing required environment variables", 
                extra={"ctx_missing": missing}
            )
            sys.exit(1)