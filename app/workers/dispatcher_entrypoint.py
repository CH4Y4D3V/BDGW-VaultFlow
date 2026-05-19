"""
Standalone entry point for dispatcher worker processes.

Run as a separate container/process from the bot:
    python -m app.workers.dispatcher_entrypoint

This process:
  - Connects to MongoDB
  - Does NOT start the Pyrogram client (uses get_bot() for delivery only,
    which reuses the singleton session file written by the bot process)
  - Starts DispatcherWorkerPool and WatermarkWorkerPool
  - Runs until SIGINT/SIGTERM

The bot process owns the Pyrogram client lifecycle. Worker processes share
the same session file and MongoDB connection but run independently.
"""

import asyncio
import signal
import sys

from app.core.database import DatabaseManager
from app.core.logger import setup_logging, get_logger
from app.bot.client import get_bot
from app.bot.delivery import execute_telegram_delivery
from app.bot.provider import fetch_distribution_content
from app.watermark.dispatcher_worker import DispatcherWorkerPool
from app.distribution.rate_limiter import RateLimiterService
from app.distribution.flood_wait import FloodWaitHandler
from app.distribution.target_balancer import TargetBalancer

logger = get_logger(__name__)


async def run_dispatcher() -> None:
    setup_logging()
    logger.info("Dispatcher worker process starting")

    # Connect to MongoDB
    try:
        await DatabaseManager.connect()
    except Exception:
        logger.error("Dispatcher: failed to connect to MongoDB", exc_info=True)
        sys.exit(1)

    # Connect Pyrogram client (shares session with bot process)
    bot = get_bot()
    try:
        await bot.start()
        logger.info("Dispatcher: Pyrogram client connected")
    except Exception:
        logger.error("Dispatcher: failed to start Pyrogram client", exc_info=True)
        await DatabaseManager.disconnect()
        sys.exit(1)

    db = DatabaseManager.get_db()
    rate_limiter = RateLimiterService()
    flood_handler = FloodWaitHandler()
    balancer = TargetBalancer()

    pool = DispatcherWorkerPool(
        db=db,
        delivery_callback=execute_telegram_delivery,
        rate_limiter=rate_limiter,
        flood_handler=flood_handler,
        target_balancer=balancer,
    )

    stop_event = asyncio.Event()

    def _signal_handler():
        logger.info("Dispatcher: shutdown signal received")
        stop_event.set()

    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, _signal_handler)
        except NotImplementedError:
            pass

    await pool.start()
    logger.info("Dispatcher worker pool running")

    try:
        await stop_event.wait()
    except asyncio.CancelledError:
        pass
    finally:
        logger.info("Dispatcher: shutting down")
        await pool.stop()
        try:
            await bot.stop()
        except Exception:
            pass
        await DatabaseManager.disconnect()
        logger.info("Dispatcher worker process stopped")


def main() -> None:
    asyncio.run(run_dispatcher())


if __name__ == "__main__":
    main()