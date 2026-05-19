"""
Standalone entry point for watermark worker processes.

Run as a separate container/process from the bot:
    python -m app.workers.watermark_entrypoint

This process:
  - Connects to MongoDB
  - Connects Pyrogram client (for media downloads via get_bot())
  - Starts WatermarkWorkerPool
  - Runs until SIGINT/SIGTERM
"""

import asyncio
import signal
import sys

from app.core.database import DatabaseManager
from app.core.logger import setup_logging, get_logger
from app.bot.client import get_bot
from app.watermark.worker_pool import WatermarkWorkerPool

logger = get_logger(__name__)


async def run_watermark() -> None:
    setup_logging()
    logger.info("Watermark worker process starting")

    try:
        await DatabaseManager.connect()
    except Exception:
        logger.error("Watermark: failed to connect to MongoDB", exc_info=True)
        sys.exit(1)

    bot = get_bot()
    try:
        await bot.start()
        logger.info("Watermark: Pyrogram client connected")
    except Exception:
        logger.error("Watermark: failed to start Pyrogram client", exc_info=True)
        await DatabaseManager.disconnect()
        sys.exit(1)

    db = DatabaseManager.get_db()
    pool = WatermarkWorkerPool(db=db)

    stop_event = asyncio.Event()

    def _signal_handler():
        logger.info("Watermark: shutdown signal received")
        stop_event.set()

    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, _signal_handler)
        except NotImplementedError:
            pass

    await pool.start()
    logger.info("Watermark worker pool running")

    try:
        await stop_event.wait()
    except asyncio.CancelledError:
        pass
    finally:
        logger.info("Watermark: shutting down")
        await pool.stop()
        try:
            await bot.stop()
        except Exception:
            pass
        await DatabaseManager.disconnect()
        logger.info("Watermark worker process stopped")


def main() -> None:
    asyncio.run(run_watermark())


if __name__ == "__main__":
    main()