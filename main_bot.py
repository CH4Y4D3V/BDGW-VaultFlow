# main_bot.py — COMPLETE FIXED FILE
import asyncio
import signal

from app.core.logger import setup_logging, get_logger
from app.core.lifecycle import AppLifecycle

logger = get_logger("main")


async def async_main() -> None:
    setup_logging(level="INFO")
    logger.info("Initializing BDGW VaultFlow main process...")

    lifecycle = AppLifecycle()
    stop_event = asyncio.Event()

    def _signal_handler() -> None:
        logger.info("OS Signal trapped, marking stop event for graceful shutdown...")
        stop_event.set()

    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, _signal_handler)
        except NotImplementedError:
            pass

    try:
        await lifecycle.start()
        logger.info("VaultFlow idle — waiting for shutdown signal.", extra={"ctx_stage": "idle_entered"})
        await stop_event.wait()
    except asyncio.CancelledError:
        logger.info("Main loop cancelled via async propagation")
    finally:
        await lifecycle.stop()
        logger.info("Main process exit complete")


def main() -> None:
    asyncio.run(async_main())


if __name__ == "__main__":
    main()