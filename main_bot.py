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
            pass  # Windows compatibility fallback

    try:
        # NOTE: AppLifecycle.start() already starts the health server internally
        # on the PORT env var (default 8080). Do NOT call start_health_server()
        # here — doing so binds the same port twice and raises OSError: [Errno 98]
        # Address already in use.
        await lifecycle.start()

        logger.info("boot_stage", stage="idle_entered")
        logger.info("Main loop running. Waiting for shutdown signal.")
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
