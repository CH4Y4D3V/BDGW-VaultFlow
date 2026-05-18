import asyncio
import os
import signal

from app.core.logger import setup_logging, get_logger
from app.core.lifecycle import AppLifecycle
from app.health import start_health_server

logger = get_logger("main")


async def async_main() -> None:
    setup_logging(level="INFO")
    logger.info("Initializing BDGW VaultFlow main process...")

    lifecycle = AppLifecycle()
    stop_event = asyncio.Event()
    health_runner = None

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
        await lifecycle.start()

        # F1: Start HTTP health check server
        port = int(os.environ.get("PORT", 8080))
        health_runner = await start_health_server(port)

        logger.info("Main loop running. Waiting for shutdown signal.")
        await stop_event.wait()
    except asyncio.CancelledError:
        logger.info("Main loop cancelled via async propagation")
    finally:
        # F1: Clean up health server before stopping lifecycle
        if health_runner is not None:
            try:
                await health_runner.cleanup()
            except Exception:
                pass

        await lifecycle.stop()
        logger.info("Main process exit complete")


def main() -> None:
    asyncio.run(async_main())


if __name__ == "__main__":
    main()