import asyncio
import signal
from typing import Optional, Callable
from app.config import settings
from app.core.database import DatabaseManager
from app.scheduler.scheduler import DistributionScheduler
from app.watermark.dispatcher_worker import DispatcherWorkerPool
from app.watermark.worker_pool import WatermarkWorkerPool
from app.distribution.rate_limiter import RateLimiterService
from app.distribution.flood_wait import FloodWaitHandler
from app.distribution.target_balancer import TargetBalancer
from app.core.supervision import SystemSupervisor
from app.utils.logger import get_logger

logger = get_logger(__name__)


class DistributionEngine:
    """
    Top-level engine that wires scheduler, dispatcher workers,
    and watermark workers into a single lifecycle.

    Usage:
        engine = DistributionEngine(
            delivery_callback=my_telegram_send_fn,
            content_provider_callback=my_content_fetcher,
        )
        await engine.start()
        # ... run until shutdown signal
        await engine.stop()

    delivery_callback signature:
        async def deliver(job_docs: list[dict], target_id: str) -> None

    content_provider_callback signature:
        async def get_channels() -> list[dict]
        Each dict must have:
            - source_channel_id: str
            - target_channel_ids: list[str]
            - content: list[dict]  (each with content_id, media_type, ...)
            - watermark_config: Optional[dict]

    IMPORTANT: The engine assumes DatabaseManager.connect() has already been called
    by the application lifecycle (AppLifecycle). The engine does NOT call connect()
    or disconnect() — doing so would double-connect or tear down a shared connection.
    """

    def __init__(
        self,
        delivery_callback: Callable,
        content_provider_callback: Callable,
        dispatcher_worker_count: Optional[int] = None,
        watermark_worker_count: Optional[int] = None,
    ):
        self._delivery_callback = delivery_callback
        self._content_provider_callback = content_provider_callback
        self._dispatcher_count = dispatcher_worker_count
        self._watermark_count = watermark_worker_count

        self._scheduler: Optional[DistributionScheduler] = None
        self._worker_pool: Optional[DispatcherWorkerPool] = None
        self._watermark_pool: Optional[WatermarkWorkerPool] = None

        # shared singletons across all distribution workers
        self._rate_limiter = RateLimiterService()
        self._flood_handler = FloodWaitHandler()
        self._balancer = TargetBalancer()
        self._supervisor = SystemSupervisor()

        # FIX: Ensure persistent floodwaits are loaded before workers start
        self._flood_handler.load_from_redis()

        self._running = False

    async def start(self) -> None:
        if self._running:
            logger.warning("Engine.start() called but engine is already running")
            return

        logger.info("Distribution engine starting")

        # Bug 7 fix: do NOT call DatabaseManager.connect() here.
        # AppLifecycle.start() already called it before engine.start().
        # Calling it again would either no-op (if _initialized guard is in place) or
        # re-open a second connection pool — both are wrong for a shared singleton.
        db = DatabaseManager.get_db()

        self._worker_pool = DispatcherWorkerPool(
            db=db,
            delivery_callback=self._delivery_callback,
            rate_limiter=self._rate_limiter,
            flood_handler=self._flood_handler,
            target_balancer=self._balancer,
            worker_count=self._dispatcher_count,
        )
        await self._worker_pool.start()

        self._watermark_pool = WatermarkWorkerPool(
            db=db,
            worker_count=self._watermark_count,
        )
        await self._watermark_pool.start()

        self._scheduler = DistributionScheduler(
            db=db,
            content_provider_callback=self._content_provider_callback,
        )
        await self._scheduler.start()

        await self._supervisor.start()

        self._running = True
        logger.info(
            "Distribution engine started",
            extra={
                "ctx_dispatchers": self._dispatcher_count or settings.DISPATCHER_WORKER_COUNT,
                "ctx_watermark_workers": self._watermark_count or settings.WATERMARK_WORKER_COUNT,
            },
        )

    async def stop(self) -> None:
        if not self._running:
            return

        logger.info("Distribution engine shutting down")

        if self._supervisor:
            await self._supervisor.stop()

        if self._scheduler:
            await self._scheduler.stop()

        if self._worker_pool:
            await self._worker_pool.stop()

        if self._watermark_pool:
            await self._watermark_pool.stop()

        # Bug 7 fix: do NOT call DatabaseManager.disconnect() here.
        # AppLifecycle.stop() owns the DB connection lifecycle and will call
        # disconnect() in the correct shutdown order AFTER the engine stops.
        # Calling it here would tear down the shared connection while other
        # components (e.g. subscription worker) may still be using it.

        self._running = False
        logger.info("Distribution engine stopped cleanly")

    async def run_until_stopped(self) -> None:
        """Convenience method for running until SIGINT/SIGTERM."""
        loop = asyncio.get_running_loop()
        stop_event = asyncio.Event()

        def _signal_handler():
            logger.info("Shutdown signal received, initiating graceful teardown...")
            stop_event.set()

        for sig in (signal.SIGINT, signal.SIGTERM):
            try:
                loop.add_signal_handler(sig, _signal_handler)
            except NotImplementedError:
                pass  # Ignore on Windows

        await self.start()
        try:
            await stop_event.wait()
        except asyncio.CancelledError:
            logger.info("Run loop cancelled, triggering shutdown...")
        finally:
            await self.stop()

    @property
    def scheduler(self) -> Optional[DistributionScheduler]:
        return self._scheduler

    @property
    def worker_pool(self) -> Optional[DispatcherWorkerPool]:
        return self._worker_pool

    @property
    def watermark_pool(self) -> Optional[WatermarkWorkerPool]:
        return self._watermark_pool

    @property
    def is_running(self) -> bool:
        return self._running