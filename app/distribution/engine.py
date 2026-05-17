import asyncio
from typing import Optional, Callable
from motor.motor_asyncio import AsyncIOMotorDatabase
from app.config import settings
from app.services.database import DatabaseManager
from app.scheduler.scheduler import DistributionScheduler
from app.workers.worker_pool import WorkerPool
from app.watermark.worker_pool import WatermarkWorkerPool
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
        async def deliver(job_doc: dict, target_id: str) -> None

    content_provider_callback signature:
        async def get_channels() -> list[dict]
        Each dict must have:
            - source_channel_id: str
            - target_channel_ids: list[str]
            - content: list[dict]  (each with content_id, media_type, ...)
            - watermark_config: Optional[dict]
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
        self._worker_pool: Optional[WorkerPool] = None
        self._watermark_pool: Optional[WatermarkWorkerPool] = None
        self._running = False

    async def start(self) -> None:
        if self._running:
            logger.warning("Engine.start() called but engine is already running")
            return

        logger.info("Distribution engine starting")

        await DatabaseManager.connect()
        db = DatabaseManager.get_db()

        self._worker_pool = WorkerPool(
            db=db,
            delivery_callback=self._delivery_callback,
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

        if self._scheduler:
            await self._scheduler.stop()

        if self._worker_pool:
            await self._worker_pool.stop()

        if self._watermark_pool:
            await self._watermark_pool.stop()

        await DatabaseManager.disconnect()

        self._running = False
        logger.info("Distribution engine stopped cleanly")

    async def run_until_stopped(self) -> None:
        """Convenience method for running until SIGINT/SIGTERM."""
        await self.start()
        try:
            while self._running:
                await asyncio.sleep(1)
        except (asyncio.CancelledError, KeyboardInterrupt):
            pass
        finally:
            await self.stop()

    @property
    def scheduler(self) -> Optional[DistributionScheduler]:
        return self._scheduler

    @property
    def worker_pool(self) -> Optional[WorkerPool]:
        return self._worker_pool

    @property
    def watermark_pool(self) -> Optional[WatermarkWorkerPool]:
        return self._watermark_pool

    @property
    def is_running(self) -> bool:
        return self._running
