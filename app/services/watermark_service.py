"""
app/services/watermark_service.py
---------------------------------
Orchestrates the video/image watermarking process.
"""
from __future__ import annotations

import asyncio
from pathlib import Path

from app.config import settings
from app.core.database import DatabaseManager
from app.repositories.queue_repository import QueueRepository
from app.watermark.worker_pool import WatermarkWorkerPool
from app.utils.logger import get_logger

logger = get_logger(__name__)


class WatermarkService:
    def __init__(self) -> None:
        self._db = DatabaseManager.get_db()
        self._queue_repo = QueueRepository(self._db)
        self._pool = WatermarkWorkerPool(db=self._db)
        self._task: asyncio.Task | None = None
        self._running = False

    async def start(self) -> None:
        if self._running:
            return
        logger.info("watermark_service_starting")
        self._running = True
        await self._pool.start()
        self._task = asyncio.create_task(self._run_dispatcher(), name="watermark_dispatcher")

    async def stop(self) -> None:
        if not self._running:
            return
        logger.info("watermark_service_stopping")
        self._running = False
        if self._task:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
        await self._pool.stop()
        logger.info("watermark_service_stopped")

    async def _run_dispatcher(self) -> None:
        while self._running:
            try:
                # NEW-10 FIX: Access nested WatermarkSettings object
                if not settings.watermark.WATERMARK_ENABLED:
                    await asyncio.sleep(60)
                    continue

                job = await self._queue_repo.get_next_watermark_job()
                if job:
                    logger.info("watermark_job_dispatching", extra={"ctx_job_id": job.id})
                    await self._pool.process_job(job)
                else:
                    await asyncio.sleep(settings.watermark.WATERMARK_POLL_INTERVAL)
            except asyncio.CancelledError:
                break
            except Exception:
                logger.exception("watermark_dispatcher_error")
                await asyncio.sleep(30)


def get_watermark_config(dest: str) -> dict | None:
    """
    Builds a watermark configuration dictionary for a given destination.
    """
    # NEW-10 FIX: Access nested WatermarkSettings object
    if not settings.watermark.WATERMARK_ENABLED:
        return None

    if dest == "nsfw":
        logo_path = settings.watermark.WATERMARK_LOGO_PATH_NSFW
        text = settings.watermark.WATERMARK_TEXT_NSFW
    elif dest == "premium":
        logo_path = settings.watermark.WATERMARK_LOGO_PATH_PREMIUM
        text = settings.watermark.WATERMARK_TEXT_PREMIUM
    else:
        return None

    if not Path(logo_path).exists():
        logger.warning(
            "watermark_logo_missing",
            extra={"ctx_path": logo_path, "ctx_dest": dest},
        )
        return None

    # NEW-10 FIX: Access nested WatermarkSettings object
    return {
        "watermark_image_path": logo_path,
        "watermark_text": text,
        "position": settings.watermark.WATERMARK_POSITION,
        "opacity": settings.watermark.WATERMARK_OPACITY,
        "scale": settings.watermark.WATERMARK_SCALE,
        "rotation": settings.watermark.WATERMARK_ROTATION,
        "destination": dest,
    }
