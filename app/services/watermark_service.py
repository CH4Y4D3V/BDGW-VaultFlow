"""
app/services/watermark_service.py
---------------------------------
Orchestrates the video/image watermarking process.

FIX L5-001: Removed all `settings.watermark.X` references — `settings` is a
flat pydantic-settings `Settings` object with no `.watermark` sub-attribute.
Every call was crashing with AttributeError at runtime. All references now
use `settings.X` directly, matching the actual field names in settings.py.
Also removed reference to the non-existent `WATERMARK_POLL_INTERVAL` setting;
falls back to `WORKER_POLL_INTERVAL` which is defined in settings.py.
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
        # FIX L5-001: was settings.watermark.WATERMARK_ENABLED (AttributeError)
        poll_interval = float(getattr(settings, "WORKER_POLL_INTERVAL", 2.0))
        while self._running:
            try:
                if not settings.WATERMARK_ENABLED:
                    await asyncio.sleep(60)
                    continue

                job = await self._queue_repo.get_next_watermark_job()
                if job:
                    logger.info("watermark_job_dispatching", extra={"ctx_job_id": job.id})
                    await self._pool.process_job(job)
                else:
                    # FIX L5-001: was settings.watermark.WATERMARK_POLL_INTERVAL (no such attr)
                    await asyncio.sleep(poll_interval)
            except asyncio.CancelledError:
                break
            except Exception:
                logger.exception("watermark_dispatcher_error")
                await asyncio.sleep(30)


def get_watermark_config(dest: str) -> dict | None:
    """
    Builds a watermark configuration dictionary for a given destination.

    Returns None if watermarking is disabled, the destination is unknown,
    or the logo file does not exist on disk.

    Args:
        dest: Either ``"nsfw"`` or ``"premium"``.

    Returns:
        A config dict for the watermark pipeline, or None.
    """
    # FIX L5-001: was settings.watermark.WATERMARK_ENABLED (AttributeError)
    if not settings.WATERMARK_ENABLED:
        return None

    if dest == "nsfw":
        logo_path = settings.WATERMARK_LOGO_PATH_NSFW
        text = settings.WATERMARK_TEXT_NSFW
    elif dest == "premium":
        logo_path = settings.WATERMARK_LOGO_PATH_PREMIUM
        text = settings.WATERMARK_TEXT_PREMIUM
    else:
        return None

    if not Path(logo_path).exists():
        logger.warning(
            "watermark_logo_missing",
            extra={"ctx_path": logo_path, "ctx_dest": dest},
        )
        return None

    # FIX L5-001: was settings.watermark.WATERMARK_* (AttributeError)
    return {
        "watermark_image_path": logo_path,
        "watermark_text": text,
        "position": settings.WATERMARK_POSITION,
        "opacity": settings.WATERMARK_OPACITY,
        "scale": settings.WATERMARK_SCALE,
        "rotation": settings.WATERMARK_ROTATION,
        "destination": dest,
    }
