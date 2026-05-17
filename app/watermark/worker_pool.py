import asyncio
import uuid
from typing import Optional
from motor.motor_asyncio import AsyncIOMotorDatabase
from app.config import settings
from app.core.models import JobStatus, MediaType, WatermarkPosition
from app.core.exceptions import WatermarkProcessingError, MediaFileNotFoundError
from app.repositories.queue_repository import QueueRepository
from app.watermark.ffmpeg_processor import FFmpegProcessor
from app.utils.logger import get_logger

logger = get_logger(__name__)


class WatermarkWorker:
    """
    Pulls jobs in WATERMARKING status and processes them with FFmpeg.
    On completion, marks the job back to PENDING so the dispatcher picks it up.
    Runs as an independent async task — never shares state with dispatcher workers.
    """

    def __init__(
        self,
        worker_id: str,
        queue_repo: QueueRepository,
        ffmpeg: FFmpegProcessor,
    ):
        self._worker_id = worker_id
        self._queue = queue_repo
        self._ffmpeg = ffmpeg
        self._running = False
        self._task: Optional[asyncio.Task] = None

    async def start(self) -> None:
        if self._running:
            return
        self._running = True
        self._task = asyncio.create_task(self._run_loop(), name=f"watermark-{self._worker_id}")
        logger.info("Watermark worker started", extra={"ctx_worker": self._worker_id})

    async def stop(self) -> None:
        self._running = False
        if self._task:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
        logger.info("Watermark worker stopped", extra={"ctx_worker": self._worker_id})

    async def _run_loop(self) -> None:
        while self._running:
            try:
                jobs = await self._queue._queue.find(
                    {"status": JobStatus.WATERMARKING}
                ).limit(settings.WORKER_BATCH_SIZE).to_list(settings.WORKER_BATCH_SIZE)

                if not jobs:
                    await asyncio.sleep(settings.WORKER_POLL_INTERVAL)
                    continue

                tasks = [self._process_job(job) for job in jobs]
                await asyncio.gather(*tasks, return_exceptions=True)

            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(
                    "Watermark worker loop error",
                    extra={"ctx_worker": self._worker_id, "ctx_error": str(e)},
                    exc_info=True,
                )
                await asyncio.sleep(5)

    async def _process_job(self, job_doc: dict) -> None:
        job_id = str(job_doc["_id"])
        media_type = job_doc.get("media_type")
        media_path = job_doc.get("media_path")
        watermark_config = job_doc.get("watermark_config") or {}
        watermark_path = watermark_config.get("watermark_image_path")

        if not media_path or not watermark_path:
            logger.error(
                "Watermark job missing required paths",
                extra={
                    "ctx_job_id": job_id,
                    "ctx_media_path": media_path,
                    "ctx_watermark_path": watermark_path,
                },
            )
            await self._queue.mark_failed(job_id, "Missing media or watermark path")
            return

        position_str = watermark_config.get("position", settings.WATERMARK_POSITION)
        try:
            position = WatermarkPosition(position_str)
        except ValueError:
            position = WatermarkPosition.BOTTOM_RIGHT

        opacity = watermark_config.get("opacity", settings.WATERMARK_OPACITY)
        scale = watermark_config.get("scale", settings.WATERMARK_SCALE)

        try:
            logger.info(
                "Processing watermark",
                extra={
                    "ctx_job_id": job_id,
                    "ctx_media_type": media_type,
                    "ctx_worker": self._worker_id,
                },
            )

            if media_type == MediaType.VIDEO:
                processed_path = await self._ffmpeg.apply_video_watermark(
                    input_path=media_path,
                    watermark_path=watermark_path,
                    position=position,
                    opacity=opacity,
                    scale=scale,
                )
            elif media_type == MediaType.PHOTO:
                processed_path = await self._ffmpeg.apply_image_watermark(
                    input_path=media_path,
                    watermark_path=watermark_path,
                    position=position,
                    opacity=opacity,
                    scale=scale,
                )
            else:
                # Non-visual media — skip watermark, push to dispatch
                await self._queue.mark_watermark_applied(job_id, media_path)
                return

            await self._queue.mark_watermark_applied(job_id, processed_path)
            logger.info(
                "Watermark applied successfully",
                extra={
                    "ctx_job_id": job_id,
                    "ctx_output": processed_path,
                },
            )

        except (WatermarkProcessingError, MediaFileNotFoundError) as e:
            logger.error(
                "Watermark processing failed",
                extra={"ctx_job_id": job_id, "ctx_error": str(e)},
                exc_info=True,
            )
            await self._queue.mark_failed(job_id, str(e))

        except Exception as e:
            logger.error(
                "Unexpected watermark error",
                extra={"ctx_job_id": job_id, "ctx_error": str(e)},
                exc_info=True,
            )
            await self._queue.mark_failed(job_id, f"Unexpected: {e}")


class WatermarkWorkerPool:
    """Manages N watermark worker tasks."""

    def __init__(self, db: AsyncIOMotorDatabase, worker_count: Optional[int] = None):
        self._db = db
        self._worker_count = worker_count or settings.WATERMARK_WORKER_COUNT
        self._workers: list[WatermarkWorker] = []
        self._ffmpeg = FFmpegProcessor()
        self._queue_repo = QueueRepository(db)

    async def start(self) -> None:
        for i in range(self._worker_count):
            worker_id = f"wm-worker-{i}"
            worker = WatermarkWorker(
                worker_id=worker_id,
                queue_repo=self._queue_repo,
                ffmpeg=self._ffmpeg,
            )
            self._workers.append(worker)
            await worker.start()

        logger.info(
            f"Watermark pool started with {self._worker_count} workers",
            extra={"ctx_count": self._worker_count},
        )

    async def stop(self) -> None:
        for worker in self._workers:
            await worker.stop()
        self._workers.clear()
        logger.info("Watermark pool stopped")
