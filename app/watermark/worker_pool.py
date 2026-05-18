import asyncio
import uuid
from pathlib import Path
from typing import Optional
from motor.motor_asyncio import AsyncIOMotorDatabase
from app.bot.client import get_bot
from app.config import settings
from app.core.logger import set_correlation_id, reset_correlation_id
from app.core.models import MediaType, WatermarkPosition
from app.repositories.queue_repository import QueueRepository
from app.watermark.ffmpeg_processor import FFmpegProcessor
from app.distribution.flood_wait import calculate_retry_delay
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
        if self._task and not self._task.done():
            logger.info("Draining watermark worker...", extra={"ctx_worker": self._worker_id})
            try:
                await asyncio.wait_for(self._task, timeout=30.0)
            except asyncio.TimeoutError:
                logger.warning(
                    "Watermark worker drain timeout, force cancelling",
                    extra={"ctx_worker": self._worker_id},
                )
                self._task.cancel()
                try:
                    await self._task
                except asyncio.CancelledError:
                    pass
        logger.info("Watermark worker stopped", extra={"ctx_worker": self._worker_id})

    async def _run_loop(self) -> None:
        while self._running:
            try:
                jobs = await self._queue.claim_watermark_jobs(
                    worker_id=self._worker_id,
                    batch_size=settings.WORKER_BATCH_SIZE,
                )

                if not jobs:
                    await asyncio.sleep(settings.WORKER_POLL_INTERVAL)
                    continue

                tasks = [self._process_job(job) for job in jobs]
                results = await asyncio.gather(*tasks, return_exceptions=True)
                for res in results:
                    if isinstance(res, asyncio.CancelledError):
                        raise res
                    elif isinstance(res, Exception):
                        logger.error("Unhandled exception in watermark handler", exc_info=res)

            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(
                    "Watermark worker loop error",
                    extra={"ctx_worker": self._worker_id, "ctx_error": str(e)},
                    exc_info=True,
                )
                await asyncio.sleep(5)

    async def _resolve_media_path(self, job_doc: dict, job_id: str) -> Optional[str]:
        """
        Bug 6 fix: Resolve local media path for a watermark job.

        Moderation-queued jobs only have media_file_id — they have no local file
        because the content was never downloaded at submission time.
        If media_path is absent or the file no longer exists on disk, download
        the media from Telegram using the bot client.

        Returns the local file path string, or None if resolution failed.
        """
        media_path = job_doc.get("media_path")

        # Fast path: path already set and file exists on disk
        if media_path and Path(media_path).exists():
            return media_path

        if media_path and not Path(media_path).exists():
            logger.warning(
                "Watermark job has media_path but file missing on disk — will re-download",
                extra={"ctx_job_id": job_id, "ctx_path": media_path},
            )

        # Fallback: download from Telegram using file_id
        media_file_id = job_doc.get("media_file_id")
        if not media_file_id:
            logger.error(
                "Watermark job has neither valid media_path nor media_file_id",
                extra={"ctx_job_id": job_id},
            )
            return None

        try:
            bot = get_bot()
            # Build a deterministic filename so we don't collide across workers
            unique_suffix = uuid.uuid4().hex[:12]
            output_dir = Path(settings.PROCESSED_MEDIA_DIR)
            output_dir.mkdir(parents=True, exist_ok=True)
            dest_path = str(output_dir / f"wm_dl_{job_id}_{unique_suffix}")

            logger.info(
                "Downloading media for watermark job",
                extra={
                    "ctx_job_id": job_id,
                    "ctx_file_id": media_file_id[:20] + "...",
                    "ctx_worker": self._worker_id,
                },
            )

            downloaded_path = await bot.download_media(
                message=media_file_id,
                file_name=dest_path,
            )

            if not downloaded_path or not Path(downloaded_path).exists():
                logger.error(
                    "Media download returned empty path or file not found",
                    extra={"ctx_job_id": job_id, "ctx_downloaded": downloaded_path},
                )
                return None

            logger.info(
                "Media downloaded successfully for watermark",
                extra={"ctx_job_id": job_id, "ctx_path": downloaded_path},
            )
            return str(downloaded_path)

        except asyncio.CancelledError:
            raise
        except Exception as e:
            logger.error(
                "Failed to download media for watermark job",
                extra={"ctx_job_id": job_id, "ctx_error": str(e)},
                exc_info=True,
            )
            return None

    async def _process_job(self, job_doc: dict) -> None:
        job_id = str(job_doc["_id"])
        corr_token = set_correlation_id(f"wm_{job_id}")
        try:
            media_type = job_doc.get("media_type")
            watermark_config = job_doc.get("watermark_config") or {}
            watermark_path = watermark_config.get("watermark_image_path")

            # Bug 6 fix: resolve local media path, downloading from Telegram if needed.
            # The original code only read job_doc.get("media_path") which is None for
            # all moderation-queued jobs (they only have media_file_id).
            media_path = await self._resolve_media_path(job_doc, job_id)

            if not media_path:
                logger.error(
                    "Watermark job: could not resolve media path — marking failed",
                    extra={"ctx_job_id": job_id},
                )
                await self._queue.mark_failed(job_id, "Could not resolve media path for watermarking")
                return

            if not watermark_path:
                logger.error(
                    "Watermark job missing watermark image path",
                    extra={"ctx_job_id": job_id, "ctx_watermark_path": watermark_path},
                )
                await self._queue.mark_failed(job_id, "Missing watermark image path")
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
                    # Non-visual media (document, text, etc.) — skip watermark, push to dispatch
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

            except asyncio.CancelledError:
                logger.warning(
                    "Watermark job cancelled during processing, releasing claim",
                    extra={"ctx_job_id": job_id},
                )
                await self._queue.release_claim(job_id)
                raise

            except Exception as e:
                logger.error(
                    "Watermark processing failed or unexpected error",
                    extra={"ctx_job_id": job_id, "ctx_error": str(e)},
                    exc_info=True,
                )
                retry_count = job_doc.get("retry_count", 0)
                max_retries = job_doc.get("max_retries", settings.MAX_RETRY_ATTEMPTS)

                if retry_count >= max_retries:
                    logger.error(
                        "Max retries exceeded for watermark job",
                        extra={"ctx_job_id": job_id},
                    )
                    await self._queue.move_to_dead_letter(job_id, str(e))
                else:
                    delay = calculate_retry_delay(retry_count)
                    await self._queue.mark_failed(job_id, str(e), next_retry_delay_seconds=delay)
        finally:
            reset_correlation_id(corr_token)


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
        if self._workers:
            # Drain concurrently to avoid blocking timeout delays
            await asyncio.gather(
                *(worker.stop() for worker in self._workers),
                return_exceptions=True,
            )
        self._workers.clear()
        logger.info("Watermark pool stopped")