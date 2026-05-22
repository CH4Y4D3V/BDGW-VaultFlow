"""
app/watermark/worker_pool.py

Watermark worker pool — pulls WATERMARKING jobs from the queue, applies
FFmpeg watermarks, and marks them PENDING for the dispatcher.

FILE_REFERENCE_EXPIRED fix
──────────────────────────
The previous implementation stored raw file_id strings at enqueue time and
called download_media(file_id_string) in the worker.  Telegram file references
expire within hours to days, so any job that waited in the queue would fail
with 400 FILE_REFERENCE_EXPIRED.

Fix: all downloads now go through app.utils.media_refresh.download_with_refresh()
which resolves a live Message object via get_messages() before downloading.
Priority order:
  1. Vault channel copy  (canonical — written by archive_to_vault at approve time)
  2. Origin chat copy    (fallback — may be deleted by user)
  3. Raw file_id         (last resort — will still fail on genuinely old references)

The worker NEVER calls download_media() with a bare string.

Indentation fix
───────────────
Previous version had _fetch_vault_message() and part of _resolve_media_path()
accidentally nested, which caused AttributeError at runtime.  All methods are
now correctly defined as proper class methods.
"""

from __future__ import annotations

import asyncio
import uuid
from pathlib import Path
from typing import Optional

from motor.motor_asyncio import AsyncIOMotorDatabase

from app.bot.client import get_bot
from app.config import settings
from app.core.logger import set_correlation_id, reset_correlation_id
from app.core.models import MediaType, WatermarkPosition
from app.distribution.flood_wait import calculate_retry_delay
from app.repositories.queue_repository import QueueRepository
from app.utils.logger import get_logger
from app.utils.media_refresh import download_with_refresh
from app.watermark.ffmpeg_processor import FFmpegProcessor

logger = get_logger(__name__)


def _safe_unlink(path: Optional[str], context: str) -> None:
    """Delete a file from disk. Best-effort — never raises."""
    if not path:
        return
    try:
        Path(path).unlink(missing_ok=True)
        logger.debug(
            "Temp file deleted",
            extra={"ctx_path": path, "ctx_context": context},
        )
    except OSError as e:
        logger.debug(
            "Could not delete temp file (already gone or permission denied)",
            extra={"ctx_path": path, "ctx_context": context, "ctx_error": str(e)},
        )
    except Exception as e:
        logger.warning(
            "Unexpected error deleting temp file",
            extra={"ctx_path": path, "ctx_context": context, "ctx_error": str(e)},
        )


class WatermarkWorker:
    """
    Pulls jobs in WATERMARKING status and processes them with FFmpeg.

    On completion, marks the job back to PENDING so the dispatcher picks it up.
    Runs as an independent async task — never shares state with dispatcher workers.

    Media download uses the vault-first refresh strategy from media_refresh.py
    to guarantee we never use an expired file reference.
    """

    def __init__(
        self,
        worker_id: str,
        queue_repo: QueueRepository,
        ffmpeg: FFmpegProcessor,
    ) -> None:
        self._worker_id = worker_id
        self._queue = queue_repo
        self._ffmpeg = ffmpeg
        self._running = False
        self._task: Optional[asyncio.Task] = None

    # ── Lifecycle ─────────────────────────────────────────────────────────────

    async def start(self) -> None:
        if self._running:
            return
        self._running = True
        self._task = asyncio.create_task(
            self._run_loop(),
            name=f"watermark-{self._worker_id}",
        )
        logger.info("Watermark worker started", extra={"ctx_worker": self._worker_id})

    async def stop(self) -> None:
        self._running = False
        if self._task and not self._task.done():
            logger.info(
                "Draining watermark worker...",
                extra={"ctx_worker": self._worker_id},
            )
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

    # ── Main loop ─────────────────────────────────────────────────────────────

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
                        logger.error(
                            "Unhandled exception in watermark handler",
                            exc_info=res,
                        )

            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(
                    "Watermark worker loop error",
                    extra={"ctx_worker": self._worker_id, "ctx_error": str(e)},
                    exc_info=True,
                )
                await asyncio.sleep(5)

    # ── Media path resolution (vault-first, FILE_REFERENCE_EXPIRED safe) ──────

    async def _resolve_media_path(self, job_doc: dict, job_id: str) -> Optional[str]:
        """
        Resolve a local file path for the job's media, downloading if necessary.

        Uses download_with_refresh() which:
          1. Returns an existing local file immediately if present.
          2. Fetches a live Message from the vault channel via get_messages().
          3. Falls back to the origin chat message.
          4. Last-resort: raw file_id (may fail on stale references).

        Never raises.  Returns None if all sources fail.
        """
        bot = get_bot()
        dest_dir = settings.PROCESSED_MEDIA_DIR

        try:
            path = await download_with_refresh(
                client=bot,
                job_doc=job_doc,
                dest_dir=dest_dir,
                job_id=job_id,
            )
        except asyncio.CancelledError:
            raise
        except Exception as e:
            logger.error(
                "_resolve_media_path: download_with_refresh raised unexpectedly",
                extra={"ctx_job_id": job_id, "ctx_error": str(e)},
                exc_info=True,
            )
            return None

        if path is None:
            metadata = job_doc.get("metadata", {})
            logger.error(
                "_resolve_media_path: all download sources exhausted",
                extra={
                    "ctx_job_id": job_id,
                    "ctx_vault_channel_id": job_doc.get("vault_channel_id") or metadata.get("vault_channel_id"),
                    "ctx_vault_message_id": job_doc.get("vault_message_id") or metadata.get("vault_message_id"),
                    "ctx_origin_chat_id": job_doc.get("origin_chat_id") or metadata.get("origin_chat_id"),
                    "ctx_origin_message_id": job_doc.get("origin_message_id") or metadata.get("origin_message_id"),
                },
            )

        return path

    # ── Job processing ────────────────────────────────────────────────────────

    async def _process_job(self, job_doc: dict) -> None:
        job_id = str(job_doc["_id"])
        corr_token = set_correlation_id(f"wm_{job_id}")

        # Track paths for cleanup — input file downloaded for this job only.
        # The processed output must NOT be deleted here; delivery.py owns that.
        downloaded_media_path: Optional[str] = None
        processed_output_path: Optional[str] = None

        try:
            media_type = job_doc.get("media_type")
            watermark_config = job_doc.get("watermark_config") or {}
            watermark_path = watermark_config.get("watermark_image_path")

            # FILE_REFERENCE_EXPIRED fix: use vault-first refresh strategy
            media_path = await self._resolve_media_path(job_doc, job_id)

            # Determine if the worker downloaded a new file that it must clean up.
            # The `finally` block will delete `downloaded_media_path`.
            original_media_path = job_doc.get("media_path")
            if media_path:
                is_new_download = True
                if original_media_path and Path(original_media_path).exists():
                    try:
                        # If the resolved path points to the same file as the original,
                        # we don't "own" it for cleanup.
                        if Path(media_path).samefile(original_media_path):
                            is_new_download = False
                    except FileNotFoundError:
                        # This can happen if media_path is a broken symlink etc.
                        pass
                if is_new_download:
                    downloaded_media_path = media_path

            if not media_path:
                logger.error(
                    "Watermark job: could not resolve media path — moving to dead-letter queue",
                    extra={"ctx_job_id": job_id},
                )
                await self._queue.move_to_dead_letter(
                    job_id,
                    "FILE_REFERENCE_EXPIRED: could not resolve media from vault or origin.",
                )
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
                        "ctx_vault_msg": job_doc.get("vault_message_id"),
                    },
                )

                if media_type == MediaType.VIDEO.value:
                    processed_output_path = await self._ffmpeg.apply_video_watermark(
                        input_path=media_path,
                        watermark_path=watermark_path,
                        position=position,
                        opacity=opacity,
                        scale=scale,
                    )
                elif media_type == MediaType.PHOTO.value:
                    processed_output_path = await self._ffmpeg.apply_image_watermark(
                        input_path=media_path,
                        watermark_path=watermark_path,
                        position=position,
                        opacity=opacity,
                        scale=scale,
                    )
                else:
                    # Non-visual media (document, animation, etc.) — skip watermark,
                    # push directly to dispatcher with the resolved media_path.
                    await self._queue.mark_watermark_applied(job_id, media_path)
                    # Input file is now the "processed" path — do NOT delete it here.
                    # delivery.py will clean it up after upload.
                    downloaded_media_path = None
                    return

                # Mark job ready for dispatcher
                await self._queue.mark_watermark_applied(job_id, processed_output_path)

                # Delete the downloaded input temp file — watermarking succeeded
                # and the output (processed_output_path) is what the dispatcher needs.
                if downloaded_media_path:
                    _safe_unlink(
                        downloaded_media_path,
                        context=f"wm_input_cleanup:{job_id}",
                    )
                    downloaded_media_path = None  # Prevent double-delete in finally

                logger.info(
                    "Watermark applied successfully",
                    extra={
                        "ctx_job_id": job_id,
                        "ctx_output": processed_output_path,
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
                    "Watermark processing failed",
                    extra={"ctx_job_id": job_id, "ctx_error": str(e)},
                    exc_info=True,
                )

                # Clean up the partial FFmpeg output on failure
                _safe_unlink(
                    processed_output_path,
                    context=f"wm_output_failure:{job_id}",
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
                    await self._queue.mark_failed(
                        job_id, str(e), next_retry_delay_seconds=delay
                    )

        finally:
            # Always clean up any downloaded temp input that wasn't cleaned above
            # (error paths).  processed_output_path is intentionally NOT cleaned
            # here — delivery.py handles that after successful upload.
            if downloaded_media_path:
                _safe_unlink(
                    downloaded_media_path,
                    context=f"wm_input_finally:{job_id}",
                )
            reset_correlation_id(corr_token)


# ── Worker pool ───────────────────────────────────────────────────────────────

class WatermarkWorkerPool:
    """Manages N watermark worker tasks."""

    def __init__(
        self,
        db: AsyncIOMotorDatabase,
        worker_count: Optional[int] = None,
    ) -> None:
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
            "Watermark pool started",
            extra={"ctx_count": self._worker_count},
        )

    async def stop(self) -> None:
        if self._workers:
            await asyncio.gather(
                *(worker.stop() for worker in self._workers),
                return_exceptions=True,
            )
        self._workers.clear()
        logger.info("Watermark pool stopped")