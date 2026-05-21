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


def _safe_unlink(path: Optional[str], context: str) -> None:
    """
    Delete a file from disk. Best-effort — never raises.
    Handles the case where the file is already gone (missing_ok equivalent).
    """
    if not path:
        return
    try:
        Path(path).unlink(missing_ok=True)
        logger.debug(
            "Temp file deleted",
            extra={"ctx_path": path, "ctx_context": context},
        )
    except OSError as e:
        # File already gone or permission issue — non-fatal
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
    Resolve local media path for a watermark job.

    Priority order:
      1. media_path already on disk → use it directly.
      2. Re-fetch the vault copy via get_messages() → download from fresh reference.
         This avoids FILE_REFERENCE_EXPIRED which occurs when using a raw file_id
         that has expired server-side.

    The vault message is the canonical fresh source because it was copy_message-d
    there at approve/queue time and Telegram always has it available.
    """
    media_path = job_doc.get("media_path")

    # Fast path: local file already present
    if media_path and Path(media_path).exists():
        return media_path

    if media_path and not Path(media_path).exists():
        logger.warning(
            "Watermark job has media_path but file missing on disk — will re-fetch from vault",
            extra={"ctx_job_id": job_id, "ctx_path": media_path},
        )

    media_file_id = job_doc.get("media_file_id")
    if not media_file_id:
        logger.error(
            "Watermark job has neither valid media_path nor media_file_id",
            extra={"ctx_job_id": job_id},
        )
        return None

    try:
        bot = get_bot()
        unique_suffix = uuid.uuid4().hex[:12]
        output_dir = Path(settings.PROCESSED_MEDIA_DIR)
        output_dir.mkdir(parents=True, exist_ok=True)
        dest_path = str(output_dir / f"wm_dl_{job_id}_{unique_suffix}")

        logger.info(
            "Downloading media for watermark job",
            extra={
                "ctx_job_id": job_id,
                "ctx_file_id_prefix": media_file_id[:20],
                "ctx_worker": self._worker_id,
            },
        )

        # ── Step 1: Refresh file reference via vault message ──────────────────
        # Raw file_id strings contain an embedded file_reference that expires.
        # Calling download_media(message=file_id_string) directly fails with
        # FILE_REFERENCE_EXPIRED once that reference is stale.
        # Fix: fetch the vault message first — Telegram returns a live, valid
        # file_reference in the message object which download_media then uses.
        fresh_message = await self._fetch_vault_message(job_doc, job_id, bot)

        if fresh_message is not None:
            logger.info(
                "Media downloaded successfully for watermark",
                extra={"ctx_job_id": job_id},
            )
            downloaded_path = await bot.download_media(
                message=fresh_message,
                file_name=dest_path,
            )
        else:
            # Vault message unavailable — fall back to direct file_id download.
            # This may still hit FILE_REFERENCE_EXPIRED on old jobs, but it's
            # the best we can do without a vault reference.
            logger.warning(
                "Vault message not found — falling back to direct file_id download",
                extra={"ctx_job_id": job_id},
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


    async def _fetch_vault_message(self, job_doc: dict, job_id: str, bot):
    """
    Fetch the vault channel copy of this job's media to get a live file reference.

    The vault collection stores vault_message_id + vault_channel_id for every
    approved/queued item. get_messages() returns a Message object with a fresh
    file_reference, which download_media() can use without hitting
    FILE_REFERENCE_EXPIRED.

    Returns the Pyrogram Message object on success, None on any failure.
    """
    from app.core.database import DatabaseManager

    content_id = job_doc.get("content_id")
    if not content_id:
        return None

    try:
        db = DatabaseManager.get_db()
        vault_doc = await db[settings.VAULT_COLLECTION].find_one(
            {"content_id": content_id},
            {"vault_message_id": 1, "vault_channel_id": 1},
        )
    except Exception as e:
        logger.warning(
            "_fetch_vault_message: DB lookup failed",
            extra={"ctx_job_id": job_id, "ctx_error": str(e)},
        )
        return None

    if not vault_doc:
        logger.debug(
            "_fetch_vault_message: no vault doc for content_id",
            extra={"ctx_job_id": job_id, "ctx_content_id": content_id},
        )
        return None

    vault_message_id = vault_doc.get("vault_message_id")
    vault_channel_id = vault_doc.get("vault_channel_id") or str(settings.VAULT_CHANNEL_ID)

    if not vault_message_id:
        logger.debug(
            "_fetch_vault_message: vault_message_id is None",
            extra={"ctx_job_id": job_id, "ctx_content_id": content_id},
        )
        return None

    try:
        # get_messages returns the message with a fresh, valid file_reference
        messages = await bot.get_messages(
            chat_id=int(vault_channel_id),
            message_ids=int(vault_message_id),
        )
        # get_messages with a single id returns a single Message
        if not isinstance(messages, list):
            messages = [messages]

        msg = next((m for m in messages if m and m.id and m.media), None)
        if msg is None:
            logger.warning(
                "_fetch_vault_message: vault message has no media or was deleted",
                extra={
                    "ctx_job_id": job_id,
                    "ctx_vault_msg_id": vault_message_id,
                    "ctx_vault_chat_id": vault_channel_id,
                },
            )
            return None

        return msg

    except Exception as e:
        logger.warning(
            "_fetch_vault_message: get_messages failed",
            extra={
                "ctx_job_id": job_id,
                "ctx_vault_msg_id": vault_message_id,
                "ctx_vault_chat_id": vault_channel_id,
                "ctx_error": str(e),
            },
        )
        return None

    async def _process_job(self, job_doc: dict) -> None:
        job_id = str(job_doc["_id"])
        corr_token = set_correlation_id(f"wm_{job_id}")
        # Track paths for cleanup after successful processing
        downloaded_media_path: Optional[str] = None
        processed_output_path: Optional[str] = None

        try:
            media_type = job_doc.get("media_type")
            watermark_config = job_doc.get("watermark_config") or {}
            watermark_path = watermark_config.get("watermark_image_path")

            # Bug 6 fix: resolve local media path, downloading from Telegram if needed.
            media_path = await self._resolve_media_path(job_doc, job_id)

            # Track whether we downloaded this file (i.e., it's a temp file we own)
            original_media_path = job_doc.get("media_path")
            is_downloaded = (
                media_path is not None
                and media_path != original_media_path
            )
            if is_downloaded:
                downloaded_media_path = media_path

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
                    processed_output_path = await self._ffmpeg.apply_video_watermark(
                        input_path=media_path,
                        watermark_path=watermark_path,
                        position=position,
                        opacity=opacity,
                        scale=scale,
                    )
                elif media_type == MediaType.PHOTO:
                    processed_output_path = await self._ffmpeg.apply_image_watermark(
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

                # Mark job ready for dispatcher — processed_output_path is the watermarked file
                await self._queue.mark_watermark_applied(job_id, processed_output_path)

                # WARNING fix: delete the downloaded input file now that watermarking
                # succeeded. The output (processed_output_path) must stay on disk until
                # the dispatcher uploads it via execute_telegram_delivery.
                if is_downloaded and downloaded_media_path:
                    _safe_unlink(downloaded_media_path, context=f"wm_input_cleanup:{job_id}")
                    downloaded_media_path = None  # prevent double-delete in finally

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
                    "Watermark processing failed or unexpected error",
                    extra={"ctx_job_id": job_id, "ctx_error": str(e)},
                    exc_info=True,
                )
                retry_count = job_doc.get("retry_count", 0)
                max_retries = job_doc.get("max_retries", settings.MAX_RETRY_ATTEMPTS)

                # FIX 5: Clean up the partial output file on failure so it doesn't
                # accumulate on disk. The finally block handles the input; this
                # covers the FFmpeg output path.
                _safe_unlink(processed_output_path, context=f"wm_output_failure:{job_id}")

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
            # Cleanup any downloaded temp input that wasn't cleaned up above
            # (e.g. on error paths). processed_output_path is NOT cleaned here —
            # the dispatcher must upload it first; delivery.py handles that cleanup.
            if downloaded_media_path:
                _safe_unlink(downloaded_media_path, context=f"wm_input_finally:{job_id}")

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
