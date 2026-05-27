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
from typing import Optional, List

from motor.motor_asyncio import AsyncIOMotorDatabase
from pyrogram.errors import FloodWait

from app.bot.client import get_bot
from app.config import settings
from app.core.exceptions import MediaFileNotFoundError, DispatcherError
from app.core.logger import reset_correlation_id, set_correlation_id
from app.core.models import JobStatus, MediaType, WatermarkPosition
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

                # Group jobs by media_group_id for atomic processing
                groups = {}
                for job in jobs:
                    gid = job.get("media_group_id") or f"single_{job['_id']}"
                    if gid not in groups:
                        groups[gid] = []
                    groups[gid].append(job)

                tasks = [self._process_group(group) for group in groups.values()]
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

    # ── Job processing ────────────────────────────────────────────────────────

    async def _resolve_media_path(self, job: dict, job_id: str) -> Optional[str]:
        """
        RC-10 FIX: Resolve live Message and download media.
        """
        bot = get_bot()
        return await download_with_refresh(
            client=bot,
            job_doc=job,
            dest_dir=settings.PROCESSED_MEDIA_DIR,
            job_id=job_id,
        )

    async def _process_group(self, jobs: List[dict]) -> None:
        group_id = jobs[0].get("media_group_id") or str(jobs[0]["_id"])
        corr_token = set_correlation_id(f"wm_grp_{group_id}")
        
        temp_files = []  # List of paths to cleanup
        new_refs = []    # List of {"album_sequence_index": int, "vault_message_id": int}
        
        bot = get_bot()
        try:
            for job in sorted(jobs, key=lambda x: x.get("album_sequence_index", 0)):
                job_id = str(job["_id"])
                media_type = job.get("media_type")
                watermark_config = job.get("watermark_config") or {}
                watermark_path = watermark_config.get("watermark_image_path")

                # 1. Download
                media_path = await self._resolve_media_path(job, job_id)
                if not media_path:
                    raise MediaFileNotFoundError(f"Could not download media for job {job_id}")
                temp_files.append(media_path)

                # 2. Watermark
                position_str = watermark_config.get("position", settings.WATERMARK_POSITION)
                position = WatermarkPosition(position_str) if position_str in WatermarkPosition.__members__ else WatermarkPosition.BOTTOM_RIGHT
                opacity = watermark_config.get("opacity", settings.WATERMARK_OPACITY)
                scale = watermark_config.get("scale", settings.WATERMARK_SCALE)

                if media_type == MediaType.VIDEO.value:
                    processed_path = await self._ffmpeg.apply_video_watermark(
                        input_path=media_path,
                        watermark_path=watermark_path,
                        position=position,
                        opacity=opacity,
                        scale=scale,
                    )
                elif media_type == MediaType.PHOTO.value:
                    processed_path = await self._ffmpeg.apply_image_watermark(
                        input_path=media_path,
                        watermark_path=watermark_path,
                        position=position,
                        opacity=opacity,
                        scale=scale,
                    )
                else:
                    processed_path = media_path

                temp_files.append(processed_path)

                # 3. Upload to vault
                uploaded_msg = None
                for attempt in range(3):
                    try:
                        if media_type == MediaType.VIDEO.value:
                            uploaded_msg = await bot.send_video(
                                chat_id=settings.VAULT_CHANNEL_ID,
                                video=processed_path,
                                caption="[WATERMARKED ALBUM ITEM]"
                            )
                        elif media_type == MediaType.PHOTO.value:
                            uploaded_msg = await bot.send_photo(
                                chat_id=settings.VAULT_CHANNEL_ID,
                                photo=processed_path,
                                caption="[WATERMARKED ALBUM ITEM]"
                            )
                        else:
                            uploaded_msg = await bot.send_document(
                                chat_id=settings.VAULT_CHANNEL_ID,
                                document=processed_path,
                                caption="[WATERMARKED ITEM]"
                            )
                        break
                    except FloodWait as e:
                        await asyncio.sleep(e.value + 1)
                    except Exception as e:
                        if attempt == 2:
                            raise
                        await asyncio.sleep(2 ** attempt)

                if not uploaded_msg:
                    raise DispatcherError(f"Failed to upload watermarked item to vault for job {job_id}")

                new_refs.append({
                    "album_sequence_index": job.get("album_sequence_index"),
                    "vault_message_id": uploaded_msg.id
                })

            # 4. Atomic Swap
            await self._queue.swap_album_vault_references(group_id, new_refs)
            logger.info("Atomic vault reference swap complete for album", extra={"ctx_group_id": group_id, "ctx_items": len(new_refs)})

        except Exception as e:
            logger.error("Watermark group processing failed", extra={"ctx_group_id": group_id, "ctx_error": str(e)}, exc_info=True)
            for job in jobs:
                retry_count = job.get("retry_count", 0)
                if retry_count >= job.get("max_retries", 3):
                    await self._queue.move_to_dead_letter(str(job["_id"]), str(e))
                else:
                    await self._queue.mark_failed(str(job["_id"]), str(e))
        finally:
            for path in temp_files:
                _safe_unlink(path, context=f"wm_group_cleanup:{group_id}")
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