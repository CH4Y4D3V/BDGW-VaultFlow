# FILE: app/watermark/worker_pool.py
"""
Watermark worker pool for BDGW VaultFlow.

Pulls WATERMARKING-status jobs from the queue, applies FFmpeg watermarks,
uploads results to the vault channel, and atomically swaps the vault
message references so the dispatcher sees the watermarked versions.

Transaction fallback: swap_album_vault_references() uses MongoDB
multi-document transactions.  On standalone MongoDB instances (Railway
default, no replica set), transactions are unsupported.  The worker
detects OperationFailure code 20 and falls back to sequential
non-transactional update_one() calls which are safe because the
WATERMARKING lock status prevents concurrent dispatcher access.
"""

from __future__ import annotations

import asyncio
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

from motor.motor_asyncio import AsyncIOMotorDatabase
from bson import ObjectId
from pymongo.errors import OperationFailure
from pyrogram.errors import (
    ChannelInvalid,
    ChatAdminRequired,
    FloodWait,
    UserIsBlocked,
)

from app.bot.client import get_bot
from app.config import settings
from app.core.exceptions import DispatcherError, MediaFileNotFoundError, ConsistencyViolationError
from app.core.logger import reset_correlation_id, set_correlation_id
from app.core.models import JobStatus, MediaType, WatermarkPosition, WatermarkState
from app.distribution.flood_wait import calculate_retry_delay
from app.repositories.queue_repository import QueueRepository
from app.utils.logger import get_logger
from app.utils.media_refresh import download_with_refresh
from app.watermark.ffmpeg_processor import FFmpegProcessor

logger = get_logger(__name__)


def _safe_unlink(path: Optional[str], context: str) -> None:
    """
    Delete a temporary file from disk.

    Best-effort — never raises.  Logs DEBUG on success, WARNING on
    unexpected errors so that disk leaks are visible without being noisy
    during normal operation.
    """
    if not path:
        return
    try:
        Path(path).unlink(missing_ok=True)
        logger.debug(
            "temp_file_deleted",
            extra={"ctx_path": path, "ctx_context": context},
        )
    except OSError as e:
        logger.debug(
            "temp_file_already_gone",
            extra={"ctx_path": path, "ctx_context": context, "ctx_error": str(e)},
        )
    except Exception as e:
        logger.warning(
            "temp_file_delete_unexpected_error",
            extra={"ctx_path": path, "ctx_context": context, "ctx_error": str(e)},
            exc_info=True,
        )


# ── Non-retryable Pyrogram upload errors ─────────────────────────────────────

_FATAL_UPLOAD_ERRORS = (ChannelInvalid, ChatAdminRequired)


class WatermarkWorker:
    """
    Single worker task that processes WATERMARKING-status jobs from the
    queue in batches.  Jobs are grouped by media_group_id and processed
    atomically within each group.

    On completion, vault references are swapped and jobs returned to
    PENDING status so the distribution dispatcher can pick them up.

    Media download uses the vault-first refresh strategy (download_with_refresh)
    to prevent FILE_REFERENCE_EXPIRED errors.

    MongoDB transaction fallback: if swap_album_vault_references raises
    OperationFailure (standalone instance, no replica set), the worker
    falls back to _swap_references_no_txn() which performs sequential
    non-transactional updates.  This is safe because the WATERMARKING
    job status prevents the dispatcher from touching these jobs concurrently.
    """

    def __init__(
        self,
        worker_id: str,
        queue_repo: QueueRepository,
        ffmpeg: FFmpegProcessor,
    ) -> None:
        """
        Args:
            worker_id:   Human-readable identifier for log correlation.
            queue_repo:  Repository wrapping the queue_jobs collection.
            ffmpeg:      Initialised FFmpegProcessor instance.
        """
        self._worker_id = worker_id
        self._queue = queue_repo
        self._ffmpeg = ffmpeg
        self._running = False
        self._task: Optional[asyncio.Task] = None

    # ── Lifecycle ─────────────────────────────────────────────────────────────

    async def start(self) -> None:
        """Spawn the worker loop as an asyncio Task."""
        if self._running:
            return
        self._running = True
        self._task = asyncio.create_task(
            self._run_loop(),
            name=f"watermark-{self._worker_id}",
        )
        logger.info("watermark_worker_started", extra={"ctx_worker": self._worker_id})

    async def stop(self) -> None:
        """
        Signal the worker loop to stop and wait up to WORKER_DRAIN_TIMEOUT
        seconds for the current batch to complete.  Force-cancels if the
        drain timeout expires.
        """
        self._running = False
        if self._task and not self._task.done():
            drain_timeout = float(getattr(settings, "WORKER_DRAIN_TIMEOUT", 30.0))
            logger.info(
                "watermark_worker_draining",
                extra={"ctx_worker": self._worker_id},
            )
            try:
                await asyncio.wait_for(self._task, timeout=drain_timeout)
            except asyncio.TimeoutError:
                logger.warning(
                    "watermark_worker_drain_timeout_force_cancel",
                    extra={"ctx_worker": self._worker_id},
                )
                self._task.cancel()
                try:
                    await self._task
                except asyncio.CancelledError:
                    pass
        logger.info("watermark_worker_stopped", extra={"ctx_worker": self._worker_id})

    # ── Main poll loop ────────────────────────────────────────────────────────

    async def _run_loop(self) -> None:
        """
        Continuously claims WATERMARKING jobs from the queue, groups them
        by media_group_id, and dispatches each group to _process_group().
        Sleeps WORKER_POLL_INTERVAL seconds when the queue is empty.
        """
        while self._running:
            try:
                jobs = await self._queue.claim_watermark_jobs(
                    worker_id=self._worker_id,
                    batch_size=settings.WORKER_BATCH_SIZE,
                )

                if not jobs:
                    await asyncio.sleep(settings.WORKER_POLL_INTERVAL)
                    continue

                groups: dict[str, list[dict]] = {}
                for job in jobs:
                    gid = job.get("media_group_id") or f"single_{job['_id']}"
                    groups.setdefault(gid, []).append(job)

                tasks = [self._process_group(group) for group in groups.values()]
                results = await asyncio.gather(*tasks, return_exceptions=True)

                for res in results:
                    if isinstance(res, asyncio.CancelledError):
                        raise res
                    elif isinstance(res, Exception):
                        logger.error(
                            "watermark_group_unhandled_exception",
                            exc_info=res,
                        )

            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(
                    "watermark_worker_loop_error",
                    extra={"ctx_worker": self._worker_id, "ctx_error": str(e)},
                    exc_info=True,
                )
                await asyncio.sleep(5)

    # ── Media resolution ──────────────────────────────────────────────────────

    async def _resolve_media_path(self, job: dict, job_id: str) -> Optional[str]:
        """
        Download the media file for a job using the vault-first refresh
        strategy (download_with_refresh) to guarantee a valid file reference
        even after Telegram's 48-hour FILE_REFERENCE expiry.

        Returns the local filesystem path on success, or None on failure.
        """
        bot = get_bot()
        return await download_with_refresh(
            client=bot,
            job_doc=job,
            dest_dir=settings.PROCESSED_MEDIA_DIR,
            job_id=job_id,
        )

    # ── Group processing ──────────────────────────────────────────────────────

    async def _process_group(self, jobs: list[dict]) -> None:
        """
        Process all jobs in one media group atomically:
          1. Download each item.
          2. Apply FFmpeg watermark.
          3. Upload watermarked file to vault channel (with FloodWait retry).
          4. Swap all vault references (transactional, with non-txn fallback).

        Temporary files are always cleaned up in the finally block.
        On error, each job is either moved to the dead-letter queue (if
        retry_count >= max_retries) or marked failed for re-queuing.
        """
        group_id = jobs[0].get("media_group_id") or str(jobs[0]["_id"])
        corr_token = set_correlation_id(f"wm_grp_{group_id}")

        logger.info(
            "watermark_group_started",
            extra={
                "ctx_group_id": group_id,
                "ctx_job_count": len(jobs),
                "ctx_worker": self._worker_id,
            },
        )

        # Use a set to avoid double-deletion when processed_path == media_path.
        temp_file_set: set[str] = set()
        new_refs: list[dict] = []
        partial_uploads: list[dict] = []   # track uploads before full success

        bot = get_bot()
        try:
            for job in sorted(jobs, key=lambda x: x.get("album_sequence_index", 0)):
                job_id = str(job["_id"])
                media_type = job.get("media_type")
                watermark_config = job.get("watermark_config") or {}
                watermark_path = watermark_config.get("watermark_image_path")
                watermark_text = watermark_config.get("watermark_text", "BDGW")

                # 1. Download
                media_path = await self._resolve_media_path(job, job_id)
                if not media_path:
                    raise MediaFileNotFoundError(
                        f"Could not download media for job {job_id}"
                    )
                temp_file_set.add(media_path)

                # 2. Watermark
                position_str = watermark_config.get("position") or getattr(
                    settings, "WATERMARK_POSITION", "BOTTOM_RIGHT"
                )
                position = (
                    WatermarkPosition(position_str)
                    if position_str in WatermarkPosition.__members__
                    else WatermarkPosition.BOTTOM_RIGHT
                )
                opacity = watermark_config.get("opacity", settings.WATERMARK_OPACITY)
                scale = watermark_config.get("scale", settings.WATERMARK_SCALE)

                if media_type == MediaType.VIDEO.value:
                    processed_path = await self._ffmpeg.apply_video_watermark(
                        input_path=media_path,
                        watermark_path=watermark_path,
                        position=position,
                        opacity=opacity,
                        scale=scale,
                        watermark_text=watermark_text,
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

                # Only add to cleanup set if it's a distinct path.
                if processed_path != media_path:
                    temp_file_set.add(processed_path)

                # 3. Upload to vault with FloodWait retry.
                # FIX: use the job's vault_chat_id (destination-specific vault
                # channel) instead of the hardcoded generic settings.VAULT_CHANNEL_ID.
                # Previously all watermarked files were uploaded to VAULT_CHANNEL_ID
                # regardless of type. For NSFW where VAULT_CHANNEL_ID ==
                # NSFW_VAULT_CHANNEL_ID this was accidentally correct, but for
                # Premium content the watermarked file landed in the NSFW vault
                # while vault_chat_id in the job still pointed at
                # PREMIUM_VAULT_CHANNEL_ID. Delivery then tried copy_message from
                # the wrong channel → failed on every Premium job.
                job_vault_chat_id = int(
                    job.get("vault_chat_id") or settings.VAULT_CHANNEL_ID
                )
                uploaded_msg = await self._upload_to_vault(
                    bot=bot,
                    media_type=media_type,
                    processed_path=processed_path,
                    job_id=job_id,
                    vault_chat_id=job_vault_chat_id,
                )

                partial_uploads.append({
                    "album_sequence_index": job.get("album_sequence_index"),
                    "vault_message_id": uploaded_msg.id,
                })
                new_refs.append({
                    "album_sequence_index": job.get("album_sequence_index"),
                    "vault_message_id": uploaded_msg.id,
                })

            # 4. Atomic reference swap (with non-transactional fallback).
            logger.info(
                "watermark_swapping_references",
                extra={"ctx_group_id": group_id, "ctx_count": len(new_refs)},
            )
            await self._swap_with_fallback(group_id, new_refs)
            logger.info(
                "watermark_swap_complete",
                extra={"ctx_group_id": group_id, "ctx_items": len(new_refs)},
            )

        except Exception as e:
            logger.error(
                "watermark_group_failed",
                extra={"ctx_group_id": group_id, "ctx_error": str(e)},
                exc_info=True,
            )
            if partial_uploads:
                logger.warning(
                    "watermark_orphaned_vault_uploads",
                    extra={
                        "ctx_group_id": group_id,
                        "ctx_orphaned_count": len(partial_uploads),
                        "ctx_orphaned_vault_ids": [
                            r["vault_message_id"] for r in partial_uploads
                        ],
                    },
                )
            for job in jobs:
                retry_count = job.get("retry_count", 0)
                if retry_count >= job.get("max_retries", 3):
                    await self._queue.move_to_dead_letter(str(job["_id"]), str(e))
                else:
                    # FIX: was mark_failed(), which sets status=PENDING
                    # unconditionally. The general dispatcher (claim_next) has
                    # no awareness of watermark_required/watermark_state and
                    # would deliver the job's CURRENT vault_message_id —
                    # which, since the swap never completed, still points at
                    # the original UN-WATERMARKED vault item. mark_watermark_
                    # failed() routes the job back to status=WATERMARKING
                    # (re-claimable only by the watermark worker) instead,
                    # so unwatermarked content can never reach the group.
                    await self._queue.mark_watermark_failed(str(job["_id"]), str(e))
        finally:
            for path in temp_file_set:
                _safe_unlink(path, context=f"wm_group_cleanup:{group_id}")
            reset_correlation_id(corr_token)

    # ── Upload helper ─────────────────────────────────────────────────────────

    async def _upload_to_vault(
        self,
        bot,
        media_type: Optional[str],
        processed_path: str,
        job_id: str,
        vault_chat_id: Optional[int] = None,
    ):
        """
        Upload one watermarked file to the correct destination vault channel.

        Args:
            vault_chat_id: The Telegram channel ID to upload to. Callers must
                pass the job's ``vault_chat_id`` field so that NSFW content
                goes to NSFW_VAULT_CHANNEL_ID and Premium content goes to
                PREMIUM_VAULT_CHANNEL_ID. Falls back to settings.VAULT_CHANNEL_ID
                only when the caller does not supply this argument (legacy path).

        Retries up to 3 times on FloodWait (sleeping the required seconds)
        and on transient errors (exponential backoff).  Raises immediately
        on non-retryable errors (ChannelInvalid, ChatAdminRequired).

        Returns the sent Message object on success.
        Raises DispatcherError if all retries are exhausted.
        """
        # Use caller-supplied vault channel; fall back to generic only as
        # a safety net for callers that don't pass the argument.
        target_channel = int(vault_chat_id) if vault_chat_id else settings.VAULT_CHANNEL_ID

        uploaded_msg = None
        for attempt in range(3):
            try:
                if media_type == MediaType.VIDEO.value:
                    uploaded_msg = await bot.send_video(
                        chat_id=target_channel,
                        video=processed_path,
                    )
                elif media_type == MediaType.PHOTO.value:
                    uploaded_msg = await bot.send_photo(
                        chat_id=target_channel,
                        photo=processed_path,
                    )
                else:
                    uploaded_msg = await bot.send_document(
                        chat_id=target_channel,
                        document=processed_path,
                    )
                break
            except _FATAL_UPLOAD_ERRORS as e:
                # These errors will not resolve on retry — fail fast.
                raise DispatcherError(
                    f"Fatal vault upload error for job {job_id} "
                    f"(target_channel={target_channel}): {e}"
                ) from e
            except FloodWait as fw:
                await asyncio.sleep(fw.value + 1)
            except Exception:
                if attempt == 2:
                    raise
                await asyncio.sleep(2 ** attempt)

        if uploaded_msg is None:
            raise DispatcherError(
                f"Failed to upload watermarked item to vault for job {job_id} "
                f"(target_channel={target_channel}) after 3 attempts"
            )
        return uploaded_msg

    # ── Swap with transaction fallback ────────────────────────────────────────

    async def _swap_with_fallback(
        self,
        group_id: str,
        new_refs: list[dict],
    ) -> None:
        """
        Attempt the transactional vault reference swap.

        If MongoDB raises OperationFailure with code 20 (transaction not
        supported on standalone instance), falls back to
        _swap_references_no_txn() which performs equivalent sequential
        update_one() calls.  Any other OperationFailure is re-raised.
        """
        try:
            await self._queue.swap_album_vault_references(group_id, new_refs)
        except OperationFailure as op_err:
            # Code 20: "Transaction numbers are only allowed on a replica
            # set member or mongos" — standalone MongoDB deployment.
            if op_err.code == 20 or "Transaction" in str(op_err):
                logger.warning(
                    "watermark_txn_unsupported_using_fallback",
                    extra={
                        "ctx_group_id": group_id,
                        "ctx_error": str(op_err),
                    },
                )
                await self._swap_references_no_txn(group_id, new_refs)
            else:
                raise

    async def _swap_references_no_txn(
        self,
        group_id: str,
        new_refs: list[dict],
    ) -> None:
        """
        Non-transactional fallback for vault reference swapping.

        Updates each queue_jobs document individually via Motor's update_one().
        Safe in this context because the WATERMARKING status lock prevents
        the dispatcher from reading these jobs concurrently.

        CRITICAL FIX: this previously queried every ref by
        {"media_group_id": group_id, "album_sequence_index": seq_idx}
        unconditionally. For a SINGLE (non-album) photo, the job's real
        media_group_id field is None — group_id passed in by _process_group
        is actually the job's OWN ObjectId string (str(jobs[0]["_id"])),
        used as a synthetic fallback identifier precisely because there is
        no real media_group_id. The query {"media_group_id": "<objectid-str>"}
        NEVER matched the document (whose media_group_id field is None),
        so update_one always returned modified_count=0 — but this method
        never checked that and logged a fake "watermark_no_txn_swap_complete"
        success regardless.

        Effect of the bug: the watermarked file was uploaded to the vault
        successfully, but the job's vault_message_id was NEVER updated to
        point at it. The job stayed stuck at status=LOCKED forever (orphaned
        — unclaimable by either the watermark worker or the dispatcher).
        Stale-lock recovery (release_claim) would eventually find watermark_
        state stuck at PROCESSING (not PENDING) and route the job to PENDING
        with the OLD un-watermarked vault_message_id, bypassing the watermark
        requirement entirely. The general dispatcher's claim_next() has no
        awareness of watermark_required/watermark_state, so it delivered the
        un-watermarked original straight to the group. Every retry repeated
        the cycle, uploading another orphaned watermarked duplicate to the
        vault each time — the "bot keeps posting the same photo to the vault"
        symptom, while the group only ever received the unwatermarked copy.

        Fix: replicate the same $or fallback used by swap_album_vault_
        references()._do_swap() for the album_sequence_index is None case
        — match by the job's own _id OR media_group_id, never blindly by
        media_group_id alone. Also check modified_count and raise
        ConsistencyViolationError if a swap fails to match, so the failure
        is surfaced (and the orphaned upload gets logged) instead of being
        silently swallowed as success.
        """
        db = self._queue._db  # type: ignore[attr-defined]
        now = datetime.now(timezone.utc)

        for ref in new_refs:
            seq_idx = ref["album_sequence_index"]
            vault_msg_id = ref["vault_message_id"]

            if seq_idx is None:
                # Single (non-album) job: group_id is the job's own ObjectId
                # string, not a real media_group_id. Match by _id OR by
                # media_group_id (covers both synthetic and real identifiers).
                try:
                    query = {
                        "$or": [
                            {"_id": ObjectId(group_id)},
                            {"media_group_id": group_id},
                        ]
                    }
                except Exception:
                    # group_id wasn't a valid ObjectId string — fall back to
                    # media_group_id-only match.
                    query = {"media_group_id": group_id}
            else:
                query = {
                    "media_group_id": group_id,
                    "album_sequence_index": seq_idx,
                }

            result = await self._queue._queue.update_one(
                query,
                {
                    "$set": {
                        "vault_message_id": vault_msg_id,
                        "watermark_state": WatermarkState.COMPLETED,
                        "status": JobStatus.PENDING.value,
                        "watermarked_at": now,
                        "updated_at": now,
                        "locked_by": None,
                        "locked_at": None,
                    }
                },
            )

            if result.modified_count == 0:
                # Surface the failure instead of silently claiming success.
                # The caller (_process_group) catches this and runs
                # mark_failed/move_to_dead_letter, which is the correct
                # behaviour when a swap genuinely cannot find its target doc.
                raise ConsistencyViolationError(
                    f"_swap_references_no_txn: failed to match job for "
                    f"group_id={group_id!r} album_sequence_index={seq_idx!r} "
                    f"(query={query!r})"
                )

        logger.info(
            "watermark_no_txn_swap_complete",
            extra={"ctx_group_id": group_id, "ctx_count": len(new_refs)},
        )


# ── Worker pool ───────────────────────────────────────────────────────────────

class WatermarkWorkerPool:
    """
    Manages N concurrent WatermarkWorker tasks.

    FFmpegProcessor is instantiated in start() rather than __init__ so
    that a missing ffmpeg binary raises at a predictable point in the
    boot sequence rather than at import/construction time.
    """

    def __init__(
        self,
        db: AsyncIOMotorDatabase,
        worker_count: Optional[int] = None,
    ) -> None:
        """
        Args:
            db:           Motor database instance passed to QueueRepository.
            worker_count: Override for settings.WATERMARK_WORKER_COUNT.
        """
        self._db = db
        self._worker_count = worker_count or settings.WATERMARK_WORKER_COUNT
        self._workers: list[WatermarkWorker] = []
        self._ffmpeg: Optional[FFmpegProcessor] = None
        self._queue_repo = QueueRepository(db)

    async def start(self) -> None:
        """Initialise FFmpegProcessor and spawn all worker tasks."""
        self._ffmpeg = FFmpegProcessor()  # Validates ffmpeg binary here

        for i in range(self._worker_count):
            worker = WatermarkWorker(
                worker_id=f"wm-worker-{i}",
                queue_repo=self._queue_repo,
                ffmpeg=self._ffmpeg,
            )
            self._workers.append(worker)
            await worker.start()

        logger.info(
            "watermark_pool_started",
            extra={"ctx_count": self._worker_count},
        )

    async def stop(self) -> None:
        """Drain all worker tasks, logging any individual stop failures."""
        if not self._workers:
            return

        results = await asyncio.gather(
            *(worker.stop() for worker in self._workers),
            return_exceptions=True,
        )
        for i, res in enumerate(results):
            if isinstance(res, Exception):
                logger.error(
                    "watermark_pool_worker_stop_failed",
                    extra={"ctx_worker_index": i, "ctx_error": str(res)},
                )

        self._workers.clear()
        logger.info("watermark_pool_stopped")