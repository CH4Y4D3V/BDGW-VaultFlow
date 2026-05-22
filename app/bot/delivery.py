"""
app/bot/delivery.py

Telegram delivery pipeline for the DistributionEngine.

FILE_REFERENCE_EXPIRED fix
──────────────────────────
The previous implementation passed raw stored file_id strings directly to
send_photo() / send_video() / send_document().  While Telegram-to-Telegram
sends are more resilient than downloads, they still expire on long-queued jobs.

Fix: for each job we first check whether a locally processed file exists
(watermarked output).  If it does, we upload from disk — no reference needed.
If it does not, we resolve a fresh Message via the vault channel before sending,
ensuring the file_reference is always current.

This module never calls download_media() — that is the watermark worker's job.
"""

from __future__ import annotations

import asyncio
from pathlib import Path
from typing import List, Optional

from pyrogram.client import Client
from pyrogram.errors import FloodWait, RPCError
from pyrogram.types import InputMediaPhoto, InputMediaVideo, InputMediaDocument, Message

from app.bot.client import get_bot
from app.core.exceptions import FloodWaitError, DispatcherError
from app.core.models import MediaType
from app.utils.logger import get_logger
from app.utils.media_refresh import resolve_send_media, extract_media_object

logger = get_logger(__name__)

_MAX_RETRIES = 3
_FLOOD_BUFFER: int  # resolved lazily to avoid circular import at module load


def _flood_buffer() -> int:
    from app.config import settings
    return settings.FLOODWAIT_EXTRA_BUFFER


def _try_delete_local_file(path: str, context: str) -> None:
    """
    Delete a local processed file after successful Telegram upload.
    Best-effort — never raises.  Only deletes real filesystem paths.
    """
    if not path:
        return
    # Telegram file_ids are long alphanumeric strings, never start with / or ./
    # and are never valid filesystem paths on the server.
    if not path.startswith("/") and not path.startswith("./") and len(path) > 60:
        return
    try:
        Path(path).unlink(missing_ok=True)
        logger.debug(
            "Delivered: local file deleted",
            extra={"ctx_path": path, "ctx_context": context},
        )
    except OSError:
        pass
    except Exception as e:
        logger.warning(
            "Could not delete local file after delivery",
            extra={"ctx_path": path, "ctx_context": context, "ctx_error": str(e)},
        )


# ── Primary entry point ───────────────────────────────────────────────────────

async def execute_telegram_delivery(job_docs: List[dict], target_id: str) -> None:
    """
    Delivery pipeline for the DistributionEngine.

    Preserves album integrity and ordering.  Uses vault-first file reference
    refresh for all sends that do not have a local processed file.
    """
    bot = get_bot()
    if not job_docs:
        return

    max_network_retries = 3
    for attempt in range(max_network_retries):
        try:
            if not bot.is_connected:
                await bot.connect()

            if len(job_docs) == 1:
                await _send_single(bot, job_docs[0], target_id)
            else:
                await _send_album(bot, job_docs, target_id)
            return

        except FloodWait as e:
            val = e.value
            seconds = int(val) if isinstance(val, (int, float, str)) else 0
            raise FloodWaitError(seconds=seconds)

        except (ConnectionError, TimeoutError, RPCError) as e:
            if attempt == max_network_retries - 1:
                raise DispatcherError(
                    f"Telegram network failure after {max_network_retries} attempts: {e}"
                ) from e
            logger.warning(
                "Telegram network error, reconnecting",
                extra={"ctx_attempt": attempt + 1},
            )
            await asyncio.sleep(2 ** attempt)

        except (FloodWaitError, DispatcherError):
            raise

        except Exception as e:
            raise DispatcherError(f"Telegram delivery failed: {e}") from e


# ── Single-message send ───────────────────────────────────────────────────────

async def _send_single(bot: Client, job: dict, target_id: str) -> None:
    media_type = job.get("media_type")
    job_id = str(job.get("_id", "unknown"))
    caption = job.get("caption", "")
    has_spoiler = bool(job.get("metadata", {}).get("has_spoiler", False))

    # Prefer local processed (watermarked) file — no reference expiry possible
    processed_path = job.get("processed_media_path")
    if processed_path and Path(processed_path).exists():
        await _send_from_local(
            bot, media_type, processed_path, target_id, caption, has_spoiler
        )
        _try_delete_local_file(processed_path, context=f"single_delivery:{job_id}")
        return

    # No local file — resolve fresh file reference via vault channel
    if media_type != MediaType.TEXT.value:
        fresh_msg = await resolve_send_media(client=bot, job_doc=job, job_id=job_id)
        if fresh_msg is not None:
            await _send_from_message(bot, fresh_msg, media_type, target_id, caption, has_spoiler)
            return

        # Absolute last resort: raw stored file_id (likely to fail on old jobs)
        raw_media = (
            job.get("processed_media_path")
            or job.get("media_path")
            or job.get("media_file_id")
        )
        if raw_media:
            logger.warning(
                "_send_single: vault refresh failed, trying raw file_id",
                extra={"ctx_job_id": job_id, "ctx_media_type": media_type},
            )
            await _send_from_file_id(
                bot, media_type, str(raw_media), target_id, caption, has_spoiler
            )
            return

        raise DispatcherError(
            f"No media source available for job {job_id} — "
            f"vault_message_id and source_message_id must be stored at enqueue time"
        )

    # Text message
    if caption:
        await bot.send_message(chat_id=target_id, text=caption)


async def _send_from_local(
    bot: Client,
    media_type: str,
    file_path: str,
    target_id: str,
    caption: str,
    has_spoiler: bool,
) -> None:
    """Upload from a local file path (watermarked output)."""
    if media_type == MediaType.PHOTO.value:
        await bot.send_photo(
            chat_id=target_id,
            photo=file_path,
            caption=caption,
            has_spoiler=has_spoiler,
        )
    elif media_type == MediaType.VIDEO.value:
        await bot.send_video(
            chat_id=target_id,
            video=file_path,
            caption=caption,
            has_spoiler=has_spoiler,
        )
    else:
        await bot.send_document(
            chat_id=target_id,
            document=file_path,
            caption=caption,
        )


async def _send_from_message(
    bot: Client,
    source_msg: Message,
    media_type: str,
    target_id: str,
    caption: str,
    has_spoiler: bool,
) -> None:
    """
    Send using a freshly fetched Message object.
    copy_message() is preferred — it strips the forwarding header.
    """
    await bot.copy_message(
        chat_id=target_id,
        from_chat_id=source_msg.chat.id,
        message_id=source_msg.id,
        caption=caption if caption else None,
    )


async def _send_from_file_id(
    bot: Client,
    media_type: str,
    file_id: str,
    target_id: str,
    caption: str,
    has_spoiler: bool,
) -> None:
    """Last-resort send from raw file_id. May fail with FILE_REFERENCE_EXPIRED."""
    if media_type == MediaType.PHOTO.value:
        await bot.send_photo(
            chat_id=target_id,
            photo=file_id,
            caption=caption,
            has_spoiler=has_spoiler,
        )
    elif media_type == MediaType.VIDEO.value:
        await bot.send_video(
            chat_id=target_id,
            video=file_id,
            caption=caption,
            has_spoiler=has_spoiler,
        )
    else:
        await bot.send_document(
            chat_id=target_id,
            document=file_id,
            caption=caption,
        )


# ── Album send ────────────────────────────────────────────────────────────────

async def _send_album(bot: Client, job_docs: List[dict], target_id: str) -> None:
    """
    Send a Telegram media group (album).

    For each item:
      1. Use local processed file if present.
      2. Resolve fresh Message via vault channel.
      3. Fall back to raw file_id (last resort).

    Uses copy_message-based album construction where possible to avoid
    forwarding headers.  Falls back to InputMedia* objects for local files.
    """
    # Sort deterministically by message_id to preserve native Telegram ordering
    sorted_docs = sorted(
        job_docs,
        key=lambda x: (x.get("metadata", {}).get("message_id", 0), str(x["_id"])),
    )

    # Attempt vault-copy album strategy first:
    # if ALL items have vault coordinates, use copy_message in sequence.
    # This is simpler and avoids needing local files for delivery.
    all_have_vault = all(
        doc.get("vault_message_id") and (
            doc.get("vault_channel_id") or str(doc.get("vault_channel_id", ""))
        )
        for doc in sorted_docs
    )

    if all_have_vault and not any(
        doc.get("processed_media_path") and Path(doc["processed_media_path"]).exists()
        for doc in sorted_docs
    ):
        await _send_album_via_copy(bot, sorted_docs, target_id)
        return

    # Mixed or watermarked album — build InputMedia list
    await _send_album_via_input_media(bot, sorted_docs, target_id)


async def _send_album_via_copy(
    bot: Client, sorted_docs: List[dict], target_id: str
) -> None:
    """
    Send each album item via copy_message sequentially.
    Used when no watermarking has been applied and all vault copies are available.
    Sends caption only on the first item.
    """
    for i, job in enumerate(sorted_docs):
        job_id = str(job.get("_id", "unknown"))
        fresh_msg = await resolve_send_media(client=bot, job_doc=job, job_id=job_id)

        if fresh_msg is None:
            logger.error(
                "_send_album_via_copy: could not resolve message for album item",
                extra={"ctx_job_id": job_id, "ctx_position": i},
            )
            raise DispatcherError(
                f"Album delivery failed: could not resolve fresh message for job {job_id}"
            )

        caption = str(job.get("caption", "")) if i == 0 else None
        await bot.copy_message(
            chat_id=target_id,
            from_chat_id=fresh_msg.chat.id,
            message_id=fresh_msg.id,
            caption=caption,
        )


async def _send_album_via_input_media(
    bot: Client, sorted_docs: List[dict], target_id: str
) -> None:
    """
    Build InputMedia* list for send_media_group.
    Uses local processed files where available, falls back to vault refresh.
    """
    media_group: list = []
    local_paths_to_clean: list[tuple[str, str]] = []  # (path, job_id)

    for i, job in enumerate(sorted_docs):
        job_id = str(job.get("_id", "unknown"))
        media_type = job.get("media_type")
        caption = str(job.get("caption", "")) if i == 0 else ""
        has_spoiler = bool(job.get("metadata", {}).get("has_spoiler", False))

        # Prefer local processed file
        processed_path = job.get("processed_media_path")
        if processed_path and Path(processed_path).exists():
            media_source = processed_path
            local_paths_to_clean.append((processed_path, job_id))
        else:
            # Resolve fresh file_id via vault
            fresh_msg = await resolve_send_media(client=bot, job_doc=job, job_id=job_id)
            if fresh_msg is None:
                # Last resort: raw stored file_id
                raw = (
                    job.get("media_path")
                    or job.get("media_file_id")
                )
                if not raw:
                    logger.error(
                        "_send_album_via_input_media: no media source for item",
                        extra={"ctx_job_id": job_id, "ctx_position": i},
                    )
                    continue
                media_source = str(raw)
                logger.warning(
                    "_send_album_via_input_media: using raw file_id (may be stale)",
                    extra={"ctx_job_id": job_id},
                )
            else:
                media_obj = extract_media_object(fresh_msg)
                media_source = media_obj.file_id if media_obj else fresh_msg.id

        if media_type == MediaType.PHOTO.value:
            media_group.append(
                InputMediaPhoto(
                    media=media_source,
                    caption=caption,
                    has_spoiler=has_spoiler,
                )
            )
        elif media_type == MediaType.VIDEO.value:
            media_group.append(
                InputMediaVideo(
                    media=media_source,
                    caption=caption,
                    has_spoiler=has_spoiler,
                )
            )
        else:
            media_group.append(
                InputMediaDocument(
                    media=media_source,
                    caption=caption,
                )
            )

    if media_group:
        await bot.send_media_group(chat_id=target_id, media=media_group)

        # Clean up local processed files after successful send
        for path, job_id in local_paths_to_clean:
            _try_delete_local_file(path, context=f"album_delivery:{job_id}")