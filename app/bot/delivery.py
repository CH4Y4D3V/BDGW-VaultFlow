import asyncio
from pathlib import Path
from typing import List

from pyrogram.client import Client
from pyrogram.types import InputMediaPhoto, InputMediaVideo, InputMediaDocument
from pyrogram.errors import FloodWait, RPCError

from app.bot.client import get_bot
from app.core.exceptions import FloodWaitError, DispatcherError
from app.core.models import MediaType
from app.utils.logger import get_logger

logger = get_logger(__name__)


def _try_delete_local_file(path: str, context: str) -> None:
    """
    WARNING fix (media cleanup): delete a local processed file after
    successful Telegram upload. Best-effort — never raises.
    Only deletes real filesystem paths (not Telegram file_ids).
    """
    if not path:
        return
    # Telegram file_ids are long alphanumeric strings, never start with /
    # and are never valid filesystem paths on the server.
    if not path.startswith("/") and not path.startswith("./") and len(path) > 60:
        # Looks like a Telegram file_id, not a local path — skip
        return
    try:
        Path(path).unlink(missing_ok=True)
        logger.debug(
            "Delivered: local file deleted",
            extra={"ctx_path": path, "ctx_context": context},
        )
    except OSError:
        pass  # Already gone — fine
    except Exception as e:
        logger.warning(
            "Could not delete local file after delivery",
            extra={"ctx_path": path, "ctx_context": context, "ctx_error": str(e)},
        )


async def execute_telegram_delivery(job_docs: List[dict], target_id: str) -> None:
    """
    Delivery pipeline for the DistributionEngine.
    Preserves album integrity, ordering, and provides reconnect handling.
    """
    bot = get_bot()
    if not job_docs:
        return

    # Telegram reconnect handling & retry-safe network boundaries
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
            # Propagate up to FloodWaitHandler explicitly
            val = e.value
            seconds = int(val) if isinstance(val, (int, float, str)) else 0
            raise FloodWaitError(seconds=seconds)
        except (ConnectionError, TimeoutError, RPCError) as e:
            if attempt == max_network_retries - 1:
                raise DispatcherError(f"Telegram network failure after {max_network_retries} attempts: {e}") from e
            logger.warning("Telegram network error, reconnecting", extra={"ctx_attempt": attempt + 1})
            await asyncio.sleep(2 ** attempt)
        except Exception as e:
            raise DispatcherError(f"Telegram delivery failed: {e}") from e


async def _send_single(bot: Client, job: dict, target_id: str) -> None:
    media_type = job.get("media_type")
    raw_media = job.get("processed_media_path") or job.get("media_path") or job.get("media_file_id")
    caption = job.get("caption", "")

    if not raw_media and media_type != MediaType.TEXT.value:
        raise DispatcherError(f"No media found for job {job.get('_id')}")

    media = str(raw_media) if raw_media else ""
    has_spoiler = bool(job.get("metadata", {}).get("has_spoiler", False))

    if media_type == MediaType.PHOTO.value:
        await bot.send_photo(chat_id=target_id, photo=media, caption=caption, has_spoiler=has_spoiler)
    elif media_type == MediaType.VIDEO.value:
        await bot.send_video(chat_id=target_id, video=media, caption=caption, has_spoiler=has_spoiler)
    elif media_type == MediaType.TEXT.value:
        await bot.send_message(chat_id=target_id, text=caption)
    else:
        await bot.send_document(chat_id=target_id, document=media, caption=caption)

    # WARNING fix (media cleanup): delete the local processed file after successful upload.
    # processed_media_path is the FFmpeg output; media_path may be a downloaded temp file.
    # Only delete processed_media_path here — media_path (original) may be shared.
    processed_path = job.get("processed_media_path")
    if processed_path:
        job_id = str(job.get("_id", ""))
        _try_delete_local_file(processed_path, context=f"single_delivery:{job_id}")


async def _send_album(bot: Client, job_docs: List[dict], target_id: str) -> None:
    media_group = []

    # Sort deterministically by message_id to preserve native Telegram ordering safely
    sorted_docs = sorted(job_docs, key=lambda x: (x.get("metadata", {}).get("message_id", 0), str(x["_id"])))

    for i, job in enumerate(sorted_docs):
        media_type = job.get("media_type")
        raw_media = job.get("processed_media_path") or job.get("media_path") or job.get("media_file_id")

        if not raw_media:
            continue

        media = str(raw_media)
        caption = str(job.get("caption", "")) if i == 0 else ""
        has_spoiler = bool(job.get("metadata", {}).get("has_spoiler", False))

        if media_type == MediaType.PHOTO.value:
            media_group.append(InputMediaPhoto(media=media, caption=caption, has_spoiler=has_spoiler))
        elif media_type == MediaType.VIDEO.value:
            media_group.append(InputMediaVideo(media=media, caption=caption, has_spoiler=has_spoiler))
        else:
            media_group.append(InputMediaDocument(media=media, caption=caption))

    if media_group:
        await bot.send_media_group(chat_id=target_id, media=media_group)

        # WARNING fix (media cleanup): delete processed output files after successful album upload.
        for job in sorted_docs:
            processed_path = job.get("processed_media_path")
            if processed_path:
                job_id = str(job.get("_id", ""))
                _try_delete_local_file(processed_path, context=f"album_delivery:{job_id}")