import asyncio
from typing import List

from pyrogram.client import Client
from pyrogram.types import InputMediaPhoto, InputMediaVideo, InputMediaDocument
from pyrogram.errors import FloodWait, RPCError

from app.bot.client import get_bot
from app.core.exceptions import FloodWaitError, DispatcherError
from app.core.models import MediaType
from app.utils.logger import get_logger

logger = get_logger(__name__)


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