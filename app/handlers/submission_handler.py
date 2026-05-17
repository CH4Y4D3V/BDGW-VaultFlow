from __future__ import annotations

import asyncio
from collections import defaultdict

from pyrogram import Client, filters
from pyrogram.enums import ParseMode
from pyrogram.errors import FloodWait, RPCError
from pyrogram.types import Message

from app.config import settings
from app.moderation import verification_hub
from app.services import submission_service
from app.utils.logger import get_logger

logger = get_logger(__name__)

_album_buffer: dict[str, list[Message]] = defaultdict(list)
_album_tasks: dict[str, asyncio.Task] = {}
_album_lock = asyncio.Lock()

_FLOOD_BUFFER = settings.FLOODWAIT_EXTRA_BUFFER
_MAX_REPLY_RETRIES = 3


async def _safe_reply(
    message: Message,
    text: str,
    parse_mode: ParseMode = ParseMode.HTML,
) -> None:
    for attempt in range(_MAX_REPLY_RETRIES):
        try:
            await message.reply_text(text, parse_mode=parse_mode)
            return
        except FloodWait as e:
            wait = int(e.value) + _FLOOD_BUFFER
            logger.warning(
                "FloodWait on reply",
                extra={"ctx_msg_id": message.id, "ctx_wait": wait, "ctx_attempt": attempt + 1},
            )
            await asyncio.sleep(wait)
        except RPCError as e:
            logger.warning(
                "RPC error replying to user",
                extra={"ctx_msg_id": message.id, "ctx_error": str(e), "ctx_attempt": attempt + 1},
            )
            if attempt == _MAX_REPLY_RETRIES - 1:
                return
            await asyncio.sleep(2 ** attempt)


async def _submit_for_review(
    client: Client,
    messages: list[Message],
    user_id: int,
) -> None:
    reference_message = messages[0]

    try:
        await submission_service.register_pending(user_id, messages)
        success = await verification_hub.forward_to_verification(client, messages, user_id)

        if success:
            await _safe_reply(
                reference_message,
                "📨 Your submission is <b>under review</b>. "
                "You'll be notified once a decision is made.",
            )
        else:
            await submission_service.reject_pending(reference_message.id)
            await _safe_reply(
                reference_message,
                "⚠️ We couldn't forward your submission right now. "
                "Please try again in a moment.",
            )

    except Exception as e:
        logger.error(
            "Unexpected error during submission pipeline",
            extra={"ctx_user_id": user_id, "ctx_error": str(e)},
            exc_info=True,
        )
        await _safe_reply(
            reference_message,
            "⚠️ An unexpected error occurred. Please try again later.",
        )


async def _flush_album(group_id: str, user_id: int, client: Client) -> None:
    try:
        await asyncio.sleep(settings.MEDIA_GROUP_TIMEOUT_SECONDS)
    except asyncio.CancelledError:
        return

    async with _album_lock:
        messages = _album_buffer.pop(group_id, [])
        _album_tasks.pop(group_id, None)

    if not messages:
        return

    messages.sort(key=lambda m: m.id)

    logger.info(
        "Album flushed for review",
        extra={"ctx_group_id": group_id, "ctx_user_id": user_id, "ctx_count": len(messages)},
    )
    await _submit_for_review(client, messages, user_id)


@Client.on_message(filters.command("start") & filters.private)
async def handle_start(client: Client, message: Message) -> None:
    if not message.from_user:
        return

    name = message.from_user.first_name or "there"
    await _safe_reply(
        message,
        f"👋 Hello, <b>{name}</b>!\n\n"
        "Send me a photo, video, document, or animation and I'll forward it "
        "to our team for review.\n\n"
        "You'll receive a notification once a decision has been made.",
    )
    logger.info("/start received", extra={"ctx_user_id": message.from_user.id})


@Client.on_message(
    (filters.photo | filters.video | filters.document | filters.animation)
    & filters.private
)
async def handle_media_submission(client: Client, message: Message) -> None:
    if not message.from_user:
        return

    user_id = message.from_user.id
    group_id = message.media_group_id

    if not group_id:
        logger.info(
            "Single media submission received",
            extra={"ctx_user_id": user_id, "ctx_msg_id": message.id},
        )
        await _submit_for_review(client, [message], user_id)
        return

    async with _album_lock:
        _album_buffer[group_id].append(message)

        existing_task = _album_tasks.get(group_id)
        if existing_task and not existing_task.done():
            existing_task.cancel()

        task = asyncio.create_task(
            _flush_album(group_id, user_id, client),
            name=f"album-flush-{group_id}",
        )
        _album_tasks[group_id] = task

    logger.debug(
        "Album message buffered",
        extra={
            "ctx_user_id": user_id,
            "ctx_group_id": group_id,
            "ctx_msg_id": message.id,
            "ctx_buffer_size": len(_album_buffer[group_id]),
        },
    )
