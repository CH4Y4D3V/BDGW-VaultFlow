from __future__ import annotations

import asyncio
from collections import defaultdict

from pyrogram import Client, filters
from pyrogram.enums import ParseMode
from pyrogram.errors import FloodWait, RPCError
from pyrogram.types import Message

from app.config import settings
from app.bot.client import get_bot_id
from app.moderation import verification_hub
from app.services import submission_service
from app.services.topic_service import get_topic_service, TOPIC_CONTENT
from app.utils.logger import get_logger

logger = get_logger(__name__)

_album_buffer: dict[str, list[Message]] = defaultdict(list)
_album_tasks: dict[str, asyncio.Task] = {}
_album_lock = asyncio.Lock()

_FLOOD_BUFFER = settings.FLOODWAIT_EXTRA_BUFFER
_MAX_RETRIES = 3

# ── Internal chat guard ───────────────────────────────────────────────────────

def _is_managed_chat(chat_id: int) -> bool:
    """
    Prevent feedback loops: never ingest from the verification hub,
    vault channel, or any destination group/channel.
    """
    managed = {
        settings.VERIFICATION_GROUP_ID,
        settings.VAULT_CHANNEL_ID,
    }
    if settings.NSFW_GROUP_ID:
        managed.add(settings.NSFW_GROUP_ID)
    if settings.PREMIUM_GROUP_ID:
        managed.add(settings.PREMIUM_GROUP_ID)
    if settings.LOG_CHANNEL_ID:
        managed.add(settings.LOG_CHANNEL_ID)
    return chat_id in managed


def _resolve_submitter_id(message: Message) -> int | None:
    """
    Resolve submitter:
    - Groups: from_user.id (real user)
    - Anonymous admin / channel post: use sender_chat.id (negative ID)
    """
    if message.from_user:
        return message.from_user.id
    if message.sender_chat:
        return message.sender_chat.id
    return None


# ── Pipeline ──────────────────────────────────────────────────────────────────

async def _submit_for_review(
    client: Client,
    messages: list[Message],
    submitter_id: int,
) -> None:
    reference = messages[0]
    try:
        topic_service = get_topic_service()
        topic_id = await topic_service.get_user_topic_id(submitter_id, TOPIC_CONTENT)

        await submission_service.register_pending(submitter_id, messages)
        success = await verification_hub.forward_to_verification(
            client=client, 
            messages=messages, 
            submitter_user_id=submitter_id,
            topic_id=topic_id
        )
        if not success:
            await submission_service.reject_pending(reference.id)
            logger.error(
                "Group submission failed to forward",
                extra={"ctx_submitter": submitter_id, "ctx_chat": reference.chat.id},
            )
    except Exception as e:
        logger.error(
            "Unexpected error in group submission pipeline",
            extra={"ctx_submitter": submitter_id, "ctx_error": str(e)},
            exc_info=True,
        )


async def _flush_album(buffer_key: str, submitter_id: int, client: Client) -> None:
    try:
        await asyncio.sleep(settings.MEDIA_GROUP_TIMEOUT_SECONDS)
    except asyncio.CancelledError:
        return

    async with _album_lock:
        messages = _album_buffer.pop(buffer_key, [])
        _album_tasks.pop(buffer_key, None)

    if not messages:
        return

    messages.sort(key=lambda m: m.id)
    logger.info(
        "Group album flushed",
        extra={"ctx_key": buffer_key, "ctx_submitter": submitter_id, "ctx_count": len(messages)},
    )
    await _submit_for_review(client, messages, submitter_id)


# ── Handler ───────────────────────────────────────────────────────────────────

@Client.on_message(
    (filters.photo | filters.video | filters.document | filters.animation)
    & (filters.group | filters.channel)
)
async def handle_group_media_submission(client: Client, message: Message) -> None:
    logger.info(
        "HANDLER: handle_group_media_submission entered",
        extra={
            "ctx_chat_id": message.chat.id if message.chat else None,
            "ctx_chat_type": str(message.chat.type) if message.chat else None,
            "ctx_from_user": message.from_user.id if message.from_user else None,
            "ctx_media_type": str(message.media) if message.media else None,
            "ctx_media_group_id": message.media_group_id,
            "ctx_is_managed": _is_managed_chat(message.chat.id),
        },
    )

    # Block managed internal chats — loop prevention
    if _is_managed_chat(message.chat.id):
        logger.debug(
            "group_handler: ignoring message from managed chat",
            extra={"ctx_chat_id": message.chat.id},
        )
        return

    submitter_id = _resolve_submitter_id(message)
    if submitter_id is None:
        logger.warning(
            "group_handler: could not resolve submitter_id — no from_user or sender_chat",
            extra={"ctx_msg_id": message.id},
        )
        return

    # FIX 8: Never process the bot's own messages.
    bot_id = get_bot_id()
    if bot_id is not None and message.from_user and message.from_user.id == bot_id:
        return

    media_group_id = message.media_group_id

    if not media_group_id:
        logger.info(
            "group_handler: single media — routing for review",
            extra={
                "ctx_submitter": submitter_id,
                "ctx_chat": message.chat.id,
                "ctx_msg_id": message.id,
            },
        )
        await _submit_for_review(client, [message], submitter_id)
        return

    buffer_key = f"grp_{message.chat.id}_{media_group_id}"

    async with _album_lock:
        _album_buffer[buffer_key].append(message)

        existing = _album_tasks.get(buffer_key)
        if existing and not existing.done():
            existing.cancel()

        task = asyncio.create_task(
            _flush_album(buffer_key, submitter_id, client),
            name=f"grp-album-{buffer_key}",
        )
        _album_tasks[buffer_key] = task

    logger.debug(
        "group_handler: album message buffered",
        extra={
            "ctx_submitter": submitter_id,
            "ctx_chat": message.chat.id,
            "ctx_group_id": media_group_id,
            "ctx_buffer_size": len(_album_buffer[buffer_key]),
        },
    )
