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
        await submission_service.register_pending(submitter_id, messages)
        success = await verification_hub.forward_to_verification(client, messages, submitter_id)
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
    # Block managed internal chats
    if _is_managed_chat(message.chat.id):
        return

    submitter_id = _resolve_submitter_id(message)
    if submitter_id is None:
        return

    # Never process the bot's own messages
    try:
        me = await client.get_me()
        if message.from_user and message.from_user.id == me.id:
            return
    except Exception:
        pass

    media_group_id = message.media_group_id

    if not media_group_id:
        logger.info(
            "Single group media submitted",
            extra={
                "ctx_submitter": submitter_id,
                "ctx_chat": message.chat.id,
                "ctx_msg_id": message.id,
            },
        )
        await _submit_for_review(client, [message], submitter_id)
        return

    # Album buffering — keyed by chat+group to prevent cross-chat collisions
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
        "Group album buffered",
        extra={
            "ctx_submitter": submitter_id,
            "ctx_chat": message.chat.id,
            "ctx_group_id": media_group_id,
            "ctx_buffer_size": len(_album_buffer[buffer_key]),
        },
    )
