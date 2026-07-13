from __future__ import annotations

import asyncio
from collections import defaultdict

from pyrogram import Client, filters
from pyrogram.enums import ParseMode
from pyrogram.errors import FloodWait, RPCError
from pyrogram.types import Message

from app.config import settings
from app.bot.client import get_bot_id
from app.core.database import DatabaseManager
from app.moderation import verification_hub
from app.services import submission_service
from app.services.topic_manager import get_topic_manager, TOPIC_CONTENT
from app.utils.logger import get_logger

logger = get_logger(__name__)

_album_buffer: dict[str, list[Message]] = defaultdict(list)
_album_tasks: dict[str, asyncio.Task] = {}
_album_lock = asyncio.Lock()

_FLOOD_BUFFER = settings.FLOODWAIT_EXTRA_BUFFER
_MAX_RETRIES = 3

# ── Prefix Auto-Delete ────────────────────────────────────────────────────────

@Client.on_message(filters.regex(r"^\./") & filters.group, group=-1)
async def handle_prefix_auto_delete(client: Client, message: Message) -> None:
    """
    Section 4.3 / Section 20: Any message starting with ./ in groups
    deleted after 10 seconds. Silent — no notification.
    """
    async def _delayed_delete():
        await asyncio.sleep(10)
        try:
            await message.delete()
            logger.debug(
                "prefix_message_deleted",
                extra={
                    "ctx_chat_id": message.chat.id,
                    "ctx_user_id": message.from_user.id if message.from_user else None,
                },
            )
        except Exception as e:
            logger.warning(
                "prefix_delete_failed",
                extra={"ctx_error": str(e), "ctx_chat_id": message.chat.id},
            )

    asyncio.create_task(_delayed_delete())


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
        topic_manager = get_topic_manager()
        topic_id = await topic_manager.get_user_topic_id(submitter_id, TOPIC_CONTENT)

        # forward_to_verification now returns list[int] (hub forwarded IDs) or []
        hub_forwarded_ids = await verification_hub.forward_to_verification(
            client=client,
            messages=messages,
            submitter_user_id=submitter_id,
            topic_id=topic_id
        )
        success = bool(hub_forwarded_ids)
        if not success:
            await submission_service.reject_pending(reference.id)
            logger.error(
                "Group submission failed to forward",
                extra={"ctx_submitter": submitter_id, "ctx_chat": reference.chat.id},
            )
            return

        # ROOT CAUSE FIX: this function previously never registered the
        # pending submission anywhere — neither in the in-memory
        # _pending_submissions dict nor as a proper MongoDB document. It only
        # called forward_to_verification() (which posts the moderation card
        # to the hub but does NOT persist any pending state) and then
        # attempted a bare update_one() against PENDING_COLLECTION filtered
        # by first_msg_id — WITHOUT upsert=True. Since no document with that
        # first_msg_id had ever been inserted, that update_one() silently
        # matched and modified zero documents every single time.
        #
        # Consequence: when an admin clicked Approve/Reject on a moderation
        # card for a GROUP-sourced submission, handle_moderation_callback's
        # submission_service.pop_pending(msg_id) found nothing in memory,
        # fell back to _recover_pending_from_db(), which ALSO found nothing
        # (no document ever existed to recover), and the callback showed
        # "Submission not found in registry or vault." every time — silently
        # and permanently, for every group-sourced submission, regardless of
        # how quickly the admin approved it.
        #
        # Fix: call create_pending_submission() exactly as submission_handler.py
        # (the private-chat path) already does. This writes BOTH the in-memory
        # cache (so pop_pending() succeeds immediately, no restart needed) and
        # the MongoDB document (first_msg_id, message_ids pointing at the
        # ORIGINAL group-chat messages, hub_forwarded_ids/hub_topic_id/
        # hub_chat_id for hub-based recovery) via a single upsert=True call.
        try:
            from app.services.submission_service import SubmissionService
            db = DatabaseManager.get_db()
            service = SubmissionService(db)
            await service.create_pending_submission(
                user_id=submitter_id,
                messages=messages,
                hub_topic_id=topic_id,
                hub_card_message_id=0,
                hub_forwarded_ids=hub_forwarded_ids,
            )
        except Exception as persist_err:
            logger.error(
                "group_handler: create_pending_submission failed",
                extra={"ctx_submitter": submitter_id, "ctx_error": str(persist_err)},
                exc_info=True,
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
    
    # ── SYSTEM 12: USER CONFIRMATION ──
    try:
        await client.send_message(
            chat_id=submitter_id,
            text=(
                "✅ <b>Album Submitted</b>\n\n"
                f"Your album (<code>{len(messages)}</code> items) has been received and sent for review.\n"
                "You will be notified once a decision is made.\n\n"
                "<i>Thank you for contributing!</i>"
            ),
            parse_mode=ParseMode.HTML
        )
    except Exception:
        pass


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
    
        # ── SYSTEM 12: USER CONFIRMATION ──
        try:
            await client.send_message(
                chat_id=submitter_id,
                text=(
                    "✅ <b>Content Submitted</b>\n\n"
                    "Your media has been successfully received and sent to our moderators for review.\n"
                    "You will be notified once a decision is made.\n\n"
                    "<i>Thank you for contributing!</i>"
                ),
                parse_mode=ParseMode.HTML
            )
        except Exception:
            pass
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
