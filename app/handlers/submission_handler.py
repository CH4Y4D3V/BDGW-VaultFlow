# app/handlers/submission_handler.py
"""
Handles all user-submitted content (images, videos, text) in private chat.
"""
from __future__ import annotations

import asyncio
from collections import defaultdict

from pyrogram import Client, filters
from pyrogram.enums import ParseMode
from pyrogram.types import Message

from app.config import settings
from app.core.database import DatabaseManager
from app.services.submission_service import SubmissionService
from app.services.topic_manager import get_topic_manager
from app.ui.submission_cards import format_submission_card
from app.utils.logger import get_logger

logger = get_logger(__name__)

# ── In-process album buffer (replaces the missing app.storage module) ─────────
# keyed by media_group_id → list of Messages
_album_buffer: dict[str, list[Message]] = defaultdict(list)
_album_tasks: dict[str, asyncio.Task] = {}
_album_lock = asyncio.Lock()


@Client.on_message(
    (filters.media | filters.text)
    & filters.private
    & ~filters.command(["start", "rules", "mystatus", "ping", "help", "takedown", "cancel", "become_creator"])
    & ~filters.bot
)
async def handle_submission(client: Client, message: Message) -> None:
    """
    Main entry point for all user submissions.
    Buffers album messages, then forwards to the verification hub.
    """
    if not message.from_user:
        return

    user_id = message.from_user.id

    # ── Check consent first ───────────────────────────────────────────────────
    db = DatabaseManager.get_db()
    service = SubmissionService(db)

    if not await service.has_consent(user_id):
        # Consent gate handled by creator_onboarding.py — raise to propagate
        from pyrogram import ContinuePropagation
        raise ContinuePropagation

    # ── Media group (album) buffering ─────────────────────────────────────────
    if message.media_group_id:
        await _handle_album_message(client, message, user_id)
        return

    # ── Single message ────────────────────────────────────────────────────────
    await _process_submission(client, user_id, [message])


async def _handle_album_message(client: Client, message: Message, user_id: int) -> None:
    """Buffer album frames and flush after ALBUM_COLLECTION_SECONDS."""
    gid = message.media_group_id

    async with _album_lock:
        is_new = gid not in _album_buffer
        _album_buffer[gid].append(message)

        # Cancel any existing flush task and reschedule
        existing = _album_tasks.get(gid)
        if existing and not existing.done():
            existing.cancel()

        task = asyncio.create_task(
            _flush_album(client, gid, user_id),
            name=f"album-flush-{gid}",
        )
        _album_tasks[gid] = task


async def _flush_album(client: Client, gid: str, user_id: int) -> None:
    """Wait for album collection window, then process all buffered frames."""
    try:
        await asyncio.sleep(getattr(settings, "ALBUM_COLLECTION_SECONDS", 3.0))
    except asyncio.CancelledError:
        return

    async with _album_lock:
        messages = _album_buffer.pop(gid, [])
        _album_tasks.pop(gid, None)

    if not messages:
        return

    messages.sort(key=lambda m: m.id)
    await _process_submission(client, user_id, messages)


async def _process_submission(client: Client, user_id: int, messages: list[Message]) -> None:
    """
    Core submission processing:
    1. Get/create user hub topic
    2. Forward content to hub
    3. Post moderation card
    4. Create pending submission record
    """
    db = DatabaseManager.get_db()
    service = SubmissionService(db)

    # ── Get user's hub topic ──────────────────────────────────────────────────
    topic_manager = get_topic_manager()
    first = messages[0]
    user = first.from_user

    topic_id = await topic_manager.get_or_create_user_topic(
        client,
        user_id,
        full_name=user.full_name if user else str(user_id),
        username=user.username if user else None,
    )

    if not topic_id:
        logger.error("submission_aborted_no_topic", extra={"ctx_user_id": user_id})
        try:
            await first.reply_text(
                "Sorry, there was an issue preparing your submission. Please try again in a moment."
            )
        except Exception:
            pass
        return

    # ── Forward to hub ────────────────────────────────────────────────────────
    try:
        await client.forward_messages(
            chat_id=settings.VERIFICATION_GROUP_ID,
            from_chat_id=user_id,
            message_ids=[m.id for m in messages],
            message_thread_id=topic_id,
        )
    except Exception as e:
        logger.error(
            "submission_forward_failed",
            extra={"ctx_user_id": user_id, "ctx_topic_id": topic_id, "ctx_error": str(e)},
        )
        try:
            await first.reply_text("Your submission could not be processed. Please try again.")
        except Exception:
            pass
        return

    # ── Post moderation card ──────────────────────────────────────────────────
    try:
        media_type = first.media.value if first.media else "text"
        card = format_submission_card(
            user_id,
            user.full_name if user else str(user_id),
            user.username if user else None,
            len(messages),
            media_type,
        )
        card_message = await client.send_message(
            chat_id=settings.VERIFICATION_GROUP_ID,
            text=card,
            message_thread_id=topic_id,
            parse_mode=ParseMode.HTML,
        )
    except Exception as e:
        logger.error(
            "submission_card_post_failed",
            extra={"ctx_user_id": user_id, "ctx_topic_id": topic_id, "ctx_error": str(e)},
        )
        try:
            await first.reply_text("Your submission could not be processed. Please try again.")
        except Exception:
            pass
        return

    # ── Persist pending record ────────────────────────────────────────────────
    try:
        await service.create_pending_submission(
            user_id=user_id,
            messages=messages,
            hub_topic_id=topic_id,
            hub_card_message_id=card_message.id,
        )
    except Exception as e:
        logger.error(
            "submission_persist_failed",
            extra={"ctx_user_id": user_id, "ctx_error": str(e)},
        )

    # ── Acknowledge user ──────────────────────────────────────────────────────
    try:
        await messages[-1].reply_text("✅ Your submission has been received and is pending review.")
    except Exception:
        pass