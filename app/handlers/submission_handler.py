"""
app/handlers/submission_handler.py
----------------------------------
Handles all user-submitted content (images, videos, text) in private chat.
"""
from __future__ import annotations

import asyncio

from pyrogram import Client, filters
from pyrogram.enums import ParseMode
from pyrogram.types import Message

from app.config import settings
from app.core.database import DatabaseManager
from app.services.submission_service import SubmissionService
from app.services.topic_manager import get_topic_manager
from app.storage import get_submission_cache
from app.ui.submission_cards import format_submission_card
from app.utils.logger import get_logger

logger = get_logger(__name__)


@Client.on_message(
    (filters.media | filters.text)
    & filters.private
    & ~filters.command(["start", "rules", "mystatus", "ping", "help"])
    & ~filters.bot
)
async def handle_submission(client: Client, message: Message) -> None:
    """
    Main entry point for all user submissions.

    This handler collects messages into an album if they arrive in a burst,
    then forwards them to the user's dedicated topic in the verification hub
    and posts a moderation card.
    """
    user_id = message.from_user.id
    cache = get_submission_cache()

    # Collect media group items
    if message.media_group_id:
        is_new = cache.add_to_group(message)
        if is_new:
            # First message in group; wait for more
            await asyncio.sleep(settings.ALBUM_COLLECTION_SECONDS)
            messages = cache.pop_group(message.media_group_id)
            if messages:
                await _process_submission(client, user_id, messages)
        return  # Subsequent messages in group are ignored

    # Handle single message
    await _process_submission(client, user_id, [message])


async def _process_submission(client: Client, user_id: int, messages: list[Message]) -> None:
    """
    Core submission processing logic.

    1. Checks for consent.
    2. Routes to user's hub topic.
    3. Posts moderation card.
    4. Creates pending submission record in DB.
    """
    db = DatabaseManager.get_db()
    service = SubmissionService(db)

    # 1. Check consent
    if not await service.has_consent(user_id):
        await messages[0].reply_text(
            "You must accept the Creator Terms of Service before submitting content. "
            "Please use the /start command to review and accept the terms."
        )
        return

    # 2. Get user's hub topic
    topic_manager = get_topic_manager()
    topic_id = await topic_manager.get_or_create_user_topic(client, user_id)
    
    # NEW-07 (REG-03) FIX: Handle topic creation failure
    if not topic_id:
        logger.error("submission_aborted_no_topic", extra={"ctx_user_id": user_id})
        await messages[0].reply_text(
            "Sorry, there was an issue preparing your submission. Please try again in a moment."
        )
        return

    # 3. Forward to hub
    try:
        hub_messages = await client.forward_messages(
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
        await messages[0].reply_text("Your submission could not be processed. Please try again.")
        return

    # 4. Post moderation card
    user = messages[0].from_user
    card = format_submission_card(
        user.id,
        user.full_name,
        user.username,
        len(messages),
        messages[0].media.value if messages[0].media else "text",
    )
    try:
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
        # Attempt to clean up the forwarded messages if the card fails
        try:
            await client.delete_messages(
                settings.VERIFICATION_GROUP_ID, [m.id for m in hub_messages]
            )
        except Exception:
            pass
        await messages[0].reply_text("Your submission could not be processed. Please try again.")
        return

    # 5. Create pending submission record
    await service.create_pending_submission(
        user_id=user_id,
        messages=messages,
        hub_topic_id=topic_id,
        hub_card_message_id=card_message.id,
    )

    await messages[-1].reply_text("✅ Your submission has been received and is pending review.")
