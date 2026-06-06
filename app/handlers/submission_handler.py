from __future__ import annotations
from typing import Optional

from pyrogram import Client, filters
from pyrogram.types import Message

from app.services import submission_service
from app.utils.logger import get_logger

logger = get_logger(__name__)


@Client.on_message(filters.private & (filters.media | filters.text))
async def handle_submission(client: Client, message: Message) -> None:
    """
    Handles all private messages from users, treating them as submissions.
    """
    if not message.from_user:
        return

    user_id = message.from_user.id
    logger.info("handle_submission", extra={"ctx_user_id": user_id, "ctx_msg_id": message.id})

    # This is a placeholder implementation.
    # It should buffer album messages and then register them as a pending submission.
    # For now, it just acknowledges the message.
    
    # Simulate registering the submission. In a real implementation, this would
    # involve forwarding to a verification group and then calling register_pending.
    # For now, we will just log it.
    
    logger.info("Simulating submission registration", extra={"ctx_user_id": user_id, "ctx_msg_id": message.id})
    
    await message.reply_text("Your submission has been received and is pending review.")
