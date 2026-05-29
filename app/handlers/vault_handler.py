from __future__ import annotations

import asyncio
from pyrogram import Client, filters
from pyrogram.types import Message

from app.config import settings
from app.core.permissions import is_moderator
from app.moderation.moderation_actions import enqueue_for_distribution
from app.utils.logger import get_logger

logger = get_logger(__name__)


@Client.on_message(filters.chat(settings.VAULT_CHANNEL_ID) & (filters.photo | filters.video))
async def handle_direct_vault_upload(client: Client, message: Message) -> None:
    """
    F-09: Listen for media uploaded directly to Vault channel.
    If caption contains #nsfw or #premium, automatically watermark and enqueue.
    """
    # Note: message.from_user might be None in channels, but we can check sender_chat or just trust the channel
    # Since it's the Vault channel, only admins should have access.
    
    caption = message.caption or ""
    if not caption:
        return

    dest = None
    if "#nsfw" in caption.lower():
        dest = "nsfw"
    elif "#premium" in caption.lower():
        dest = "premium"

    if not dest:
        return

    logger.info(
        "Direct vault upload detected",
        extra={
            "ctx_msg_id": message.id,
            "ctx_dest": dest,
            "ctx_caption": caption
        }
    )

    try:
        # Since it's already in the vault, we just need the vault_message_id
        vault_ids = [message.id]
        
        # Enqueue for distribution
        # We use a dummy submitter_user_id (0) for admin uploads
        success = await enqueue_for_distribution(
            messages=[message],
            dest=dest,
            submitter_user_id=0,
            vault_message_ids=vault_ids
        )

        if success:
            logger.info("Direct vault upload enqueued successfully", extra={"ctx_msg_id": message.id})
            # Optionally react or reply to acknowledge
            try:
                await message.reply_text(f"✅ Auto-enqueued for <b>{dest.upper()}</b>.", quote=True)
            except:
                pass
        else:
            logger.error("Failed to enqueue direct vault upload", extra={"ctx_msg_id": message.id})

    except Exception as e:
        logger.exception("Error handling direct vault upload", extra={"ctx_error": str(e)})
