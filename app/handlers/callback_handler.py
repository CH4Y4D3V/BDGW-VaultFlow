from __future__ import annotations

import asyncio
from typing import Optional

from pyrogram import Client, filters
from pyrogram.enums import ParseMode
from pyrogram.types import CallbackQuery

from app.config import settings
from app.core.permissions import is_moderator
from app.moderation.verification_hub import parse_callback_data
from app.moderation.moderation_actions import (
    execute_queue,
    execute_reject,
    safe_edit_message,
)
from app.services import submission_service
from app.utils.logger import get_logger

logger = get_logger(__name__)

_MAX_RETRIES = 3

async def _recover_pending_from_db(
    client: Client,
    msg_id: int,
) -> Optional[tuple[int, list]]:
    try:
        from app.core.database import DatabaseManager
        db = DatabaseManager.get_db()
        pending_doc = await db[settings.PENDING_COLLECTION].find_one({"key": msg_id})
    except Exception as e:
        logger.warning("_recover_pending_from_db: DB lookup failed", extra={"ctx_msg_id": msg_id, "ctx_error": str(e)})
        return None

    if not pending_doc:
        return None

    chat_id = pending_doc.get("chat_id", 0)
    message_ids = pending_doc.get("message_ids", [])
    submitter_user_id = pending_doc.get("submitter_user_id", 0)

    if not chat_id or not message_ids:
        return None

    try:
        messages = await client.get_messages(chat_id=chat_id, message_ids=message_ids)
        if not isinstance(messages, list):
            messages = [messages]
        messages = [m for m in messages if m and m.id]
        if not messages:
            return None
        return (submitter_user_id, messages)
    except Exception as e:
        logger.warning("_recover_pending_from_db: Telegram re-fetch failed", extra={"ctx_msg_id": msg_id, "ctx_error": str(e)})
        return None


@Client.on_callback_query(filters.regex(r"^mod_(app_nsfw|app_prem|reject):"))
async def handle_moderation_callback(client: Client, callback: CallbackQuery) -> None:
    try:
        await callback.answer()
        
        logger.info(
            "moderation_callback_received",
            extra={
                "ctx_moderator_id": callback.from_user.id,
                "ctx_data": callback.data
            }
        )
        
        if not callback.message or not callback.message.chat:
            return

        if callback.message.chat.id != settings.VERIFICATION_GROUP_ID:
            await callback.answer("Unauthorized location.", show_alert=True)
            return

        moderator_id = callback.from_user.id
        if not is_moderator(moderator_id):
            await callback.answer("⛔ Unauthorized.", show_alert=True)
            return

        parsed = parse_callback_data(callback.data)
        if parsed is None:
            await callback.answer("Malformed data.", show_alert=True)
            return

        action = parsed["action"]
        submitter_id = parsed["submitter_id"]
        msg_id = parsed["msg_id"]
        moderator_name = callback.from_user.first_name or str(moderator_id)

        # Recover messages
        entry = submission_service.pop_pending(msg_id)
        if entry is None:
            recovered = await _recover_pending_from_db(client, msg_id)
            if recovered is None:
                await callback.answer("Submission not found.", show_alert=True)
                return
            submitter_id, messages = recovered
        else:
            _, messages = entry

        if action == "reject":
            await execute_reject(
                client=client,
                submitter_user_id=submitter_id,
                mod_card_chat_id=callback.message.chat.id,
                mod_card_message_id=callback.message.id,
                moderator_name=moderator_name,
                moderator_id=moderator_id,
            )
        else:
            dest = "nsfw" if action == "app_nsfw" else "premium"
            await callback.answer(f"⏳ Queuing for {dest.upper()}...", show_alert=False)
            
            await execute_queue(
                client=client,
                messages=messages,
                submitter_user_id=submitter_id,
                dest=dest,
                mod_card_chat_id=callback.message.chat.id,
                mod_card_message_id=callback.message.id,
                moderator_name=moderator_name,
                moderator_id=moderator_id,
            )

    except Exception as e:
        logger.exception("Moderation callback failed", exc_info=True)
        try:
            await callback.answer("⚠️ Error occurred.", show_alert=True)
        except:
            pass
