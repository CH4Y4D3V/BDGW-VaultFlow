from __future__ import annotations

import asyncio
from typing import Optional

from pyrogram import Client, filters
from pyrogram.enums import ParseMode
from pyrogram.types import CallbackQuery, ForceReply

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
    """
    RC-15: Robust recovery from Vault.
    Instead of re-fetching from the source chat (which might be deleted),
    we fetch from the Vault channel where the content was archived 
    during submission.
    """
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

    if not message_ids:
        return None

    try:
        vault_col = db[settings.VAULT_COLLECTION]
        vault_messages = []
        
        for mid in message_ids:
            # Match by source Chat ID and Message ID
            v_doc = await vault_col.find_one({
                "source_chat_id": str(chat_id),
                "source_message_id": mid
            })
            
            if v_doc and v_doc.get("vault_message_id"):
                v_msg = await client.get_messages(
                    chat_id=settings.VAULT_CHANNEL_ID,
                    message_ids=v_doc["vault_message_id"]
                )
                if v_msg and not v_msg.empty:
                    vault_messages.append(v_msg)

        if not vault_messages:
            # Fallback to source chat if vault lookup failed (e.g. legacy submissions)
            logger.info("Recovery: Vault lookup failed, falling back to source chat", extra={"ctx_msg_id": msg_id})
            messages = await client.get_messages(chat_id=chat_id, message_ids=message_ids)
            if not isinstance(messages, list):
                messages = [messages]
            return (submitter_user_id, [m for m in messages if m and not m.empty])

        return (submitter_user_id, vault_messages)

    except Exception as e:
        logger.warning("_recover_pending_from_db: Recovery failed", extra={"ctx_msg_id": msg_id, "ctx_error": str(e)})
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
            # ── SYSTEM 13: REJECTION FLOW ──
            # Instead of immediate reject, prompt moderator for reason
            await client.send_message(
                chat_id=callback.message.chat.id,
                text=(
                    f"📝 <b>Rejection Reason</b>\n\n"
                    f"Please reply to this message with the reason for rejecting submission from <code>{submitter_id}</code>.\n\n"
                    f"<i>The user will be notified and a support ticket will be opened.</i>"
                ),
                message_thread_id=callback.message.message_thread_id,
                reply_markup=ForceReply(placeholder="Reason for rejection..."),
            )
            # Store context in Redis to catch the reply
            redis = get_redis()
            ctx_key = f"mod_reject_ctx:{callback.message.chat.id}:{callback.message.id}"
            import json
            await redis.set(ctx_key, json.dumps({"submitter_id": submitter_id, "msg_id": msg_id}), ex=600)
            return
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


@Client.on_message(filters.chat(settings.VERIFICATION_GROUP_ID) & filters.reply)
async def handle_mod_reject_reason_reply(client: Client, message: Message) -> None:
    """Catches the reply with rejection reason and executes rejection."""
    if not message.from_user or not is_moderator(message.from_user.id):
        return

    replied_to = message.reply_to_message
    if not replied_to or not replied_to.from_user or not replied_to.from_user.is_bot:
        return

    # Check if this bot message was a rejection reason prompt
    if "Rejection Reason" not in (replied_to.text or ""):
        return

    redis = get_redis()
    # We need the original card ID to clean up.
    # The card was replied to by the bot to create the prompt.
    card_message = replied_to.reply_to_message
    if not card_message:
        return

    ctx_key = f"mod_reject_ctx:{card_message.chat.id}:{card_message.id}"
    ctx_raw = await redis.get(ctx_key)
    if not ctx_raw:
        return

    import json
    ctx = json.loads(ctx_raw)
    await redis.delete(ctx_key)

    reason = message.text.strip()
    submitter_id = ctx["submitter_id"]
    msg_id = ctx["msg_id"]
    
    # Recover messages for cleanup
    recovered = await _recover_pending_from_db(client, msg_id)
    messages = recovered[1] if recovered else []

    await execute_reject(
        client=client,
        submitter_user_id=submitter_id,
        mod_card_chat_id=card_message.chat.id,
        mod_card_message_id=card_message.id,
        moderator_name=message.from_user.first_name or str(message.from_user.id),
        moderator_id=message.from_user.id,
        messages=messages,
        reason=reason
    )
    
    # Cleanup moderator's input and bot's prompt
    try:
        await message.delete()
        await replied_to.delete()
    except:
        pass
