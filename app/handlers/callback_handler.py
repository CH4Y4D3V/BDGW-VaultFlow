from __future__ import annotations

import asyncio
from typing import Optional

from pyrogram import Client, ContinuePropagation, filters
from pyrogram.enums import ParseMode
from pyrogram.types import CallbackQuery, ForceReply, Message

from app.config import settings
from app.core.permissions import is_moderator
from app.core.redis_client import get_redis
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
    RC-15: Recover submission messages from vault when in-memory registry
    has been cleared (e.g. after bot restart).
    Fetches messages from the vault channel using stored vault_message_ids.
    """
    try:
        from app.core.database import DatabaseManager
        db = DatabaseManager.get_db()
        pending_doc = await db[settings.PENDING_COLLECTION].find_one({"first_msg_id": msg_id})
    except Exception as e:
        logger.warning(
            "_recover_pending_from_db: DB lookup failed",
            extra={"ctx_msg_id": msg_id, "ctx_error": str(e)},
        )
        return None

    if not pending_doc:
        return None

    submitter_user_id = pending_doc.get("user_id", 0)
    # Private chats: chat_id == user_id (no separate chat_id field is stored)
    chat_id = submitter_user_id
    message_ids = pending_doc.get("message_ids", [])

    if not message_ids:
        return None

    try:
        from app.core.database import DatabaseManager
        db = DatabaseManager.get_db()
        vault_col = db[settings.VAULT_COLLECTION]
        vault_messages = []

        for mid in message_ids:
            v_doc = await vault_col.find_one({
                "source_chat_id": str(chat_id),
                "source_message_id": mid,
            })

            if v_doc and v_doc.get("vault_message_id"):
                try:
                    v_msg = await client.get_messages(
                        chat_id=settings.VAULT_CHANNEL_ID,
                        message_ids=v_doc["vault_message_id"],
                    )
                    if v_msg and not getattr(v_msg, "empty", True):
                        vault_messages.append(v_msg)
                except Exception:
                    pass

        if vault_messages:
            return (submitter_user_id, vault_messages)

        # Fallback to source chat (may have been deleted)
        logger.info(
            "_recover_pending_from_db: vault lookup failed, trying source chat",
            extra={"ctx_msg_id": msg_id},
        )
        messages = await client.get_messages(chat_id=chat_id, message_ids=message_ids)
        if not isinstance(messages, list):
            messages = [messages]
        valid = [m for m in messages if m and not getattr(m, "empty", True)]
        if valid:
            return (submitter_user_id, valid)

    except Exception as e:
        logger.warning(
            "_recover_pending_from_db: recovery failed",
            extra={"ctx_msg_id": msg_id, "ctx_error": str(e)},
        )

    return None


# ── Moderation callback ───────────────────────────────────────────────────────

@Client.on_callback_query(filters.regex(r"^mod_(app_nsfw|app_prem|reject):"))
async def handle_moderation_callback(client: Client, callback: CallbackQuery) -> None:
    try:
        await callback.answer()

        logger.info(
            "moderation_callback_received",
            extra={
                "ctx_moderator_id": callback.from_user.id,
                "ctx_data": callback.data,
            },
        )

        if not callback.message or not callback.message.chat:
            return

        if callback.message.chat.id != settings.VERIFICATION_GROUP_ID:
            await callback.answer("Unauthorized location.", show_alert=True)
            return

        moderator_id = callback.from_user.id
        if not await is_moderator(moderator_id):
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

        # Recover messages from in-memory registry or DB fallback
        entry = submission_service.pop_pending(msg_id)
        if entry is None:
            recovered = await _recover_pending_from_db(client, msg_id)
            if recovered is None:
                await callback.answer(
                    "Submission not found in registry or vault.", show_alert=True
                )
                return
            submitter_id, messages = recovered
        else:
            _, messages = entry

        if action == "reject":
            # Section 14.5: Admin must TYPE a reason — prompt with ForceReply
            try:
                await client.send_message(
                    chat_id=callback.message.chat.id,
                    text=(
                        f"📝 <b>Rejection Reason Required</b>\n\n"
                        f"Reply to this message with your reason for rejecting "
                        f"submission from <code>{submitter_id}</code>.\n\n"
                        f"<i>The user will be notified and a support ticket "
                        f"will be opened automatically.</i>"
                    ),
                    message_thread_id=getattr(
                        callback.message, "message_thread_id", None
                    ),
                    reply_markup=ForceReply(
                        placeholder="Type rejection reason here..."
                    ),
                    parse_mode=ParseMode.HTML,
                )
            except Exception as e:
                logger.warning(
                    "Failed to send rejection reason prompt",
                    extra={"ctx_error": str(e)},
                )

            # Store context in Redis for the reply handler to pick up
            redis = get_redis()
            ctx_key = f"mod_reject_ctx:{callback.message.chat.id}:{callback.message.id}"
            import json
            await redis.set(
                ctx_key,
                json.dumps({"submitter_id": submitter_id, "msg_id": msg_id}),
                ex=600,
            )
            return

        else:
            dest = "nsfw" if action == "app_nsfw" else "premium"
            await callback.answer(
                f"⏳ Queuing for {dest.upper()}...", show_alert=False
            )

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
        logger.exception(
            "Moderation callback failed",
            extra={"ctx_error": str(e)},
        )
        try:
            await callback.answer("⚠️ Error occurred.", show_alert=True)
        except Exception:
            pass


# ── Reject reason reply handler ───────────────────────────────────────────────

@Client.on_message(filters.chat(settings.VERIFICATION_GROUP_ID) & filters.reply)
async def handle_mod_reject_reason_reply(
    client: Client, message: Message
) -> None:
    """
    Catches moderator's typed rejection reason and executes the reject flow.
    Section 14.5: Rejection reason is mandatory — cannot be skipped.

    NOTE: this filter matches ANY reply in the hub chat, not just rejection
    reason replies. A plain `return` here STOPS Pyrogram's dispatch chain
    entirely (only `raise ContinuePropagation` continues to the next
    handler), which silently swallowed admin replies meant for
    topic_router.route_admin_reply_to_user (e.g. an admin using "Reply" to
    respond to a user in their support topic). Every early-exit below must
    raise ContinuePropagation, not return.
    """
    if not message.from_user or not await is_moderator(message.from_user.id):
        raise ContinuePropagation

    replied_to = message.reply_to_message
    if not replied_to:
        raise ContinuePropagation

    # Check if the replied-to message is our rejection reason prompt
    # (from the bot, containing "Rejection Reason Required")
    if not replied_to.from_user or not replied_to.from_user.is_bot:
        raise ContinuePropagation

    if "Rejection Reason Required" not in (replied_to.text or ""):
        raise ContinuePropagation

    # Find the original moderation card (the message the bot replied to)
    card_message = replied_to.reply_to_message
    if not card_message:
        raise ContinuePropagation

    redis = get_redis()
    ctx_key = f"mod_reject_ctx:{card_message.chat.id}:{card_message.id}"
    ctx_raw = await redis.get(ctx_key)
    if not ctx_raw:
        # Redis key expired or was never set — this is NOT a rejection reason
        # reply we should handle. Yield to the next handler (topic_router) so
        # the message can be bridged to the user as a normal admin reply.
        # Previously used `return` here which silently dropped the message:
        # neither execute_reject ran nor did topic_router get to bridge it.
        raise ContinuePropagation

    import json
    ctx = json.loads(ctx_raw)
    await redis.delete(ctx_key)

    reason = (message.text or "").strip()
    if not reason:
        await message.reply_text("❌ Rejection reason cannot be empty.")
        # StopPropagation not ContinuePropagation: the message context is consumed
        # (Redis key was already deleted above). We own this message — don't let
        # route_admin_reply_to_user also try to forward the empty-text message.
        raise StopPropagation

    submitter_id = ctx["submitter_id"]
    msg_id = ctx["msg_id"]

    # Recover messages for hub cleanup
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
        reason=reason,
    )

    # Clean up: delete moderator's input and the prompt message
    try:
        await message.delete()
    except Exception:
        pass
    try:
        await replied_to.delete()
    except Exception:
        pass
    # StopPropagation: rejection is fully handled. Without this, Pyrogram
    # moves to group=10 (route_admin_reply_to_user) which tries copy_message
    # on the now-deleted admin message → logged as MessageIdInvalid every time.
    raise StopPropagation