from __future__ import annotations

from typing import Optional

from pyrogram import Client, filters
from pyrogram.enums import ParseMode
from pyrogram.types import CallbackQuery, InlineKeyboardButton, InlineKeyboardMarkup

from app.config import settings
from app.core.permissions import is_moderator
from app.moderation.verification_hub import parse_callback_data
from app.moderation.moderation_actions import (
    execute_approve,
    execute_queue,
    execute_reject,
    safe_edit_message,
)
from app.services import submission_service
from app.utils.logger import get_logger

logger = get_logger(__name__)


# ── Destination selection keyboard ────────────────────────────────────────────

def _destination_keyboard(
    action: str, submitter_id: int, msg_id: int
) -> InlineKeyboardMarkup:
    if action == "approve":
        label_nsfw = "🔞 NSFW"
        label_premium = "⭐ Premium"
    else:
        label_nsfw = "🔞 NSFW Queue"
        label_premium = "⭐ Premium Queue"

    return InlineKeyboardMarkup([
        [
            InlineKeyboardButton(
                label_nsfw,
                callback_data=f"mod_dest:{action}:nsfw:{submitter_id}:{msg_id}",
            ),
            InlineKeyboardButton(
                label_premium,
                callback_data=f"mod_dest:{action}:premium:{submitter_id}:{msg_id}",
            ),
        ]
    ])


# ── Pending restart recovery ──────────────────────────────────────────────────

async def _recover_pending_from_db(
    client: Client,
    msg_id: int,
) -> Optional[tuple[int, list]]:
    """
    WARNING fix: race condition — pop_pending() returns None after restart.

    When the in-memory pending registry is empty (e.g. after bot restart),
    look up the submission in MongoDB and re-fetch messages from Telegram.

    Returns (submitter_user_id, messages) on success, None otherwise.
    Never raises.
    """
    try:
        from app.core.database import DatabaseManager
        db = DatabaseManager.get_db()
        pending_doc = await db[settings.PENDING_COLLECTION].find_one({"key": msg_id})
    except Exception as e:
        logger.warning(
            "_recover_pending_from_db: DB lookup failed",
            extra={"ctx_msg_id": msg_id, "ctx_error": str(e)},
        )
        return None

    if not pending_doc:
        # Not in DB either — genuinely already actioned or expired
        return None

    chat_id = pending_doc.get("chat_id", 0)
    message_ids = pending_doc.get("message_ids", [])
    submitter_user_id = pending_doc.get("submitter_user_id", 0)

    if not chat_id or not message_ids:
        logger.warning(
            "_recover_pending_from_db: DB record incomplete",
            extra={"ctx_msg_id": msg_id, "ctx_doc": str(pending_doc)},
        )
        return None

    logger.info(
        "_recover_pending_from_db: found in DB, re-fetching from Telegram",
        extra={
            "ctx_msg_id": msg_id,
            "ctx_chat_id": chat_id,
            "ctx_submitter": submitter_user_id,
            "ctx_message_count": len(message_ids),
        },
    )

    try:
        messages = await client.get_messages(
            chat_id=chat_id,
            message_ids=message_ids,
        )
        # get_messages can return a single Message or a list
        if not isinstance(messages, list):
            messages = [messages]

        # Filter out empty/deleted messages
        messages = [m for m in messages if m and m.id]

        if not messages:
            logger.warning(
                "_recover_pending_from_db: Telegram messages no longer exist",
                extra={"ctx_msg_id": msg_id, "ctx_chat_id": chat_id},
            )
            return None

        logger.info(
            "_recover_pending_from_db: messages re-fetched successfully",
            extra={
                "ctx_msg_id": msg_id,
                "ctx_recovered_count": len(messages),
            },
        )
        return (submitter_user_id, messages)

    except Exception as e:
        logger.warning(
            "_recover_pending_from_db: Telegram re-fetch failed",
            extra={
                "ctx_msg_id": msg_id,
                "ctx_chat_id": chat_id,
                "ctx_error": str(e),
            },
        )
        return None


# ── Handler ───────────────────────────────────────────────────────────────────

@Client.on_callback_query(filters.regex(r"^mod_(approve|queue|reject|dest):"))
async def handle_moderation_callback(
    client: Client, callback: CallbackQuery
) -> None:
    """
    Two-step moderation state machine.

    RC-7 fix: structured entry logging with full callback context.
    RC-3 fix: top-level try-except — moderation failures are logged
              and the admin receives an actionable error message instead
              of the callback timing out silently.
    WARNING fix: when pop_pending() returns None, attempt DB + Telegram recovery
                 before surfacing "already actioned" error.
    """
    logger.info(
        "HANDLER: handle_moderation_callback entered",
        extra={
            "ctx_from_user": (
                callback.from_user.id if callback.from_user else None
            ),
            "ctx_data": callback.data,
            "ctx_chat_id": (
                callback.message.chat.id
                if callback.message and callback.message.chat
                else None
            ),
            "ctx_message_id": (
                callback.message.id if callback.message else None
            ),
        },
    )

    try:
        # ── Gate 1: correct chat ─────────────────────────────────────────────
        if callback.message.chat.id != settings.VERIFICATION_GROUP_ID:
            await callback.answer(
                "This action is not available here.", show_alert=True
            )
            logger.warning(
                "Moderation callback outside verification group",
                extra={
                    "ctx_chat_id": callback.message.chat.id,
                    "ctx_user_id": callback.from_user.id,
                },
            )
            return

        # ── Gate 2: authorisation ────────────────────────────────────────────
        moderator_id = callback.from_user.id
        if not is_moderator(moderator_id):
            await callback.answer(
                "⛔ You are not authorised to action submissions.",
                show_alert=True,
            )
            logger.warning(
                "Unauthorised moderation attempt",
                extra={
                    "ctx_user_id": moderator_id,
                    "ctx_data": callback.data,
                },
            )
            return

        # ── Parse ────────────────────────────────────────────────────────────
        parsed = parse_callback_data(callback.data)
        if parsed is None:
            await callback.answer("Malformed callback data.", show_alert=True)
            logger.error(
                "Failed to parse moderation callback",
                extra={"ctx_data": callback.data},
            )
            return

        moderator_name = callback.from_user.first_name or str(moderator_id)
        chat_id = callback.message.chat.id
        card_message_id = callback.message.id

        logger.info(
            "Moderation callback parsed",
            extra={
                "ctx_step": parsed["step"],
                "ctx_action": parsed["action"],
                "ctx_submitter_id": parsed.get("submitter_id"),
                "ctx_msg_id": parsed.get("msg_id"),
                "ctx_dest": parsed.get("dest"),
                "ctx_moderator": moderator_id,
            },
        )

        # ── Step 1 ───────────────────────────────────────────────────────────
        if parsed["step"] == 1:
            action = parsed["action"]
            submitter_id = parsed["submitter_id"]
            msg_id = parsed["msg_id"]

            if action == "reject":
                entry = submission_service.pop_pending(msg_id)
                if entry is None:
                    # WARNING fix: attempt DB + Telegram recovery before giving up
                    logger.warning(
                        "Reject: pop_pending returned None — attempting DB recovery",
                        extra={"ctx_msg_id": msg_id, "ctx_moderator": moderator_id},
                    )
                    recovered = await _recover_pending_from_db(client, msg_id)
                    if recovered is None:
                        await callback.answer(
                            "Submission not found — already actioned or expired.",
                            show_alert=True,
                        )
                        logger.warning(
                            "Reject: submission not recoverable from DB or Telegram",
                            extra={"ctx_msg_id": msg_id, "ctx_moderator": moderator_id},
                        )
                        return
                    # Recovery succeeded — use recovered submitter_id
                    submitter_id = recovered[0]
                    logger.info(
                        "Reject: recovered submitter from DB",
                        extra={"ctx_msg_id": msg_id, "ctx_submitter": submitter_id},
                    )

                await callback.answer("❌ Rejected.", show_alert=False)
                await execute_reject(
                    client=client,
                    submitter_user_id=submitter_id,
                    mod_card_chat_id=chat_id,
                    mod_card_message_id=card_message_id,
                    moderator_name=moderator_name,
                    moderator_id=moderator_id,
                )
                logger.info(
                    "Moderation: reject executed",
                    extra={
                        "ctx_submitter": submitter_id,
                        "ctx_moderator": moderator_id,
                    },
                )
                return

            # Approve / Queue — show destination picker
            prompt = (
                "Select destination:"
                if action == "approve"
                else "Queue for which destination?"
            )
            keyboard = _destination_keyboard(action, submitter_id, msg_id)

            try:
                await callback.message.edit_text(
                    f"📬 <b>{prompt}</b>\n"
                    f"👤 Submitter: <code>{submitter_id}</code>",
                    parse_mode=ParseMode.HTML,
                    reply_markup=keyboard,
                )
                await callback.answer()
            except Exception as e:
                logger.exception(
                    "Could not edit card to destination picker",
                    extra={"ctx_error": str(e)},
                )
                await callback.answer(
                    "Error showing destination picker.", show_alert=True
                )

            return

        # ── Step 2 ───────────────────────────────────────────────────────────
        if parsed["step"] == 2:
            action = parsed["action"]
            dest = parsed["dest"]
            submitter_id = parsed["submitter_id"]
            msg_id = parsed["msg_id"]

            entry = submission_service.pop_pending(msg_id)
            if entry is None:
                # WARNING fix: attempt DB + Telegram recovery before giving up.
                # This is the critical path — after a bot restart the in-memory
                # pending registry is empty but the submission may still be valid.
                logger.warning(
                    "Step-2 moderation: pop_pending returned None — attempting DB recovery",
                    extra={
                        "ctx_msg_id": msg_id,
                        "ctx_action": action,
                        "ctx_dest": dest,
                        "ctx_moderator": moderator_id,
                    },
                )
                recovered = await _recover_pending_from_db(client, msg_id)
                if recovered is None:
                    await callback.answer(
                        "Submission not found — already actioned or expired.",
                        show_alert=True,
                    )
                    logger.warning(
                        "Step-2 moderation: submission not recoverable from DB or Telegram",
                        extra={
                            "ctx_msg_id": msg_id,
                            "ctx_action": action,
                            "ctx_dest": dest,
                            "ctx_moderator": moderator_id,
                        },
                    )
                    return
                # Recovery succeeded — use the re-fetched data
                submitter_id, messages = recovered
                logger.info(
                    "Step-2 moderation: submission recovered from DB and Telegram",
                    extra={
                        "ctx_msg_id": msg_id,
                        "ctx_submitter": submitter_id,
                        "ctx_recovered_count": len(messages),
                    },
                )
            else:
                _, messages = entry

            await callback.answer(
                "✅ Processing..." if action == "approve" else "⏳ Queuing...",
                show_alert=False,
            )

            logger.info(
                "Moderation: executing action",
                extra={
                    "ctx_action": action,
                    "ctx_dest": dest,
                    "ctx_submitter": submitter_id,
                    "ctx_moderator": moderator_id,
                    "ctx_msg_count": len(messages),
                },
            )

            if action == "approve":
                await execute_approve(
                    client=client,
                    messages=messages,
                    submitter_user_id=submitter_id,
                    dest=dest,
                    mod_card_chat_id=chat_id,
                    mod_card_message_id=card_message_id,
                    moderator_name=moderator_name,
                    moderator_id=moderator_id,
                )
            elif action == "queue":
                await execute_queue(
                    client=client,
                    messages=messages,
                    submitter_user_id=submitter_id,
                    dest=dest,
                    mod_card_chat_id=chat_id,
                    mod_card_message_id=card_message_id,
                    moderator_name=moderator_name,
                    moderator_id=moderator_id,
                )

            logger.info(
                "Moderation: action complete",
                extra={
                    "ctx_action": action,
                    "ctx_dest": dest,
                    "ctx_submitter": submitter_id,
                    "ctx_moderator": moderator_id,
                },
            )

    except Exception as e:
        # RC-3 fix: catch everything — moderation errors must never time out silently
        logger.exception(
            "HANDLER: handle_moderation_callback unhandled exception",
            extra={
                "ctx_data": callback.data,
                "ctx_from_user": (
                    callback.from_user.id if callback.from_user else None
                ),
                "ctx_error": str(e),
            },
            exc_info=True,
        )
        try:
            await callback.answer(
                "⚠️ An error occurred processing this action. "
                "Please try again or check logs.",
                show_alert=True,
            )
        except Exception as e:
            logger.exception(
                "moderation_callback_answer_failed",
                extra={"ctx_error": str(e)},
            )
            pass
