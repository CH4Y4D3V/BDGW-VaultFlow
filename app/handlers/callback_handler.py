from __future__ import annotations

from pyrogram import Client, filters
from pyrogram.enums import ParseMode
from pyrogram.types import CallbackQuery, InlineKeyboardButton, InlineKeyboardMarkup

from app.config import settings
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


# ── Access control ────────────────────────────────────────────────────────────

def _is_moderator(user_id: int) -> bool:
    return (
        user_id == settings.OWNER_ID
        or user_id in settings.ADMIN_IDS
        or user_id in settings.SUDO_IDS
    )


# ── Destination selection keyboard ────────────────────────────────────────────

def _destination_keyboard(action: str, submitter_id: int, msg_id: int) -> InlineKeyboardMarkup:
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


# ── Handlers ──────────────────────────────────────────────────────────────────

@Client.on_callback_query(filters.regex(r"^mod_(approve|queue|reject|dest):"))
async def handle_moderation_callback(client: Client, callback: CallbackQuery) -> None:
    """
    Two-step moderation state machine.

    Step 1 — Approve / Queue / Reject:
      - Reject: executes immediately
      - Approve / Queue: show destination picker

    Step 2 — Pick NSFW / Premium:
      - Execute the full approve or queue flow with moderator_id for audit.
    """
    # ── Gate 1: correct chat ─────────────────────────────────────────────────
    if callback.message.chat.id != settings.VERIFICATION_GROUP_ID:
        await callback.answer("This action is not available here.", show_alert=True)
        logger.warning(
            "Moderation callback outside verification group",
            extra={"ctx_chat_id": callback.message.chat.id, "ctx_user_id": callback.from_user.id},
        )
        return

    # ── Gate 2: authorisation ────────────────────────────────────────────────
    moderator_id = callback.from_user.id
    if not _is_moderator(moderator_id):
        await callback.answer("⛔ You are not authorised to action submissions.", show_alert=True)
        logger.warning(
            "Unauthorised moderation attempt",
            extra={"ctx_user_id": moderator_id, "ctx_data": callback.data},
        )
        return

    # ── Parse ────────────────────────────────────────────────────────────────
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

    # ── Step 1 ───────────────────────────────────────────────────────────────
    if parsed["step"] == 1:
        action = parsed["action"]
        submitter_id = parsed["submitter_id"]
        msg_id = parsed["msg_id"]

        if action == "reject":
            entry = submission_service.pop_pending(msg_id)
            if entry is None:
                await callback.answer(
                    "Submission not found — already actioned.",
                    show_alert=True,
                )
                return

            await callback.answer("❌ Rejected.", show_alert=False)
            await execute_reject(
                client=client,
                submitter_user_id=submitter_id,
                mod_card_chat_id=chat_id,
                mod_card_message_id=card_message_id,
                moderator_name=moderator_name,
                moderator_id=moderator_id,  # P1-B: wired through
            )
            return

        prompt = (
            "Select destination:" if action == "approve"
            else "Queue for which destination?"
        )
        keyboard = _destination_keyboard(action, submitter_id, msg_id)

        try:
            await callback.message.edit_text(
                f"📬 <b>{prompt}</b>\n👤 Submitter: <code>{submitter_id}</code>",
                parse_mode=ParseMode.HTML,
                reply_markup=keyboard,
            )
            await callback.answer()
        except Exception as e:
            logger.warning(
                "Could not edit card to destination picker",
                extra={"ctx_error": str(e)},
            )
            await callback.answer("Error showing destination picker.", show_alert=True)

        return

    # ── Step 2 ───────────────────────────────────────────────────────────────
    if parsed["step"] == 2:
        action = parsed["action"]
        dest = parsed["dest"]
        submitter_id = parsed["submitter_id"]
        msg_id = parsed["msg_id"]

        entry = submission_service.pop_pending(msg_id)
        if entry is None:
            await callback.answer(
                "Submission not found — already actioned.",
                show_alert=True,
            )
            return

        _, messages = entry

        await callback.answer(
            "✅ Processing..." if action == "approve" else "⏳ Queuing...",
            show_alert=False,
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
                moderator_id=moderator_id,  # P1-B: wired through
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
                moderator_id=moderator_id,  # P1-B: wired through
            )