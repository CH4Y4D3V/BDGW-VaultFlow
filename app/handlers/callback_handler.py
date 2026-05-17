from __future__ import annotations

import asyncio
from typing import Optional

from pyrogram import Client, filters
from pyrogram.errors import FloodWait, RPCError
from pyrogram.types import CallbackQuery

from app.config import settings
from app.moderation.verification_hub import parse_callback_data
from app.services import submission_service
from app.utils.logger import get_logger

logger = get_logger(__name__)

_FLOOD_BUFFER = settings.FLOODWAIT_EXTRA_BUFFER
_MAX_RETRIES = 3


# ── Access control ────────────────────────────────────────────────────────────

def _is_moderator(user_id: int) -> bool:
    return (
        user_id == settings.OWNER_ID
        or user_id in settings.ADMIN_IDS
        or user_id in settings.SUDO_IDS
    )


# ── Helpers ───────────────────────────────────────────────────────────────────

async def _safe_dm(client: Client, user_id: int, text: str) -> None:
    """
    Send a private message to a user with FloodWait-safe retries.
    Failures are logged but never propagated — a DM notification
    is best-effort and must not block the moderation flow.
    """
    for attempt in range(_MAX_RETRIES):
        try:
            await client.send_message(
                chat_id=user_id,
                text=text,
                parse_mode="html",
            )
            return
        except FloodWait as e:
            wait = int(e.value) + _FLOOD_BUFFER
            logger.warning(
                "FloodWait on DM, sleeping",
                extra={"ctx_user_id": user_id, "ctx_wait": wait, "ctx_attempt": attempt + 1},
            )
            await asyncio.sleep(wait)
        except RPCError as e:
            logger.warning(
                "Failed to DM submitter",
                extra={
                    "ctx_user_id": user_id,
                    "ctx_error": str(e),
                    "ctx_attempt": attempt + 1,
                },
            )
            if attempt == _MAX_RETRIES - 1:
                return
            await asyncio.sleep(2 ** attempt)


async def _safe_edit(callback: CallbackQuery, text: str) -> None:
    """
    Edit the moderation message text in place.
    Silently swallows MESSAGE_NOT_MODIFIED and other RPC errors
    to avoid crashing the callback flow on double-clicks.
    """
    for attempt in range(_MAX_RETRIES):
        try:
            await callback.message.edit_text(text, parse_mode="html")
            return
        except FloodWait as e:
            wait = int(e.value) + _FLOOD_BUFFER
            await asyncio.sleep(wait)
        except RPCError as e:
            # MESSAGE_NOT_MODIFIED is expected on duplicate actions — log & exit
            logger.warning(
                "Could not edit moderation message",
                extra={
                    "ctx_chat_id": callback.message.chat.id,
                    "ctx_msg_id": callback.message.id,
                    "ctx_error": str(e),
                    "ctx_attempt": attempt + 1,
                },
            )
            return


# ── Handler ───────────────────────────────────────────────────────────────────

@Client.on_callback_query(filters.regex(r"^mod_(approve|reject):"))
async def handle_moderation_callback(client: Client, callback: CallbackQuery) -> None:
    """
    Handles Approve / Reject inline button presses from the verification group.

    Authorization gates (in order):
    1. Chat must be the configured VERIFICATION_GROUP_ID.
    2. Acting user must be OWNER, ADMIN, or SUDO.

    On approve: ingests via MediaIngestionPipeline, edits card, DMs submitter.
    On reject:  discards pending entry, edits card, DMs submitter.
    """
    # ── Gate 1: correct chat ─────────────────────────────────────────────────
    if callback.message.chat.id != settings.VERIFICATION_GROUP_ID:
        await callback.answer("This action is not available here.", show_alert=True)
        logger.warning(
            "Moderation callback fired outside verification group",
            extra={
                "ctx_chat_id": callback.message.chat.id,
                "ctx_user_id": callback.from_user.id,
            },
        )
        return

    # ── Gate 2: moderator authorisation ─────────────────────────────────────
    moderator_id = callback.from_user.id
    if not _is_moderator(moderator_id):
        await callback.answer(
            "⛔ You are not authorised to action submissions.",
            show_alert=True,
        )
        logger.warning(
            "Unauthorised moderation attempt",
            extra={"ctx_user_id": moderator_id, "ctx_data": callback.data},
        )
        return

    # ── Parse callback payload ───────────────────────────────────────────────
    parsed = parse_callback_data(callback.data)
    if parsed is None:
        await callback.answer("Malformed callback data.", show_alert=True)
        logger.error(
            "Could not parse moderation callback data",
            extra={"ctx_data": callback.data, "ctx_user_id": moderator_id},
        )
        return

    action, submitter_id, msg_id = parsed
    moderator_name = callback.from_user.first_name or str(moderator_id)

    # ── Approve ──────────────────────────────────────────────────────────────
    if action == "approve":
        result_uid = await submission_service.ingest_approved(msg_id)

        if result_uid is None:
            await callback.answer(
                "Submission not found — it may have already been actioned.",
                show_alert=True,
            )
            return

        await _safe_edit(
            callback,
            f"✅ <b>Approved</b> by {moderator_name} "
            f"(<code>{moderator_id}</code>)\n"
            f"👤 Submitter: <code>{submitter_id}</code>",
        )
        await _safe_dm(
            client,
            submitter_id,
            "✅ <b>Your submission has been approved</b> and is now queued for the vault.",
        )
        await callback.answer("✅ Approved.", show_alert=False)

        logger.info(
            "Submission approved",
            extra={
                "ctx_submitter_id": submitter_id,
                "ctx_msg_id": msg_id,
                "ctx_moderator_id": moderator_id,
            },
        )

    # ── Reject ───────────────────────────────────────────────────────────────
    elif action == "reject":
        result_uid = await submission_service.reject_pending(msg_id)

        if result_uid is None:
            await callback.answer(
                "Submission not found — it may have already been actioned.",
                show_alert=True,
            )
            return

        await _safe_edit(
            callback,
            f"❌ <b>Rejected</b> by {moderator_name} "
            f"(<code>{moderator_id}</code>)\n"
            f"👤 Submitter: <code>{submitter_id}</code>",
        )
        await _safe_dm(
            client,
            submitter_id,
            "❌ <b>Your submission was not approved.</b> "
            "Please review our content guidelines and feel free to try again.",
        )
        await callback.answer("❌ Rejected.", show_alert=False)

        logger.info(
            "Submission rejected",
            extra={
                "ctx_submitter_id": submitter_id,
                "ctx_msg_id": msg_id,
                "ctx_moderator_id": moderator_id,
            },
        )
