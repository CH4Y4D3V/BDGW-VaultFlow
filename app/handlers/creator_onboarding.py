from __future__ import annotations

"""
Creator onboarding gate.

Every user must complete consent attestation before submitting content.
This module provides:
  - check_and_gate_creator()  : call from submission_handler before accepting media
  - handle_consent_callback() : inline button handler for "I Agree"
  - handle_onboarding_start() : /become_creator command

Flow:
  1. User tries to submit content
  2. submission_handler calls check_and_gate_creator()
  3. If not verified → show attestation + inline button → return False (block submission)
  4. User reads attestation and clicks "✅ I Agree & Confirm"
  5. ConsentRecord created, CreatorProfile activated
  6. User is told to resubmit their content

Design notes:
  - Using inline button (not text reply) prevents accidental confirmations
  - Attestation text is hardcoded in consent_service so version is always linked to record
  - No FSM state needed — the callback itself is the confirmation trigger
  - If user closes the bot without clicking, they just retry submission to see the prompt again
"""

import asyncio

from pyrogram import Client, filters
from pyrogram.enums import ParseMode
from pyrogram.errors import FloodWait, RPCError
from pyrogram.types import (
    CallbackQuery,
    InlineKeyboardButton,
    InlineKeyboardMarkup,
    Message,
)

from app.config import settings
from app.services.consent_service import ConsentService, ATTESTATION_TEXT, ATTESTATION_VERSION
from app.services.audit_service import get_audit, AuditAction
from app.utils.logger import get_logger

logger = get_logger(__name__)

_consent_service = ConsentService()
_FLOOD_BUFFER = settings.FLOODWAIT_EXTRA_BUFFER


# ── Public gate function ──────────────────────────────────────────────────────

async def check_and_gate_creator(client: Client, message: Message) -> bool:
    """
    Call this at the top of any submission handler.

    Returns True  → user is a verified creator, allow submission to proceed.
    Returns False → user is not verified, onboarding message sent, block submission.
    """
    if not message.from_user:
        return False

    user_id = message.from_user.id
    is_verified = await _consent_service.is_verified_creator(user_id)

    if is_verified:
        return True

    await _send_onboarding_prompt(client, message)
    return False


# ── Onboarding prompt ─────────────────────────────────────────────────────────

async def _send_onboarding_prompt(client: Client, message: Message) -> None:
    user_id = message.from_user.id
    profile = await _consent_service.get_creator_profile(user_id)

    if profile and profile.get("status") == "suspended":
        await message.reply_text(
            "🚫 Your creator account is currently <b>suspended</b>.\n\n"
            "Contact an admin for assistance.",
            parse_mode=ParseMode.HTML,
        )
        return

    if profile and profile.get("status") == "banned":
        await message.reply_text(
            "🚫 Your account has been <b>permanently banned</b> from content submission.",
            parse_mode=ParseMode.HTML,
        )
        return

    # Show attestation and consent button
    consent_text = (
        "📋 <b>Creator Consent Attestation Required</b>\n\n"
        "Before you can submit content, you must read and agree to the following "
        "declaration. This is a <b>legal attestation</b> — please read carefully.\n\n"
        f"<blockquote>{ATTESTATION_TEXT}</blockquote>\n\n"
        f"<i>Attestation version: {ATTESTATION_VERSION}</i>\n\n"
        "By clicking <b>✅ I Agree & Confirm</b>, you are making a legally binding "
        "declaration. Your Telegram identity will be permanently logged.\n\n"
        "⚠️ <b>Submitting content that violates the above is grounds for permanent "
        "ban and potential legal action.</b>"
    )

    keyboard = InlineKeyboardMarkup([
        [InlineKeyboardButton(
            "✅ I Agree & Confirm",
            callback_data=f"consent:agree:{user_id}",
        )],
        [InlineKeyboardButton(
            "❌ I Do Not Agree",
            callback_data="consent:decline",
        )],
    ])

    for attempt in range(3):
        try:
            await message.reply_text(
                consent_text,
                parse_mode=ParseMode.HTML,
                reply_markup=keyboard,
            )
            logger.info(
                "Consent prompt shown",
                extra={"ctx_user_id": user_id},
            )
            return
        except FloodWait as e:
            await asyncio.sleep(int(e.value) + _FLOOD_BUFFER)
        except RPCError as e:
            logger.warning(
                "Could not send consent prompt",
                extra={"ctx_user_id": user_id, "ctx_error": str(e), "ctx_attempt": attempt + 1},
            )
            if attempt == 2:
                return
            await asyncio.sleep(2 ** attempt)


# ── Callback: I Agree ────────────────────────────────────────────────────────

@Client.on_callback_query(filters.regex(r"^consent:agree:(\d+)$"))
async def handle_consent_agree(client: Client, callback: CallbackQuery) -> None:
    """
    Fires when user clicks '✅ I Agree & Confirm'.

    Security check: the user_id in callback_data must match the actual clicker.
    This prevents someone else clicking on behalf of another user.
    """
    clicker_id = callback.from_user.id
    declared_id = int(callback.data.split(":")[2])

    if clicker_id != declared_id:
        await callback.answer(
            "This confirmation is not for your account.",
            show_alert=True,
        )
        return

    user_id = clicker_id
    username = callback.from_user.username

    # Check not already verified (double-click protection)
    already = await _consent_service.is_verified_creator(user_id)
    if already:
        await callback.answer("✅ You are already a verified creator.", show_alert=False)
        await callback.message.edit_text(
            "✅ You are already verified. Go ahead and submit your content.",
            parse_mode=ParseMode.HTML,
        )
        return

    # Create immutable consent record
    record_id = await _consent_service.create_consent_record(
        user_id=user_id,
        telegram_username=username,
    )

    # Register creator profile
    await _consent_service.register_creator(
        user_id=user_id,
        consent_record_id=record_id,
        telegram_username=username,
    )

    # Audit log
    await get_audit().log(
        action=AuditAction.CREATOR_ONBOARD,
        performed_by=user_id,
        target_user_id=user_id,
        details={
            "consent_record_id": record_id,
            "attestation_version": ATTESTATION_VERSION,
            "telegram_username": username,
        },
    )

    await callback.answer("✅ Consent recorded. You are now a verified creator.", show_alert=False)
    await callback.message.edit_text(
        "✅ <b>Consent recorded.</b>\n\n"
        "Your identity has been logged internally. You are now a verified creator.\n\n"
        "You can now send your content — please resubmit it now.",
        parse_mode=ParseMode.HTML,
    )

    logger.info(
        "Creator onboarding complete",
        extra={
            "ctx_user_id": user_id,
            "ctx_record_id": record_id,
            "ctx_version": ATTESTATION_VERSION,
        },
    )


# ── Callback: I Do Not Agree ──────────────────────────────────────────────────

@Client.on_callback_query(filters.regex(r"^consent:decline$"))
async def handle_consent_decline(client: Client, callback: CallbackQuery) -> None:
    await callback.answer()
    await callback.message.edit_text(
        "You have declined the consent attestation.\n\n"
        "Content submission requires this agreement. "
        "You can try again at any time by sending content to this bot.",
        parse_mode=ParseMode.HTML,
    )


# ── /become_creator command ───────────────────────────────────────────────────

@Client.on_message(filters.command("become_creator") & filters.private)
async def handle_become_creator(client: Client, message: Message) -> None:
    """Entry point for users who want to register proactively."""
    if not message.from_user:
        return

    user_id = message.from_user.id
    already = await _consent_service.is_verified_creator(user_id)
    if already:
        await message.reply_text(
            "✅ You are already a verified creator. Send content directly to submit.",
            parse_mode=ParseMode.HTML,
        )
        return

    await _send_onboarding_prompt(client, message)