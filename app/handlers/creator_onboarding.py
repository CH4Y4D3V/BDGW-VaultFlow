from __future__ import annotations

"""
Creator onboarding gate.

RC-5 FIX: check_and_gate_creator() previously let DB exceptions propagate
          unchecked. ConsentService.is_verified_creator() makes multiple async
          DB calls — if MongoDB is slow or temporarily unavailable, the
          exception propagated through handle_media_submission with no fallback
          reply. User received silence.
          Now wraps all DB calls with explicit exception handling and always
          sends a user-visible message on failure.

RC-2 FIX: _send_onboarding_prompt now catches ALL exceptions (not just
          FloodWait and RPCError) in its retry loop.

RC-7 FIX: Entry-point logging on all handlers and the gate function.

RC-3 FIX: All handlers have top-level try-except with fallback answers.
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
from app.services.consent_service import (
    ConsentService,
    ATTESTATION_TEXT,
    ATTESTATION_VERSION,
)
from app.services.audit_service import get_audit, AuditAction
from app.utils.logger import get_logger

logger = get_logger(__name__)

_consent_service = ConsentService()
_FLOOD_BUFFER = settings.FLOODWAIT_EXTRA_BUFFER


# ── Public gate function ──────────────────────────────────────────────────────

async def check_and_gate_creator(client: Client, message: Message) -> bool:
    """
    Call this at the top of any submission handler.

    Returns True  → user is a verified creator, allow submission.
    Returns False → user is not verified OR an error occurred.
                    In both cases a user-visible message has been sent.

    RC-5 fix: all DB calls are wrapped in try-except. On any failure,
    the user receives "try again later" instead of silence.
    """
    if not message.from_user:
        return False

    user_id = message.from_user.id

    logger.info(
        "check_and_gate_creator: checking",
        extra={"ctx_user_id": user_id},
    )

    try:
        is_verified = await _consent_service.is_verified_creator(user_id)
    except Exception as e:
        # RC-5 fix: DB error path must still give user feedback
        logger.error(
            "check_and_gate_creator: is_verified_creator raised",
            extra={"ctx_user_id": user_id, "ctx_error": str(e)},
            exc_info=True,
        )
        await _send_error_reply(
            client,
            message,
            "⚠️ We couldn't verify your creator status right now. "
            "Please try again in a moment.",
        )
        return False

    if is_verified:
        logger.info(
            "check_and_gate_creator: verified — allowing submission",
            extra={"ctx_user_id": user_id},
        )
        return True

    logger.info(
        "check_and_gate_creator: not verified — showing onboarding prompt",
        extra={"ctx_user_id": user_id},
    )
    await _send_onboarding_prompt(client, message)
    return False


async def _send_error_reply(
    client: Client,
    message: Message,
    text: str,
) -> None:
    """
    Best-effort fallback reply. NEVER raises.
    """
    try:
        await message.reply_text(text, parse_mode=ParseMode.HTML)
    except Exception as e:
        logger.error(
            "_send_error_reply: could not send fallback",
            extra={"ctx_error": str(e)},
        )


# ── Onboarding prompt ─────────────────────────────────────────────────────────

async def _send_onboarding_prompt(client: Client, message: Message) -> None:
    user_id = message.from_user.id

    # Check creator profile status — suspended/banned get a different message
    try:
        profile = await _consent_service.get_creator_profile(user_id)
    except Exception as e:
        logger.error(
            "_send_onboarding_prompt: get_creator_profile raised",
            extra={"ctx_user_id": user_id, "ctx_error": str(e)},
            exc_info=True,
        )
        await _send_error_reply(
            client,
            message,
            "⚠️ Could not load your creator profile. Please try again.",
        )
        return

    if profile and profile.get("status") == "suspended":
        await _safe_reply(
            message,
            "🚫 Your creator account is currently <b>suspended</b>.\n\n"
            "Contact an admin for assistance.",
        )
        return

    if profile and profile.get("status") == "banned":
        await _safe_reply(
            message,
            "🚫 Your account has been <b>permanently banned</b> from content submission.",
        )
        return

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

    sent = await _safe_reply(message, consent_text, reply_markup=keyboard)
    if sent:
        logger.info(
            "Consent prompt shown",
            extra={"ctx_user_id": user_id},
        )
    else:
        logger.error(
            "_send_onboarding_prompt: all reply attempts failed",
            extra={"ctx_user_id": user_id},
        )


async def _safe_reply(
    message: Message,
    text: str,
    reply_markup=None,
) -> bool:
    """
    RC-2 fix: catches ALL exceptions in retry loop.
    Returns True on success, False if all attempts failed.
    """
    for attempt in range(3):
        try:
            await message.reply_text(
                text,
                parse_mode=ParseMode.HTML,
                reply_markup=reply_markup,
            )
            return True
        except FloodWait as e:
            await asyncio.sleep(int(e.value) + _FLOOD_BUFFER)
        except RPCError as e:
            logger.warning(
                "_safe_reply: RPCError",
                extra={"ctx_error": str(e), "ctx_attempt": attempt + 1},
            )
            if attempt == 2:
                return False
            await asyncio.sleep(2 ** attempt)
        except Exception as e:
            # RC-2 fix: catch everything else
            logger.error(
                "_safe_reply: unexpected exception",
                extra={"ctx_error": str(e), "ctx_attempt": attempt + 1},
                exc_info=True,
            )
            if attempt == 2:
                return False
            await asyncio.sleep(2 ** attempt)
    return False


# ── Callback: I Agree ────────────────────────────────────────────────────────

@Client.on_callback_query(filters.regex(r"^consent:agree:(\d+)$"))
async def handle_consent_agree(client: Client, callback: CallbackQuery) -> None:
    logger.info(
        "HANDLER: handle_consent_agree entered",
        extra={
            "ctx_from_user": (
                callback.from_user.id if callback.from_user else None
            ),
            "ctx_data": callback.data,
        },
    )

    try:
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

        # Double-click protection
        try:
            already = await _consent_service.is_verified_creator(user_id)
        except Exception as e:
            logger.error(
                "handle_consent_agree: is_verified_creator raised",
                extra={"ctx_user_id": user_id, "ctx_error": str(e)},
                exc_info=True,
            )
            await callback.answer(
                "⚠️ Could not verify status. Please try again.",
                show_alert=True,
            )
            return

        if already:
            await callback.answer(
                "✅ You are already a verified creator.",
                show_alert=False,
            )
            try:
                await callback.message.edit_text(
                    "✅ You are already verified. Go ahead and submit your content.",
                    parse_mode=ParseMode.HTML,
                )
            except Exception:
                pass
            return

        # Create consent record
        try:
            record_id = await _consent_service.create_consent_record(
                user_id=user_id,
                telegram_username=username,
            )
            await _consent_service.register_creator(
                user_id=user_id,
                consent_record_id=record_id,
                telegram_username=username,
            )
        except Exception as e:
            logger.error(
                "handle_consent_agree: consent record creation failed",
                extra={"ctx_user_id": user_id, "ctx_error": str(e)},
                exc_info=True,
            )
            await callback.answer(
                "⚠️ Could not record your consent. Please try again.",
                show_alert=True,
            )
            return

        # Audit log — non-fatal if fails
        try:
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
        except Exception as e:
            logger.warning(
                "handle_consent_agree: audit log failed (non-fatal)",
                extra={"ctx_error": str(e)},
            )

        await callback.answer(
            "✅ Consent recorded. You are now a verified creator.",
            show_alert=False,
        )
        try:
            await callback.message.edit_text(
                "✅ <b>Consent recorded.</b>\n\n"
                "Your identity has been logged internally. "
                "You are now a verified creator.\n\n"
                "You can now send your content — please resubmit it now.",
                parse_mode=ParseMode.HTML,
            )
        except Exception as e:
            logger.warning(
                "handle_consent_agree: could not edit confirmation message",
                extra={"ctx_error": str(e)},
            )

        logger.info(
            "Creator onboarding complete",
            extra={
                "ctx_user_id": user_id,
                "ctx_record_id": record_id,
                "ctx_version": ATTESTATION_VERSION,
            },
        )

    except Exception as e:
        logger.error(
            "HANDLER: handle_consent_agree unhandled exception",
            extra={"ctx_error": str(e)},
            exc_info=True,
        )
        try:
            await callback.answer(
                "⚠️ An error occurred. Please try again.",
                show_alert=True,
            )
        except Exception:
            pass


# ── Callback: I Do Not Agree ──────────────────────────────────────────────────

@Client.on_callback_query(filters.regex(r"^consent:decline$"))
async def handle_consent_decline(client: Client, callback: CallbackQuery) -> None:
    logger.info(
        "HANDLER: handle_consent_decline entered",
        extra={
            "ctx_from_user": (
                callback.from_user.id if callback.from_user else None
            ),
        },
    )

    try:
        await callback.answer()
        await callback.message.edit_text(
            "You have declined the consent attestation.\n\n"
            "Content submission requires this agreement. "
            "You can try again at any time by sending content to this bot.",
            parse_mode=ParseMode.HTML,
        )
    except Exception as e:
        logger.error(
            "HANDLER: handle_consent_decline unhandled exception",
            extra={"ctx_error": str(e)},
            exc_info=True,
        )
        try:
            await callback.answer()
        except Exception:
            pass


# ── /become_creator command ───────────────────────────────────────────────────

@Client.on_message(filters.command("become_creator") & filters.private)
async def handle_become_creator(client: Client, message: Message) -> None:
    logger.info(
        "HANDLER: handle_become_creator entered",
        extra={
            "ctx_from_user": (
                message.from_user.id if message.from_user else None
            ),
        },
    )

    try:
        if not message.from_user:
            return

        user_id = message.from_user.id

        try:
            already = await _consent_service.is_verified_creator(user_id)
        except Exception as e:
            logger.error(
                "handle_become_creator: is_verified_creator raised",
                extra={"ctx_user_id": user_id, "ctx_error": str(e)},
                exc_info=True,
            )
            await _safe_reply(
                message,
                "⚠️ Could not check your status right now. Please try again.",
            )
            return

        if already:
            await _safe_reply(
                message,
                "✅ You are already a verified creator. "
                "Send content directly to submit.",
            )
            return

        await _send_onboarding_prompt(client, message)

    except Exception as e:
        logger.error(
            "HANDLER: handle_become_creator unhandled exception",
            extra={"ctx_error": str(e)},
            exc_info=True,
        )
        await _safe_reply(
            message,
            "⚠️ An error occurred. Please try again.",
        )
