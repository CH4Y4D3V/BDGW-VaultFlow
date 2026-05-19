from __future__ import annotations

"""
Payment handler — premium subscription purchase flow.

RC-2 FIX: _safe_reply and _safe_send now catch ALL exceptions, not just
          FloodWait and RPCError.
RC-3 FIX: Every handler has a top-level try-except with fallback user response.
RC-7 FIX: Entry-point logging on every handler.

IMPORTANT — handler ordering note:
  handle_payment_proof_capture matches (photo | document | text) & private.
  handle_media_submission in submission_handler.py also matches (photo | ...) & private.
  Both handlers fire (Pyrogram dispatches to ALL matching handlers in group 0).
  handle_payment_proof_capture returns immediately when no payment state exists,
  so it does NOT block the submission handler.
  If handle_payment_proof_capture raises, the exception is now caught here and
  does NOT propagate to kill the submission handler's turn.
"""

import asyncio
from datetime import datetime, timezone
from typing import Optional

from pyrogram import Client, filters
from pyrogram.enums import ParseMode
from pyrogram.errors import FloodWait, RPCError, UserIsBlocked, PeerIdInvalid
from pyrogram.types import (
    CallbackQuery,
    InlineKeyboardButton,
    InlineKeyboardMarkup,
    Message,
)

from app.config import settings
from app.core.database import DatabaseManager
from app.core.permissions import is_payment_admin
from app.models.subscription import Plan
from app.services.subscription_service import SubscriptionService
from app.services.invite_service import InviteService
from app.services.topic_service import get_topic_service
from app.services.audit_service import get_audit, AuditAction
from app.utils.logger import get_logger

logger = get_logger(__name__)

_sub_service = SubscriptionService()
_invite_service = InviteService()
_FLOOD_BUFFER = settings.FLOODWAIT_EXTRA_BUFFER
_MAX_RETRIES = 3

# ── Plan definitions ──────────────────────────────────────────────────────────

_PLANS = [
    {"label": "30 Days — Premium 🌟", "callback": "plan:premium:30", "days": 30},
    {"label": "90 Days — Premium 💎", "callback": "plan:premium:90", "days": 90},
    {"label": "Lifetime — Premium ♾️", "callback": "plan:premium:lifetime", "days": None},
]

_PLAN_MAP = {p["callback"]: p for p in _PLANS}


# ── DB helpers for payment state ──────────────────────────────────────────────

async def _set_payment_state(user_id: int, plan: str, duration: str) -> None:
    try:
        db = DatabaseManager.get_db()
        key = f"payment_state:{user_id}"
        await db["bot_config"].update_one(
            {"key": key},
            {
                "$set": {
                    "key": key,
                    "plan": plan,
                    "duration": duration,
                    "created_at": datetime.now(timezone.utc),
                }
            },
            upsert=True,
        )
    except Exception as e:
        logger.error(
            "_set_payment_state: failed",
            extra={"ctx_user_id": user_id, "ctx_error": str(e)},
            exc_info=True,
        )
        raise


async def _get_payment_state(user_id: int) -> Optional[dict]:
    try:
        db = DatabaseManager.get_db()
        return await db["bot_config"].find_one(
            {"key": f"payment_state:{user_id}"}
        )
    except Exception as e:
        logger.error(
            "_get_payment_state: DB error",
            extra={"ctx_user_id": user_id, "ctx_error": str(e)},
            exc_info=True,
        )
        return None


async def _clear_payment_state(user_id: int) -> None:
    try:
        db = DatabaseManager.get_db()
        await db["bot_config"].delete_one(
            {"key": f"payment_state:{user_id}"}
        )
    except Exception as e:
        logger.warning(
            "_clear_payment_state: failed (non-fatal)",
            extra={"ctx_user_id": user_id, "ctx_error": str(e)},
        )


# ── Safe messaging helpers ────────────────────────────────────────────────────

async def _safe_reply(message: Message, text: str, reply_markup=None) -> bool:
    """
    RC-2 fix: catches ALL exception types including non-RPCError.
    Returns True on success.
    """
    for attempt in range(_MAX_RETRIES):
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
            if attempt == _MAX_RETRIES - 1:
                logger.warning(
                    "_safe_reply: RPCError on final attempt",
                    extra={"ctx_error": str(e)},
                )
                return False
            await asyncio.sleep(2 ** attempt)
        except Exception as e:
            # RC-2 fix: catch all other exceptions
            logger.error(
                "_safe_reply: unexpected exception",
                extra={"ctx_error": str(e), "ctx_attempt": attempt + 1},
                exc_info=True,
            )
            if attempt == _MAX_RETRIES - 1:
                return False
            await asyncio.sleep(2 ** attempt)
    return False


async def _safe_send(
    client: Client, user_id: int, text: str, reply_markup=None
) -> bool:
    """
    RC-2 fix: catches ALL exception types.
    Returns True on success, False if user unreachable.
    """
    for attempt in range(_MAX_RETRIES):
        try:
            await client.send_message(
                chat_id=user_id,
                text=text,
                parse_mode=ParseMode.HTML,
                reply_markup=reply_markup,
            )
            return True
        except (UserIsBlocked, PeerIdInvalid):
            return False
        except FloodWait as e:
            await asyncio.sleep(int(e.value) + _FLOOD_BUFFER)
        except RPCError as e:
            if attempt == _MAX_RETRIES - 1:
                return False
            await asyncio.sleep(2 ** attempt)
        except Exception as e:
            # RC-2 fix: catch all other exceptions
            logger.error(
                "_safe_send: unexpected exception",
                extra={"ctx_user_id": user_id, "ctx_error": str(e)},
                exc_info=True,
            )
            if attempt == _MAX_RETRIES - 1:
                return False
            await asyncio.sleep(2 ** attempt)
    return False


# ── Callback: menu:premium ────────────────────────────────────────────────────

@Client.on_callback_query(filters.regex(r"^menu:premium$"))
async def handle_premium_menu(client: Client, callback: CallbackQuery) -> None:
    logger.info(
        "HANDLER: handle_premium_menu entered",
        extra={
            "ctx_from_user": (
                callback.from_user.id if callback.from_user else None
            ),
        },
    )

    try:
        keyboard = InlineKeyboardMarkup([
            [InlineKeyboardButton(p["label"], callback_data=p["callback"])]
            for p in _PLANS
        ])
        await callback.message.edit_text(
            "💎 <b>Choose a Premium Plan</b>\n\n"
            "Select the plan that suits you. After selecting, you'll receive "
            "payment instructions.",
            parse_mode=ParseMode.HTML,
            reply_markup=keyboard,
        )
        await callback.answer()

    except Exception as e:
        logger.error(
            "HANDLER: handle_premium_menu unhandled exception",
            extra={"ctx_error": str(e)},
            exc_info=True,
        )
        try:
            await callback.answer(
                "⚠️ Error loading premium menu. Please try again.",
                show_alert=True,
            )
        except Exception:
            pass


# ── Callback: plan:{plan}:{duration} ─────────────────────────────────────────

@Client.on_callback_query(
    filters.regex(r"^plan:premium:(30|90|lifetime)$")
)
async def handle_plan_selection(client: Client, callback: CallbackQuery) -> None:
    logger.info(
        "HANDLER: handle_plan_selection entered",
        extra={
            "ctx_from_user": (
                callback.from_user.id if callback.from_user else None
            ),
            "ctx_data": callback.data,
        },
    )

    try:
        plan_data = _PLAN_MAP.get(callback.data)
        if not plan_data:
            await callback.answer("Unknown plan.", show_alert=True)
            return

        label = plan_data["label"]
        parts = callback.data.split(":")
        plan_key = parts[1]
        duration_key = parts[2]

        keyboard = InlineKeyboardMarkup([[
            InlineKeyboardButton(
                "📸 Submit Payment Proof",
                callback_data=f"payment:submit:{plan_key}:{duration_key}",
            )
        ]])

        await callback.message.edit_text(
            f"💳 <b>Payment Instructions — {label}</b>\n\n"
            "1. Send payment to our bKash / Nagad number: "
            "<b>[CONFIGURED_NUMBER]</b>\n"
            "2. Take a screenshot of your payment confirmation.\n"
            "3. Tap <b>Submit Payment Proof</b> below, then send the screenshot.\n\n"
            "<i>Our team will verify and activate your subscription within 24 hours.</i>",
            parse_mode=ParseMode.HTML,
            reply_markup=keyboard,
        )
        await callback.answer()

    except Exception as e:
        logger.error(
            "HANDLER: handle_plan_selection unhandled exception",
            extra={"ctx_data": callback.data, "ctx_error": str(e)},
            exc_info=True,
        )
        try:
            await callback.answer(
                "⚠️ Error. Please try again.", show_alert=True
            )
        except Exception:
            pass


# ── Callback: payment:submit:{plan}:{duration} ────────────────────────────────

@Client.on_callback_query(
    filters.regex(r"^payment:submit:premium:(30|90|lifetime)$")
)
async def handle_payment_submit(client: Client, callback: CallbackQuery) -> None:
    logger.info(
        "HANDLER: handle_payment_submit entered",
        extra={
            "ctx_from_user": (
                callback.from_user.id if callback.from_user else None
            ),
            "ctx_data": callback.data,
        },
    )

    try:
        parts = callback.data.split(":")
        plan = parts[2]
        duration = parts[3]
        user_id = callback.from_user.id

        await _set_payment_state(user_id, plan, duration)

        await callback.message.edit_text(
            "📸 <b>Send Your Payment Screenshot</b>\n\n"
            "Please send a clear screenshot or photo of your payment confirmation now.\n\n"
            "<i>Your message will be forwarded to our verification team.</i>",
            parse_mode=ParseMode.HTML,
        )
        await callback.answer()

        logger.info(
            "handle_payment_submit: payment state set",
            extra={"ctx_user_id": user_id, "ctx_plan": plan, "ctx_duration": duration},
        )

    except Exception as e:
        logger.error(
            "HANDLER: handle_payment_submit unhandled exception",
            extra={"ctx_data": callback.data, "ctx_error": str(e)},
            exc_info=True,
        )
        try:
            await callback.answer(
                "⚠️ Error. Please try again.", show_alert=True
            )
        except Exception:
            pass


# ── Private message: capture payment proof ────────────────────────────────────

@Client.on_message(
    (filters.photo | filters.document | filters.text)
    & filters.private
)
async def handle_payment_proof_capture(client: Client, message: Message) -> None:
    """
    Captures payment proof screenshots from users who are in the payment flow.

    IMPORTANT: This handler ALSO matches photos/documents that are content
    submissions. It returns immediately when no payment state exists, so it
    does NOT block handle_media_submission from executing.

    RC-3 fix: full top-level try-except so any exception here does NOT
    propagate and interfere with other handlers for the same update.

    RC-7 fix: entry logging only when payment state exists (avoids noise
    for every private message).
    """
    if not message.from_user:
        return

    user_id = message.from_user.id

    # Fast-path: no payment state — return immediately without any side effects
    try:
        state = await _get_payment_state(user_id)
    except Exception as e:
        logger.error(
            "handle_payment_proof_capture: _get_payment_state raised",
            extra={"ctx_user_id": user_id, "ctx_error": str(e)},
            exc_info=True,
        )
        return  # RC-3: don't block other handlers

    if not state:
        return

    # Payment state exists — this IS a payment proof message
    logger.info(
        "HANDLER: handle_payment_proof_capture — payment proof received",
        extra={
            "ctx_user_id": user_id,
            "ctx_plan": state.get("plan"),
            "ctx_duration": state.get("duration"),
        },
    )

    try:
        plan = state.get("plan", "premium")
        duration = state.get("duration", "?")

        topic_service = get_topic_service()
        try:
            topic_id = await topic_service.get_or_create_user_topic(
                client, user_id, "payment"
            )
        except Exception as e:
            logger.error(
                "handle_payment_proof_capture: failed to create payment topic",
                extra={"ctx_user_id": user_id, "ctx_error": str(e)},
            )
            await _safe_reply(
                message,
                "⚠️ Could not create your payment verification topic. "
                "Please contact an admin directly.",
            )
            return

        try:
            await client.copy_message(
                chat_id=settings.VERIFICATION_GROUP_ID,
                from_chat_id=message.chat.id,
                message_id=message.id,
                message_thread_id=topic_id,
            )
            await client.send_message(
                chat_id=settings.VERIFICATION_GROUP_ID,
                text=(
                    f"💰 <b>Payment Proof Received</b>\n"
                    f"👤 User ID: <code>{user_id}</code>\n"
                    f"📦 Plan: <b>{plan}</b> / Duration: <b>{duration}</b>\n\n"
                    f"To approve: "
                    f"<code>/approve_payment {user_id} {plan} {duration}</code>"
                ),
                parse_mode=ParseMode.HTML,
                message_thread_id=topic_id,
            )
        except Exception as e:
            logger.error(
                "handle_payment_proof_capture: failed to forward proof to topic",
                extra={"ctx_user_id": user_id, "ctx_error": str(e)},
            )
            await _safe_reply(
                message,
                "⚠️ Failed to forward your proof. Please contact an admin directly.",
            )
            return

        await _clear_payment_state(user_id)

        await _safe_reply(
            message,
            "✅ <b>Payment proof received!</b>\n\n"
            "Our team will verify and activate your subscription within 24 hours.\n"
            "You'll receive a notification once approved.",
        )

        logger.info(
            "handle_payment_proof_capture: proof forwarded",
            extra={
                "ctx_user_id": user_id,
                "ctx_plan": plan,
                "ctx_duration": duration,
                "ctx_topic_id": topic_id,
            },
        )

    except Exception as e:
        logger.error(
            "HANDLER: handle_payment_proof_capture unhandled exception",
            extra={"ctx_user_id": user_id, "ctx_error": str(e)},
            exc_info=True,
        )
        await _safe_reply(
            message,
            "⚠️ An error occurred processing your payment proof. "
            "Please try again or contact an admin.",
        )


# ── Admin command: /approve_payment ──────────────────────────────────────────

@Client.on_message(
    filters.command("approve_payment")
    & filters.chat(settings.VERIFICATION_GROUP_ID)
)
async def handle_approve_payment(client: Client, message: Message) -> None:
    logger.info(
        "HANDLER: handle_approve_payment entered",
        extra={
            "ctx_from_user": (
                message.from_user.id if message.from_user else None
            ),
            "ctx_text": (message.text or "")[:80],
        },
    )

    try:
        if not message.from_user or not is_payment_admin(message.from_user.id):
            return

        parts = message.text.split()
        if len(parts) < 4:
            await message.reply_text(
                "Usage: <code>/approve_payment {user_id} {plan} {days}</code>\n"
                "Example: <code>/approve_payment 123456789 premium 30</code>\n"
                "Use <code>0</code> for lifetime.",
                parse_mode=ParseMode.HTML,
            )
            return

        try:
            user_id = int(parts[1])
            plan_str = parts[2].lower()
            days_str = parts[3]
            days: Optional[int] = (
                None if days_str in ("0", "lifetime") else int(days_str)
            )
        except (ValueError, IndexError):
            await message.reply_text(
                "Invalid arguments. user_id and days must be integers."
            )
            return

        try:
            plan = Plan(plan_str)
        except ValueError:
            await message.reply_text(
                f"Unknown plan '{plan_str}'. "
                f"Valid: {', '.join(p.value for p in Plan)}"
            )
            return

        admin_id = message.from_user.id

        try:
            sub = await _sub_service.grant(
                user_id=user_id,
                plan=plan,
                duration_days=days,
                granted_by=admin_id,
                notes=f"Payment approved by admin {admin_id}",
            )
        except Exception as e:
            await message.reply_text(f"❌ Failed to grant subscription: {e}")
            logger.error(
                "handle_approve_payment: subscription grant failed",
                extra={"ctx_error": str(e)},
            )
            return

        invite_link = None
        premium_chat_id = settings.PREMIUM_GROUP_ID
        if premium_chat_id:
            try:
                invite = await _invite_service.generate_premium_invite(
                    client=client,
                    user_id=user_id,
                    chat_id=premium_chat_id,
                    granted_by=admin_id,
                    plan=plan_str,
                )
                invite_link = invite.telegram_link
            except Exception as e:
                logger.error(
                    "handle_approve_payment: failed to generate invite",
                    extra={"ctx_user_id": user_id, "ctx_error": str(e)},
                )

        expiry_info = f"{days} days" if days else "Lifetime ♾️"
        dm_text = (
            f"✅ <b>Payment Approved!</b>\n\n"
            f"Your <b>{plan_str.capitalize()}</b> subscription has been activated.\n"
            f"Duration: <b>{expiry_info}</b>\n\n"
        )
        if invite_link:
            dm_text += (
                f"🔗 <b>Join the premium channel:</b>\n{invite_link}\n\n"
                "<i>This invite link expires in 24 hours and is single-use.</i>"
            )
        else:
            dm_text += (
                "An admin will manually add you to the premium channel shortly."
            )

        dm_sent = await _safe_send(client, user_id, dm_text)

        await get_audit().log(
            action=AuditAction.SUB_GRANT,
            performed_by=admin_id,
            target_user_id=user_id,
            details={
                "plan": plan_str,
                "days": days,
                "invite_link_generated": invite_link is not None,
            },
        )

        await message.reply_text(
            f"✅ <b>Approved</b>\n"
            f"👤 User <code>{user_id}</code> granted "
            f"<b>{plan_str}</b> ({expiry_info})\n"
            f"DM sent: {'✅' if dm_sent else '❌ (user may have blocked bot)'}\n"
            f"Invite link: {'✅' if invite_link else '❌ (check PREMIUM_GROUP_ID)'}",
            parse_mode=ParseMode.HTML,
        )

        logger.info(
            "handle_approve_payment: payment approved",
            extra={
                "ctx_user_id": user_id,
                "ctx_plan": plan_str,
                "ctx_days": days,
                "ctx_admin": admin_id,
            },
        )

    except Exception as e:
        logger.error(
            "HANDLER: handle_approve_payment unhandled exception",
            extra={"ctx_error": str(e)},
            exc_info=True,
        )
        try:
            await message.reply_text(
                "⚠️ An unexpected error occurred. Please try again."
            )
        except Exception:
            pass