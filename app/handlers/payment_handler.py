from __future__ import annotations

"""
Payment handler — premium subscription purchase flow.
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
from app.core.permissions import Role, permission_required
from app.models.subscription import Plan
from app.services.subscription_service import SubscriptionService
from app.services.invite_service import InviteService
from app.services.topic_service import get_topic_service
from app.services.audit_service import get_audit, AuditAction
from app.utils.logger import get_logger

logger = get_logger(__name__)

def _get_sub_service():
    return SubscriptionService()

def _get_invite_service():
    return InviteService()
_FLOOD_BUFFER = settings.FLOODWAIT_EXTRA_BUFFER
_MAX_RETRIES = 3

_PLANS = [
    {"label": "30 Days — Premium 🌟", "callback": "plan:premium:30", "days": 30},
    {"label": "90 Days — Premium 💎", "callback": "plan:premium:90", "days": 90},
    {"label": "Lifetime — Premium ♾️", "callback": "plan:premium:lifetime", "days": None},
]

_PLAN_MAP = {p["callback"]: p for p in _PLANS}

# FIX 18: TTL for the Redis payment session key (seconds).
# Slightly longer than any realistic payment proof window.
_PAY_SESSION_TTL = 3600


async def _set_payment_state(user_id: int, plan: str, duration: str) -> None:
    """
    Persist payment session state to MongoDB (authoritative) and Redis (fast gate).

    FIX 18: writing to Redis with a 1-hour TTL means submission_handler can
    check Redis in O(1) instead of hitting MongoDB on every private media message.
    """
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
            "_set_payment_state: MongoDB write failed",
            extra={"ctx_user_id": user_id, "ctx_error": str(e)},
            exc_info=True,
        )
        raise

    # FIX 18: mirror to Redis for fast gate checks
    try:
        from app.core.redis_client import get_redis
        redis = get_redis()
        await redis.setex(f"pay_session:{user_id}", _PAY_SESSION_TTL, "1")
        logger.debug(
            "_set_payment_state: Redis key set",
            extra={"ctx_user_id": user_id, "ctx_ttl": _PAY_SESSION_TTL},
        )
    except Exception as e:
        # Non-fatal — MongoDB is the source of truth; Redis is a cache.
        # submission_handler falls back to MongoDB if Redis misses.
        logger.warning(
            "_set_payment_state: Redis setex failed (non-fatal)",
            extra={"ctx_user_id": user_id, "ctx_error": str(e)},
        )


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
    """
    Remove payment session state from MongoDB and Redis.

    FIX 18: deleting the Redis key immediately so submission_handler stops
    routing media to the payment handler as soon as the proof is submitted.
    """
    try:
        db = DatabaseManager.get_db()
        await db["bot_config"].delete_one(
            {"key": f"payment_state:{user_id}"}
        )
    except Exception as e:
        logger.warning(
            "_clear_payment_state: MongoDB delete failed (non-fatal)",
            extra={"ctx_user_id": user_id, "ctx_error": str(e)},
        )

    # FIX 18: clear Redis key
    try:
        from app.core.redis_client import get_redis
        redis = get_redis()
        await redis.delete(f"pay_session:{user_id}")
        logger.debug(
            "_clear_payment_state: Redis key deleted",
            extra={"ctx_user_id": user_id},
        )
    except Exception as e:
        logger.warning(
            "_clear_payment_state: Redis delete failed (non-fatal)",
            extra={"ctx_user_id": user_id, "ctx_error": str(e)},
        )


async def _safe_reply(message: Message, text: str, reply_markup=None) -> bool:
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
            logger.error(
                "_safe_send: unexpected exception",
                extra={"ctx_user_id": user_id, "ctx_error": str(e)},
                exc_info=True,
            )
            if attempt == _MAX_RETRIES - 1:
                return False
            await asyncio.sleep(2 ** attempt)
    return False


@Client.on_callback_query(filters.regex(r"^menu:premium$"))
async def handle_premium_menu(client: Client, callback: CallbackQuery) -> None:
    logger.info(
        "HANDLER: handle_premium_menu entered",
        extra={"ctx_from_user": callback.from_user.id if callback.from_user else None},
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


@Client.on_callback_query(filters.regex(r"^plan:premium:(30|90|lifetime)$"))
async def handle_plan_selection(client: Client, callback: CallbackQuery) -> None:
    logger.info(
        "HANDLER: handle_plan_selection entered",
        extra={
            "ctx_from_user": callback.from_user.id if callback.from_user else None,
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
            await callback.answer("⚠️ Error. Please try again.", show_alert=True)
        except Exception:
            pass


@Client.on_callback_query(filters.regex(r"^payment:submit:premium:(30|90|lifetime)$"))
async def handle_payment_submit(client: Client, callback: CallbackQuery) -> None:
    logger.info(
        "HANDLER: handle_payment_submit entered",
        extra={
            "ctx_from_user": callback.from_user.id if callback.from_user else None,
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
            await callback.answer("⚠️ Error. Please try again.", show_alert=True)
        except Exception:
            pass


@Client.on_message(
    (filters.photo | filters.document | filters.text)
    & filters.private
)
async def handle_payment_proof_capture(client: Client, message: Message) -> None:
    if not message.from_user:
        return

    user_id = message.from_user.id

    # Always log entry so we can confirm this handler ran, even on early exits.
    logger.debug(
        "HANDLER: handle_payment_proof_capture entered",
        extra={"ctx_user_id": user_id},
    )

    try:
        state = await _get_payment_state(user_id)
    except Exception as e:
        logger.error(
            "handle_payment_proof_capture: _get_payment_state raised",
            extra={"ctx_user_id": user_id, "ctx_error": str(e)},
            exc_info=True,
        )
        return

    if not state:
        # User is not in a payment flow — silent exit, this is the normal path.
        return

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


@Client.on_message(
    filters.command("approve_payment")
    & filters.chat(settings.VERIFICATION_GROUP_ID)
)
@permission_required(Role.PAYMENT_ADMIN)
async def handle_approve_payment(client: Client, message: Message) -> None:
    logger.info(
        "HANDLER: handle_approve_payment entered",
        extra={
            "ctx_from_user": message.from_user.id if message.from_user else None,
            "ctx_text": (message.text or "")[:80],
        },
    )

    try:
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
            sub = await _get_sub_service().grant(
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
                invite = await _get_invite_service().generate_premium_invite(
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
                f"<i>This invite link expires in {settings.INVITE_EXPIRY_MINUTES} minutes "
                f"and is single-use.</i>"
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
t Exception:
            pass
