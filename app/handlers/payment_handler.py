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
    db = DatabaseManager.get_db()
    key = f"payment_state:{user_id}"
    await db["bot_config"].update_one(
        {"key": key},
        {"$set": {"key": key, "plan": plan, "duration": duration, "created_at": datetime.now(timezone.utc)}},
        upsert=True,
    )


async def _get_payment_state(user_id: int) -> Optional[dict]:
    db = DatabaseManager.get_db()
    return await db["bot_config"].find_one({"key": f"payment_state:{user_id}"})


async def _clear_payment_state(user_id: int) -> None:
    db = DatabaseManager.get_db()
    await db["bot_config"].delete_one({"key": f"payment_state:{user_id}"})


# ── Safe messaging helpers ────────────────────────────────────────────────────

async def _safe_reply(message: Message, text: str, reply_markup=None) -> None:
    for attempt in range(_MAX_RETRIES):
        try:
            await message.reply_text(
                text,
                parse_mode=ParseMode.HTML,
                reply_markup=reply_markup,
            )
            return
        except FloodWait as e:
            await asyncio.sleep(int(e.value) + _FLOOD_BUFFER)
        except RPCError as e:
            if attempt == _MAX_RETRIES - 1:
                logger.warning("Failed to reply", extra={"ctx_error": str(e)})
            await asyncio.sleep(2 ** attempt)


async def _safe_send(client: Client, user_id: int, text: str, reply_markup=None) -> bool:
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
    return False


# ── Callback: menu:premium ────────────────────────────────────────────────────

@Client.on_callback_query(filters.regex(r"^menu:premium$") & filters.private)
async def handle_premium_menu(client: Client, callback: CallbackQuery) -> None:
    keyboard = InlineKeyboardMarkup([
        [InlineKeyboardButton(p["label"], callback_data=p["callback"])]
        for p in _PLANS
    ])
    await callback.message.edit_text(
        "💎 <b>Choose a Premium Plan</b>\n\n"
        "Select the plan that suits you. After selecting, you'll receive payment instructions.",
        parse_mode=ParseMode.HTML,
        reply_markup=keyboard,
    )
    await callback.answer()


# ── Callback: plan:{plan}:{duration} ─────────────────────────────────────────

@Client.on_callback_query(filters.regex(r"^plan:premium:(30|90|lifetime)$") & filters.private)
async def handle_plan_selection(client: Client, callback: CallbackQuery) -> None:
    plan_data = _PLAN_MAP.get(callback.data)
    if not plan_data:
        await callback.answer("Unknown plan.", show_alert=True)
        return

    label = plan_data["label"]
    keyboard = InlineKeyboardMarkup([[
        InlineKeyboardButton(
            "📸 Submit Payment Proof",
            callback_data=f"payment:submit:{callback.data.split(':')[1]}:{callback.data.split(':')[2]}",
        )
    ]])

    await callback.message.edit_text(
        f"💳 <b>Payment Instructions — {label}</b>\n\n"
        "1. Send payment to our bKash / Nagad number: <b>[CONFIGURED_NUMBER]</b>\n"
        "2. Take a screenshot of your payment confirmation.\n"
        "3. Tap <b>Submit Payment Proof</b> below, then send the screenshot.\n\n"
        "<i>Our team will verify and activate your subscription within 24 hours.</i>",
        parse_mode=ParseMode.HTML,
        reply_markup=keyboard,
    )
    await callback.answer()


# ── Callback: payment:submit:{plan}:{duration} ────────────────────────────────

@Client.on_callback_query(filters.regex(r"^payment:submit:premium:(30|90|lifetime)$") & filters.private)
async def handle_payment_submit(client: Client, callback: CallbackQuery) -> None:
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


# ── Private message: capture payment proof ────────────────────────────────────

@Client.on_message(
    (filters.photo | filters.document | filters.text)
    & filters.private
)
async def handle_payment_proof_capture(client: Client, message: Message) -> None:
    if not message.from_user:
        return

    user_id = message.from_user.id
    state = await _get_payment_state(user_id)
    if not state:
        return

    plan = state.get("plan", "premium")
    duration = state.get("duration", "?")

    topic_service = get_topic_service()
    try:
        topic_id = await topic_service.get_or_create_user_topic(client, user_id, "payment")
    except Exception as e:
        logger.error(
            "Failed to create payment topic",
            extra={"ctx_user_id": user_id, "ctx_error": str(e)},
        )
        await _safe_reply(
            message,
            "⚠️ Could not create your payment verification topic. Please contact an admin directly.",
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
                f"To approve: <code>/approve_payment {user_id} {plan} {duration}</code>"
            ),
            parse_mode=ParseMode.HTML,
            message_thread_id=topic_id,
        )
    except Exception as e:
        logger.error(
            "Failed to forward payment proof to topic",
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
        "Payment proof forwarded",
        extra={"ctx_user_id": user_id, "ctx_plan": plan, "ctx_duration": duration, "ctx_topic_id": topic_id},
    )


# ── Admin command: /approve_payment ──────────────────────────────────────────

@Client.on_message(
    filters.command("approve_payment")
    & filters.chat(settings.VERIFICATION_GROUP_ID)
)
async def handle_approve_payment(client: Client, message: Message) -> None:
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
        days: Optional[int] = None if days_str in ("0", "lifetime") else int(days_str)
    except (ValueError, IndexError):
        await message.reply_text("Invalid arguments. user_id and days must be integers.")
        return

    try:
        plan = Plan(plan_str)
    except ValueError:
        await message.reply_text(
            f"Unknown plan '{plan_str}'. Valid: {', '.join(p.value for p in Plan)}"
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
        logger.error("Payment approval: subscription grant failed", extra={"ctx_error": str(e)})
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
                "Failed to generate premium invite",
                extra={"ctx_user_id": user_id, "ctx_error": str(e)},
            )

    expiry_info = f"{days} days" if days else "Lifetime ♾️"
    dm_text = (
        f"✅ <b>Payment Approved!</b>\n\n"
        f"Your <b>{plan_str.capitalize()}</b> subscription has been activated.\n"
        f"Duration: <b>{expiry_info}</b>\n\n"
    )
    if invite_link:
        dm_text += f"🔗 <b>Join the premium channel:</b>\n{invite_link}\n\n"
        dm_text += "<i>This invite link expires in 24 hours and is single-use.</i>"
    else:
        dm_text += "An admin will manually add you to the premium channel shortly."

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
        f"👤 User <code>{user_id}</code> granted <b>{plan_str}</b> ({expiry_info})\n"
        f"DM sent: {'✅' if dm_sent else '❌ (user may have blocked bot)'}\n"
        f"Invite link: {'✅' if invite_link else '❌ (check PREMIUM_GROUP_ID)'}",
        parse_mode=ParseMode.HTML,
    )

    logger.info(
        "Payment approved",
        extra={
            "ctx_user_id": user_id,
            "ctx_plan": plan_str,
            "ctx_days": days,
            "ctx_admin": admin_id,
        },
    )