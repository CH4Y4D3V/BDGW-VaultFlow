from __future__ import annotations

import asyncio
from datetime import datetime, timezone

from pyrogram import Client, ContinuePropagation, filters
from pyrogram.enums import ParseMode
from pyrogram.types import (
    CallbackQuery,
    InlineKeyboardButton,
    InlineKeyboardMarkup,
    Message,
)

from app.config import settings
from app.core.permissions import is_support_admin
from app.payments import get_payment_service
from app.payments.models import PaymentStatus
from app.payments.service import PLANS
from app.utils.logger import get_logger

logger = get_logger(__name__)

@Client.on_callback_query(filters.regex(r"^menu:premium$"))
async def handle_premium_menu(client: Client, callback: CallbackQuery) -> None:
    """Shows premium plan selection."""
    await callback.answer()
    text = (
        "Premium gives you access to exclusive BDGW content channels.\n\n"
        "Select a plan:"
    )
    
    buttons = []
    for plan_id, plan in PLANS.items():
        buttons.append([
            InlineKeyboardButton(
                f"{plan['label']} — ৳{plan['price']}",
                callback_data=f"pay:select:{plan_id}"
            )
        ])
    
    buttons.append([
        InlineKeyboardButton("📊 Check Status", callback_data="pay:status"),
        InlineKeyboardButton("🔄 Renew", callback_data="menu:premium")
    ])
    buttons.append([InlineKeyboardButton("← Back", callback_data="menu:home")])
    
    await callback.message.edit_text(
        text,
        reply_markup=InlineKeyboardMarkup(buttons),
        parse_mode=ParseMode.HTML
    )
    await callback.answer()


@Client.on_callback_query(filters.regex(r"^pay:select:(?P<plan_id>.+)$"))
async def handle_plan_selection(client: Client, callback: CallbackQuery) -> None:
    plan_id = callback.matches[0].group("plan_id")
    user_id = callback.from_user.id
    
    await callback.answer()

    service = get_payment_service()
    
    # Check for existing active session
    existing = await service.get_active_session(user_id)
    if existing:
        await callback.answer("You already have an active payment session.", show_alert=True)
        return

    try:
        session = await service.create_session(user_id, plan_id)
        
        text = (
            f"<b>Plan:</b> {PLANS[plan_id]['label']}\n"
            f"<b>Amount to Pay:</b> ৳{session.locked_amount}\n\n"
            "Select your payment method:"
        )
        
        buttons = [
            [
                InlineKeyboardButton("bKash", callback_data=f"pay:method:bkash:{session.id}"),
                InlineKeyboardButton("Nagad", callback_data=f"pay:method:nagad:{session.id}")
            ],
            [InlineKeyboardButton("Crypto (USDT)", callback_data=f"pay:method:crypto:{session.id}")],
            [InlineKeyboardButton("❌ Cancel", callback_data=f"pay:cancel:{session.id}")]
        ]
        
        await callback.message.edit_text(
            text,
            reply_markup=InlineKeyboardMarkup(buttons),
            parse_mode=ParseMode.HTML
        )
        await callback.answer()
        
    except Exception as e:
        logger.exception("Failed to start payment session", extra={"ctx_user_id": user_id, "ctx_error": str(e)})
        await callback.answer("Could not initiate payment. Please try again.", show_alert=True)


@Client.on_callback_query(filters.regex(r"^pay:status$"))
async def handle_payment_status(client: Client, callback: CallbackQuery) -> None:
    user_id = callback.from_user.id
    service = get_payment_service()
    session = await service.get_active_session(user_id)
    if not session:
        await callback.answer("No active payment session.", show_alert=True)
        return

    text = (
        f"<b>Payment Status</b>\n\n"
        f"Plan: {PLANS[session.plan_id]['label']}\n"
        f"Amount: ৳{session.locked_amount}\n"
        f"Status: {session.status.value}"
    )
    await callback.message.edit_text(
        text,
        reply_markup=InlineKeyboardMarkup([
            [InlineKeyboardButton("❌ Cancel", callback_data=f"pay:cancel:{session.id}")],
            [InlineKeyboardButton("← Back", callback_data="menu:premium")],
        ]),
        parse_mode=ParseMode.HTML,
    )
    await callback.answer()


@Client.on_callback_query(filters.regex(r"^pay:cancel:(?P<sid>.+)$"))
async def handle_payment_cancel(client: Client, callback: CallbackQuery) -> None:
    session_id = callback.matches[0].group("sid")
    service = get_payment_service()
    cancelled = await service.update_status(session_id, PaymentStatus.CANCELLED)
    if not cancelled:
        await callback.answer("Could not cancel this session.", show_alert=True)
        return
    await callback.message.edit_text(
        "Payment session cancelled.",
        reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("← Back", callback_data="menu:premium")]]),
    )
    await callback.answer()


@Client.on_callback_query(filters.regex(r"^pay:method:(?P<method>\w+):(?P<sid>.+)$"))
async def handle_payment_method(client: Client, callback: CallbackQuery) -> None:
    method = callback.matches[0].group("method")
    session_id = callback.matches[0].group("sid")
    
    service = get_payment_service()
    session = await service.get_session(session_id)
    
    if not session or session.status != PaymentStatus.WAITING_PAYMENT_DETAILS:
        await callback.answer("Session expired or invalid.", show_alert=True)
        return

    # Update session to REQUESTED
    updated = await service.update_status(
        session_id, 
        PaymentStatus.REQUESTED,
        payment_method=method
    )
    if not updated:
        await callback.answer("Could not request details. Try again.", show_alert=True)
        return

    await callback.message.edit_text(
        "⏳ <b>Requesting payment details...</b>\n\n"
        "An admin has been notified. You will receive the payment "
        "number/QR here shortly. Please keep this chat open.",
        reply_markup=InlineKeyboardMarkup([
            [InlineKeyboardButton("❌ Cancel", callback_data=f"pay:cancel:{session_id}")]
        ]),
        parse_mode=ParseMode.HTML
    )
    
    # Notify Admins of the REQUEST
    await _notify_admins_of_request(client, session, method)
    await callback.answer()


async def _notify_admins_of_request(client: Client, session: PaymentSession, method: str) -> None:
    from app.services.topic_service import get_topic_service
    topic_service = get_topic_service()
    
    try:
        topic_id = await topic_service.get_or_create_payments_topic(client)
    except Exception as e:
        logger.error("Failed to get payments topic", extra={"ctx_error": str(e)})
        topic_id = None

    text = (
        "💎 <b>New Payment Request</b>\n\n"
        f"👤 User: <code>{session.user_id}</code>\n"
        f"📦 Plan: {PLANS[session.plan_id]['label']}\n"
        f"💰 <b>৳{session.locked_amount:.2f}</b>\n"
        f"📱 Method: {method.capitalize()}\n"
        f"🆔 Session: <code>{session.id}</code>"
    )
    
    buttons = [
        [
            InlineKeyboardButton("📩 Send Payment Details", callback_data=f"pay:admin:send:{session.id}"),
            InlineKeyboardButton("❌ Reject Request", callback_data=f"pay:admin:rej_req:{session.id}")
        ]
    ]
    
    await client.send_message(
        chat_id=settings.VERIFICATION_GROUP_ID,
        text=text,
        reply_markup=InlineKeyboardMarkup(buttons),
        parse_mode=ParseMode.HTML,
        message_thread_id=topic_id
    )


async def _notify_admins_of_submission(client: Client, session: PaymentSession, txid: str, file_id: str) -> None:
    from app.services.topic_service import get_topic_service
    topic_service = get_topic_service()
    
    try:
        topic_id = await topic_service.get_or_create_payments_topic(client)
    except Exception as e:
        logger.error("Failed to get payments topic", extra={"ctx_error": str(e)})
        topic_id = None

    text = (
        "💎 <b>Payment Proof Received</b>\n\n"
        f"👤 User: <code>{session.user_id}</code>\n"
        f"📦 Plan: {PLANS[session.plan_id]['label']}\n"
        f"💰 <b>৳{session.locked_amount:.2f}</b>\n"
        f"📱 Method: {session.payment_method}\n"
        f"🔑 TXID: <code>{txid}</code>\n"
        f"🆔 Session: <code>{session.id}</code>"
    )
    
    buttons = [
        [
            InlineKeyboardButton("✅ Approve", callback_data=f"pay:admin:approve:{session.id}"),
            InlineKeyboardButton("❌ Reject", callback_data=f"pay:admin:reject:{session.id}")
        ]
    ]
    
    await client.send_photo(
        chat_id=settings.VERIFICATION_GROUP_ID,
        photo=file_id,
        caption=text,
        reply_markup=InlineKeyboardMarkup(buttons),
        parse_mode=ParseMode.HTML,
        message_thread_id=topic_id
    )


@Client.on_callback_query(filters.regex(r"^pay:admin:(?P<action>approve|reject):(?P<sid>.+)$"))
async def handle_admin_decision(client: Client, callback: CallbackQuery) -> None:
    action = callback.matches[0].group("action")
    session_id = callback.matches[0].group("sid")
    admin_id = callback.from_user.id
    
    if not is_support_admin(admin_id):
        await callback.answer("Unauthorized.", show_alert=True)
        return
        
    service = get_payment_service()
    
    if action == "approve":
        success = await service.approve_payment(client, session_id, admin_id)
        if success:
            await callback.message.edit_caption(
                callback.message.caption + f"\n\n✅ Approved by {callback.from_user.first_name}",
                reply_markup=None
            )
            await callback.answer("Payment approved.")
        else:
            await callback.answer("Could not process. Already handled?", show_alert=True)
            
    elif action == "reject":
        # Show rejection reasons
        buttons = [
            [InlineKeyboardButton("Invalid TXID", callback_data=f"pay:admin:rej_rsn:txid:{session_id}")],
            [InlineKeyboardButton("Wrong Amount", callback_data=f"pay:admin:rej_rsn:amount:{session_id}")],
            [InlineKeyboardButton("Duplicate TX", callback_data=f"pay:admin:rej_rsn:dup:{session_id}")],
            [InlineKeyboardButton("Screenshot Unclear", callback_data=f"pay:admin:rej_rsn:unclear:{session_id}")]
        ]
        await callback.message.edit_reply_markup(InlineKeyboardMarkup(buttons))
        await callback.answer()


@Client.on_callback_query(filters.regex(r"^pay:admin:rej_rsn:(?P<reason>\w+):(?P<sid>.+)$"))
async def handle_rejection_reason(client: Client, callback: CallbackQuery) -> None:
    reason_code = callback.matches[0].group("reason")
    session_id = callback.matches[0].group("sid")
    admin_id = callback.from_user.id
    
    reason_text = {
        "txid": "Invalid Transaction ID",
        "amount": "Incorrect Payment Amount",
        "dup": "Duplicate Transaction Reference",
        "unclear": "Payment Screenshot is Unclear"
    }.get(reason_code, "Payment rejected")
    
    service = get_payment_service()
    success = await service.reject_payment(session_id, reason_text, admin_id)
    
    if success:
        session = await service.get_session(session_id)
        await callback.message.edit_caption(
            callback.message.caption + f"\n\n❌ Rejected: {reason_text}",
            reply_markup=None
        )
        
        # Notify User
        try:
            await client.send_message(
                session.user_id,
                f"❌ <b>Your payment was rejected.</b>\n\n"
                f"<b>Reason:</b> {reason_text}\n\n"
                "Please try again or contact support if you have questions.",
                parse_mode=ParseMode.HTML
            )
        except Exception as e:
            logger.warning("Could not notify user of rejection", extra={"ctx_user_id": session.user_id, "ctx_error": str(e)})
            
        await callback.answer("Payment rejected.")
    else:
        await callback.answer("Error processing rejection.", show_alert=True)
