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

from app.ui.payment_cards import (
    build_plan_selection_card,
    build_payment_instruction_card,
    build_payment_status_card,
    build_proof_received_card,
    build_premium_activated_card,
    build_payment_rejected_card
)

@Client.on_callback_query(filters.regex(r"^menu:premium$"))
async def handle_premium_menu(client: Client, callback: CallbackQuery) -> None:
    """Shows premium plan selection."""
    await callback.answer()
    
    text, reply_markup = build_plan_selection_card(PLANS)
    
    await callback.message.edit_text(
        text,
        reply_markup=reply_markup,
        parse_mode=ParseMode.HTML
    )


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
        plan = PLANS[plan_id]
        
        text, reply_markup = build_payment_instruction_card(session, plan)
        
        await callback.message.edit_text(
            text,
            reply_markup=reply_markup,
            parse_mode=ParseMode.HTML
        )
        
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

    plan = PLANS.get(session.plan_id, {"label": "Unknown", "price": 0})
    text, reply_markup = build_payment_status_card(session, plan)
    
    await callback.message.edit_text(
        text,
        reply_markup=reply_markup,
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


@Client.on_message(filters.private & ~filters.regex(r"^/"))
async def handle_payment_inputs(client: Client, message: Message) -> None:
    """Captures TXID and Screenshot in sequence."""
    if not message.from_user:
        return
    user_id = message.from_user.id
    
    # B-06 FIX: Redis-backed fast check
    from app.core.redis_client import RedisClient
    try:
        redis = await RedisClient.get_client()
        if not await redis.exists(f"pay_session:{user_id}"):
            raise ContinuePropagation
    except ContinuePropagation:
        raise
    except Exception as e:
        logger.warning(
            "Redis fast-path failed in handle_payment_inputs — falling back to DB",
            extra={"ctx_user_id": user_id, "ctx_error": str(e)}
        )

    service = get_payment_service()
    
    session = await service.get_active_session(user_id)
    if not session or session.status not in [PaymentStatus.AWAITING_PAYMENT, PaymentStatus.WAITING_SCREENSHOT]:
        raise ContinuePropagation

    if session.status == PaymentStatus.AWAITING_PAYMENT:
        if not message.text:
            await message.reply_text("Please send your Transaction ID (TXID) as text.")
            return
        
        await service.update_status(session.id, PaymentStatus.WAITING_SCREENSHOT, txid=message.text)
        await message.reply_text("✅ TXID received. Now please send a screenshot of the payment proof.")
        
    elif session.status == PaymentStatus.WAITING_SCREENSHOT:
        if not (message.photo or message.document):
            await message.reply_text("Please send a photo or document as proof.")
            return
        
        file_id = message.photo.file_id if message.photo else message.document.file_id
        session.txid = session.txid or message.text
        await service.update_status(session.id, PaymentStatus.UNDER_REVIEW, screenshot_file_id=file_id)
        
        text = build_proof_received_card(session.id)
        
        await message.reply_text(
            text,
            parse_mode=ParseMode.HTML
        )
        
        # Notify Admins
        await _notify_admins_of_submission(client, session, session.txid or "", file_id)


async def _notify_admins_of_request(client: Client, session: PaymentSession, method: str) -> None:
    from app.services.topic_manager import get_topic_manager, TOPIC_PAYMENT
    topic_manager = get_topic_manager()
    
    try:
        topic_id = await topic_manager.get_or_create_user_topic(client, session.user_id, TOPIC_PAYMENT)
    except Exception as e:
        logger.error("Failed to get user payment topic", extra={"ctx_user_id": session.user_id, "ctx_error": str(e)})
        topic_id = None

    from app.ui.admin_cards import build_admin_payment_review_card, build_admin_payment_actions
    
    # Get user object for better card details
    user = await client.get_users(session.user_id)
    plan = PLANS.get(session.plan_id, {"label": "Unknown", "price": 0})
    
    text = build_admin_payment_review_card(user, session, plan)
    reply_markup = build_admin_payment_actions(session.id, session.user_id)
    
    await client.send_message(
        chat_id=settings.VERIFICATION_GROUP_ID,
        text=text,
        reply_markup=reply_markup,
        parse_mode=ParseMode.HTML,
        message_thread_id=topic_id
    )


@Client.on_callback_query(filters.regex(r"^pay:admin:send:(?P<sid>.+)$"))
async def handle_admin_send_details(client: Client, callback: CallbackQuery) -> None:
    session_id = callback.matches[0].group("sid")
    admin_id = callback.from_user.id
    
    if not is_support_admin(admin_id):
        await callback.answer("Unauthorized.", show_alert=True)
        return
        
    service = get_payment_service()
    updated = await service.update_status(session_id, PaymentStatus.PENDING_DETAILS)
    
    if not updated:
        await callback.answer("Could not update session. Already handled?", show_alert=True)
        return

    await callback.message.edit_text(
        callback.message.text + "\n\n⏳ <b>Waiting for Admin Input</b>\n"
        f"Please <b>Reply</b> to this message with the payment instructions (bKash number, QR code, etc.).",
        reply_markup=InlineKeyboardMarkup([
            [InlineKeyboardButton("❌ Cancel Send", callback_data=f"pay:admin:cancel_send:{session_id}")]
        ]),
        parse_mode=ParseMode.HTML
    )
    await callback.answer("Ready to receive payment details.")


@Client.on_callback_query(filters.regex(r"^pay:admin:cancel_send:(?P<sid>.+)$"))
async def handle_admin_cancel_send(client: Client, callback: CallbackQuery) -> None:
    session_id = callback.matches[0].group("sid")
    admin_id = callback.from_user.id
    
    if not is_support_admin(admin_id):
        await callback.answer("Unauthorized.", show_alert=True)
        return
        
    service = get_payment_service()
    # Revert back to REQUESTED
    updated = await service.update_status(session_id, PaymentStatus.REQUESTED)
    
    if not updated:
        await callback.answer("Could not revert session.", show_alert=True)
        return

    # Restore original markup
    buttons = [
        [
            InlineKeyboardButton("📩 Send Payment Details", callback_data=f"pay:admin:send:{session_id}"),
            InlineKeyboardButton("❌ Reject Request", callback_data=f"pay:admin:rej_req:{session_id}")
        ]
    ]
    # Remove the "Waiting for admin input" text
    original_text = callback.message.text.split("\n\n⏳")[0]
    await callback.message.edit_text(
        original_text,
        reply_markup=InlineKeyboardMarkup(buttons),
        parse_mode=ParseMode.HTML
    )
    await callback.answer("Cancelled sending details.")


@Client.on_callback_query(filters.regex(r"^pay:admin:rej_req:(?P<sid>.+)$"))
async def handle_admin_reject_request(client: Client, callback: CallbackQuery) -> None:
    session_id = callback.matches[0].group("sid")
    admin_id = callback.from_user.id
    
    if not is_support_admin(admin_id):
        await callback.answer("Unauthorized.", show_alert=True)
        return
        
    service = get_payment_service()
    success = await service.reject_payment(session_id, "Payment request rejected by admin", admin_id)
    
    if success:
        session = await service.get_session(session_id)
        await callback.message.edit_text(
            callback.message.text + f"\n\n❌ Request Rejected by {callback.from_user.first_name}",
            reply_markup=None
        )
        try:
            await client.send_message(
                session.user_id,
                "❌ <b>Your payment request was rejected.</b>\n\n"
                "Please try again or contact support.",
                parse_mode=ParseMode.HTML
            )
        except Exception:
            pass
        await callback.answer("Request rejected.")
    else:
        await callback.answer("Could not reject request.", show_alert=True)


@Client.on_message(filters.chat(settings.VERIFICATION_GROUP_ID) & filters.reply)
async def handle_admin_manual_details_reply(client: Client, message: Message) -> None:
    if not message.from_user or not is_support_admin(message.from_user.id):
        return

    replied_to = message.reply_to_message
    if not replied_to or not replied_to.from_user or not replied_to.from_user.is_bot:
        return

    if not replied_to.reply_markup:
        return

    session_id = None
    for row in replied_to.reply_markup.inline_keyboard:
        for btn in row:
            if btn.callback_data and btn.callback_data.startswith("pay:admin:cancel_send:"):
                session_id = btn.callback_data.split(":")[-1]
                break
        if session_id:
            break

    if not session_id:
        return

    service = get_payment_service()
    session = await service.get_session(session_id)
    if not session or session.status != PaymentStatus.PENDING_DETAILS:
        return

    try:
        await client.copy_message(
            chat_id=session.user_id,
            from_chat_id=message.chat.id,
            message_id=message.id,
        )
    except Exception as e:
        logger.error("Failed to forward payment details to user", extra={"ctx_user_id": session.user_id, "ctx_error": str(e)})
        await message.reply_text("❌ Failed to deliver payment details to user. They may have blocked the bot.")
        return

    updated = await service.update_status(session_id, PaymentStatus.AWAITING_PAYMENT)
    if not updated:
        return

    timeout_started = await service.start_timeout(session_id)
    if not timeout_started:
        logger.warning(
            "Payment timeout was not started after manual details delivery",
            extra={"ctx_payment_id": session_id, "ctx_user_id": session.user_id},
        )

    await message.reply_text("✅ Payment details delivered to user. Timeout started.")

    try:
        buttons = [
            [
                InlineKeyboardButton("✅ Approve", callback_data=f"pay:admin:approve:{session.id}"),
                InlineKeyboardButton("❌ Reject", callback_data=f"pay:admin:reject:{session.id}")
            ]
        ]
        original_text = (replied_to.text or replied_to.caption or "").split("\n\n⏳")[0]
        
        if replied_to.text:
            await client.edit_message_text(
                chat_id=replied_to.chat.id,
                message_id=replied_to.id,
                text=original_text + "\n\n✅ <b>Payment Details Delivered</b>\nWaiting for user to submit proof...",
                reply_markup=InlineKeyboardMarkup(buttons),
                parse_mode=ParseMode.HTML
            )
    except Exception as e:
        logger.warning("Failed to edit moderation card after delivery", extra={"ctx_error": str(e)})


async def _notify_admins_of_submission(client: Client, session: PaymentSession, txid: str, file_id: str) -> None:
    from app.services.topic_manager import get_topic_manager, TOPIC_PAYMENT
    topic_manager = get_topic_manager()
    
    try:
        topic_id = await topic_manager.get_or_create_user_topic(client, session.user_id, TOPIC_PAYMENT)
    except Exception as e:
        logger.error("Failed to get user payment topic", extra={"ctx_user_id": session.user_id, "ctx_error": str(e)})
        topic_id = None

    from app.ui.admin_cards import build_admin_payment_review_card, build_admin_payment_actions
    
    # Get user object for better card details
    user = await client.get_users(session.user_id)
    plan = PLANS.get(session.plan_id, {"label": "Unknown", "price": 0})
    
    text = build_admin_payment_review_card(user, session, plan)
    reply_markup = build_admin_payment_actions(session.id, session.user_id)
    
    try:
        await client.send_photo(
            chat_id=settings.VERIFICATION_GROUP_ID,
            photo=file_id,
            caption=text,
            reply_markup=reply_markup,
            parse_mode=ParseMode.HTML,
            message_thread_id=topic_id
        )
    except Exception as e:
        logger.warning("Failed to send photo for submission notification, falling back to text", extra={"ctx_error": str(e)})
        # Fallback to text message if photo fails (e.g. invalid file_id or it was a document)
        await client.send_message(
            chat_id=settings.VERIFICATION_GROUP_ID,
            text=f"{text}\n\n⚠️ <i>Screenshot could not be loaded directly. File ID: <code>{file_id}</code></i>",
            reply_markup=reply_markup,
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
    from app.ui.admin_cards import build_admin_rejection_reasons
    
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
        reply_markup = build_admin_rejection_reasons(session_id)
        await callback.message.edit_reply_markup(reply_markup)
        await callback.answer()

@Client.on_callback_query(filters.regex(r"^pay:admin:back:(?P<sid>.+)$"))
async def handle_admin_back_to_main(client: Client, callback: CallbackQuery) -> None:
    session_id = callback.matches[0].group("sid")
    from app.ui.admin_cards import build_admin_payment_actions
    
    service = get_payment_service()
    session = await service.get_session(session_id)
    if not session:
        await callback.answer("Session no longer exists.")
        return
        
    await callback.message.edit_reply_markup(build_admin_payment_actions(session.id, session.user_id))
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
        
        # Notify User with new UI
        try:
            text, reply_markup = build_payment_rejected_card(reason_text, session_id)
            await client.send_message(
                session.user_id,
                text,
                reply_markup=reply_markup,
                parse_mode=ParseMode.HTML
            )
        except Exception as e:
            logger.warning("Could not notify user of rejection", extra={"ctx_user_id": session.user_id, "ctx_error": str(e)})
            
        await callback.answer("Payment rejected.")
    else:
        await callback.answer("Error processing rejection.", show_alert=True)
