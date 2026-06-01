from __future__ import annotations

import asyncio
from datetime import datetime, timezone
from typing import Optional

from pyrogram import Client, ContinuePropagation, filters
from pyrogram.enums import ParseMode
from pyrogram.types import (
    CallbackQuery,
    InlineKeyboardButton,
    InlineKeyboardMarkup,
    Message,
)

from app.config import settings
from app.core.permissions import is_moderator
from app.payments import get_payment_service
from app.payments.models import PaymentStatus
from app.payments.service import PLANS
from app.utils.logger import get_logger

logger = get_logger(__name__)

# ── Admin FSM state tracking (in-memory, restart-recovered separately) ────────
# Key: admin_id → {"session_id": str, "step": str, "card_message_id": int}
_admin_states: dict[int, dict] = {}


# ── Internal helpers ──────────────────────────────────────────────────────────

async def _get_payments_topic(client: Client) -> Optional[int]:
    try:
        from app.services.topic_service import get_topic_service
        topic_service = get_topic_service()
        return await topic_service.get_or_create_payments_topic(client)
    except Exception as e:
        logger.error("Failed to get payments topic", extra={"ctx_error": str(e)})
        return None


async def _post_payment_request_card(
    client: Client,
    session_id: str,
    user_id: int,
    plan_id: str,
    amount: float,
    method: str,
) -> None:
    """Post initial request card in admin hub — admin must click to send details."""
    topic_id = await _get_payments_topic(client)
    plan = PLANS.get(plan_id, {})

    text = (
        "💎 <b>New Premium Request</b>\n\n"
        f"👤 User: <code>{user_id}</code>\n"
        f"📦 Plan: {plan.get('label', plan_id)}\n"
        f"💰 Amount: ৳{amount:.2f}\n"
        f"📱 Method: {method.upper()}\n"
        f"🆔 Session: <code>{session_id}</code>\n\n"
        "<i>User is waiting for payment details.</i>"
    )
    buttons = InlineKeyboardMarkup([[
        InlineKeyboardButton(
            "📩 Send Payment Details",
            callback_data=f"pay:admin:send_details:{session_id}",
        )
    ]])

    try:
        await client.send_message(
            chat_id=settings.VERIFICATION_GROUP_ID,
            text=text,
            reply_markup=buttons,
            parse_mode=ParseMode.HTML,
            message_thread_id=topic_id,
        )
        # Persist topic_id on session for later proof card
        service = get_payment_service()
        session = await service.get_session(session_id)
        if session and topic_id:
            session.topic_id = topic_id
            await service.repository.save_session(session)
    except Exception as e:
        logger.error(
            "Failed to post payment request card",
            extra={"ctx_session": session_id, "ctx_error": str(e)},
        )


async def _post_proof_card(
    client: Client,
    session_id: str,
    user_id: int,
    txid: str,
    file_id: Optional[str],
    topic_id: Optional[int],
) -> None:
    """Post payment proof card in admin hub with Approve/Reject buttons."""
    service = get_payment_service()
    session = await service.get_session(session_id)
    if not session:
        return

    plan = PLANS.get(session.plan_id, {})
    caption = (
        "💰 <b>Payment Proof Received</b>\n\n"
        f"👤 User: <code>{user_id}</code>\n"
        f"📦 Plan: {plan.get('label', session.plan_id)}\n"
        f"💰 Amount: ৳{session.locked_amount:.2f}\n"
        f"📱 Method: {session.payment_method or 'N/A'}\n"
        f"🔑 TXID: <code>{txid}</code>\n"
        f"🆔 Session: <code>{session_id}</code>"
    )
    buttons = InlineKeyboardMarkup([[
        InlineKeyboardButton("✅ Approve", callback_data=f"pay:admin:approve:{session_id}"),
        InlineKeyboardButton("❌ Reject", callback_data=f"pay:admin:reject:{session_id}"),
    ]])

    try:
        if file_id:
            await client.send_photo(
                chat_id=settings.VERIFICATION_GROUP_ID,
                photo=file_id,
                caption=caption,
                reply_markup=buttons,
                parse_mode=ParseMode.HTML,
                message_thread_id=topic_id,
            )
        else:
            await client.send_message(
                chat_id=settings.VERIFICATION_GROUP_ID,
                text=caption,
                reply_markup=buttons,
                parse_mode=ParseMode.HTML,
                message_thread_id=topic_id,
            )
    except Exception as e:
        logger.error(
            "Failed to post proof card",
            extra={"ctx_session": session_id, "ctx_error": str(e)},
        )


async def _execute_rejection(
    client: Client,
    session_id: str,
    reason: str,
    admin_id: int,
    card_message: Optional[Message] = None,
) -> bool:
    service = get_payment_service()
    success = await service.reject_payment(session_id, reason, admin_id)
    if not success:
        logger.warning(
            "Rejection failed — session may be already processed",
            extra={"ctx_session": session_id},
        )
        return False

    session = await service.get_session(session_id)

    # Edit admin card
    if card_message:
        try:
            suffix = f"\n\n❌ Rejected: {reason}"
            if card_message.photo or card_message.caption:
                await card_message.edit_caption(
                    (card_message.caption or "") + suffix,
                    reply_markup=None,
                )
            else:
                await card_message.edit_text(
                    (card_message.text or "") + suffix,
                    reply_markup=None,
                )
        except Exception as e:
            logger.warning("Could not edit rejection card", extra={"ctx_error": str(e)})

    # Notify user
    if session:
        try:
            await client.send_message(
                session.user_id,
                f"❌ <b>Your payment was rejected.</b>\n\n"
                f"<b>Reason:</b> {reason}\n\n"
                "Please try again or contact support.",
                parse_mode=ParseMode.HTML,
                reply_markup=InlineKeyboardMarkup([[
                    InlineKeyboardButton("🔄 Try Again", callback_data="menu:premium"),
                    InlineKeyboardButton("🆘 Support", callback_data="menu:support"),
                ]]),
            )
        except Exception as e:
            logger.warning(
                "Could not notify user of rejection",
                extra={"ctx_user_id": session.user_id, "ctx_error": str(e)},
            )

    logger.info(
        "Payment rejected",
        extra={"ctx_session": session_id, "ctx_reason": reason, "ctx_admin": admin_id},
    )
    return True


# ── User-facing handlers ──────────────────────────────────────────────────────

@Client.on_callback_query(filters.regex(r"^menu:premium$"))
async def handle_premium_menu(client: Client, callback: CallbackQuery) -> None:
    await callback.answer()
    user_id = callback.from_user.id
    service = get_payment_service()
    existing = await service.get_active_session(user_id)

    text = (
        "Premium gives you access to exclusive BDGW content channels.\n\n"
        "Select a plan:"
    )
    buttons = []
    for plan_id, plan in PLANS.items():
        buttons.append([InlineKeyboardButton(
            f"{plan['label']} — ৳{plan['price']}",
            callback_data=f"pay:select:{plan_id}",
        )])

    if existing:
        buttons.append([InlineKeyboardButton(
            "📊 Check Status", callback_data="pay:status",
        )])

    buttons.append([InlineKeyboardButton("← Back", callback_data="menu:home")])

    await callback.message.edit_text(
        text,
        reply_markup=InlineKeyboardMarkup(buttons),
        parse_mode=ParseMode.HTML,
    )


@Client.on_callback_query(filters.regex(r"^pay:select:(.+)$"))
async def handle_plan_selection(client: Client, callback: CallbackQuery) -> None:
    plan_id = callback.data.split(":", 2)[2]
    user_id = callback.from_user.id
    await callback.answer()

    if plan_id not in PLANS:
        await callback.answer("Invalid plan.", show_alert=True)
        return

    service = get_payment_service()
    existing = await service.get_active_session(user_id)
    if existing:
        await callback.answer(
            "You already have an active payment session.", show_alert=True
        )
        return

    plan = PLANS[plan_id]
    text = (
        f"<b>Plan:</b> {plan['label']}\n"
        f"<b>Price:</b> ৳{plan['price']}\n\n"
        "Select your payment method:"
    )
    buttons = [
        [
            InlineKeyboardButton("bKash", callback_data=f"pay:method:bkash:{plan_id}"),
            InlineKeyboardButton("Nagad", callback_data=f"pay:method:nagad:{plan_id}"),
        ],
        [InlineKeyboardButton("Crypto (USDT)", callback_data=f"pay:method:crypto:{plan_id}")],
        [InlineKeyboardButton("← Back", callback_data="menu:premium")],
    ]
    await callback.message.edit_text(
        text,
        reply_markup=InlineKeyboardMarkup(buttons),
        parse_mode=ParseMode.HTML,
    )


@Client.on_callback_query(filters.regex(r"^pay:method:(\w+):(.+)$"))
async def handle_payment_method(client: Client, callback: CallbackQuery) -> None:
    parts = callback.data.split(":", 3)
    method = parts[2]
    plan_id = parts[3]
    user_id = callback.from_user.id
    await callback.answer()

    service = get_payment_service()

    # Double-check no active session exists
    existing = await service.get_active_session(user_id)
    if existing:
        await callback.answer("You already have an active payment session.", show_alert=True)
        return

    try:
        session = await service.create_session(user_id, plan_id, method)
    except Exception as e:
        logger.exception(
            "Failed to create payment session",
            extra={"ctx_user_id": user_id, "ctx_error": str(e)},
        )
        await callback.answer("Could not initiate payment. Please try again.", show_alert=True)
        return

    plan = PLANS[plan_id]
    await callback.message.edit_text(
        f"<b>Plan:</b> {plan['label']}\n"
        f"<b>Amount:</b> ৳{session.locked_amount:.2f}\n"
        f"<b>Method:</b> {method.upper()}\n\n"
        "✅ Request sent to admins.\n"
        "Please wait — payment details will be sent to you shortly.\n\n"
        f"<i>Session: <code>{session.id}</code></i>",
        reply_markup=InlineKeyboardMarkup([[
            InlineKeyboardButton("❌ Cancel", callback_data=f"pay:cancel:{session.id}"),
        ]]),
        parse_mode=ParseMode.HTML,
    )

    # Notify admins — they must manually send payment details
    await _post_payment_request_card(
        client=client,
        session_id=session.id,
        user_id=user_id,
        plan_id=plan_id,
        amount=session.locked_amount,
        method=method,
    )


@Client.on_callback_query(filters.regex(r"^pay:status$"))
async def handle_payment_status(client: Client, callback: CallbackQuery) -> None:
    user_id = callback.from_user.id
    await callback.answer()
    service = get_payment_service()
    session = await service.get_active_session(user_id)
    if not session:
        await callback.message.edit_text(
            "No active payment session.",
            reply_markup=InlineKeyboardMarkup([[
                InlineKeyboardButton("← Back", callback_data="menu:premium"),
            ]]),
        )
        return

    plan = PLANS.get(session.plan_id, {})
    status_display = session.status.value.replace("_", " ").title()
    text = (
        f"<b>Payment Status</b>\n\n"
        f"Plan: {plan.get('label', session.plan_id)}\n"
        f"Amount: ৳{session.locked_amount:.2f}\n"
        f"Method: {session.payment_method or 'N/A'}\n"
        f"Status: {status_display}"
    )
    if session.expires_at:
        remaining = (session.expires_at - datetime.now(timezone.utc)).total_seconds()
        if remaining > 0:
            text += f"\n⏰ Remaining: {int(remaining // 60)}m {int(remaining % 60)}s"
        else:
            text += "\n⌛ Session expired"

    await callback.message.edit_text(
        text,
        reply_markup=InlineKeyboardMarkup([[
            InlineKeyboardButton("❌ Cancel", callback_data=f"pay:cancel:{session.id}"),
            InlineKeyboardButton("← Back", callback_data="menu:premium"),
        ]]),
        parse_mode=ParseMode.HTML,
    )


@Client.on_callback_query(filters.regex(r"^pay:cancel:(.+)$"))
async def handle_payment_cancel(client: Client, callback: CallbackQuery) -> None:
    session_id = callback.data.split(":", 2)[2]
    user_id = callback.from_user.id
    await callback.answer()

    service = get_payment_service()
    session = await service.get_session(session_id)
    if not session or session.user_id != user_id:
        await callback.answer("Session not found.", show_alert=True)
        return

    cancelled = await service.update_status(session_id, PaymentStatus.CANCELLED)
    if cancelled:
        await service.repository.clear_timeout(session_id)
        await callback.message.edit_text(
            "Payment session cancelled.",
            reply_markup=InlineKeyboardMarkup([[
                InlineKeyboardButton("← Back", callback_data="menu:premium"),
            ]]),
        )
        logger.info("Payment cancelled by user", extra={"ctx_session": session_id, "ctx_user": user_id})
    else:
        await callback.answer(
            "Cannot cancel at this stage.", show_alert=True
        )


# ── User private message handler for TXID + screenshot ───────────────────────

@Client.on_message(filters.private & ~filters.regex(r"^/"))
async def handle_payment_inputs(client: Client, message: Message) -> None:
    """Captures TXID and screenshot in sequence from the active payment session."""
    if not message.from_user:
        return
    user_id = message.from_user.id
    service = get_payment_service()

    session = await service.get_active_session(user_id)
    if not session:
        raise ContinuePropagation

    # Reject statuses that don't need user input
    if session.status not in (
        PaymentStatus.WAITING_TXID,
        PaymentStatus.WAITING_SCREENSHOT,
    ):
        raise ContinuePropagation

    # Check expiry
    if session.expires_at and datetime.now(timezone.utc) > session.expires_at:
        await service.update_status(session.id, PaymentStatus.EXPIRED)
        await message.reply_text(
            "⌛ Your payment session has expired.\n"
            "Please start a new request.",
            reply_markup=InlineKeyboardMarkup([[
                InlineKeyboardButton("💎 New Request", callback_data="menu:premium"),
            ]]),
        )
        return

    # ── TXID submission ───────────────────────────────────────────────────────
    if session.status == PaymentStatus.WAITING_TXID:
        if not message.text or message.text.strip().startswith("/"):
            await message.reply_text(
                "Please send your Transaction ID (TXID) as a text message.\n"
                "This is the reference number from your payment app."
            )
            return

        txid = message.text.strip()

        # Validate TXID uniqueness — critical fraud prevention
        is_unique = await service.check_txid_unique(txid)
        if not is_unique:
            await message.reply_text(
                "❌ This Transaction ID has already been submitted.\n\n"
                "If you believe this is an error, please contact support.",
                reply_markup=InlineKeyboardMarkup([[
                    InlineKeyboardButton("🆘 Support", callback_data="menu:support"),
                ]]),
            )
            logger.warning(
                "Duplicate TXID submission rejected",
                extra={"ctx_user_id": user_id, "ctx_txid_prefix": txid[:8]},
            )
            return

        await service.update_status(session.id, PaymentStatus.WAITING_SCREENSHOT, txid=txid)
        await message.reply_text(
            "✅ TXID received.\n\n"
            "Now please send a screenshot of your payment confirmation.\n"
            "Or type <code>skip</code> to continue without one.",
            parse_mode=ParseMode.HTML,
        )
        return

    # ── Screenshot submission ─────────────────────────────────────────────────
    if session.status == PaymentStatus.WAITING_SCREENSHOT:
        file_id = None
        skipped = False

        if message.photo:
            file_id = message.photo.file_id
        elif message.document:
            file_id = message.document.file_id
        elif message.text and message.text.lower().strip() == "skip":
            skipped = True
        else:
            await message.reply_text(
                "Please send a screenshot of your payment, or type <code>skip</code> to proceed.",
                parse_mode=ParseMode.HTML,
            )
            return

        await service.update_status(
            session.id,
            PaymentStatus.UNDER_REVIEW,
            screenshot_file_id=file_id,
        )

        await message.reply_text(
            "✅ Proof submitted. Our admins will review it shortly.\n\n"
            f"<b>Session:</b> <code>{session.id}</code>",
            parse_mode=ParseMode.HTML,
        )

        await _post_proof_card(
            client=client,
            session_id=session.id,
            user_id=user_id,
            txid=session.txid or "N/A",
            file_id=file_id,
            topic_id=session.topic_id,
        )
        logger.info(
            "Payment proof submitted",
            extra={
                "ctx_session": session.id,
                "ctx_user_id": user_id,
                "ctx_has_screenshot": file_id is not None,
                "ctx_skipped": skipped,
            },
        )


# ── Admin handlers: Send Payment Details ─────────────────────────────────────

@Client.on_callback_query(filters.regex(r"^pay:admin:send_details:(.+)$"))
async def handle_admin_send_details_click(client: Client, callback: CallbackQuery) -> None:
    session_id = callback.data.split(":", 3)[3]
    admin_id = callback.from_user.id

    if not is_moderator(admin_id):
        await callback.answer("Unauthorized.", show_alert=True)
        return

    service = get_payment_service()
    session = await service.get_session(session_id)
    if not session:
        await callback.answer("Session not found.", show_alert=True)
        return
    if session.status != PaymentStatus.WAITING_PAYMENT_DETAILS:
        await callback.answer(
            "Session no longer waiting for payment details.", show_alert=True
        )
        return

    # Enter admin FSM state
    _admin_states[admin_id] = {
        "session_id": session_id,
        "step": "send_details",
        "topic_id": getattr(callback.message, "message_thread_id", None),
        "card_message_id": callback.message.id,
    }

    await callback.answer()
    await callback.message.reply(
        f"📩 <b>Send payment details for session <code>{session_id}</code></b>\n\n"
        "Your next message in this topic will be forwarded directly to the user.\n"
        "You can send text, photo, QR code, or any file.",
        parse_mode=ParseMode.HTML,
    )


# ── Admin hub message handler (FSM states) ────────────────────────────────────
# This fires for all human messages in VERIFICATION_GROUP_ID.
# If the sender has an active admin FSM state, it handles the message.
# Otherwise raises ContinuePropagation so topic_router.py can process it.

@Client.on_message(filters.chat(settings.VERIFICATION_GROUP_ID) & ~filters.bot)
async def handle_admin_hub_state_messages(client: Client, message: Message) -> None:
    if not message.from_user:
        raise ContinuePropagation

    admin_id = message.from_user.id
    state = _admin_states.get(admin_id)

    if not state:
        raise ContinuePropagation

    step = state.get("step")
    session_id = state["session_id"]

    if step == "send_details":
        await _process_send_details_message(client, message, session_id, admin_id)
    elif step == "custom_rejection":
        await _process_custom_rejection_message(client, message, session_id, admin_id, state)
    else:
        raise ContinuePropagation


async def _process_send_details_message(
    client: Client,
    message: Message,
    session_id: str,
    admin_id: int,
) -> None:
    service = get_payment_service()
    session = await service.get_session(session_id)

    if not session or session.status != PaymentStatus.WAITING_PAYMENT_DETAILS:
        _admin_states.pop(admin_id, None)
        raise ContinuePropagation

    # Relay admin's message to the user
    try:
        await client.copy_message(
            chat_id=session.user_id,
            from_chat_id=message.chat.id,
            message_id=message.id,
        )

        # CRITICAL: start timeout ONLY after confirmed delivery
        await service.update_status(
            session_id,
            PaymentStatus.WAITING_TXID,
            payment_method=session.payment_method,
        )
        started = await service.start_timeout(session_id)

        _admin_states.pop(admin_id, None)

        timeout_note = "20-minute timer started." if started else "⚠️ Timer could not start — check session."
        await message.reply(
            f"✅ Payment details delivered to user <code>{session.user_id}</code>.\n{timeout_note}",
            parse_mode=ParseMode.HTML,
        )
        logger.info(
            "Payment details relayed — timeout started",
            extra={"ctx_session": session_id, "ctx_user": session.user_id, "ctx_admin": admin_id},
        )

    except Exception as e:
        _admin_states.pop(admin_id, None)
        logger.error(
            "Failed to relay payment details",
            extra={"ctx_session": session_id, "ctx_error": str(e)},
        )
        await message.reply(
            f"⚠️ Failed to deliver to user <code>{session.user_id}</code>.\n"
            f"Error: {e}\n\nPlease try again.",
            parse_mode=ParseMode.HTML,
        )


async def _process_custom_rejection_message(
    client: Client,
    message: Message,
    session_id: str,
    admin_id: int,
    state: dict,
) -> None:
    if not message.text:
        await message.reply("Please type your rejection reason as text.")
        return

    reason = message.text.strip()
    _admin_states.pop(admin_id, None)

    # Retrieve the original proof card message to edit it
    card_message = None
    card_msg_id = state.get("card_message_id")
    if card_msg_id:
        try:
            result = await client.get_messages(
                chat_id=settings.VERIFICATION_GROUP_ID,
                message_ids=card_msg_id,
            )
            card_message = result if not isinstance(result, list) else (result[0] if result else None)
        except Exception:
            pass

    success = await _execute_rejection(
        client, session_id, reason, admin_id, card_message
    )
    if success:
        await message.reply(f"✅ Rejection recorded: {reason}")
    else:
        await message.reply("⚠️ Could not process rejection — session may already be handled.")


# ── Admin: Approve ────────────────────────────────────────────────────────────

@Client.on_callback_query(filters.regex(r"^pay:admin:approve:(.+)$"))
async def handle_admin_approve(client: Client, callback: CallbackQuery) -> None:
    session_id = callback.data.split(":", 3)[3]
    admin_id = callback.from_user.id

    if not is_moderator(admin_id):
        await callback.answer("Unauthorized.", show_alert=True)
        return

    await callback.answer("Processing...")

    service = get_payment_service()
    success = await service.approve_payment(client, session_id, admin_id)

    if success:
        suffix = f"\n\n✅ Approved by {callback.from_user.first_name}"
        try:
            msg = callback.message
            if msg.photo or msg.caption:
                await msg.edit_caption((msg.caption or "") + suffix, reply_markup=None)
            else:
                await msg.edit_text((msg.text or "") + suffix, reply_markup=None)
        except Exception as e:
            logger.warning("Could not edit approval card", extra={"ctx_error": str(e)})
        logger.info("Payment approved", extra={"ctx_session": session_id, "ctx_admin": admin_id})
    else:
        await callback.answer(
            "Could not approve — already processed or session invalid.", show_alert=True
        )


# ── Admin: Reject (step 1 — choose reason) ───────────────────────────────────

@Client.on_callback_query(filters.regex(r"^pay:admin:reject:(.+)$"))
async def handle_admin_reject(client: Client, callback: CallbackQuery) -> None:
    session_id = callback.data.split(":", 3)[3]
    admin_id = callback.from_user.id

    if not is_moderator(admin_id):
        await callback.answer("Unauthorized.", show_alert=True)
        return

    await callback.answer()

    buttons = [
        [InlineKeyboardButton("Invalid TXID", callback_data=f"pay:admin:rej_rsn:txid:{session_id}")],
        [InlineKeyboardButton("Wrong Amount", callback_data=f"pay:admin:rej_rsn:amount:{session_id}")],
        [InlineKeyboardButton("Duplicate TX", callback_data=f"pay:admin:rej_rsn:dup:{session_id}")],
        [InlineKeyboardButton("Screenshot Unclear", callback_data=f"pay:admin:rej_rsn:unclear:{session_id}")],
        [InlineKeyboardButton("✏️ Custom Reason", callback_data=f"pay:admin:rej_custom:{session_id}")],
    ]

    try:
        msg = callback.message
        if msg.photo:
            await msg.edit_reply_markup(InlineKeyboardMarkup(buttons))
        else:
            await msg.edit_reply_markup(InlineKeyboardMarkup(buttons))
    except Exception as e:
        logger.warning("Could not edit reject reason buttons", extra={"ctx_error": str(e)})


@Client.on_callback_query(filters.regex(r"^pay:admin:rej_rsn:(\w+):(.+)$"))
async def handle_rejection_reason(client: Client, callback: CallbackQuery) -> None:
    parts = callback.data.split(":", 4)
    reason_code = parts[3]
    session_id = parts[4]
    admin_id = callback.from_user.id

    if not is_moderator(admin_id):
        await callback.answer("Unauthorized.", show_alert=True)
        return

    reason_text = {
        "txid": "Invalid Transaction ID",
        "amount": "Incorrect Payment Amount",
        "dup": "Duplicate Transaction Reference",
        "unclear": "Payment Screenshot is Unclear",
    }.get(reason_code, "Payment rejected")

    await callback.answer()
    success = await _execute_rejection(
        client, session_id, reason_text, admin_id, callback.message
    )
    if not success:
        await callback.answer("Could not process — already handled.", show_alert=True)


@Client.on_callback_query(filters.regex(r"^pay:admin:rej_custom:(.+)$"))
async def handle_rejection_custom_start(client: Client, callback: CallbackQuery) -> None:
    session_id = callback.data.split(":", 3)[3]
    admin_id = callback.from_user.id

    if not is_moderator(admin_id):
        await callback.answer("Unauthorized.", show_alert=True)
        return

    _admin_states[admin_id] = {
        "session_id": session_id,
        "step": "custom_rejection",
        "topic_id": getattr(callback.message, "message_thread_id", None),
        "card_message_id": callback.message.id,
    }

    await callback.answer()
    await callback.message.reply(
        "✏️ Type your rejection reason and send it now:",
        parse_mode=ParseMode.HTML,
    )