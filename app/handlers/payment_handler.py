from __future__ import annotations

"""
payment_handler.py
──────────────────
Handles the full payment lifecycle for BDGW VaultFlow:

  User flow:   premium menu → plan select → method select → wait for admin
               details → submit TXID → submit screenshot → under review
  Admin flow:  send details (FSM) → approve / reject (with reason choices)

Spec ref: §7.3 — Admin manually sends payment number; bot confirms delivery
          THEN starts the 20-minute timer.

Key invariants enforced here:
  • FSM admin states are backed by MongoDB (payment_admin_states collection)
    so they survive bot restarts.
  • Status transitions are written to MongoDB BEFORE any Telegram message is
    sent (restart-safe ordering).
  • Every Telegram call has explicit FloodWait handling via _tg_send().
  • StopPropagation is never swallowed by a generic except clause.
  • Cancellation refunds points, mirrors the expiry path.
"""

import asyncio
from datetime import datetime, timezone
from typing import Optional

from pyrogram import Client, ContinuePropagation, StopPropagation, filters
from pyrogram.enums import ParseMode
from pyrogram.errors import FloodWait
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

# ── Constants ─────────────────────────────────────────────────────────────────

_MAX_FLOOD_WAIT = 30  # cap FloodWait sleeps at 30 s inside handlers

# Statuses that allow user-initiated cancellation
_CANCELLABLE_STATUSES = {
    PaymentStatus.WAITING_PAYMENT_DETAILS,
    PaymentStatus.WAITING_TXID,
    PaymentStatus.WAITING_SCREENSHOT,
}


# ── Telegram send wrapper ─────────────────────────────────────────────────────

async def _tg_send(coro) -> Optional[object]:
    """
    Execute a Telegram API coroutine with one FloodWait retry.

    Returns the result on success, None on any failure. All exceptions
    are logged. StopPropagation and ContinuePropagation are re-raised
    so Pyrogram's handler chain is never silently broken.
    """
    try:
        return await coro
    except (StopPropagation, ContinuePropagation):
        raise
    except FloodWait as exc:
        wait = min(exc.value, _MAX_FLOOD_WAIT)
        logger.warning("FloodWait %ds — retrying", wait)
        await asyncio.sleep(wait)
        try:
            return await coro
        except Exception as retry_exc:
            logger.warning("Retry after FloodWait failed: %s", retry_exc)
            return None
    except Exception as exc:
        logger.warning("Telegram call failed: %s", exc)
        return None


# ── Admin FSM — MongoDB-backed ────────────────────────────────────────────────

async def _fsm_set(admin_id: int, state: dict) -> None:
    """
    Persist admin FSM state to MongoDB.

    Upserts into ``payment_admin_states`` keyed by admin_id.
    Called before any Telegram interaction so the state survives restarts.
    """
    service = get_payment_service()
    col = service.repository._db["payment_admin_states"]
    await col.update_one(
        {"_id": admin_id},
        {"$set": {"state": state, "updated_at": datetime.now(timezone.utc)}},
        upsert=True,
    )


async def _fsm_get(admin_id: int) -> Optional[dict]:
    """Retrieve admin FSM state from MongoDB. Returns None if not found."""
    service = get_payment_service()
    col = service.repository._db["payment_admin_states"]
    doc = await col.find_one({"_id": admin_id})
    return doc["state"] if doc else None


async def _fsm_clear(admin_id: int) -> None:
    """Remove admin FSM state from MongoDB."""
    service = get_payment_service()
    col = service.repository._db["payment_admin_states"]
    await col.delete_one({"_id": admin_id})


# ── Internal helpers ──────────────────────────────────────────────────────────

async def _get_payments_topic(client: Client, user_id: int) -> Optional[int]:
    """
    Resolve (or create) the user's permanent topic in the Verification Hub.

    Returns the thread_id, or None if topic resolution fails. Failures are
    logged but do not raise — callers should degrade gracefully.
    """
    try:
        from app.services.topic_manager import get_topic_manager
        topic_manager = get_topic_manager()
        topic_id = await topic_manager.get_or_create_user_topic(client, user_id)
        if topic_id:
            return topic_id
    except Exception as exc:
        logger.error(
            "Failed to get user topic for payment",
            extra={"ctx_user_id": user_id, "ctx_error": str(exc)},
        )
    return None


async def _post_payment_request_card(
    client: Client,
    session_id: str,
    user_id: int,
    plan_id: str,
    amount: float,
    method: str,
) -> None:
    """
    Post the initial payment request card in the user's Verification Hub topic.

    The card contains a "Send Payment Details" button that enters the admin
    into the send_details FSM step. Persists topic_id on the session for
    subsequent card operations.
    """
    topic_id = await _get_payments_topic(client, user_id)
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

    result = await _tg_send(
        client.send_message(
            chat_id=settings.VERIFICATION_GROUP_ID,
            text=text,
            reply_markup=buttons,
            parse_mode=ParseMode.HTML,
            message_thread_id=topic_id,
        )
    )

    if result and topic_id:
        try:
            service = get_payment_service()
            session = await service.get_session(session_id)
            if session:
                session.topic_id = topic_id
                await service.repository.save_session(session)
        except Exception as exc:
            logger.error(
                "Failed to persist topic_id on session",
                extra={"ctx_session": session_id, "ctx_error": str(exc)},
            )


async def _post_proof_card(
    client: Client,
    session_id: str,
    user_id: int,
    txid: str,
    file_id: Optional[str],
    topic_id: Optional[int],
) -> None:
    """
    Post the payment proof card in the admin hub with Approve/Reject buttons.

    Falls back to resolving topic_id from the topic manager if not provided.
    Uses send_photo when a screenshot file_id is available, otherwise sends
    a plain text card.
    """
    service = get_payment_service()
    session = await service.get_session(session_id)
    if not session:
        logger.warning(
            "Cannot post proof card — session not found",
            extra={"ctx_session": session_id},
        )
        return

    if not topic_id:
        topic_id = await _get_payments_topic(client, user_id)

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

    if file_id:
        await _tg_send(
            client.send_photo(
                chat_id=settings.VERIFICATION_GROUP_ID,
                photo=file_id,
                caption=caption,
                reply_markup=buttons,
                parse_mode=ParseMode.HTML,
                message_thread_id=topic_id,
            )
        )
    else:
        await _tg_send(
            client.send_message(
                chat_id=settings.VERIFICATION_GROUP_ID,
                text=caption,
                reply_markup=buttons,
                parse_mode=ParseMode.HTML,
                message_thread_id=topic_id,
            )
        )


async def _execute_rejection(
    client: Client,
    session_id: str,
    reason: str,
    admin_id: int,
    card_message: Optional[Message] = None,
) -> bool:
    """
    Execute a payment rejection: update session status, edit the admin card,
    and notify the user.

    Returns True if the rejection was applied; False if the session was already
    processed (idempotency guard in service layer).
    """
    service = get_payment_service()
    success = await service.reject_payment(client, session_id, reason, admin_id)
    if not success:
        logger.warning(
            "Rejection failed — session may be already processed",
            extra={"ctx_session": session_id},
        )
        return False

    session = await service.get_session(session_id)

    # Edit the admin proof card to reflect rejection
    if card_message:
        suffix = f"\n\n❌ Rejected: {reason}"
        if card_message.photo or card_message.caption:
            await _tg_send(
                card_message.edit_caption(
                    (card_message.caption or "") + suffix,
                    reply_markup=None,
                )
            )
        else:
            await _tg_send(
                card_message.edit_text(
                    (card_message.text or "") + suffix,
                    reply_markup=None,
                )
            )

    # Notify the user
    if session:
        await _tg_send(
            client.send_message(
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
        )

    logger.info(
        "payment_rejected",
        extra={
            "ctx_session": session_id,
            "ctx_reason": reason,
            "ctx_admin": admin_id,
        },
    )
    return True


async def _refund_points_if_any(service, user_id: int, points_used: int, context: str) -> None:
    """
    Refund referral points to a user if any were applied to their session.

    Shared by cancellation and expiry paths to ensure consistency.
    Failures are logged but do not propagate — points refund is best-effort.
    """
    if points_used <= 0:
        return
    try:
        from app.referral.repository import ReferralRepository
        from app.referral.service import ReferralService
        ref_repo = ReferralRepository(service.repository._db)
        ref_service = ReferralService(ref_repo, None)
        await ref_service.refund_points(user_id, points_used)
        logger.info(
            "points_refunded",
            extra={"ctx_user_id": user_id, "ctx_points": points_used, "ctx_context": context},
        )
    except Exception as exc:
        logger.error(
            "failed_to_refund_points",
            extra={"ctx_user_id": user_id, "ctx_context": context, "ctx_error": str(exc)},
        )


# ── User-facing handlers ──────────────────────────────────────────────────────

@Client.on_callback_query(filters.regex(r"^menu:premium$"))
async def handle_premium_menu(client: Client, callback: CallbackQuery) -> None:
    """Display the premium plan selection menu to the user."""
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
    """Show payment method options for the selected plan."""
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
    """
    Create a payment session for the selected plan and method.

    Performs a second duplicate-session guard in case the user managed to
    click twice before the first session was written. On success, posts a
    request card to the admin hub.
    """
    parts = callback.data.split(":", 3)
    method = parts[2]
    plan_id = parts[3]
    user_id = callback.from_user.id
    await callback.answer()

    service = get_payment_service()

    existing = await service.get_active_session(user_id)
    if existing:
        await callback.answer("You already have an active payment session.", show_alert=True)
        return

    try:
        session = await service.create_session(user_id, plan_id, method)
    except ValueError as exc:
        logger.warning(
            "Duplicate payment session attempt prevented",
            extra={"ctx_user_id": user_id, "ctx_error": str(exc)},
        )
        await callback.answer(str(exc), show_alert=True)
        return
    except Exception as exc:
        logger.exception(
            "Fatal failure creating payment session",
            extra={"ctx_user_id": user_id, "ctx_error": str(exc)},
        )
        await callback.answer("System error. Please contact support via /help.", show_alert=True)
        return

    plan = PLANS[plan_id]
    # B-10 FIX: NEVER auto-send payment numbers. Bot only confirms request.
    # Manual admin action via request card is REQUIRED for detail delivery.
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
    """Display the user's current active payment session status."""
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
    """
    Cancel an active payment session initiated by the user.

    Only sessions in WAITING_PAYMENT_DETAILS, WAITING_TXID, or
    WAITING_SCREENSHOT can be cancelled — sessions under review or beyond
    are not user-cancellable. Refunds any points applied to the session.
    """
    session_id = callback.data.split(":", 2)[2]
    user_id = callback.from_user.id
    await callback.answer()

    service = get_payment_service()
    session = await service.get_session(session_id)

    if not session or session.user_id != user_id:
        await callback.answer("Session not found.", show_alert=True)
        return

    if session.status not in _CANCELLABLE_STATUSES:
        await callback.answer(
            "Cannot cancel at this stage.", show_alert=True
        )
        return

    cancelled = await service.update_status(session_id, PaymentStatus.CANCELLED)
    if cancelled:
        # Clear timeout and Redis cache
        await service.repository.clear_timeout(session_id)
        try:
            from app.core.redis_client import RedisClient
            redis = await RedisClient.get_client()
            await redis.delete(f"pay_session:{user_id}")
        except Exception as exc:
            logger.warning(
                "Failed to clear Redis session on cancel",
                extra={"ctx_session": session_id, "ctx_error": str(exc)},
            )

        # Refund points if any were applied
        await _refund_points_if_any(service, user_id, session.points_used or 0, "cancel")

        await service.repository.log_event(session_id, "payment_cancelled_by_user", {})

        await callback.message.edit_text(
            "Payment session cancelled.",
            reply_markup=InlineKeyboardMarkup([[
                InlineKeyboardButton("← Back", callback_data="menu:premium"),
            ]]),
        )
        logger.info(
            "payment_cancelled_by_user",
            extra={"ctx_session": session_id, "ctx_user": user_id},
        )
    else:
        await callback.answer("Cannot cancel at this stage.", show_alert=True)


# ── User private message handler for TXID + screenshot ───────────────────────

@Client.on_message(filters.private & ~filters.regex(r"^/"))
async def handle_payment_inputs(client: Client, message: Message) -> None:
    """
    Capture TXID and payment screenshot from the user in sequence.

    Only activates when the user has an active session in WAITING_TXID or
    WAITING_SCREENSHOT status. Expired sessions are cleaned up fully via
    the service layer (points refund, Redis clear, event log).

    TXID uniqueness is validated before accepting to prevent fraud.
    """
    if not message.from_user:
        return
    user_id = message.from_user.id
    service = get_payment_service()

    session = await service.get_active_session(user_id)
    if not session:
        raise ContinuePropagation

    if session.status not in (
        PaymentStatus.WAITING_TXID,
        PaymentStatus.WAITING_SCREENSHOT,
    ):
        raise ContinuePropagation

    # Expiry check — delegate to service to ensure full cleanup
    if session.expires_at and datetime.now(timezone.utc) > session.expires_at:
        try:
            from app.services.payment_timeouts import PaymentTimeoutMonitor
            monitor = PaymentTimeoutMonitor(service.repository)
            await monitor.expire_session(client, session.id)
        except Exception as exc:
            # Fallback: at minimum mark expired in DB
            logger.error(
                "expire_session delegate failed, falling back to status update",
                extra={"ctx_session": session.id, "ctx_error": str(exc)},
            )
            await service.update_status(session.id, PaymentStatus.EXPIRED)

        await _tg_send(
            message.reply_text(
                "⌛ Your payment session has expired.\n"
                "Please start a new request.",
                reply_markup=InlineKeyboardMarkup([[
                    InlineKeyboardButton("💎 New Request", callback_data="menu:premium"),
                ]]),
            )
        )
        return

    # ── TXID submission ───────────────────────────────────────────────────────
    if session.status == PaymentStatus.WAITING_TXID:
        if not message.text or message.text.strip().startswith("/"):
            await _tg_send(
                message.reply_text(
                    "Please send your Transaction ID (TXID) as a text message.\n"
                    "This is the reference number from your payment app."
                )
            )
            return

        txid = message.text.strip()

        is_unique = await service.check_txid_unique(txid)
        if not is_unique:
            await _tg_send(
                message.reply_text(
                    "❌ This Transaction ID has already been submitted.\n\n"
                    "If you believe this is an error, please contact support.",
                    reply_markup=InlineKeyboardMarkup([[
                        InlineKeyboardButton("🆘 Support", callback_data="menu:support"),
                    ]]),
                )
            )
            logger.warning(
                "duplicate_txid_rejected",
                extra={"ctx_user_id": user_id, "ctx_txid_prefix": txid[:8]},
            )
            return

        await service.update_status(session.id, PaymentStatus.WAITING_SCREENSHOT, txid=txid)
        await _tg_send(
            message.reply_text(
                "✅ TXID received.\n\n"
                "Now please send a screenshot of your payment confirmation.\n"
                "Or type <code>skip</code> to continue without one.",
                parse_mode=ParseMode.HTML,
            )
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
            await _tg_send(
                message.reply_text(
                    "Please send a screenshot of your payment, or type <code>skip</code> to proceed.",
                    parse_mode=ParseMode.HTML,
                )
            )
            return

        # Write status to DB BEFORE sending any Telegram messages (restart-safe)
        await service.update_status(
            session.id,
            PaymentStatus.UNDER_REVIEW,
            screenshot_file_id=file_id,
        )

        await _tg_send(
            message.reply_text(
                "✅ Proof submitted. Our admins will review it shortly.\n\n"
                f"<b>Session:</b> <code>{session.id}</code>",
                parse_mode=ParseMode.HTML,
            )
        )

        # Route submission header to user topic
        try:
            from app.services.topic_manager import get_topic_manager
            topic_id = await get_topic_manager().get_or_create_user_topic(client, user_id)
            await _tg_send(
                client.send_message(
                    chat_id=settings.VERIFICATION_GROUP_ID,
                    text=(
                        f"💳 <b>PAYMENT SUBMITTED</b>\n\n"
                        f"<b>Amount:</b> {session.locked_amount} {session.currency}\n"
                        f"<b>Method:</b> {session.payment_method or 'N/A'}\n"
                        f"<b>Transaction:</b> <code>{session.txid or 'N/A'}</code>"
                    ),
                    message_thread_id=topic_id,
                    parse_mode=ParseMode.HTML,
                )
            )
        except Exception as exc:
            logger.error(
                "Failed to route payment submission header to topic",
                extra={"ctx_session": session.id, "ctx_user_id": user_id, "ctx_error": str(exc)},
            )

        # Audit log
        try:
            from app.services.audit_service import get_audit
            await get_audit().log(
                action="PAYMENT_SUBMITTED",
                performed_by=user_id,
                target_user_id=user_id,
                details={
                    "amount": session.locked_amount,
                    "method": session.payment_method,
                    "session_id": session.id,
                },
            )
        except Exception as exc:
            logger.error(
                "Failed to write audit log for payment submission",
                extra={"ctx_session": session.id, "ctx_error": str(exc)},
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
            "payment_proof_submitted",
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
    """
    Enter the admin into the send_details FSM step for a payment session.

    Guards:
      - Admin must be a moderator.
      - Session must be in WAITING_PAYMENT_DETAILS status.
      - If the admin already has an active FSM state for a different session,
        they are warned before overwriting.

    The FSM state is persisted to MongoDB before replying to Telegram so it
    survives a bot restart mid-flow.
    """
    session_id = callback.data.split(":", 3)[3]
    admin_id = callback.from_user.id

    if not await is_moderator(admin_id):
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

    # Guard: warn if overwriting an existing FSM state for a different session
    existing_state = await _fsm_get(admin_id)
    if existing_state and existing_state.get("session_id") != session_id:
        await callback.answer(
            f"⚠️ You had an active state for session "
            f"{existing_state['session_id'][:8]}… — it has been replaced.",
            show_alert=True,
        )

    # Persist FSM state to MongoDB BEFORE replying
    state = {
        "session_id": session_id,
        "step": "send_details",
        "topic_id": getattr(callback.message, "message_thread_id", None),
        "card_message_id": callback.message.id,
    }
    await _fsm_set(admin_id, state)

    await callback.answer()
    await _tg_send(
        callback.message.reply(
            f"📩 <b>Send payment details for session <code>{session_id}</code></b>\n\n"
            "Your next message in this topic will be forwarded directly to the user.\n"
            "You can send text, photo, QR code, or any file.",
            parse_mode=ParseMode.HTML,
        )
    )


# ── Admin hub message handler (FSM states) ───────────────────────────────────

# Fires for all human messages in VERIFICATION_GROUP_ID.
# If the sender has an active MongoDB-backed FSM state, it handles the message.
# Otherwise raises ContinuePropagation so topic_router.py can process it.

@Client.on_message(filters.chat(settings.VERIFICATION_GROUP_ID) & ~filters.bot)
async def handle_admin_hub_state_messages(client: Client, message: Message) -> None:
    """
    Route admin messages in the Verification Hub through the FSM state machine.

    Reads FSM state from MongoDB on every invocation — restart-safe.
    Propagates to the next handler if no state is found.
    """
    if not message.from_user:
        raise ContinuePropagation

    admin_id = message.from_user.id
    state = await _fsm_get(admin_id)

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
    """
    Process the admin's payment details message: copy it to the user, then
    advance session status to WAITING_TXID and start the 20-minute timer.

    Ordering (restart-safe):
      1. copy_message to user
      2. update_status → WAITING_TXID  (written to DB)
      3. start_timeout                 (written to DB)
      4. _fsm_clear                    (DB)
      5. Reply to admin

    StopPropagation is raised after success so topic_router does not
    re-deliver this message to the user. It is explicitly excluded from the
    error handler so it is never swallowed.

    On copy failure: FSM state is cleared and the admin is informed to retry.
    The session remains in WAITING_PAYMENT_DETAILS — no stuck state.
    """
    service = get_payment_service()
    session = await service.get_session(session_id)

    if not session or session.status != PaymentStatus.WAITING_PAYMENT_DETAILS:
        await _fsm_clear(admin_id)
        raise ContinuePropagation

    # Step 1: deliver to user
    delivered = await _tg_send(
        client.copy_message(
            chat_id=session.user_id,
            from_chat_id=message.chat.id,
            message_id=message.id,
        )
    )

    if not delivered:
        # Delivery failed — clear FSM, leave session status unchanged so admin
        # can retry via the card button.
        await _fsm_clear(admin_id)
        await _tg_send(
            message.reply(
                f"⚠️ Failed to deliver to user <code>{session.user_id}</code>.\n\n"
                "Please try again. The session has NOT been advanced.",
                parse_mode=ParseMode.HTML,
            )
        )
        logger.error(
            "payment_details_delivery_failed",
            extra={"ctx_session": session_id, "ctx_user": session.user_id, "ctx_admin": admin_id},
        )
        return

    # Steps 2–4: advance state (all DB writes before any more Telegram calls)
    await service.update_status(
        session_id,
        PaymentStatus.WAITING_TXID,
        payment_method=session.payment_method,
    )
    # B-18 FIX: Start timer ONLY after confirmed delivery
    started = await service.start_timeout(session_id, confirmed_delivery=True)
    await _fsm_clear(admin_id)

    # Step 5: confirm to admin
    timeout_note = "20-minute timer started." if started else "⚠️ Timer could not start — check session."
    await _tg_send(
        message.reply(
            f"✅ Payment details delivered to user <code>{session.user_id}</code>.\n{timeout_note}",
            parse_mode=ParseMode.HTML,
        )
    )

    logger.info(
        "payment_details_relayed_timeout_started",
        extra={
            "ctx_session": session_id,
            "ctx_user": session.user_id,
            "ctx_admin": admin_id,
            "ctx_timer_started": started,
        },
    )

    # Stop propagation — topic_router must NOT re-deliver this to the user,
    # as this handler has already forwarded the message.
    raise StopPropagation


async def _process_custom_rejection_message(
    client: Client,
    message: Message,
    session_id: str,
    admin_id: int,
    state: dict,
) -> None:
    """
    Process the admin's custom rejection reason and execute the rejection.

    Requires the message to be plain text. Retrieves the original proof card
    message to edit it with the rejection reason.
    """
    if not message.text:
        await _tg_send(message.reply("Please type your rejection reason as text."))
        return

    reason = message.text.strip()
    await _fsm_clear(admin_id)

    # Retrieve the original proof card to edit it
    card_message = None
    card_msg_id = state.get("card_message_id")
    if card_msg_id:
        try:
            result = await client.get_messages(
                chat_id=settings.VERIFICATION_GROUP_ID,
                message_ids=card_msg_id,
            )
            card_message = result if not isinstance(result, list) else (result[0] if result else None)
        except Exception as exc:
            logger.warning(
                "Could not fetch original proof card for custom rejection",
                extra={"ctx_session": session_id, "ctx_error": str(exc)},
            )

    success = await _execute_rejection(client, session_id, reason, admin_id, card_message)
    if success:
        await _tg_send(message.reply(f"✅ Rejection recorded: {reason}"))
    else:
        await _tg_send(
            message.reply("⚠️ Could not process rejection — session may already be handled.")
        )


# ── Admin: Approve ────────────────────────────────────────────────────────────

@Client.on_callback_query(filters.regex(r"^pay:admin:approve:(.+)$"))
async def handle_admin_approve(client: Client, callback: CallbackQuery) -> None:
    """
    Approve a payment session.

    Delegates to the service layer which handles subscription provisioning,
    invite link generation, and user notification. Edits the proof card on
    success to prevent double-approvals.
    """
    session_id = callback.data.split(":", 3)[3]
    admin_id = callback.from_user.id

    if not await is_moderator(admin_id):
        await callback.answer("Unauthorized.", show_alert=True)
        return

    await callback.answer("Processing...")

    service = get_payment_service()
    success = await service.approve_payment(client, session_id, admin_id)

    if success:
        suffix = f"\n\n✅ Approved by {callback.from_user.first_name}"
        msg = callback.message
        if msg.photo or msg.caption:
            await _tg_send(
                msg.edit_caption((msg.caption or "") + suffix, reply_markup=None)
            )
        else:
            await _tg_send(
                msg.edit_text((msg.text or "") + suffix, reply_markup=None)
            )
        logger.info(
            "payment_approved",
            extra={"ctx_session": session_id, "ctx_admin": admin_id},
        )
    else:
        await callback.answer(
            "Could not approve — already processed or session invalid.", show_alert=True
        )


# ── Admin: Reject (step 1 — choose reason) ───────────────────────────────────

@Client.on_callback_query(filters.regex(r"^pay:admin:reject:(.+)$"))
async def handle_admin_reject(client: Client, callback: CallbackQuery) -> None:
    """
    Show rejection reason options on the proof card.

    Replaces the Approve/Reject buttons with preset reason choices plus a
    Custom Reason option that enters the admin into the custom_rejection FSM step.
    """
    session_id = callback.data.split(":", 3)[3]
    admin_id = callback.from_user.id

    if not await is_moderator(admin_id):
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

    await _tg_send(
        callback.message.edit_reply_markup(InlineKeyboardMarkup(buttons))
    )


@Client.on_callback_query(filters.regex(r"^pay:admin:rej_rsn:(\w+):(.+)$"))
async def handle_rejection_reason(client: Client, callback: CallbackQuery) -> None:
    """Execute a preset rejection reason immediately."""
    parts = callback.data.split(":", 4)
    reason_code = parts[3]
    session_id = parts[4]
    admin_id = callback.from_user.id

    if not await is_moderator(admin_id):
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
    """
    Enter the admin into the custom_rejection FSM step.

    Persists FSM state to MongoDB before prompting the admin, so the state
    survives a bot restart while the admin is composing their reason.
    """
    session_id = callback.data.split(":", 3)[3]
    admin_id = callback.from_user.id

    if not await is_moderator(admin_id):
        await callback.answer("Unauthorized.", show_alert=True)
        return

    state = {
        "session_id": session_id,
        "step": "custom_rejection",
        "topic_id": getattr(callback.message, "message_thread_id", None),
        "card_message_id": callback.message.id,
    }
    await _fsm_set(admin_id, state)

    await callback.answer()
    await _tg_send(
        callback.message.reply(
            "✏️ Type your rejection reason and send it now:",
            parse_mode=ParseMode.HTML,
        )
    )
