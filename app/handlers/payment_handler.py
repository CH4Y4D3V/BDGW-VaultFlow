"""
payment_handler.py
──────────
Universal payment proof submission handler.

I18N FIX: all user-facing strings resolved through get_text(key, lang) /
t(key, lang, **kwargs) using the user's DB-persisted language preference.
Language is fetched once per handler entry point via get_user_lang(db, user_id).
Admin-facing strings intentionally remain English.
"""

import logging
from typing import Optional

from aiogram import Bot, F, Router
from aiogram.fsm.context import FSMContext
from aiogram.types import CallbackQuery, Message

from config import BotConfig
from keyboards import admin_payment_keyboard, cancel_keyboard, my_payments_keyboard
from services.notify import notify_admins, notify_admins_photo
from services.message_tracker import (
    delete_user_messages as _delete_intent_msgs,
    track_message as _track_message,
    CONTEXT_PAYMENT_INTENT,
    CONTEXT_PAYMENT_SUBMISSION,
)
from database.repository import Database
from states import AdminFSM, PaymentFSM
from locales import get_text, get_user_lang, t

log = logging.getLogger(__name__)
router = Router(name="payment")

_MIN_TX_LEN = 4
_MAX_PENDING_PAYMENTS = 2


async def _ensure_payment_slot(user_id: int, db: Database) -> bool:
    pending_count = await db.get_pending_payment_count_for_user(user_id)
    return pending_count < _MAX_PENDING_PAYMENTS


# ── User taps "Submit Payment Proof" (group flow) ─────────────────────────────

@router.callback_query(F.data.startswith("pay:submit:"))
async def cb_pay_submit(
    callback: CallbackQuery, state: FSMContext, db: Database
) -> None:
    parts = callback.data.split(":")
    raw_rid = parts[2] if len(parts) > 2 else "0"
    request_id = int(raw_rid) if raw_rid.isdigit() and raw_rid != "0" else None
    service_type = parts[3] if len(parts) > 3 else "unknown"
    svc_id = parts[4] if len(parts) > 4 else ""

    user = callback.from_user
    await db.upsert_user(user.id, user.username, user.full_name)

    lang = (await get_user_lang(db, user.id)) or "en"

    if not await _ensure_payment_slot(user.id, db):
        await callback.answer(
            get_text("payment_slot_full", lang),
            show_alert=True,
        )
        return

    await state.set_state(PaymentFSM.waiting_tx_id)
    await state.update_data(
        request_id=request_id,
        service_type=service_type,
        service_id=svc_id,
    )

    await callback.message.edit_text(
        get_text("payment_secure_submission", lang),
        parse_mode="HTML",
        reply_markup=cancel_keyboard(lang),
    )
    await callback.answer()


# ── Entry for approved cam / one-to-one / meetup ──────────────────────────────

@router.callback_query(F.data.startswith("approved_pay:"))
async def cb_start_approved_payment(
    callback: CallbackQuery, state: FSMContext, db: Database
) -> None:
    parts = callback.data.split(":")
    request_id = int(parts[1])
    svc_id = parts[2]
    user_id = callback.from_user.id

    user = callback.from_user
    await db.upsert_user(user.id, user.username, user.full_name)

    lang = (await get_user_lang(db, user_id)) or "en"

    if not await _ensure_payment_slot(user_id, db):
        await callback.answer(
            get_text("payment_slot_full_wait", lang),
            show_alert=True,
        )
        return

    req = await db.get_approved_service_request(request_id, user_id)
    log.info(
        "[PAYMENT CHECK] user=%d request_id=%d status=%s",
        user_id, request_id, req["status"] if req else "NOT_FOUND",
    )

    if req is None:
        log.warning(
            "[PAYMENT] approved_pay callback: request_id=%d user=%d — "
            "not found or status is not 'approved'. "
            "Possible causes: already processed, wrong user, or status mismatch.",
            request_id, user_id,
        )
        await callback.answer(
            get_text("payment_link_invalid", lang),
            show_alert=True,
        )
        return

    await state.set_state(PaymentFSM.waiting_tx_id)
    await state.update_data(
        request_id=request_id,
        service_id=svc_id,
        service_type=req["service_type"],
        payment_method=req.get("payment_method", ""),
    )

    await callback.message.edit_text(
        get_text("payment_secure_submission_approved", lang),
        parse_mode="HTML",
        reply_markup=cancel_keyboard(lang),
    )
    await callback.answer()


# ── TX ID received ────────────────────────────────────────────────────────────

@router.message(PaymentFSM.waiting_tx_id, F.text)
async def msg_tx_id(message: Message, state: FSMContext, db: Database) -> None:
    tx_id = message.text.strip()
    user_id = message.from_user.id
    lang = (await get_user_lang(db, user_id)) or "en"

    if len(tx_id) < _MIN_TX_LEN:
        await message.answer(
            t("tx_too_short", lang, length=len(tx_id)),
            parse_mode="HTML",
        )
        return

    if await db.has_pending_tx(user_id, tx_id):
        await message.answer(
            t("tx_duplicate_own", lang, tx_id=tx_id),
            parse_mode="HTML",
            reply_markup=cancel_keyboard(lang),
        )
        return

    if await db.has_any_pending_tx_globally(tx_id):
        await message.answer(
            t("tx_duplicate_global", lang, tx_id=tx_id),
            parse_mode="HTML",
            reply_markup=cancel_keyboard(lang),
        )
        log.warning("[PAYMENT] Cross-user duplicate TX: user=%d tx_id=%s", user_id, tx_id)
        return

    await state.update_data(tx_id=tx_id)
    await state.set_state(PaymentFSM.waiting_screenshot)

    await message.answer(
        get_text("screenshot_prompt", lang),
        parse_mode="HTML",
        reply_markup=cancel_keyboard(lang),
    )


# ── Screenshot or skip ────────────────────────────────────────────────────────

@router.message(PaymentFSM.waiting_screenshot, F.photo)
async def msg_screenshot(
    message: Message, state: FSMContext, bot: Bot, db: Database, settings: BotConfig
) -> None:
    await _submit_payment_proof(
        message, state, bot, db, settings, message.photo[-1].file_id
    )


@router.message(PaymentFSM.waiting_screenshot, F.text)
async def msg_skip_screenshot(
    message: Message, state: FSMContext, bot: Bot, db: Database, settings: BotConfig
) -> None:
    user_id = message.from_user.id
    lang = (await get_user_lang(db, user_id)) or "en"

    if message.text.strip().lower() != "skip":
        await message.answer(
            get_text("screenshot_not_skip", lang),
            parse_mode="HTML",
        )
        return
    await _submit_payment_proof(message, state, bot, db, settings, screenshot_file_id=None)


# ── Core submission ───────────────────────────────────────────────────────────

async def _submit_payment_proof(
    message: Message,
    state: FSMContext,
    bot: Bot,
    db: Database,
    settings: BotConfig,
    screenshot_file_id: Optional[str],
) -> None:
    """
    Core payment proof submission logic.

    FIX-2: clear payment intent immediately after successful create_payment().
    FIX-2b: handle None return from create_payment() (DB UNIQUE race condition).
    I18N FIX: all user-facing text resolved against the user's stored language.
    """
    data = await state.get_data()
    await state.clear()

    user = message.from_user
    lang = (await get_user_lang(db, user.id)) or "en"

    request_id: Optional[int] = data.get("request_id")
    service_type: str = data.get("service_type", "unknown")
    payment_method: str = data.get("payment_method", "")
    tx_id: str = data.get("tx_id", "N/A")

    await db.upsert_user(user.id, user.username, user.full_name)

    service_id = data.get("service_id", "")
    if not service_id and request_id:
        req = await db.get_service_request(request_id)
        if req:
            service_id = req.get("service_id", "")
            if not payment_method:
                payment_method = req.get("payment_method", "")

    svc = next((s for s in settings.services if s.id == service_id), None)
    base_price = svc.price if svc else None
    discount_pct = await db.get_discount()
    if base_price and discount_pct > 0:
        amount = int(base_price * (1 - discount_pct / 100))
    else:
        amount = base_price

    service_name = svc.name if svc else (service_id or "Unknown Service")

    # ── Final duplicate check (application-layer safety net) ──────────────────
    if await db.has_pending_tx(user.id, tx_id):
        await message.answer(
            t("tx_duplicate_app_layer", lang, tx_id=tx_id),
            parse_mode="HTML",
        )
        log.warning("[PAYMENT] Duplicate TX blocked at app layer: user=%d tx=%s", user.id, tx_id)
        return

    pending_count = await db.get_pending_payment_count_for_user(user.id)
    if pending_count >= _MAX_PENDING_PAYMENTS:
        await message.answer(
            get_text("flood_blocked", lang),
            parse_mode="HTML",
        )
        log.warning("[PAYMENT] Flood blocked user %d (%d pending)", user.id, pending_count)
        return

    # ── DB insert — returns None on UNIQUE tx_id violation ────────────────────
    payment_id = await db.create_payment(
        user_id=user.id,
        package_id=service_id,
        tx_id=tx_id,
        amount=amount,
        screenshot=screenshot_file_id,
    )

    # ── FIX-2b: Handle DB-level duplicate (race condition) ─────────────────────
    if payment_id is None:
        log.warning(
            "[PAYMENT] DB-level tx_id uniqueness violation: user=%d tx=%s — "
            "concurrent submission race blocked.",
            user.id, tx_id,
        )
        await message.answer(
            t("tx_duplicate_race", lang, tx_id=tx_id),
            parse_mode="HTML",
        )
        return

    # ── FIX-2: Clear payment intent IMMEDIATELY after successful submission ────
    await db.clear_payment_intent(user.id)
    await _delete_intent_msgs(bot, db, user.id, CONTEXT_PAYMENT_INTENT)

    log.info(
        "[PAYMENT] Intent cleared for user %d after proof submission "
        "(payment #%d, tx=%s). Auto-ban timer deactivated.",
        user.id, payment_id, tx_id,
    )
    log.info(
        "[PAYMENT SUBMITTED] user=%d payment_id=%d service=%s tx=%s",
        user.id, payment_id, service_id, tx_id,
    )

    amount_str = f"৳{amount:,}" if amount else "To be confirmed"

    # Send confirmation to user in their language
    confirm_msg = await message.answer(
        t(
            "payment_submitted",
            lang,
            service=service_name,
            tx_id=tx_id,
            amount=amount_str,
            payment_id=payment_id,
        ),
        parse_mode="HTML",
        reply_markup=my_payments_keyboard(lang),
    )
    await _track_message(db, user.id, confirm_msg.message_id, CONTEXT_PAYMENT_SUBMISSION)

    # ── Admin notification (always English) ───────────────────────────────────
    user_info = (
        f"{user.full_name} (@{user.username or 'no_username'}) "
        f"[<code>{user.id}</code>]"
    )
    method_label_en = {
        "bkash":     "bKash",
        "nagad":     "Nagad",
        "crypto":    "Crypto",
        "foreigner": "Foreign Transfer",
    }.get(payment_method, payment_method or "N/A")

    proof_caption = (
        f"💳 <b>New Payment Proof</b>\n\n"
        f"👤 User: {user_info}\n"
        f"📦 Service: <b>{service_name}</b>\n"
        f"🔑 TX ID: <code>{tx_id}</code>\n"
        f"💰 Amount: {amount_str}\n"
        f"📱 Method: {method_label_en}\n"
        f"🆔 Payment ID: #{payment_id}"
    )
    if request_id:
        proof_caption += f"\n🔗 Request ID: #{request_id}"

    kb = admin_payment_keyboard(payment_id)

    if screenshot_file_id:
        await notify_admins_photo(
            bot, settings,
            photo=screenshot_file_id,
            caption=proof_caption,
            db=db,
            entity_type="payment_review",
            entity_id=payment_id,
            reply_markup=kb,
            user_id=user.id,
        )
    else:
        await notify_admins(
            bot,
            settings,
            proof_caption,
            db=db,
            entity_type="payment_review",
            entity_id=payment_id,
            reply_markup=kb,
            user_id=user.id,
        )

    # Brief audit line to GENERAL for monitoring
    if settings.admin_group_id:
        audit_line = (
            f"📋 <b>Payment #{payment_id}</b> submitted\n"
            f"👤 <code>{user.id}</code> | {service_name} | "
            f"TX: <code>{tx_id}</code>\n"
            f"→ Review in user topic"
        )
        try:
            await bot.send_message(
                chat_id=settings.admin_group_id,
                text=audit_line,
                parse_mode="HTML",
            )
        except Exception:
            pass

    log.info("[PAYMENT NOTIFY] Admin notification sent for payment #%d", payment_id)


# ── Cancel ────────────────────────────────────────────────────────────────────

@router.callback_query(F.data == "cancel:payment")
async def cb_cancel_payment(
    callback: CallbackQuery, state: FSMContext, db: Database
) -> None:
    await state.clear()

    user_id = callback.from_user.id
    lang = (await get_user_lang(db, user_id)) or "en"

    from keyboards import category_keyboard
    await callback.message.edit_text(
        get_text("payment_cancelled", lang),
        reply_markup=category_keyboard(lang),
    )
    await callback.answer()