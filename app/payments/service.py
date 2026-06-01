from __future__ import annotations

import uuid
from datetime import datetime, timedelta, timezone
from typing import Optional

from pyrogram import Client

from app.config import settings
from app.models.subscription import Plan
from app.payments.models import PaymentSession, PaymentStatus
from app.payments.repository import PaymentRepository
from app.referral.repository import ReferralRepository
from app.services.subscription_service import SubscriptionService
from app.utils.logger import get_logger

logger = get_logger(__name__)

# ── Plan catalogue ────────────────────────────────────────────────────────────

PLANS: dict[str, dict] = {
    "1month":  {"label": "1 Month",  "price": 499,  "days": 30},
    "3months": {"label": "3 Months", "price": 1299, "days": 90},
    "6months": {"label": "6 Months", "price": 2499, "days": 180},
}

SESSION_TIMEOUT_MINUTES = 20  # Timer starts ONLY after payment details are delivered

# ── State machine ─────────────────────────────────────────────────────────────

ALLOWED_TRANSITIONS: dict[PaymentStatus, set[PaymentStatus]] = {
    PaymentStatus.WAITING_PAYMENT_DETAILS: {
        PaymentStatus.WAITING_TXID,
        PaymentStatus.CANCELLED,
        PaymentStatus.EXPIRED,
    },
    PaymentStatus.WAITING_TXID: {
        PaymentStatus.WAITING_SCREENSHOT,
        PaymentStatus.CANCELLED,
        PaymentStatus.EXPIRED,
    },
    PaymentStatus.WAITING_SCREENSHOT: {
        PaymentStatus.UNDER_REVIEW,
        PaymentStatus.CANCELLED,
        PaymentStatus.EXPIRED,
    },
    PaymentStatus.UNDER_REVIEW: {
        PaymentStatus.PROCESSING,
        PaymentStatus.CANCELLED,
    },
    PaymentStatus.PROCESSING: {
        PaymentStatus.APPROVED,
        PaymentStatus.REJECTED,
        PaymentStatus.UNDER_REVIEW,  # Rollback on activation failure
    },
    # Terminal states — no transitions out
    PaymentStatus.APPROVED: set(),
    PaymentStatus.REJECTED: set(),
    PaymentStatus.EXPIRED: set(),
    PaymentStatus.CANCELLED: set(),
}


class PaymentService:
    def __init__(
        self,
        repository: PaymentRepository,
        referral_repo: ReferralRepository,
    ) -> None:
        self.repository = repository
        self.referral_repo = referral_repo

    # ── Session lifecycle ─────────────────────────────────────────────────────

    async def create_session(
        self,
        user_id: int,
        plan_id: str,
        method: str = "",
    ) -> PaymentSession:
        """
        Create a new payment session.

        Applies any available referral points as a discount.
        The locked_amount is snapshotted at creation — NEVER recalculated.
        Points are deducted atomically at session creation.
        """
        if plan_id not in PLANS:
            raise ValueError(f"Invalid plan_id: {plan_id!r}")

        plan = PLANS[plan_id]
        base_price = plan["price"]

        # Referral discount: 1 point = ৳1
        wallet = await self.referral_repo.get_wallet(user_id)
        available_points = wallet.get("points_balance", 0) if wallet else 0
        discount = min(available_points, base_price)  # Cannot exceed plan price
        locked_amount = float(base_price - discount)

        # Deduct points atomically before creating session
        if discount > 0:
            deducted = await self.referral_repo.deduct_points(user_id, discount)
            if not deducted:
                # Wallet changed between check and deduct — use 0 discount
                discount = 0
                locked_amount = float(base_price)
                logger.warning(
                    "Referral point deduction failed — proceeding without discount",
                    extra={"ctx_user_id": user_id, "ctx_points": available_points},
                )

        session = PaymentSession(
            id=str(uuid.uuid4()),
            user_id=user_id,
            plan_id=plan_id,
            locked_amount=locked_amount,
            payment_method=method or None,
            status=PaymentStatus.WAITING_PAYMENT_DETAILS,
        )

        await self.repository.save_session(session)
        await self.repository.log_event(
            session.id,
            "session_created",
            {
                "base_price": base_price,
                "discount_applied": discount,
                "locked_amount": locked_amount,
                "method": method,
            },
        )

        logger.info(
            "Payment session created",
            extra={
                "ctx_session": session.id,
                "ctx_user_id": user_id,
                "ctx_plan": plan_id,
                "ctx_amount": locked_amount,
                "ctx_discount": discount,
            },
        )
        return session

    async def get_session(self, payment_id: str) -> Optional[PaymentSession]:
        return await self.repository.get_session(payment_id)

    async def get_active_session(self, user_id: int) -> Optional[PaymentSession]:
        return await self.repository.get_active_session(user_id)

    # ── TXID validation ───────────────────────────────────────────────────────

    async def check_txid_unique(self, txid: str) -> bool:
        """
        Returns True if the TXID has not been used in any payment record.

        Checks ALL statuses including cancelled/expired to prevent
        TXID reuse across different payment attempts (fraud prevention).
        """
        if not txid or not txid.strip():
            return False
        existing = await self.repository.get_by_txid(txid.strip())
        return existing is None

    # ── State transitions ─────────────────────────────────────────────────────

    async def update_status(
        self,
        payment_id: str,
        new_status: PaymentStatus,
        **kwargs,
    ) -> bool:
        """
        Transition session to new_status, validated against ALLOWED_TRANSITIONS.
        Extra kwargs are applied as field updates on the session.
        """
        session = await self.repository.get_session(payment_id)
        if not session:
            logger.warning(
                "update_status: session not found",
                extra={"ctx_payment_id": payment_id, "ctx_status": new_status.value},
            )
            return False

        allowed = ALLOWED_TRANSITIONS.get(session.status, set())
        if new_status not in allowed:
            logger.warning(
                "update_status: invalid transition",
                extra={
                    "ctx_payment_id": payment_id,
                    "ctx_from": session.status.value,
                    "ctx_to": new_status.value,
                },
            )
            return False

        session.status = new_status
        session.updated_at = datetime.now(timezone.utc)

        for key, value in kwargs.items():
            if hasattr(session, key):
                setattr(session, key, value)
            else:
                logger.debug(
                    "update_status: unknown field ignored",
                    extra={"ctx_field": key},
                )

        await self.repository.save_session(session)
        await self.repository.log_event(
            payment_id,
            f"status_changed_{new_status.value}",
            {k: str(v) for k, v in kwargs.items()},
        )
        return True

    # ── Timeout ───────────────────────────────────────────────────────────────

    async def start_timeout(self, payment_id: str) -> bool:
        """
        Start the 20-minute session timeout.

        MUST be called ONLY after payment details have been successfully
        delivered to the user — never at session creation time.
        """
        session = await self.repository.get_session(payment_id)
        if not session:
            return False

        expires_at = datetime.now(timezone.utc) + timedelta(minutes=SESSION_TIMEOUT_MINUTES)
        session.expires_at = expires_at
        session.updated_at = datetime.now(timezone.utc)
        await self.repository.save_session(session)
        await self.repository.schedule_timeout(
            payment_id, session.user_id, expires_at
        )
        await self.repository.log_event(
            payment_id,
            "timeout_started",
            {"expires_at": expires_at.isoformat(), "timeout_minutes": SESSION_TIMEOUT_MINUTES},
        )
        logger.info(
            "Payment timeout started",
            extra={"ctx_session": payment_id, "ctx_expires_at": expires_at.isoformat()},
        )
        return True

    # ── Approval (atomic) ─────────────────────────────────────────────────────

    async def approve_payment(
        self,
        client: Client,
        payment_id: str,
        admin_id: int,
    ) -> bool:
        """
        Atomically approve a payment:
          1. Acquire processing lock (UNDER_REVIEW → PROCESSING)
          2. Activate subscription
          3. Generate one-time invite link
          4. Deliver invite to user
          5. Persist approval and subscription history

        Returns False if lock cannot be acquired (already handled by another admin).
        """
        # Step 1: Atomic lock — prevents duplicate approval
        lock_acquired = await self.repository.acquire_processing_lock(payment_id)
        if not lock_acquired:
            logger.warning(
                "approve_payment: lock not acquired — already processing",
                extra={"ctx_session": payment_id, "ctx_admin": admin_id},
            )
            return False

        session = await self.repository.get_session(payment_id)
        if not session:
            logger.error(
                "approve_payment: session disappeared after lock",
                extra={"ctx_session": payment_id},
            )
            return False

        plan = PLANS.get(session.plan_id, {})

        try:
            # Step 2: Activate subscription
            sub_service = SubscriptionService()
            subscription = await sub_service.grant(
                user_id=session.user_id,
                plan=Plan.PREMIUM,
                duration_days=plan.get("days", 30),
                granted_by=admin_id,
                notes=f"Payment approved: {payment_id}",
            )

            # Step 3: Generate invite link (single-use, 30-min expiry)
            invite_link = None
            premium_chat_id = (
                getattr(settings, "PREMIUM_GROUP_ID", None)
                or getattr(settings, "PREMIUM_CHANNEL_ID", None)
            )
            if premium_chat_id and int(premium_chat_id) != 0:
                try:
                    now = datetime.now(timezone.utc)
                    invite = await client.create_chat_invite_link(
                        chat_id=int(premium_chat_id),
                        member_limit=1,
                        expire_date=now + timedelta(minutes=30),
                        name=f"payment_{payment_id[:8]}",
                    )
                    invite_link = invite.invite_link
                except Exception as e:
                    logger.error(
                        "approve_payment: failed to generate invite link",
                        extra={"ctx_session": payment_id, "ctx_error": str(e)},
                    )

            # Step 4: Deliver to user
            user_msg = (
                "✅ <b>Payment Approved!</b>\n\n"
                f"📦 Plan: {plan.get('label', session.plan_id)}\n"
                f"💰 Amount: ৳{session.locked_amount:.2f}\n\n"
            )
            if invite_link:
                user_msg += (
                    "🔓 Your premium access is ready.\n\n"
                    "👇 Join using your private invite link:\n"
                    f"<a href='{invite_link}'>JOIN PREMIUM</a>\n\n"
                    "⚠️ This link is for you only. It expires in 30 minutes and works once."
                )
            else:
                user_msg += (
                    "Your subscription is active.\n"
                    "Contact support to receive your invite link."
                )

            await client.send_message(
                session.user_id,
                user_msg,
                parse_mode=ParseMode.HTML,
            )

            # Step 5: Finalise session
            session.status = PaymentStatus.APPROVED
            session.approved_at = datetime.now(timezone.utc)
            session.approved_by = admin_id
            await self.repository.save_session(session)
            await self.repository.clear_timeout(payment_id)
            await self.repository.record_subscription_history(
                payment_id,
                {
                    "user_id": session.user_id,
                    "plan_id": session.plan_id,
                    "locked_amount": session.locked_amount,
                    "subscription_expires_at": (
                        subscription.expires_at.isoformat()
                        if subscription.expires_at else None
                    ),
                    "approved_by": admin_id,
                    "invite_link": invite_link,
                },
            )
            await self.repository.log_event(payment_id, "payment_approved", {"admin_id": admin_id})

            logger.info(
                "Payment approved successfully",
                extra={
                    "ctx_session": payment_id,
                    "ctx_user": session.user_id,
                    "ctx_plan": session.plan_id,
                    "ctx_admin": admin_id,
                },
            )
            return True

        except Exception as e:
            logger.error(
                "approve_payment: activation failed — reverting to under_review",
                extra={"ctx_session": payment_id, "ctx_error": str(e)},
                exc_info=True,
            )
            # Revert to reviewable state so admin can retry
            session.status = PaymentStatus.UNDER_REVIEW
            session.updated_at = datetime.now(timezone.utc)
            await self.repository.save_session(session)
            return False

    # ── Rejection (atomic) ────────────────────────────────────────────────────

    async def reject_payment(
        self,
        payment_id: str,
        reason: str,
        admin_id: int,
    ) -> bool:
        """
        Atomically reject a payment:
          1. Acquire processing lock (UNDER_REVIEW → PROCESSING)
          2. Restore referral points if any were used
          3. Mark as REJECTED
        """
        lock_acquired = await self.repository.acquire_processing_lock(payment_id)
        if not lock_acquired:
            logger.warning(
                "reject_payment: lock not acquired",
                extra={"ctx_session": payment_id, "ctx_admin": admin_id},
            )
            return False

        session = await self.repository.get_session(payment_id)
        if not session:
            return False

        # Restore referral points that were deducted at session creation
        original_price = PLANS.get(session.plan_id, {}).get("price", 0)
        discount_used = int(original_price - session.locked_amount)
        if discount_used > 0:
            try:
                await self.referral_repo.increment_balance(session.user_id, discount_used)
                logger.info(
                    "Referral points restored after rejection",
                    extra={"ctx_user_id": session.user_id, "ctx_points": discount_used},
                )
            except Exception as e:
                logger.error(
                    "Failed to restore referral points on rejection",
                    extra={"ctx_session": payment_id, "ctx_error": str(e)},
                )

        session.status = PaymentStatus.REJECTED
        session.rejection_reason = reason
        session.rejected_at = datetime.now(timezone.utc)
        session.rejected_by = admin_id
        session.updated_at = datetime.now(timezone.utc)
        await self.repository.save_session(session)
        await self.repository.clear_timeout(payment_id)
        await self.repository.log_event(
            payment_id,
            "payment_rejected",
            {"admin_id": admin_id, "reason": reason},
        )

        logger.info(
            "Payment rejected",
            extra={
                "ctx_session": payment_id,
                "ctx_user": session.user_id,
                "ctx_reason": reason,
                "ctx_admin": admin_id,
            },
        )
        return True

    # ── Recovery helpers ──────────────────────────────────────────────────────

    async def get_sessions_for_recovery(self) -> list[PaymentSession]:
        """
        Return all sessions in active states for startup recovery.
        Called by lifecycle.py on_startup to restore timeout tasks.
        """
        active_statuses = [
            PaymentStatus.WAITING_PAYMENT_DETAILS.value,
            PaymentStatus.WAITING_TXID.value,
            PaymentStatus.WAITING_SCREENSHOT.value,
            PaymentStatus.UNDER_REVIEW.value,
            PaymentStatus.PROCESSING.value,
        ]
        docs = await self.repository._collection.find(
            {"status": {"$in": active_statuses}}
        ).to_list(length=None)
        return [PaymentSession.from_dict(d) for d in docs]


# Import needed for send_message in approve_payment
from pyrogram.enums import ParseMode  # noqa: E402