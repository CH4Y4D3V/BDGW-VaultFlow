from __future__ import annotations

import uuid
import random
from datetime import datetime, timedelta, timezone
from typing import Optional

from pyrogram import Client
from pyrogram.enums import ParseMode

from app.config import settings
from app.models.subscription import Plan
from app.payments.models import PaymentSession, PaymentStatus
from app.payments.repository import PaymentRepository
from app.referral.repository import ReferralRepository
from app.services.subscription_service import SubscriptionService
from app.utils.logger import get_logger

logger = get_logger(__name__)

PLANS = {
    "1month":  {"label": "1 Month",  "price": 499,  "days": 30},
    "3months": {"label": "3 Months", "price": 1299, "days": 90},
    "6months": {"label": "6 Months", "price": 2499, "days": 180},
}

SESSION_TIMEOUT_MINUTES = 20

# --- GAP 1 FIX: Correct FSM transitions ---
ALLOWED_TRANSITIONS = {
    PaymentStatus.WAITING_PAYMENT_DETAILS: {
        PaymentStatus.REQUESTED,
        PaymentStatus.CANCELLED,
        PaymentStatus.EXPIRED,
    },
    PaymentStatus.REQUESTED: {
        PaymentStatus.PENDING_DETAILS,
        PaymentStatus.CANCELLED,
        PaymentStatus.EXPIRED,
    },
    PaymentStatus.PENDING_DETAILS: {
        PaymentStatus.AWAITING_PAYMENT,
        PaymentStatus.REQUESTED,  # Allow cancel send
        PaymentStatus.CANCELLED,
        PaymentStatus.EXPIRED,
    },
    PaymentStatus.AWAITING_PAYMENT: {
        PaymentStatus.WAITING_SCREENSHOT,
        PaymentStatus.CANCELLED,
        PaymentStatus.EXPIRED,
    },
    PaymentStatus.WAITING_SCREENSHOT: {
        PaymentStatus.SUBMITTED,
        PaymentStatus.CANCELLED,
        PaymentStatus.EXPIRED,
    },
    PaymentStatus.SUBMITTED: {
        PaymentStatus.UNDER_REVIEW,
        PaymentStatus.CANCELLED,
    },
    PaymentStatus.UNDER_REVIEW: {
        PaymentStatus.PROCESSING,
        PaymentStatus.REJECTED,
    },
    PaymentStatus.PROCESSING: {
        PaymentStatus.APPROVED,
        PaymentStatus.REJECTED,
        PaymentStatus.UNDER_REVIEW,
    },
}


class PaymentService:
    def __init__(self, repository: PaymentRepository, referral_repo: ReferralRepository):
        self.repository = repository
        self.referral_repo = referral_repo

    async def create_session(
        self,
        user_id: int,
        plan_id: str,
        method: Optional[str] = None,
    ) -> PaymentSession:
        if plan_id not in PLANS:
            raise ValueError(f"Invalid plan: {plan_id}")

        plan = PLANS[plan_id]
        base_price = plan["price"]

        # Referral discount: 1 point = ৳1
        wallet = await self.referral_repo.get_wallet(user_id)
        points = wallet.get("points_balance", 0) if wallet else 0

        # ── SYSTEM 14: SNAPSHOT & LOCK POINTS ──
        if points > 0:
            from app.referral.service import ReferralService
            # We use the repository directly to deduct to avoid circular imports
            await self.referral_repo.deduct_points(user_id, points)
            logger.info("points_locked_for_session", extra={"ctx_user_id": user_id, "ctx_points": points})

        base_payable = max(0, base_price - points)

        # Unique identifying offset: ৳0.01 to ৳0.50
        offset = round(random.uniform(0.01, 0.50), 2)
        locked_amount = float(base_payable) + offset

        session = PaymentSession(
            id=str(uuid.uuid4()),
            user_id=user_id,
            plan_id=plan_id,
            locked_amount=locked_amount,
            points_used=points,
            payment_method=method,
        )

        await self.repository.save_session(session)
        await self.repository.log_event(session.id, "session_created", {
            "base_price": base_price,
            "points_used": points,
            "locked_amount": locked_amount
        })

        return session

    async def get_session(self, payment_id: str) -> Optional[PaymentSession]:
        return await self.repository.get_session(payment_id)

    async def check_txid_unique(self, txid: str) -> bool:
        """
        FIX 9: Verify TxID is not already in use.
        """
        try:
            from app.repositories.txid_repository import TXIDRepository
            from app.core.database import DatabaseManager
            repo = TXIDRepository(DatabaseManager.get_db())
            existing = await repo.get_by_txid(txid)
            return existing is None
        except Exception as e:
            logger.error("txid_uniqueness_check_failed", extra={"ctx_error": str(e)})
            return True # Fail open to allow payment but log error

    async def get_active_session(self, user_id: int) -> Optional[PaymentSession]:
        return await self.repository.get_active_session(user_id)

    async def update_status(self, payment_id: str, status: PaymentStatus, **kwargs) -> bool:
        session = await self.repository.get_session(payment_id)
        if not session:
            return False

        # --- GAP 1: FSM VALIDATION ---
        if status not in ALLOWED_TRANSITIONS.get(session.status, set()) and status != session.status:
            logger.warning(
                "payment_state_transition_rejected",
                extra={
                    "ctx_payment_id": payment_id,
                    "ctx_from": session.status.value,
                    "ctx_to": status.value,
                },
            )
            return False

        session.status = status
        session.updated_at = datetime.now(timezone.utc)

        for key, value in kwargs.items():
            if hasattr(session, key):
                setattr(session, key, value)

        await self.repository.save_session(session)
        await self.repository.log_event(payment_id, f"status_changed_{status.value}", kwargs)
        return True

    async def start_timeout(self, payment_id: str, remaining_seconds: Optional[int] = None) -> bool:
        """Start or resume the payment timeout."""
        session = await self.repository.get_session(payment_id)
        if not session:
            return False

        if remaining_seconds is not None:
            expires_at = datetime.now(timezone.utc) + timedelta(seconds=remaining_seconds)
        else:
            expires_at = datetime.now(timezone.utc) + timedelta(minutes=SESSION_TIMEOUT_MINUTES)

        session.expires_at = expires_at
        session.updated_at = datetime.now(timezone.utc)

        await self.repository.save_session(session)
        await self.repository.schedule_timeout(payment_id, session.user_id, session.expires_at)
        await self.repository.log_event(
            payment_id,
            "timeout_started",
            {"expires_at": session.expires_at},
        )
        return True

    async def approve_payment(self, client: Client, payment_id: str, admin_id: int) -> bool:
        """
        --- 7.5 SUBSCRIPTION ACTIVATION (ATOMIC) ---
        """
        # Step 3: Acquire distributed lock
        lock_acquired = await self.repository.acquire_processing_lock(payment_id)
        if not lock_acquired:
            return False

        try:
            # Step 1 & 2: Reload and validate state
            session = await self.repository.get_session(payment_id)
            if not session or session.status in (PaymentStatus.APPROVED, PaymentStatus.REJECTED):
                return False

            # Step 4: Activate subscription
            plan = PLANS[session.plan_id]
            subscription = await SubscriptionService().grant(
                user_id=session.user_id,
                plan=Plan.PREMIUM,
                duration_days=plan["days"],
                granted_by=admin_id,
                notes=f"Payment approved: {payment_id}",
            )

            # Step 5: Generate unique one-time invite link (30 min expiry)
            from app.services.invite_service import InviteService
            invite_service = InviteService()

            invite_link = None
            premium_chat_id = getattr(settings, "PREMIUM_CHANNEL_ID", None) or getattr(
                settings, "PREMIUM_GROUP_ID", None
            )

            if premium_chat_id:
                try:
                    invite_obj = await invite_service.generate_premium_invite(
                        client=client,
                        user_id=session.user_id,
                        chat_id=int(premium_chat_id),
                        granted_by=admin_id,
                        plan=session.plan_id
                    )
                    invite_link = invite_obj.telegram_link
                except Exception as e:
                    logger.warning("failed_to_generate_invite_during_activation", extra={"ctx_error": str(e)})

            # Step 7: Save payment history
            await self.repository.record_subscription_history(
                payment_id,
                {
                    "user_id": session.user_id,
                    "plan_id": session.plan_id,
                    "locked_amount": session.locked_amount,
                    "subscription_expires_at": subscription.expires_at,
                    "approved_by": admin_id,
                    "invite_link": invite_link
                },
            )

            # Notify user
            message = "✅ <b>Payment Approved!</b>\n\nYour premium access has been activated."
            if invite_link:
                message += f"\n\n🔗 <b>Your unique invite link:</b>\n{invite_link}\n\n<i>This link expires in 30 minutes and can only be used once.</i>"

            await client.send_message(session.user_id, message, parse_mode=ParseMode.HTML)

            # Step 9: Clear session
            session.status = PaymentStatus.APPROVED
            session.approved_at = datetime.now(timezone.utc)
            session.approved_by = admin_id
            await self.repository.save_session(session)
            await self.repository.clear_timeout(payment_id)

            await self.repository.log_event(payment_id, "payment_approved", {"admin_id": admin_id})
            return True

        except Exception as e:
            logger.exception("Atomic approval failed", extra={"ctx_payment_id": payment_id, "ctx_error": str(e)})
            await self.update_status(payment_id, PaymentStatus.UNDER_REVIEW)
            return False

    async def reject_payment(self, payment_id: str, reason: str, admin_id: int) -> bool:
        # Bypass lock if coming from fraud check or other internal path
        # But for admin manual reject, we want the lock
        session = await self.repository.get_session(payment_id)
        if not session: return False

        if session.status == PaymentStatus.UNDER_REVIEW:
            lock_acquired = await self.repository.acquire_processing_lock(payment_id)
            if not lock_acquired:
                return False

        await self.update_status(
            payment_id,
            PaymentStatus.REJECTED,
            rejection_reason=reason,
            rejected_at=datetime.now(timezone.utc),
            rejected_by=admin_id
        )
        return True

    async def resume_active_sessions(self) -> None:
        try:
            reset_count = await self.repository.reset_stuck_processing()
            if reset_count:
                logger.info(
                    "resume_active_sessions: recovered stuck PROCESSING sessions",
                    extra={"ctx_count": reset_count},
                )

            active_statuses = [
                PaymentStatus.AWAITING_PAYMENT.value,
                PaymentStatus.WAITING_SCREENSHOT.value,
                PaymentStatus.UNDER_REVIEW.value,
            ]
            docs = await self.repository._collection.find(
                {"status": {"$in": active_statuses}}
            ).to_list(length=None)
            active_sessions = [PaymentSession.from_dict(d) for d in docs]

            now = datetime.now(timezone.utc)
            for session in active_sessions:
                if not session.expires_at:
                    await self.start_timeout(session.id)
                    continue
                expires = session.expires_at
                if expires.tzinfo is None:
                    expires = expires.replace(tzinfo=timezone.utc)
                remaining = (expires - now).total_seconds()
                if remaining > 0:
                    await self.start_timeout(session.id, remaining_seconds=int(remaining))
                    logger.info(
                        "Resumed payment session timer",
                        extra={"ctx_payment_id": session.id, "ctx_remaining": int(remaining)},
                    )
                else:
                    await self.update_status(session.id, PaymentStatus.EXPIRED)
                    logger.info(
                        "Closed expired session on startup",
                        extra={"ctx_payment_id": session.id},
                    )

        except Exception as e:
            logger.error(
                "Failed to resume active sessions",
                extra={"ctx_error": str(e)},
            )
