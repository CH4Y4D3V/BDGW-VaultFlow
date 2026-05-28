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

PLANS = {
    "1month":  {"label": "1 Month",  "price": 499,  "days": 30},
    "3months": {"label": "3 Months", "price": 1299, "days": 90},
    "6months": {"label": "6 Months", "price": 2499, "days": 180},
}

SESSION_TIMEOUT_MINUTES = 30

ALLOWED_TRANSITIONS = {
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

    async def create_session(self, user_id: int, plan_id: str) -> PaymentSession:
        if plan_id not in PLANS:
            raise ValueError(f"Invalid plan: {plan_id}")

        plan = PLANS[plan_id]
        base_price = plan["price"]

        # Referral discount: 1 point = ৳1
        wallet = await self.referral_repo.get_wallet(user_id)
        points = wallet.get("points_balance", 0) if wallet else 0
        
        locked_amount = max(0, base_price - points)

        session = PaymentSession(
            id=str(uuid.uuid4()),
            user_id=user_id,
            plan_id=plan_id,
            locked_amount=locked_amount,
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

    async def get_active_session(self, user_id: int) -> Optional[PaymentSession]:
        return await self.repository.get_active_session(user_id)

    async def update_status(self, payment_id: str, status: PaymentStatus, **kwargs) -> bool:
        session = await self.repository.get_session(payment_id)
        if not session:
            return False
        if status not in ALLOWED_TRANSITIONS.get(session.status, set()):
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

    async def start_timeout(self, payment_id: str) -> bool:
        """Start the payment timeout after payment instructions are delivered."""
        session = await self.repository.get_session(payment_id)
        if not session:
            return False

        session.expires_at = datetime.now(timezone.utc) + timedelta(
            minutes=SESSION_TIMEOUT_MINUTES
        )
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
        Approval Sequence:
        1. Atomic lock
        2. Activate subscription (TODO: call subscription_service)
        3. Notify user with invite link
        4. Audit log
        """
        session = await self.repository.get_session(payment_id)
        if not session or session.status != PaymentStatus.UNDER_REVIEW:
            return False

        lock_acquired = await self.repository.acquire_processing_lock(payment_id)
        if not lock_acquired:
            return False

        try:
            plan = PLANS[session.plan_id]
            subscription = await SubscriptionService().grant(
                user_id=session.user_id,
                plan=Plan.PREMIUM,
                duration_days=plan["days"],
                granted_by=admin_id,
                notes=f"Payment approved: {payment_id}",
            )

            invite_link = None
            premium_chat_id = getattr(settings, "PREMIUM_CHANNEL_ID", None) or getattr(
                settings, "PREMIUM_GROUP_ID", None
            )
            if premium_chat_id:
                invite = await client.create_chat_invite_link(
                    chat_id=int(premium_chat_id),
                    member_limit=1,
                )
                invite_link = invite.invite_link

            message = "Payment approved. Your premium access is active."
            if invite_link:
                message += f"\n\nJoin: {invite_link}"
            await client.send_message(session.user_id, message)
            
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
                    "subscription_expires_at": subscription.expires_at,
                    "approved_by": admin_id,
                },
            )
            
            await self.repository.log_event(payment_id, "payment_approved", {"admin_id": admin_id})
            return True
            
        except Exception as e:
            logger.exception("Failed to approve payment", extra={"ctx_payment_id": payment_id, "ctx_error": str(e)})
            # Revert to under_review if activation fails?
            await self.update_status(payment_id, PaymentStatus.UNDER_REVIEW)
            return False

    async def reject_payment(self, payment_id: str, reason: str, admin_id: int) -> bool:
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
