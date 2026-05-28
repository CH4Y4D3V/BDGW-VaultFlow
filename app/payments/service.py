from __future__ import annotations

import uuid
from datetime import datetime, timedelta, timezone
from typing import Optional

from pyrogram import Client

from app.config import settings
from app.payments.models import PaymentSession, PaymentStatus
from app.payments.repository import PaymentRepository
from app.referral.repository import ReferralRepository
from app.utils.logger import get_logger

logger = get_logger(__name__)

PLANS = {
    "1month":  {"label": "1 Month",  "price": 499,  "days": 30},
    "3months": {"label": "3 Months", "price": 1299, "days": 90},
    "6months": {"label": "6 Months", "price": 2499, "days": 180},
}

SESSION_TIMEOUT_MINUTES = 30


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
            expires_at=datetime.now(timezone.utc) + timedelta(minutes=SESSION_TIMEOUT_MINUTES)
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

        session.status = status
        session.updated_at = datetime.now(timezone.utc)
        
        for key, value in kwargs.items():
            if hasattr(session, key):
                setattr(session, key, value)

        await self.repository.save_session(session)
        await self.repository.log_event(payment_id, f"status_changed_{status.value}", kwargs)
        return True

    async def approve_payment(self, client: Client, payment_id: str, admin_id: int) -> bool:
        """
        Approval Sequence:
        1. Atomic lock
        2. Activate subscription (TODO: call subscription_service)
        3. Notify user with invite link
        4. Audit log
        """
        lock_acquired = await self.repository.acquire_processing_lock(payment_id)
        if not lock_acquired:
            return False

        session = await self.repository.get_session(payment_id)
        if not session:
            return False

        try:
            # TODO: Subscription activation logic here
            # In Phase 7 we will link this to subscription_service
            
            session.status = PaymentStatus.APPROVED
            session.approved_at = datetime.now(timezone.utc)
            session.approved_by = admin_id
            await self.repository.save_session(session)
            
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
