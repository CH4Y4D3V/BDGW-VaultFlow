from __future__ import annotations

import random
import uuid
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

# ── FSM transition whitelist ──────────────────────────────────────────────────
# Each key = current status; value = set of allowed next statuses.
ALLOWED_TRANSITIONS: dict[PaymentStatus, set[PaymentStatus]] = {
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
        PaymentStatus.CANCELLED,
        PaymentStatus.EXPIRED,
        PaymentStatus.REQUESTED,  # admin cancels "send details" flow
    },
    PaymentStatus.AWAITING_PAYMENT: {
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
        PaymentStatus.UNDER_REVIEW,  # unlock on failed atomic approve
    },
}


class PaymentService:
    def __init__(self, repository: PaymentRepository, referral_repo: ReferralRepository):
        self.repository = repository
        self.referral_repo = referral_repo

    # ── Session creation ──────────────────────────────────────────────────────

    async def create_session(self, user_id: int, plan_id: str) -> PaymentSession:
        if plan_id not in PLANS:
            raise ValueError(f"Invalid plan: {plan_id}")

        plan = PLANS[plan_id]
        base_price = plan["price"]

        # Referral discount: 1 point = ৳1
        wallet = await self.referral_repo.get_wallet(user_id)
        points = wallet.get("points_balance", 0) if wallet else 0

        # FIX: Use referral_repo directly — snapshot and deduct atomically
        if points > 0:
            success = await self.referral_repo.deduct_points(user_id, points)
            if not success:
                # Concurrent spend — fetch fresh balance
                wallet = await self.referral_repo.get_wallet(user_id)
                points = wallet.get("points_balance", 0) if wallet else 0
                if points > 0:
                    await self.referral_repo.deduct_points(user_id, points)
                else:
                    points = 0
            logger.info(
                "points_locked_for_session",
                extra={"ctx_user_id": user_id, "ctx_points": points},
            )

        base_payable = max(0, base_price - points)

        # Unique identifying offset ৳0.01–0.50 so operator can match transfer amount
        offset = round(random.uniform(0.01, 0.50), 2)
        locked_amount = float(base_payable) + offset

        session = PaymentSession(
            id=str(uuid.uuid4()),
            user_id=user_id,
            plan_id=plan_id,
            locked_amount=locked_amount,
            points_used=points,
        )

        await self.repository.save_session(session)
        await self.repository.log_event(
            session.id,
            "session_created",
            {
                "base_price": base_price,
                "points_used": points,
                "locked_amount": locked_amount,
            },
        )

        # Fast-path Redis cache: mark user has active session
        try:
            from app.core.redis_client import get_redis
            redis = get_redis()
            await redis.set(
                f"pay_session:{user_id}",
                session.id,
                ex=SESSION_TIMEOUT_MINUTES * 60 + 120,  # slight buffer
            )
        except Exception as e:
            logger.warning("payment_session_redis_cache_failed", extra={"ctx_error": str(e)})

        return session

    # ── Reads ─────────────────────────────────────────────────────────────────

    async def get_session(self, payment_id: str) -> Optional[PaymentSession]:
        return await self.repository.get_session(payment_id)

    async def get_active_session(self, user_id: int) -> Optional[PaymentSession]:
        return await self.repository.get_active_session(user_id)

    # ── Status transitions ────────────────────────────────────────────────────

    async def update_status(
        self, payment_id: str, status: PaymentStatus, **kwargs
    ) -> bool:
        session = await self.repository.get_session(payment_id)
        if not session:
            return False

        allowed = ALLOWED_TRANSITIONS.get(session.status, set())
        if status not in allowed:
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
        await self.repository.log_event(
            payment_id, f"status_changed_{status.value}", kwargs
        )
        return True

    # ── Timeout management ────────────────────────────────────────────────────

    async def start_timeout(self, payment_id: str) -> bool:
        """
        Start the 20-minute payment timeout after payment instructions are delivered.
        CRITICAL: Only called AFTER confirmed Telegram delivery (Section 7.3).
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
            {"expires_at": expires_at.isoformat()},
        )

        # Update Redis TTL
        try:
            from app.core.redis_client import get_redis
            redis = get_redis()
            remaining_seconds = int((expires_at - datetime.now(timezone.utc)).total_seconds())
            if remaining_seconds > 0:
                await redis.set(
                    f"pay_session:{session.user_id}",
                    payment_id,
                    ex=remaining_seconds + 120,
                )
        except Exception as e:
            logger.warning("payment_timeout_redis_update_failed", extra={"ctx_error": str(e)})

        return True

    async def resume_timeout_from_db(self, payment_id: str) -> bool:
        """
        FIX (GAP 8 — Restart Safety): Resume timer using REMAINING time from
        session.expires_at rather than starting a fresh 20-minute timer.
        Called by resume_active_sessions() on bot restart.
        """
        session = await self.repository.get_session(payment_id)
        if not session or not session.expires_at:
            return False

        now = datetime.now(timezone.utc)
        remaining_seconds = (session.expires_at - now).total_seconds()

        if remaining_seconds <= 0:
            # Already expired while bot was down — expire now
            await self._force_expire_session(session)
            return False

        # Reschedule the timeout record to fire at the original expires_at
        await self.repository.schedule_timeout(
            payment_id, session.user_id, session.expires_at
        )

        # Update Redis with remaining TTL
        try:
            from app.core.redis_client import get_redis
            redis = get_redis()
            await redis.set(
                f"pay_session:{session.user_id}",
                payment_id,
                ex=int(remaining_seconds) + 120,
            )
        except Exception as e:
            logger.warning("payment_resume_redis_failed", extra={"ctx_error": str(e)})

        logger.info(
            "payment_session_timeout_resumed",
            extra={
                "ctx_payment_id": payment_id,
                "ctx_remaining_seconds": int(remaining_seconds),
            },
        )
        return True

    async def _force_expire_session(self, session: PaymentSession) -> None:
        """Expire a session that ran out while bot was offline."""
        session.status = PaymentStatus.EXPIRED
        session.updated_at = datetime.now(timezone.utc)
        await self.repository.save_session(session)
        await self.repository.clear_timeout(session.id)
        await self.repository.log_event(session.id, "expired_on_restart", {})
        logger.warning(
            "payment_session_expired_on_restart",
            extra={"ctx_payment_id": session.id, "ctx_user_id": session.user_id},
        )

    # ── Approval (Atomic 9-step per Section 7.5) ──────────────────────────────

    async def approve_payment(
        self, client: Client, payment_id: str, admin_id: int
    ) -> bool:
        """
        Atomic 9-step subscription activation per Section 7.5:
          1. Validate session is active
          2. Validate not already processed (idempotency)
          3. Acquire distributed lock
          4. Activate subscription in DB
          5. Generate unique one-time invite link (30-min expiry)
          6. Save invite metadata
          7. Save payment history
          8. Save invite history (covered by InviteService)
          9. Clear session
        """
        # Step 3: Acquire distributed lock (also transitions to PROCESSING)
        lock_acquired = await self.repository.acquire_processing_lock(payment_id)
        if not lock_acquired:
            logger.warning(
                "approve_payment_lock_failed",
                extra={"ctx_payment_id": payment_id},
            )
            return False

        try:
            # Steps 1 & 2: Reload and validate
            session = await self.repository.get_session(payment_id)
            if not session:
                return False
            if session.status in (PaymentStatus.APPROVED, PaymentStatus.REJECTED):
                logger.warning(
                    "approve_payment_already_finalized",
                    extra={
                        "ctx_payment_id": payment_id,
                        "ctx_status": session.status.value,
                    },
                )
                return False
            if session.status != PaymentStatus.PROCESSING:
                # lock acquisition moved it to PROCESSING; if not, something is wrong
                logger.error(
                    "approve_payment_unexpected_status",
                    extra={
                        "ctx_payment_id": payment_id,
                        "ctx_status": session.status.value,
                    },
                )
                return False

            # Step 4: Activate subscription
            plan_config = PLANS[session.plan_id]
            subscription_service = SubscriptionService()
            subscription = await subscription_service.grant(
                user_id=session.user_id,
                plan=Plan.PREMIUM,
                duration_days=plan_config["days"],
                granted_by=admin_id,
                notes=f"Payment approved: {payment_id}",
            )

            # Steps 5 & 6: Generate unique invite link
            invite_link: Optional[str] = None
            premium_chat_id = (
                getattr(settings, "PREMIUM_CHANNEL_ID", None)
                or getattr(settings, "PREMIUM_GROUP_ID", None)
            )

            if premium_chat_id:
                try:
                    from app.services.invite_service import InviteService
                    invite_service = InviteService()
                    invite_obj = await invite_service.generate_premium_invite(
                        client=client,
                        user_id=session.user_id,
                        chat_id=int(premium_chat_id),
                        granted_by=admin_id,
                        plan=session.plan_id,
                    )
                    invite_link = invite_obj.telegram_link
                except Exception as e:
                    logger.warning(
                        "approve_payment_invite_failed",
                        extra={"ctx_payment_id": payment_id, "ctx_error": str(e)},
                    )

            # Step 7: Save payment history and TXID registry
            if session.txid:
                try:
                    from app.repositories.txid_repository import TXIDRepository
                    txid_repo = TXIDRepository(self.repository._db)
                    await txid_repo.register(session.txid, session.user_id, session.id)
                except Exception as tx_err:
                    logger.warning(
                        "txid_registration_failed_on_approve",
                        extra={"ctx_txid": session.txid, "ctx_error": str(tx_err)},
                    )

            await self.repository.record_subscription_history(
                payment_id,
                {
                    "user_id": session.user_id,
                    "plan_id": session.plan_id,
                    "locked_amount": session.locked_amount,
                    "subscription_expires_at": subscription.expires_at,
                    "approved_by": admin_id,
                    "invite_link": invite_link,
                },
            )

            # Notify user
            expiry_str = (
                subscription.expires_at.strftime("%Y-%m-%d")
                if subscription.expires_at
                else "Lifetime"
            )
            message_text = (
                "✅ <b>Payment Approved!</b>\n\n"
                f"Your <b>{plan_config['label']}</b> subscription is now active.\n"
                f"📅 Expires: <code>{expiry_str}</code>"
            )
            if invite_link:
                message_text += (
                    f"\n\n🔗 <b>Your exclusive invite link:</b>\n{invite_link}\n\n"
                    "<i>⚠️ This link expires in 30 minutes and can only be used once.</i>"
                )
            else:
                message_text += (
                    "\n\n⚠️ Invite link generation failed — please contact support "
                    "and we'll send you one immediately."
                )

            try:
                await client.send_message(
                    chat_id=session.user_id,
                    text=message_text,
                    parse_mode=ParseMode.HTML,
                )
            except Exception as e:
                logger.warning(
                    "approve_payment_notify_user_failed",
                    extra={"ctx_user_id": session.user_id, "ctx_error": str(e)},
                )

            # Step 9: Clear session — mark as APPROVED
            session.status = PaymentStatus.APPROVED
            session.approved_at = datetime.now(timezone.utc)
            session.approved_by = admin_id
            session.updated_at = datetime.now(timezone.utc)
            await self.repository.save_session(session)
            await self.repository.clear_timeout(payment_id)
            await self.repository.log_event(
                payment_id, "payment_approved", {"admin_id": admin_id}
            )

            # Clear Redis fast-path
            try:
                from app.core.redis_client import get_redis
                redis = get_redis()
                await redis.delete(f"pay_session:{session.user_id}")
            except Exception:
                pass

            logger.info(
                "payment_approved_successfully",
                extra={"ctx_payment_id": payment_id, "ctx_user_id": session.user_id},
            )
            return True

        except Exception as e:
            logger.exception(
                "atomic_approval_failed",
                extra={"ctx_payment_id": payment_id, "ctx_error": str(e)},
            )
            # Unlock: revert to UNDER_REVIEW so admin can retry
            try:
                session = await self.repository.get_session(payment_id)
                if session and session.status == PaymentStatus.PROCESSING:
                    session.status = PaymentStatus.UNDER_REVIEW
                    session.updated_at = datetime.now(timezone.utc)
                    await self.repository.save_session(session)
            except Exception as revert_err:
                logger.error(
                    "approve_payment_revert_failed",
                    extra={"ctx_error": str(revert_err)},
                )
            return False

    # ── Rejection ─────────────────────────────────────────────────────────────

    async def reject_payment(
        self, payment_id: str, reason: str, admin_id: int, admin_name: Optional[str] = None
    ) -> bool:
        """
        Reject a payment.
        """
        session = await self.repository.get_session(payment_id)
        if not session:
            return False

        # ... (rest of logic will be updated in smaller blocks or full replacement if needed)

        # For TXID fraud path — session may be in AWAITING_PAYMENT
        if session.status == PaymentStatus.AWAITING_PAYMENT:
            # Force-update directly without going through normal FSM
            session.status = PaymentStatus.REJECTED
            session.rejection_reason = reason
            session.rejected_at = datetime.now(timezone.utc)
            session.rejected_by = admin_id
            session.updated_at = datetime.now(timezone.utc)
            await self.repository.save_session(session)
            await self.repository.clear_timeout(payment_id)
            await self.repository.log_event(
                payment_id,
                "payment_rejected_fraud",
                {"reason": reason, "admin_id": admin_id, "admin_name": admin_name},
            )

            from app.services.audit_service import get_audit, AuditAction
            await get_audit().log(
                action=AuditAction.REJECT,
                performed_by=admin_id,
                performed_by_name=admin_name,
                target_user_id=session.user_id,
                details={"payment_id": payment_id, "reason": reason, "path": "fraud_txid"}
            )
            # Clear Redis
            try:
                from app.core.redis_client import get_redis
                redis = get_redis()
                await redis.delete(f"pay_session:{session.user_id}")
            except Exception:
                pass
            return True

        # Normal rejection path — requires processing lock
        lock_acquired = await self.repository.acquire_processing_lock(payment_id)
        if not lock_acquired:
            return False

        session = await self.repository.get_session(payment_id)
        if not session or session.status != PaymentStatus.PROCESSING:
            return False

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
            {"reason": reason, "admin_id": admin_id},
        )

        # Refund points
        if session.points_used > 0:
            try:
                await self.referral_repo.increment_balance(
                    session.user_id, session.points_used
                )
                logger.info(
                    "points_refunded_on_rejection",
                    extra={
                        "ctx_user_id": session.user_id,
                        "ctx_points": session.points_used,
                    },
                )
            except Exception as e:
                logger.error(
                    "points_refund_failed",
                    extra={"ctx_user_id": session.user_id, "ctx_error": str(e)},
                )

        # Clear Redis
        try:
            from app.core.redis_client import get_redis
            redis = get_redis()
            await redis.delete(f"pay_session:{session.user_id}")
        except Exception:
            pass

        return True

    # ── Restart recovery ──────────────────────────────────────────────────────

    async def resume_active_sessions(self) -> None:
        """
        FIX (GAP 8 — Section 25 Restart Safety):
        On bot startup, restore timers for all active payment sessions using
        REMAINING time from session.expires_at, NOT a fresh 20-minute window.
        Sessions that expired while bot was offline are immediately expired.
        """
        try:
            active_statuses = [
                PaymentStatus.AWAITING_PAYMENT,
                PaymentStatus.WAITING_SCREENSHOT,
                PaymentStatus.UNDER_REVIEW,
                PaymentStatus.PENDING_DETAILS,
            ]
            active_sessions = await self.repository.get_sessions_by_status(
                active_statuses
            )

            resumed = 0
            expired_offline = 0

            for session in active_sessions:
                if session.expires_at is None:
                    # No timer set yet (e.g. still waiting for admin to send details)
                    continue

                success = await self.resume_timeout_from_db(session.id)
                if success:
                    resumed += 1
                else:
                    expired_offline += 1

            logger.info(
                "payment_sessions_resumed_on_startup",
                extra={"ctx_resumed": resumed, "ctx_expired_offline": expired_offline},
            )

        except Exception as e:
            logger.error(
                "resume_active_sessions_failed",
                extra={"ctx_error": str(e)},
            )
