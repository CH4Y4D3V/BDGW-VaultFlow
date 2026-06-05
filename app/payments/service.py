from __future__ import annotations

import asyncio
import uuid
import random
from datetime import datetime, timedelta, timezone
from typing import Optional

from pyrogram import Client
from pyrogram.enums import ParseMode
from pyrogram.errors import FloodWait

from app.config import settings
from app.core.database import DatabaseManager  # FIX: was missing, caused NameError in reject_payment
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

_MAX_FLOOD_WAIT_SECONDS = 60

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


async def _send_safe(
    client: Client,
    chat_id: int,
    text: str,
    parse_mode: ParseMode = ParseMode.HTML,
) -> bool:
    """
    Send a Telegram message with explicit FloodWait handling and a single capped retry.
    Returns True on success, False on any failure.
    Used internally by PaymentService to avoid duplicating FloodWait boilerplate.
    """
    try:
        await client.send_message(chat_id=chat_id, text=text, parse_mode=parse_mode)
        return True
    except FloodWait as exc:
        wait = min(exc.value, _MAX_FLOOD_WAIT_SECONDS)
        logger.warning(
            "FloodWait %ds on send_message to chat %d — sleeping.",
            wait, chat_id,
        )
        await asyncio.sleep(wait)
        try:
            await client.send_message(chat_id=chat_id, text=text, parse_mode=parse_mode)
            return True
        except Exception as retry_exc:
            logger.warning(
                "Retry after FloodWait failed for chat %d: %s", chat_id, retry_exc
            )
            return False
    except Exception as exc:
        logger.warning("send_message failed for chat %d: %s", chat_id, exc)
        return False


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
        """
        Create a new payment session for the given user and plan.
        Referral points (if any) are snapshotted as a discount and deducted
        AFTER the session record is persisted, satisfying restart-safety:
        if the deduction fails, the session document exists with points_used > 0
        and can be reconciled on recovery.

        BUG FIX: Original deducted points BEFORE saving the session. If save_session
        raised, points were permanently lost with no recovery record. Fixed by
        reversing the order.
        """
        if plan_id not in PLANS:
            raise ValueError(f"Invalid plan: {plan_id}")

        plan = PLANS[plan_id]
        base_price = plan["price"]

        # Snapshot referral balance — read only, no mutation yet
        wallet = await self.referral_repo.get_wallet(user_id)
        points = wallet.get("points_balance", 0) if wallet else 0

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

        # FIX: Persist the session BEFORE deducting points (restart-safety)
        await self.repository.save_session(session)
        await self.repository.log_event(session.id, "session_created", {
            "base_price": base_price,
            "points_used": points,
            "locked_amount": locked_amount
        })

        # Deduct points after session is safely persisted
        if points > 0:
            try:
                await self.referral_repo.deduct_points(user_id, points)
                logger.info(
                    "points_locked_for_session",
                    extra={"ctx_user_id": user_id, "ctx_points": points},
                )
            except Exception as exc:
                # Session exists with points_used > 0 — recovery job must reconcile.
                logger.error(
                    "points_deduction_failed_after_session_created",
                    extra={
                        "ctx_user_id": user_id,
                        "ctx_points": points,
                        "ctx_session_id": session.id,
                        "ctx_error": str(exc),
                    },
                )

        return session

    async def get_session(self, payment_id: str) -> Optional[PaymentSession]:
        """Fetch a payment session by its ID. Returns None if not found."""
        return await self.repository.get_session(payment_id)

    async def check_txid_unique(self, txid: str) -> bool:
        """
        Verify that a TXID is not already associated with a submitted payment.
        Returns True if the TXID is unused, False if a duplicate is detected.
        Fails open (returns True) on database error, but logs the failure.
        """
        try:
            from app.repositories.txid_repository import TXIDRepository
            repo = TXIDRepository(DatabaseManager.get_db())
            existing = await repo.get_by_txid(txid)
            return existing is None
        except Exception as e:
            logger.error("txid_uniqueness_check_failed", extra={"ctx_error": str(e)})
            return True  # Fail open to allow payment but log error

    async def get_active_session(self, user_id: int) -> Optional[PaymentSession]:
        """Fetch the current active (non-terminal) session for a user."""
        return await self.repository.get_active_session(user_id)

    async def update_status(self, payment_id: str, status: PaymentStatus, **kwargs) -> bool:
        """
        Transition a payment session to a new status, enforcing the FSM.
        Additional fields can be set via keyword arguments (must be valid session attributes).
        Returns False if the transition is not permitted by ALLOWED_TRANSITIONS or if
        the session does not exist.
        """
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
        """
        Start or resume the payment session timeout.
        If remaining_seconds is provided, the timer starts from that remaining window.
        Otherwise the full SESSION_TIMEOUT_MINUTES window is used.
        Returns False if the session does not exist.
        """
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
        Atomic subscription activation flow (Spec Section 7.5).

        Steps:
          1. Acquire distributed processing lock (prevents concurrent approval).
          2. Reload and validate session state.
          3. Activate subscription via SubscriptionService.
          4. Generate one-time premium invite link.
          5. Persist payment history record.
          6. Notify user via DM (FloodWait-safe).
          7. Mark session APPROVED and clear timeout.
          8. Route notification to user's Verification Hub topic.
          9. Emit to Admin Logs topic.
         10. Release lock in finally block.

        Returns True on success, False if lock not acquired, session invalid, or
        an unrecoverable error occurs (session rolled back to UNDER_REVIEW).

        BUG FIX: session.currency replaced with getattr(session, 'currency', 'BDT')
        to prevent AttributeError — PaymentSession has no currency field.
        BUG FIX: Added FloodWait handling on user DM send.
        BUG FIX: Lock is now released in a finally block.
        """
        lock_acquired = await self.repository.acquire_processing_lock(payment_id)
        if not lock_acquired:
            return False

        try:
            session = await self.repository.get_session(payment_id)
            if not session or session.status in (PaymentStatus.APPROVED, PaymentStatus.REJECTED):
                return False

            plan = PLANS[session.plan_id]
            currency = getattr(session, "currency", "BDT")  # FIX: was session.currency → AttributeError

            # Activate subscription
            subscription = await SubscriptionService().grant(
                user_id=session.user_id,
                plan=Plan.PREMIUM,
                duration_days=plan["days"],
                granted_by=admin_id,
                notes=f"Payment approved: {payment_id}",
            )

            # Generate unique one-time invite link
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
                    logger.warning(
                        "failed_to_generate_invite_during_activation",
                        extra={"ctx_error": str(e)},
                    )

            # Persist payment history
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

            # Notify user via DM (FloodWait-safe)
            message = "✅ <b>Payment Approved!</b>\n\nYour premium access has been activated."
            if invite_link:
                message += (
                    f"\n\n🔗 <b>Your unique invite link:</b>\n{invite_link}\n\n"
                    "<i>This link expires in 30 minutes and can only be used once.</i>"
                )

            await _send_safe(client, session.user_id, message)

            # Mark session APPROVED and clear timeout
            session.status = PaymentStatus.APPROVED
            session.approved_at = datetime.now(timezone.utc)
            session.approved_by = admin_id
            await self.repository.save_session(session)
            await self.repository.clear_timeout(payment_id)
            await self.repository.log_event(payment_id, "payment_approved", {"admin_id": admin_id})

            # Route to user's Verification Hub topic
            try:
                from app.services.topic_manager import get_topic_manager
                topic_id = await get_topic_manager().get_or_create_user_topic(client, session.user_id)
                await client.send_message(
                    chat_id=settings.VERIFICATION_GROUP_ID,
                    text=(
                        f"✅ <b>PAYMENT APPROVED</b>\n\n"
                        f"<b>Amount:</b> {session.locked_amount} {currency}\n"
                        f"<b>Plan:</b> {plan['label']}\n"
                        f"<b>Admin:</b> {admin_id}"
                    ),
                    message_thread_id=topic_id,
                    parse_mode=ParseMode.HTML,
                )
            except Exception as exc:
                logger.warning(
                    "approve_payment_topic_routing_failed",
                    extra={"ctx_payment_id": payment_id, "ctx_error": str(exc)},
                )

            # Emit to Admin Logs topic
            try:
                from app.services.admin_logger import get_admin_logger
                await get_admin_logger().log(
                    client=client,
                    action="PAYMENT APPROVED",
                    admin_id=admin_id,
                    admin_name=f"Admin {admin_id}",
                    target_user_id=session.user_id,
                    details=f"Plan: {plan['label']}\nAmount: {session.locked_amount} {currency}"
                )
            except Exception as exc:
                logger.warning(
                    "approve_payment_admin_log_failed",
                    extra={"ctx_payment_id": payment_id, "ctx_error": str(exc)},
                )

            return True

        except Exception as e:
            logger.exception(
                "Atomic approval failed",
                extra={"ctx_payment_id": payment_id, "ctx_error": str(e)},
            )
            await self.update_status(payment_id, PaymentStatus.UNDER_REVIEW)
            return False

        finally:
            # FIX: Always release the processing lock, whether approval succeeded or failed.
            try:
                await self.repository.release_processing_lock(payment_id)
            except Exception as exc:
                logger.warning(
                    "approve_payment_lock_release_failed",
                    extra={"ctx_payment_id": payment_id, "ctx_error": str(exc)},
                )

    async def reject_payment(self, client: Client, payment_id: str, reason: str, admin_id: int) -> bool:
        """
        Reject a payment session, notify the user, route to their topic, and emit admin log.

        Lock is always acquired before state mutation (regardless of current status) to
        prevent races. Lock is released in a finally block.

        BUG FIX: DatabaseManager was never imported — raised NameError on the
        db["user_topics"].update_one call. Import now at module level.
        BUG FIX: User was never notified of rejection. Added client.send_message to user
        with FloodWait handling.
        BUG FIX: Lock was only acquired for UNDER_REVIEW status. Moved outside the
        status check so it always guards the state mutation.
        BUG FIX: Lock was never released. Added try/finally.
        """
        session = await self.repository.get_session(payment_id)
        if not session:
            return False

        # FIX: Always acquire lock before mutation, not only for UNDER_REVIEW
        lock_acquired = await self.repository.acquire_processing_lock(payment_id)
        if not lock_acquired:
            return False

        try:
            await self.update_status(
                payment_id,
                PaymentStatus.REJECTED,
                rejection_reason=reason,
                rejected_at=datetime.now(timezone.utc),
                rejected_by=admin_id,
            )

            # FIX: Notify user of rejection (was missing entirely)
            rejection_msg = (
                "❌ <b>Payment Rejected</b>\n\n"
                f"<b>Reason:</b> {reason}\n\n"
                "If you believe this is an error, please contact support via /help."
            )
            await _send_safe(client, session.user_id, rejection_msg)

            # Route to user's Verification Hub topic
            try:
                from app.services.topic_manager import get_topic_manager
                topic_id = await get_topic_manager().get_or_create_user_topic(client, session.user_id)

                await client.send_message(
                    chat_id=settings.VERIFICATION_GROUP_ID,
                    text=(
                        f"❌ <b>PAYMENT REJECTED</b>\n\n"
                        f"<b>Moderator:</b> {admin_id}\n"
                        f"<b>Reason:</b> {reason}"
                    ),
                    message_thread_id=topic_id,
                    parse_mode=ParseMode.HTML,
                )

                # Reopen support session so user can follow up
                db = DatabaseManager.get_db()  # FIX: DatabaseManager now imported
                await db["user_topics"].update_one(
                    {"user_id": session.user_id},
                    {"$set": {"status": "pending", "updated_at": datetime.now(timezone.utc)}},
                )
            except Exception as exc:
                logger.warning(
                    "reject_payment_topic_routing_failed",
                    extra={"ctx_payment_id": payment_id, "ctx_error": str(exc)},
                )

            # Emit to Admin Logs topic
            try:
                from app.services.admin_logger import get_admin_logger
                await get_admin_logger().log(
                    client=client,
                    action="PAYMENT REJECTED",
                    admin_id=admin_id,
                    admin_name=f"Admin {admin_id}",
                    target_user_id=session.user_id,
                    details=f"Reason: {reason}"
                )
            except Exception as exc:
                logger.warning(
                    "reject_payment_admin_log_failed",
                    extra={"ctx_payment_id": payment_id, "ctx_error": str(exc)},
                )

            return True

        except Exception as exc:
            logger.exception(
                "reject_payment_failed",
                extra={"ctx_payment_id": payment_id, "ctx_error": str(exc)},
            )
            return False

        finally:
            # FIX: Always release the processing lock.
            try:
                await self.repository.release_processing_lock(payment_id)
            except Exception as exc:
                logger.warning(
                    "reject_payment_lock_release_failed",
                    extra={"ctx_payment_id": payment_id, "ctx_error": str(exc)},
                )

    async def resume_active_sessions(self) -> None:
        """
        On bot startup, restore timeout timers for all active payment sessions.
        Sessions whose expires_at has already passed are expired immediately.
        Sessions still within their window have their remaining time resumed.
        Sessions with no expires_at are given a fresh full timeout.

        Resets any sessions stuck in PROCESSING state (crashed mid-approval) back
        to UNDER_REVIEW before scanning.
        """
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

            # Use a public repository method rather than accessing _collection directly
            active_sessions = await self.repository.get_sessions_by_statuses(active_statuses)

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
                        extra={
                            "ctx_payment_id": session.id,
                            "ctx_remaining": int(remaining),
                        },
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