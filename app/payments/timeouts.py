from __future__ import annotations

"""
timeouts.py
───────────
PaymentTimeoutMonitor — scans active payment timeouts and sends progressive
warnings or expires sessions when the deadline is reached.

Warning schedule (from session start):
  +5 min  → "25 minutes remaining" warning
  +10 min → "20 minutes remaining" warning
  +20 min → "10 minutes remaining" (URGENT) warning
  +30 min → session expired

Flag naming convention: flags reflect REMAINING time at the point the warning
is sent (e.g. ``warning_25min_remaining_sent`` means the warning that fires
when 25 minutes are still left on the clock).
"""

import asyncio
from datetime import datetime, timedelta, timezone
from typing import Optional

from pyrogram import Client
from pyrogram.enums import ParseMode
from pyrogram.errors import FloodWait

from app.payments.models import PaymentStatus
from app.payments.repository import PaymentRepository
from app.utils.logger import get_logger

logger = get_logger(__name__)

_MAX_FLOOD_WAIT = 30  # cap FloodWait sleeps at 30 s


# ── Telegram send wrapper ─────────────────────────────────────────────────────

async def _tg_send(coro) -> Optional[object]:
    """
    Execute a Telegram API coroutine with one FloodWait retry.

    Returns the result on success, None on any failure.
    All exceptions are logged; no silent swallowing.
    """
    try:
        return await coro
    except FloodWait as exc:
        wait = min(exc.value, _MAX_FLOOD_WAIT)
        logger.warning("FloodWait %ds in timeout monitor — retrying", wait)
        await asyncio.sleep(wait)
        try:
            return await coro
        except Exception as retry_exc:
            logger.warning("Retry after FloodWait failed: %s", retry_exc)
            return None
    except Exception as exc:
        logger.warning("Telegram call failed in timeout monitor: %s", exc)
        return None


class PaymentTimeoutMonitor:
    """
    Scans the ``payment_timeouts`` collection and takes action on sessions
    approaching or past their expiry deadline.

    Intended to be called by APScheduler on a short interval (e.g. every
    60 seconds). Each call is a single linear pass — no background tasks
    are spawned internally.
    """

    def __init__(self, repository: PaymentRepository) -> None:
        self.repository = repository

    def _get_timeouts_col(self):
        """
        Return the payment_timeouts collection via the repository's DB handle.

        Using a named method rather than accessing ``_db`` inline keeps the
        coupling explicit and makes future refactors easier.
        """
        return self.repository._db["payment_timeouts"]

    async def check_timeouts(self, client: Client) -> None:
        """
        Single pass: send progressive warnings and expire overdue sessions.

        Processing order matters — expiry is checked first. Warning flags
        use remaining-time semantics: a flag named ``warning_25min_remaining_sent``
        is set when the session has approximately 25 minutes left.

        All per-document exceptions are caught and logged individually so a
        single broken document does not abort the entire pass.
        """
        try:
            now = datetime.now(timezone.utc)
            col = self._get_timeouts_col()

            # 1. Expire sessions that have passed their deadline
            expired_cursor = col.find({"expires_at": {"$lte": now}})
            async for doc in expired_cursor:
                try:
                    await self.expire_session(client, doc["payment_id"])
                except Exception as exc:
                    logger.exception(
                        "Failed to expire session",
                        extra={
                            "ctx_payment_id": doc.get("payment_id"),
                            "ctx_user_id": doc.get("user_id"),
                            "ctx_error": str(exc),
                        },
                    )

            # 2. Progressive warnings for the 20-minute session window.
            #
            # BUG-4 FIX: Previous code used cutoff = now + timedelta(minutes=25/20)
            # which was ALWAYS true for a 20-minute session because:
            #   expires_at = created_at + 20min
            #   now + 25min > expires_at  →  true from the moment of creation
            # Both the "25 min" and "20 min" reminders fired simultaneously on the
            # very first scheduler tick after session creation.
            #
            # Fix: Use a TWO-SIDED range on expires_at (remaining time window):
            #   lower_bound = now + timedelta(minutes=X - grace)   (not yet fired earlier reminder)
            #   upper_bound = now + timedelta(minutes=X)            (at most X minutes remaining)
            # This ensures only one warning fires per window.
            #
            # Schedule for 20-min sessions:
            #   ≤10 min remaining  (10 min elapsed):  "10 minutes remaining" warning
            #   ≤5  min remaining  (15 min elapsed):  "5 minutes remaining" URGENT

            # Fires when 10 minutes or fewer remain (not already sent)
            await self._send_warnings(
                client,
                col,
                lower_cutoff=now,                           # session not yet expired
                upper_cutoff=now + timedelta(minutes=10),   # ≤10 min remaining
                flag="warning_10min_remaining_sent",
                text="⚠️ <b>Payment reminder:</b> Your session will expire in 10 minutes.",
            )

            # Fires when 5 minutes or fewer remain (URGENT, not already sent)
            await self._send_warnings(
                client,
                col,
                lower_cutoff=now,                           # session not yet expired
                upper_cutoff=now + timedelta(minutes=5),    # ≤5 min remaining
                flag="warning_5min_remaining_sent",
                text="🚨 <b>URGENT:</b> Your payment session will expire in 5 minutes!",
            )

        except Exception as exc:
            logger.exception(
                "PaymentTimeoutMonitor.check_timeouts failed",
                extra={
                    "ctx_error_type": type(exc).__name__,
                    "ctx_error_message": str(exc),
                },
            )
            raise

    async def _send_warnings(
        self,
        client: Client,
        col,
        lower_cutoff: datetime,
        upper_cutoff: datetime,
        flag: str,
        text: str,
    ) -> None:
        """
        Send a timed warning to sessions with remaining time in (lower, upper].

        BUG-4 FIX: Replaced single `cutoff` with a two-sided range.
        `lower_cutoff` excludes expired sessions and earlier warning windows.
        `upper_cutoff` matches only sessions close enough to expiry.

        Args:
            client:        Pyrogram Client instance.
            col:           Motor collection reference for ``payment_timeouts``.
            lower_cutoff:  Sessions expiring AFTER this datetime qualify.
            upper_cutoff:  Sessions expiring AT OR BEFORE this datetime qualify.
            flag:          Boolean field used as an idempotency guard.
            text:          HTML-formatted warning message to send.
        """
        cursor = col.find({
            "expires_at": {
                "$gt": lower_cutoff,
                "$lte": upper_cutoff,
            },
            flag: {"$ne": True},
        })
        async for doc in cursor:
            try:
                result = await _tg_send(
                    client.send_message(
                        doc["user_id"],
                        text,
                        parse_mode=ParseMode.HTML,
                    )
                )
                if result is not None:
                    # Only mark the flag if delivery confirmed
                    await col.update_one(
                        {"_id": doc["_id"]},
                        {"$set": {flag: True}},
                    )
                else:
                    logger.warning(
                        "Warning not delivered, flag not set — will retry next pass",
                        extra={"ctx_user_id": doc.get("user_id"), "ctx_flag": flag},
                    )
            except Exception as exc:
                logger.warning(
                    "Failed to send timeout warning",
                    extra={
                        "ctx_user_id": doc.get("user_id"),
                        "ctx_flag": flag,
                        "ctx_error": str(exc),
                    },
                )

    async def expire_session(self, client: Client, payment_id: str) -> bool:
        """
        Fully expire a payment session.

        Steps (in restart-safe order — DB writes before Telegram calls):
          1. Fetch session; skip if already in a terminal status.
          2. Refund referral points if any were applied.
          3. Mark session EXPIRED and persist.
          4. Clear the timeout document.
          5. Log the expiry event.
          6. Remove the Redis fast-path cache entry.
          7. Notify the user.

        Returns True if the session was expired by this call; False if it was
        already in a terminal status or not found (idempotent).
        """
        user_id: str = "unknown"
        try:
            session = await self.repository.get_session(payment_id)
            if not session:
                return False

            if session.status in {
                PaymentStatus.APPROVED,
                PaymentStatus.CANCELLED,
                PaymentStatus.EXPIRED,
                PaymentStatus.REJECTED,
            }:
                # Already terminal — clear the stale timeout doc if it exists
                await self.repository.clear_timeout(payment_id)
                return False

            user_id = session.user_id

            # ── Step 2: Refund points ─────────────────────────────────────────
            if session.points_used and session.points_used > 0:
                try:
                    from app.referral.repository import ReferralRepository
                    from app.referral.service import ReferralService
                    ref_repo = ReferralRepository(self.repository._db)
                    ref_service = ReferralService(ref_repo, client)
                    await ref_service.refund_points(user_id, session.points_used)
                    logger.info(
                        "points_refunded_on_expiry",
                        extra={"ctx_user_id": user_id, "ctx_points": session.points_used},
                    )
                except Exception as exc:
                    logger.error(
                        "failed_to_refund_points_on_expiry",
                        extra={"ctx_user_id": user_id, "ctx_error": str(exc)},
                    )

            # ── Steps 3–5: DB writes (before any Telegram call) ───────────────
            session.status = PaymentStatus.EXPIRED
            session.updated_at = datetime.now(timezone.utc)
            await self.repository.save_session(session)
            await self.repository.clear_timeout(payment_id)
            await self.repository.log_event(payment_id, "payment_expired", {})

            # ── Step 6: Redis cleanup ─────────────────────────────────────────
            try:
                from app.core.redis_client import RedisClient
                redis = await RedisClient.get_client()
                await redis.delete(f"pay_session:{user_id}")
            except Exception as exc:
                logger.warning(
                    "Failed to clear Redis cache on session expiry",
                    extra={"ctx_payment_id": payment_id, "ctx_error": str(exc)},
                )

            # ── Step 7: Notify user ───────────────────────────────────────────
            await _tg_send(
                client.send_message(
                    session.user_id,
                    "❌ <b>Your payment session has expired.</b>\n\n"
                    "Please start again if you still wish to upgrade.",
                    parse_mode=ParseMode.HTML,
                )
            )

            logger.info(
                "payment_session_expired_successfully",
                extra={"ctx_payment_id": payment_id, "ctx_user_id": user_id},
            )
            return True

        except Exception as exc:
            logger.exception(
                "payment_session_expiration_failed",
                extra={
                    "ctx_payment_id": payment_id,
                    "ctx_user_id": user_id,
                    "ctx_error": str(exc),
                },
            )
            return False
