from __future__ import annotations

from datetime import datetime, timezone

from pyrogram import Client

from app.payments.models import PaymentStatus
from app.payments.repository import PaymentRepository
from app.utils.logger import get_logger

logger = get_logger(__name__)


class PaymentTimeoutMonitor:
    def __init__(self, repository: PaymentRepository) -> None:
        self.repository = repository

    async def check_timeouts(self, client: Client) -> None:
        """Scan active timeouts and send progressive warnings or expire sessions."""
        now = datetime.now(timezone.utc)
        
        # We query the repository for timeouts that need attention
        # This is a bit high-level, let's assume we can get them from repo
        db = self.repository._db
        timeouts_col = db["payment_timeouts"]
        
        # 1. Expire sessions (>= 30 mins)
        expired_cursor = timeouts_col.find({"expires_at": {"$lte": now}})
        async for doc in expired_cursor:
            await self.expire_session(client, doc["payment_id"])

        # 2. Progressive warnings
        # Warning 1: +5 mins (25 mins left)
        # Warning 2: +10 mins (20 mins left)
        # Warning 3: +20 mins (10 mins left)
        # Note: session expires at now + 30 mins.
        
        # +5 mins warning (25 mins remaining)
        w1_cutoff = now + timedelta(minutes=25)
        await self._send_warnings(client, timeouts_col, w1_cutoff, "five_minute_warning_sent", 
                                "⚠️ <b>Payment reminder:</b> Your session will expire in 25 minutes.")

        # +10 mins warning (20 mins remaining)
        w2_cutoff = now + timedelta(minutes=20)
        await self._send_warnings(client, timeouts_col, w2_cutoff, "ten_minute_warning_sent", 
                                "⚠️ <b>Payment reminder:</b> Your session will expire in 20 minutes.")
        
        # +20 mins warning (10 mins remaining)
        w3_cutoff = now + timedelta(minutes=10)
        await self._send_warnings(client, timeouts_col, w3_cutoff, "twenty_minute_warning_sent", 
                                "⚠️ <b>URGENT:</b> Your payment session will expire in 10 minutes!")

    async def _send_warnings(self, client: Client, col, cutoff, flag, text):
        cursor = col.find({
            "expires_at": {"$lte": cutoff},
            flag: False
        })
        async for doc in cursor:
            try:
                await client.send_message(doc["user_id"], text, parse_mode=ParseMode.HTML)
                await col.update_one({"_id": doc["_id"]}, {"$set": {flag: True}})
            except Exception as e:
                logger.warning(f"Failed to send {flag}", extra={"ctx_user_id": doc["user_id"], "ctx_error": str(e)})

    async def expire_session(self, client: Client, payment_id: str) -> bool:
        session = await self.repository.get_session(payment_id)
        if not session or session.status in {
            PaymentStatus.APPROVED,
            PaymentStatus.CANCELLED,
            PaymentStatus.EXPIRED,
            PaymentStatus.REJECTED
        }:
            return False

        session.status = PaymentStatus.EXPIRED
        session.updated_at = datetime.now(timezone.utc)
        await self.repository.save_session(session)
        await self.repository.clear_timeout(payment_id)
        await self.repository.log_event(payment_id, "payment_expired", {})

        try:
            await client.send_message(
                session.user_id,
                "❌ <b>Your payment session has expired.</b>\n\nPlease start again if you still wish to upgrade.",
                parse_mode=ParseMode.HTML
            )
        except Exception as e:
            logger.debug(
                "payment_timeout_notify_failed",
                extra={"ctx_payment_id": payment_id, "ctx_error": str(e)},
            )

        return True
