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

    async def expire_session(self, client: Client, payment_id: str) -> bool:
        session = await self.repository.get_session(payment_id)
        if not session or session.status not in {
            PaymentStatus.WAITING_TXID,
            PaymentStatus.WAITING_SCREENSHOT,
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
                "Payment session expired. Start again from Premium Access.",
            )
        except Exception as e:
            logger.exception(
                "payment_timeout_notify_failed",
                extra={"ctx_payment_id": payment_id, "ctx_error": str(e)},
            )

        return True
