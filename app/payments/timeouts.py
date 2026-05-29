from __future__ import annotations

from datetime import datetime, timedelta, timezone

from pyrogram import Client
from pyrogram.enums import ParseMode

from app.payments.models import PaymentStatus
from app.payments.repository import PaymentRepository
from app.utils.logger import get_logger

logger = get_logger(__name__)


class PaymentTimeoutMonitor:
    def __init__(self, repository: PaymentRepository) -> None:
        self.repository = repository

    async def check_timeouts(self, client: Client) -> None:
        """Scan active timeouts and send progressive warnings or expire sessions."""
        try:
            now = datetime.now(timezone.utc)
            
            # We query the repository for timeouts that need attention
            db = self.repository._db
            timeouts_col = db["payment_timeouts"]
            
            # 1. Expire sessions (>= 30 mins)
            expired_cursor = timeouts_col.find({"expires_at": {"$lte": now}})
            async for doc in expired_cursor:
                try:
                    await self.expire_session(client, doc["payment_id"])
                except Exception as e:
                    logger.exception(
                        "Failed to expire session",
                        extra={
                            "ctx_payment_id": doc.get("payment_id"),
                            "ctx_user_id": doc.get("user_id"),
                            "ctx_error": str(e)
                        }
                    )

            # 2. Progressive warnings
            
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
        except Exception as e:
            logger.exception(
                "PaymentTimeoutMonitor.check_timeouts failed",
                extra={
                    "ctx_error_type": type(e).__name__,
                    "ctx_error_message": str(e),
                }
            )
            raise

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
                logger.warning("Failed to send warning", extra={"ctx_user_id": doc["user_id"], "ctx_flag": flag, "ctx_error": str(e)})

    async def expire_session(self, client: Client, payment_id: str) -> bool:
        user_id = "unknown"
        try:
            session = await self.repository.get_session(payment_id)
            if not session or session.status in {
                PaymentStatus.APPROVED,
                PaymentStatus.CANCELLED,
                PaymentStatus.EXPIRED,
                PaymentStatus.REJECTED
            }:
                return False

            user_id = session.user_id
            session.status = PaymentStatus.EXPIRED
            session.updated_at = datetime.now(timezone.utc)
            await self.repository.save_session(session)
            await self.repository.clear_timeout(payment_id)
            await self.repository.log_event(payment_id, "payment_expired", {})

            # Remove from Redis fast-path
            from app.core.redis_client import RedisClient
            try:
                redis = await RedisClient.get_client()
                await redis.delete(f"pay_session:{user_id}")
            except Exception:
                pass

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

            logger.info("payment_session_expired_successfully", extra={"ctx_payment_id": payment_id, "ctx_user_id": user_id})
            return True
        except Exception as e:
            logger.exception(
                "payment_session_expiration_failed",
                extra={
                    "payment_id": payment_id,
                    "user_id": user_id,
                    "ctx_error": str(e)
                }
            )
            return False
