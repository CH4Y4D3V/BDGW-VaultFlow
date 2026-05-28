from __future__ import annotations

from pyrogram import Client

from app.config import settings
from app.payments.repository import PaymentRepository
from app.utils.logger import get_logger

logger = get_logger(__name__)


class PaymentTopicManager:
    def __init__(self, repository: PaymentRepository) -> None:
        self.repository = repository

    async def create_topic(self, client: Client, payment_id: str, user_id: int) -> int:
        try:
            topic = await client.create_forum_topic(
                chat_id=settings.VERIFICATION_GROUP_ID,
                name=f"payment-{user_id}",
            )
            topic_id = topic.id
            await self.repository.map_topic(topic_id, payment_id)
            logger.info(
                "payment_topic_created",
                extra={
                    "ctx_payment_id": payment_id,
                    "ctx_user_id": user_id,
                    "ctx_topic_id": topic_id,
                },
            )
            return topic_id
        except Exception as e:
            logger.exception(
                "payment_topic_creation_failed",
                extra={
                    "ctx_payment_id": payment_id,
                    "ctx_user_id": user_id,
                    "ctx_error": str(e),
                },
            )
            raise
