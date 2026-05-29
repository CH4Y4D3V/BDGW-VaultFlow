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
        import random
        from pyrogram import raw
        from pyrogram.errors import RPCError
        try:
            peer = await client.resolve_peer(settings.VERIFICATION_GROUP_ID)
            result = await client.invoke(
                raw.functions.channels.CreateForumTopic(
                    channel=peer,
                    title=f"💎 Payment-{user_id}",
                    random_id=random.randint(1, 2**31 - 1),
                )
            )
            topic_id = None
            for update in result.updates:
                if hasattr(update, "id"):
                    topic_id = update.id
                    break
            if topic_id is None:
                raise RuntimeError("CreateForumTopic returned no topic id")

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
            logger.error(
                "payment_topic_creation_failed",
                extra={
                    "ctx_payment_id": payment_id,
                    "ctx_user_id": user_id,
                    "ctx_error": repr(e),
                },
                exc_info=True
            )
            raise
