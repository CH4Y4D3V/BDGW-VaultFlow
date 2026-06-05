from __future__ import annotations

from pyrogram import Client

from app.config import settings
from app.payments.repository import PaymentRepository
from app.utils.logger import get_logger

logger = get_logger(__name__)


class PaymentTopicManager:
    def __init__(self, repository: PaymentRepository) -> None:
        self.repository = repository

    async def get_or_create_payments_topic(self, client: Client, payment_id: str, user_id: int) -> int:
        """
        Retrieves an existing topic ID for the payment or creates a new one.
        This provides the missing delegation requested by the TopicService.
        """
        # 1. Check if we already have a mapping (e.g. session resume)
        # We need a reverse lookup: payment_id -> topic_id
        # The repository has get_payment_by_topic(topic_id), but we need the opposite.
        # Let's check the repository for a direct lookup method.
        
        # Looking at app/payments/repository.py, it only has get_payment_by_topic.
        # We might need to query the payment_topics collection directly or add a method.
        # For now, we'll check if the repository can find it.
        
        # Actually, let's look at the mapping logic in repository.py:
        # async def map_topic(self, topic_id: int, payment_id: str):
        #    await self._topics_collection.update_one({"_id": topic_id}, {"$set": {"payment_id": payment_id}}, upsert=True)
        
        # To find topic_id from payment_id:
        doc = await self.repository._topics_collection.find_one({"payment_id": payment_id})
        if doc:
            return doc["_id"]
            
        # 2. Not found -> Create new
        return await self.create_topic(client, payment_id, user_id)

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
