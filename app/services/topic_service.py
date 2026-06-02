from __future__ import annotations

"""
app/services/topic_service.py

Thin wrapper around TopicManager for payment-specific topic operations.
Created to satisfy the import in app/payments/handlers.py.
"""

from typing import Optional
from pyrogram.client import Client
from app.services.topic_manager import get_topic_manager
from app.utils.logger import get_logger

logger = get_logger(__name__)

_PAYMENTS_TOPIC_KEY = "payments"
_PAYMENTS_TOPIC_TITLE = "💎 Payments"


class TopicService:
    """
    Wraps TopicManager for shared (non-user-scoped) hub topics.
    Currently exposes payment topic creation.
    """

    async def get_or_create_payments_topic(
        self, client: Client
    ) -> Optional[int]:
        """
        Get or create the shared Payments hub topic.
        Returns the topic_id or None on failure.
        """
        try:
            topic_manager = get_topic_manager()
            topic_id = await topic_manager.get_or_create_shared_topic(
                client=client,
                key=_PAYMENTS_TOPIC_KEY,
                title=_PAYMENTS_TOPIC_TITLE,
            )
            return topic_id
        except Exception as e:
            logger.error(
                "TopicService: failed to get or create payments topic",
                extra={"ctx_error": str(e)},
            )
            return None


_topic_service: Optional[TopicService] = None


def get_topic_service() -> TopicService:
    global _topic_service
    if _topic_service is None:
        _topic_service = TopicService()
    return _topic_service
