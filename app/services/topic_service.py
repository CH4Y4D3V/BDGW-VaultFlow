from __future__ import annotations

"""
DEPRECATED: Use app.services.topic_manager instead.
This module is kept for backward compatibility and routes to TopicManager.
"""

from typing import Optional
from pyrogram.client import Client

from app.services.topic_manager import (
    get_topic_manager,
    TOPIC_CONTENT,
    TOPIC_SUPPORT,
    TOPIC_PAYMENT
)

def get_topic_service() -> "TopicServiceBridge":
    return TopicServiceBridge()

class TopicServiceBridge:
    def __init__(self):
        self._manager = get_topic_manager()

    async def get_or_create_user_topic(
        self,
        client: Client,
        user_id: int,
        topic_type: str,
    ) -> int:
        return await self._manager.get_or_create_user_topic(client, user_id, topic_type)

    async def get_user_by_topic(self, topic_id: int) -> Optional[dict]:
        from app.core.database import DatabaseManager
        db = DatabaseManager.get_db()
        return await db["user_topics"].find_one({"topic_id": topic_id})

    async def get_or_create_rejected_topic(self, client: Client) -> int:
        return await self._manager.get_or_create_shared_topic(client, "rejected", "❌ Rejected Content")

    async def get_or_create_payments_topic(self, client: Client) -> int:
        return await self._manager.get_or_create_shared_topic(client, "payments", "💎 Payments")

    async def get_user_topic_id(self, user_id: int, topic_type: str) -> Optional[int]:
        from app.core.database import DatabaseManager
        db = DatabaseManager.get_db()
        doc = await db["user_topics"].find_one({"user_id": user_id, "topic_type": topic_type})
        return doc["topic_id"] if doc else None
