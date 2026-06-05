from __future__ import annotations
from typing import Optional
from pyrogram import Client
from app.utils.logger import get_logger

logger = get_logger(__name__)


class UserTopicsRepo:
    """
    Repository for the user_topics collection.
    Provides get_or_create() to resolve/create a user's permanent hub topic.
    """

    async def get_or_create(self, client: Client, user_id: int) -> Optional[int]:
        from app.core.database import DatabaseManager
        db = DatabaseManager.get_db()

        # Fast path: existing mapping
        doc = await db["user_topics"].find_one({"user_id": user_id})
        if doc:
            return int(doc["topic_id"])

        # Fetch user display info for topic title
        user_doc = await db["users"].find_one({"user_id": user_id}) or {}
        full_name: str = user_doc.get("full_name") or str(user_id)
        username: Optional[str] = user_doc.get("username")

        # Delegate creation to topic_manager
        try:
            from app.services.topic_manager import get_or_create_user_topic
            return await get_or_create_user_topic(
                client,
                user_id=user_id,
                full_name=full_name,
                username=username,
            )
        except Exception as exc:
            logger.error(
                "user_topics_repo.get_or_create failed for user %s: %s",
                user_id, exc,
            )
            return None


user_topics_repo = UserTopicsRepo()