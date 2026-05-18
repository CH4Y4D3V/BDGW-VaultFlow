from __future__ import annotations

from datetime import datetime, timezone
from typing import Optional

from app.repositories.base import BaseRepository


class SupportRepository(BaseRepository):
    collection_name = "support_messages"

    async def save_message(self, doc: dict) -> str:
        """Persist a support message record. Returns inserted _id as string."""
        doc.setdefault("created_at", datetime.now(timezone.utc))
        result = await self.insert_one(doc)
        return str(result)

    async def get_by_topic(self, topic_id: int) -> list[dict]:
        """Return all messages for a given topic, ordered oldest-first."""
        return await self.find_many(
            {"topic_id": topic_id},
            sort=[("created_at", 1)],
        )

    async def get_user_topic_mapping(self, user_id: int) -> Optional[dict]:
        """Return the most recent topic mapping for a user (any topic type)."""
        docs = await self.find_many(
            {"user_id": user_id},
            sort=[("created_at", -1)],
            limit=1,
        )
        return docs[0] if docs else None