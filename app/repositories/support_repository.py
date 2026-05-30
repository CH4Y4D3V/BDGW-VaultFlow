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

    async def update_ticket_status(self, user_id: int, topic_type: str, status: str) -> bool:
        """Update the status of a specific user topic."""
        result = await self.db["user_topics"].update_one(
            {"user_id": user_id, "topic_type": topic_type},
            {"$set": {"status": status, "updated_at": datetime.now(timezone.utc)}}
        )
        return result.modified_count > 0

    async def get_ticket_status(self, user_id: int, topic_type: str) -> Optional[str]:
        """Get the current status of a user topic."""
        doc = await self.db["user_topics"].find_one({"user_id": user_id, "topic_type": topic_type})
        return doc.get("status") if doc else None