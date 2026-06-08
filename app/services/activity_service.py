from __future__ import annotations

from typing import Optional

from motor.motor_asyncio import AsyncIOMotorDatabase

from app.models.activity import ActivityAction
from app.repositories.activity_repository import ActivityRepository


class ActivityService:
    """
    Service layer for logging user and system activities.

    This provides a semantic layer on top of the generic ActivityRepository
    to ensure consistent logging of specific, business-critical events.
    """

    def __init__(self, db: AsyncIOMotorDatabase) -> None:
        self._repo = ActivityRepository(db)

    async def log_support_session_start(self, user_id: int, session_id: str) -> None:
        """Log the initiation of a new support session."""
        await self._repo.log_activity(
            user_id=user_id,
            action=ActivityAction.SUPPORT_SESSION_START,
            metadata={"session_id": session_id},
        )

    async def log_support_session_close(
        self,
        user_id: int,
        session_id: str,
        closed_by: int,
    ) -> None:
        """Log the closure of a support session."""
        await self._repo.log_activity(
            user_id=user_id,
            action=ActivityAction.SUPPORT_SESSION_CLOSE,
            performed_by=closed_by,
            metadata={"session_id": session_id},
        )

    async def log_content_submission(self, user_id: int, content_id: str) -> None:
        """Log a new content submission."""
        await self._repo.log_activity(
            user_id=user_id,
            action=ActivityAction.UPLOAD,
            metadata={"content_id": content_id},
        )

    async def log_content_moderation(
        self,
        user_id: int,
        content_id: str,
        moderator_id: int,
        verdict: str,
    ) -> None:
        """Log a moderation verdict (approve, reject, queue)."""
        action = ActivityAction(verdict.upper())
        await self._repo.log_activity(
            user_id=user_id,
            action=action,
            performed_by=moderator_id,
            metadata={"content_id": content_id},
        )
