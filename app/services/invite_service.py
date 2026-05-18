from __future__ import annotations

import secrets
from datetime import datetime, timedelta, timezone
from typing import Optional

from pyrogram.client import Client

from app.models.invite import Invite, InviteStatus
from app.repositories.invite_repository import InviteRepository
from app.services.audit_service import get_audit, AuditAction
from app.utils.logger import get_logger

logger = get_logger(__name__)

_UTC = timezone.utc


class InviteService:
    def __init__(self) -> None:
        self._repo = InviteRepository()

    async def generate_premium_invite(
        self,
        client: Client,
        user_id: int,
        chat_id: int,
        granted_by: int,
        plan: str,
    ) -> Invite:
        """
        Create a single-use, 24-hour invite link to the premium chat.
        Persists the invite record and writes an audit log entry.
        Returns the Invite with telegram_link set.
        """
        now = datetime.now(_UTC)
        expires_at = now + timedelta(hours=24)

        tg_result = await client.create_chat_invite_link(
            chat_id=chat_id,
            member_limit=1,
            expire_date=expires_at,
        )

        token = secrets.token_urlsafe(16)

        invite = Invite(
            token=token,
            created_by=granted_by,
            chat_id=chat_id,
            plan_grant=plan,
            max_uses=1,
            uses_remaining=1,
            created_at=now,
            expires_at=expires_at,
            status=InviteStatus.ACTIVE,
            telegram_link=tg_result.invite_link,
            notes=f"Payment approval for user {user_id}, plan {plan}",
        )

        await self._repo.create(invite)

        await get_audit().log(
            action=AuditAction.INVITE_GENERATE,
            performed_by=granted_by,
            target_user_id=user_id,
            details={
                "token": token,
                "chat_id": chat_id,
                "plan": plan,
                "expires_at": expires_at.isoformat(),
            },
        )

        logger.info(
            "Premium invite generated",
            extra={
                "ctx_user_id": user_id,
                "ctx_chat_id": chat_id,
                "ctx_plan": plan,
                "ctx_granted_by": granted_by,
            },
        )
        return invite