from __future__ import annotations

import secrets
from datetime import datetime, timedelta, timezone
from typing import Optional

from pyrogram.client import Client

from app.config import settings
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
        Create a single-use invite link to the premium chat.

        B-02 Step 2: Before creating a new invite, revoke all previously issued
        unexpired ACTIVE invites for this (user_id, chat_id) combination.

        FIX 10: Sets intended_user_id field on the invite document so that
        invite_repository can query by indexed field instead of notes regex.
        Notes field is still written for human-readable audit purposes.
        """
        # ── B-02 Step 2: Revoke any previously active invites for this user+chat ──
        try:
            revoked_links = await self._repo.revoke_all_active_for_user_chat(
                user_id=user_id,
                chat_id=chat_id,
            )
            for link in revoked_links:
                try:
                    await client.revoke_chat_invite_link(
                        chat_id=chat_id,
                        invite_link=link,
                    )
                    logger.info(
                        "generate_premium_invite: revoked stale invite on Telegram",
                        extra={
                            "ctx_user_id": user_id,
                            "ctx_chat_id": chat_id,
                            "ctx_link_prefix": link[:30] if link else None,
                        },
                    )
                except Exception as e:
                    logger.warning(
                        "generate_premium_invite: failed to revoke stale Telegram link (non-fatal)",
                        extra={
                            "ctx_user_id": user_id,
                            "ctx_chat_id": chat_id,
                            "ctx_error": str(e),
                        },
                    )
            if revoked_links:
                logger.info(
                    "generate_premium_invite: revoked prior active invites before generating new",
                    extra={
                        "ctx_user_id": user_id,
                        "ctx_chat_id": chat_id,
                        "ctx_revoked_count": len(revoked_links),
                    },
                )
        except Exception as e:
            logger.warning(
                "generate_premium_invite: revoke_all_active_for_user_chat failed (non-fatal)",
                extra={"ctx_user_id": user_id, "ctx_chat_id": chat_id, "ctx_error": str(e)},
            )

        # ── Create new invite ──────────────────────────────────────────────────
        now = datetime.now(_UTC)
        expires_at = now + timedelta(minutes=settings.INVITE_EXPIRY_MINUTES)

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
            # FIX 10: notes kept for human-readable audit; queries now use intended_user_id
            notes=f"user_{user_id} plan:{plan} granted_by:{granted_by}",
        )

        # FIX 10: write intended_user_id as a proper indexed field
        invite_doc = invite.to_dict()
        invite_doc["intended_user_id"] = user_id

        await self._repo.collection.insert_one(invite_doc)

        await get_audit().log(
            action=AuditAction.INVITE_GENERATE,
            performed_by=granted_by,
            target_user_id=user_id,
            details={
                "token": token,
                "chat_id": chat_id,
                "plan": plan,
                "expires_at": expires_at.isoformat(),
                "expiry_minutes": settings.INVITE_EXPIRY_MINUTES,
            },
        )

        logger.info(
            "Premium invite generated",
            extra={
                "ctx_user_id": user_id,
                "ctx_chat_id": chat_id,
                "ctx_plan": plan,
                "ctx_granted_by": granted_by,
                "ctx_expires_at": expires_at.isoformat(),
                "ctx_expiry_minutes": settings.INVITE_EXPIRY_MINUTES,
            },
        )
        return invite
