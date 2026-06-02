from __future__ import annotations

"""
AuditService — Permanent, immutable moderation action log.

Design:
  - Records are NEVER deleted (use TTL only on low-value debug events if needed)
  - Every admin action (approve, reject, takedown, ban, lock) creates a record
  - Queryable for compliance, dispute resolution, legal requests
  - Append-only: no update operations on existing records
"""

from datetime import datetime, timezone
from typing import Any, Optional

from pyrogram.enums import ParseMode

from app.config import settings
from app.core.database import DatabaseManager
from app.utils.logger import get_logger

logger = get_logger(__name__)

# ── Action constants ──────────────────────────────────────────────────────────

class AuditAction:
    # Content moderation
    APPROVE         = "approve"
    REJECT          = "reject"
    QUEUE           = "queue"
    VAULT_ARCHIVE   = "vault_archive"

    # Content safety
    CONTENT_LOCK    = "content_lock"
    CONTENT_UNLOCK  = "content_unlock"
    CONTENT_REMOVE  = "content_remove"
    TAKEDOWN_REQUEST = "takedown_request"
    TAKEDOWN_EXECUTE = "takedown_execute"
    TAKEDOWN_DISMISS = "takedown_dismiss"

    # Creator lifecycle
    CREATOR_ONBOARD  = "creator_onboard"
    CREATOR_SUSPEND  = "creator_suspend"
    CREATOR_BAN      = "creator_ban"
    CREATOR_REINSTATE = "creator_reinstate"
    CONSENT_RECORD   = "consent_record"
    CONSENT_WITHDRAW = "consent_withdraw"

    # Subscription
    SUB_GRANT       = "sub_grant"
    SUB_REVOKE      = "sub_revoke"
    SUB_EXPIRE      = "sub_expire"
    INVITE_GENERATE = "invite_generate"

    # Access control
    MEMBER_KICK     = "member_kick"
    MEMBER_BAN      = "member_ban"
    MEMBER_UNBAN    = "member_unban"


class AuditService:
    """
    Thin async wrapper around the moderation_audit collection.

    Usage:
        audit = AuditService()
        await audit.log(
            action=AuditAction.APPROVE,
            performed_by=moderator_id,
            content_id="chat_123_456",
            details={"destination": "nsfw"},
        )
    """

    async def log(
        self,
        action: str,
        performed_by: int,
        content_id: Optional[str] = None,
        target_user_id: Optional[int] = None,
        details: Optional[dict[str, Any]] = None,
    ) -> str:
        """
        Write a single immutable audit record.
        Returns the inserted record ID.
        Never raises — audit failure is logged but does not crash the caller.
        """
        db = DatabaseManager.get_db()
        now = datetime.now(timezone.utc)

        record = {
            "action": action,
            "performed_by": performed_by,
            "timestamp": now,
        }
        if content_id is not None:
            record["content_id"] = content_id
        if target_user_id is not None:
            record["target_user_id"] = target_user_id
        if details:
            record["details"] = details

        try:
            result = await db["moderation_audit"].insert_one(record)

            # ── SYSTEM 18: HUB LOGGING ──
            from app.bot.client import get_bot
            client = get_bot()
            if settings.HUB_TOPIC_AUDIT and client.is_connected:
                log_text = f"🛡 <b>[AUDIT]</b>\n"
                log_text += f"┣ 🏷 <b>Action:</b> <code>{action}</code>\n"
                log_text += f"┣ 👤 <b>Admin:</b> <code>{performed_by}</code>\n"
                if target_user_id:
                    log_text += f"┣ 🎯 <b>Target:</b> <code>{target_user_id}</code>\n"
                if content_id:
                    log_text += f"┣ 📦 <b>Content:</b> <code>{content_id}</code>\n"
                import json
                details_str = json.dumps(details, indent=2) if details else "{}"
                log_text += f"┗ 📝 <b>Details:</b> <code>{details_str}</code>"

                try:
                    await client.send_message(
                        chat_id=settings.VERIFICATION_GROUP_ID,
                        text=log_text,
                        message_thread_id=settings.HUB_TOPIC_AUDIT,
                        parse_mode=ParseMode.HTML
                    )
                except Exception:
                    pass  # Don't block if TG logging fails

            return str(result.inserted_id)
        except Exception as e:
            logger.error(
                "Audit log write failed",
                extra={
                    "ctx_action": action,
                    "ctx_performed_by": performed_by,
                    "ctx_error": str(e),
                },
            )
            return ""

    # ── Query helpers ─────────────────────────────────────────────────────────

    async def get_content_history(self, content_id: str, limit: int = 50) -> list[dict]:
        db = DatabaseManager.get_db()
        cursor = (
            db["moderation_audit"]
            .find({"content_id": content_id})
            .sort("timestamp", -1)
            .limit(limit)
        )
        return await cursor.to_list(length=None)

    async def get_admin_actions(
        self,
        performed_by: int,
        since: Optional[datetime] = None,
        limit: int = 100,
    ) -> list[dict]:
        db = DatabaseManager.get_db()
        query: dict = {"performed_by": performed_by}
        if since:
            query["timestamp"] = {"$gte": since}
        cursor = (
            db["moderation_audit"]
            .find(query)
            .sort("timestamp", -1)
            .limit(limit)
        )
        return await cursor.to_list(length=None)

    async def get_user_history(self, target_user_id: int, limit: int = 100) -> list[dict]:
        db = DatabaseManager.get_db()
        cursor = (
            db["moderation_audit"]
            .find({"target_user_id": target_user_id})
            .sort("timestamp", -1)
            .limit(limit)
        )
        return await cursor.to_list(length=None)

    async def get_recent(self, action: Optional[str] = None, limit: int = 100) -> list[dict]:
        db = DatabaseManager.get_db()
        query: dict = {}
        if action:
            query["action"] = action
        cursor = (
            db["moderation_audit"]
            .find(query)
            .sort("timestamp", -1)
            .limit(limit)
        )
        return await cursor.to_list(length=None)


# ── Module-level singleton ────────────────────────────────────────────────────

_audit: Optional[AuditService] = None


def get_audit() -> AuditService:
    global _audit
    if _audit is None:
        _audit = AuditService()
    return _audit