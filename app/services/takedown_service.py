from __future__ import annotations

"""
TakedownService — DMCA / report / content claim lifecycle management.

Auto-lock on report: any submitted report immediately locks the vault item
and pauses pending queue jobs. Admins review and either execute (remove) or dismiss (restore).
"""

from datetime import datetime, timezone
from typing import Optional

from app.config import settings
from app.core.database import DatabaseManager
from app.services.audit_service import get_audit, AuditAction
from app.utils.logger import get_logger

logger = get_logger(__name__)

_UTC = timezone.utc


class TakedownService:
    """
    Manages the lifecycle of takedown requests against vault content.
    All state mutations are atomic MongoDB operations.
    """

    async def submit_report(
        self,
        content_id: str,
        reported_by: int,
        reason: str,
        report_type: str,  # 'report' | 'dmca' | 'claim'
    ) -> str:
        """
        Submit a takedown request:
        1. Insert takedown record
        2. Auto-lock vault item (distribution_state = 'locked')
        3. Pause PENDING queue jobs for this content_id
        4. Notify admins via LOG_CHANNEL_ID
        Returns: record_id
        """
        db = DatabaseManager.get_db()
        now = datetime.now(_UTC)

        doc = {
            "content_id": content_id,
            "reported_by": reported_by,
            "reason": reason,
            "type": report_type,
            "status": "pending",
            "created_at": now,
            "reviewed_by": None,
            "reviewed_at": None,
        }
        result = await db["takedown_requests"].insert_one(doc)
        record_id = str(result.inserted_id)

        # Auto-lock vault item
        await db[settings.VAULT_COLLECTION].update_one(
            {"content_id": content_id},
            {
                "$set": {
                    "distribution_state": "locked",
                    "locked_at": now,
                    "lock_reason": f"takedown_report:{record_id}",
                }
            },
        )

        # Pause PENDING queue jobs for this content
        queue_result = await db[settings.QUEUE_COLLECTION].update_many(
            {
                "content_id": content_id,
                "status": "pending",
            },
            {
                "$set": {
                    "status": "locked",
                    "updated_at": now,
                    "lock_reason": f"takedown_report:{record_id}",
                }
            },
        )

        logger.warning(
            "Takedown report submitted — vault item locked",
            extra={
                "ctx_content_id": content_id,
                "ctx_record_id": record_id,
                "ctx_type": report_type,
                "ctx_reported_by": reported_by,
                "ctx_jobs_paused": queue_result.modified_count,
            },
        )

        # Notify LOG_CHANNEL_ID
        if settings.LOG_CHANNEL_ID:
            try:
                from app.bot.client import get_bot
                bot = get_bot()
                await bot.send_message(
                    chat_id=settings.LOG_CHANNEL_ID,
                    text=(
                        f"⚠️ <b>Takedown Report [{report_type.upper()}]</b>\n"
                        f"Content ID: <code>{content_id}</code>\n"
                        f"Reported by: <code>{reported_by}</code>\n"
                        f"Reason: {reason[:300]}\n"
                        f"Record ID: <code>{record_id}</code>\n\n"
                        f"To execute: <code>/execute_takedown {content_id}</code>\n"
                        f"To dismiss: <code>/dismiss_report {content_id}</code>"
                    ),
                    parse_mode="html",
                )
            except Exception as e:
                logger.warning(
                    "Failed to notify LOG_CHANNEL_ID",
                    extra={"ctx_error": str(e)},
                )

        return record_id

    async def execute_takedown(self, content_id: str, reviewed_by: int) -> bool:
        """
        Execute a takedown:
        - Set vault distribution_state = 'removed'
        - Update takedown request status = 'executed'
        - Write audit log
        """
        db = DatabaseManager.get_db()
        now = datetime.now(_UTC)

        vault_result = await db[settings.VAULT_COLLECTION].update_one(
            {"content_id": content_id},
            {"$set": {"distribution_state": "removed", "removed_at": now}},
        )

        await db["takedown_requests"].update_many(
            {"content_id": content_id, "status": {"$in": ["pending", "reviewed"]}},
            {
                "$set": {
                    "status": "executed",
                    "reviewed_by": reviewed_by,
                    "reviewed_at": now,
                }
            },
        )

        await get_audit().log(
            action=AuditAction.TAKEDOWN_EXECUTE,
            performed_by=reviewed_by,
            content_id=content_id,
            details={"vault_modified": vault_result.modified_count},
        )

        logger.warning(
            "Takedown executed",
            extra={"ctx_content_id": content_id, "ctx_reviewed_by": reviewed_by},
        )
        return vault_result.modified_count > 0

    async def dismiss_report(self, content_id: str, reviewed_by: int) -> bool:
        """
        Dismiss a takedown request:
        - Restore vault distribution_state = 'pending' (ModerationState.PENDING.value)
        - Restore LOCKED queue jobs back to PENDING
        - Update takedown request status = 'dismissed'
        - Write audit log
        """
        db = DatabaseManager.get_db()
        now = datetime.now(_UTC)

        # Restore vault
        from app.core.models import ModerationState
        vault_result = await db[settings.VAULT_COLLECTION].update_one(
            {"content_id": content_id, "distribution_state": "locked"},
            {
                "$set": {
                    "distribution_state": ModerationState.PENDING.value,
                    "lock_reason": None,
                }
            },
        )

        # Restore paused queue jobs
        queue_result = await db[settings.QUEUE_COLLECTION].update_many(
            {"content_id": content_id, "status": "locked"},
            {
                "$set": {
                    "status": "pending",
                    "lock_reason": None,
                    "updated_at": now,
                }
            },
        )

        # Update takedown record
        await db["takedown_requests"].update_many(
            {"content_id": content_id, "status": "pending"},
            {
                "$set": {
                    "status": "dismissed",
                    "reviewed_by": reviewed_by,
                    "reviewed_at": now,
                }
            },
        )

        await get_audit().log(
            action=AuditAction.TAKEDOWN_DISMISS,
            performed_by=reviewed_by,
            content_id=content_id,
            details={
                "vault_restored": vault_result.modified_count,
                "jobs_restored": queue_result.modified_count,
            },
        )

        logger.info(
            "Takedown report dismissed — content restored",
            extra={
                "ctx_content_id": content_id,
                "ctx_reviewed_by": reviewed_by,
                "ctx_jobs_restored": queue_result.modified_count,
            },
        )
        return True