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
        1. Insert takedown record with status='pending', auto_locked=True
        2. Auto-lock vault item (distribution_state = 'locked')
        3. Pause PENDING/WATERMARKING queue jobs for this content_id
        4. Notify admins via LOG_CHANNEL_ID
        5. Write audit log
        Returns: record_id
        """
        db = DatabaseManager.get_db()
        now = datetime.now(_UTC)

        doc = {
            "content_id": content_id,
            "reported_by": reported_by,
            "reason": reason,
            "report_type": report_type,
            "status": "pending",
            "created_at": now,
            "reviewed_by": None,
            "reviewed_at": None,
            "auto_locked": True,
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
                    "lock_reason": f"report:{record_id}",
                }
            },
        )

        # Pause PENDING and WATERMARKING queue jobs for this content
        queue_result = await db[settings.QUEUE_COLLECTION].update_many(
            {
                "content_id": content_id,
                "status": {"$in": ["pending", "watermarking"]},
            },
            {
                "$set": {
                    "status": "locked",
                    "updated_at": now,
                    "lock_reason": "takedown_pending",
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

        # Write audit log
        await get_audit().log(
            action=AuditAction.TAKEDOWN_REQUEST,
            performed_by=reported_by,
            content_id=content_id,
            details={"reason": reason, "report_type": report_type},
        )

        # Notify LOG_CHANNEL_ID
        if settings.LOG_CHANNEL_ID:
            try:
                from app.bot.client import get_bot
                bot = get_bot()
                for attempt in range(3):
                    try:
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
                        break
                    except Exception as e:
                        import asyncio
                        from pyrogram.errors import FloodWait
                        if isinstance(e, FloodWait):
                            await asyncio.sleep(int(e.value) + settings.FLOODWAIT_EXTRA_BUFFER)
                        else:
                            logger.warning(
                                "Failed to notify LOG_CHANNEL_ID",
                                extra={"ctx_error": str(e), "ctx_attempt": attempt + 1},
                            )
                            break
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
        - Move locked queue jobs to dead letter
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

        # Move locked queue jobs to dead letter
        from app.repositories.queue_repository import QueueRepository
        queue_repo = QueueRepository(db)
        locked_jobs_cursor = db[settings.QUEUE_COLLECTION].find(
            {"content_id": content_id, "status": "locked"}
        )
        async for job in locked_jobs_cursor:
            try:
                await queue_repo.move_to_dead_letter(str(job["_id"]), "takedown_executed")
            except Exception as e:
                logger.warning(
                    "Could not dead-letter locked job during takedown",
                    extra={"ctx_job_id": str(job["_id"]), "ctx_error": str(e)},
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
        - Restore vault distribution_state = 'pending'
        - Restore LOCKED queue jobs (with lock_reason='takedown_pending') back to PENDING
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
            {
                "content_id": content_id,
                "status": "locked",
                "lock_reason": "takedown_pending",
            },
            {
                "$set": {
                    "status": "pending",
                    "lock_reason": None,
                    "locked_by": None,
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

    async def get_pending_reports(self) -> list[dict]:
        """Return all pending takedown requests sorted by created_at DESC."""
        db = DatabaseManager.get_db()
        cursor = db["takedown_requests"].find(
            {"status": "pending"}
        ).sort("created_at", -1)
        return await cursor.to_list(length=None)