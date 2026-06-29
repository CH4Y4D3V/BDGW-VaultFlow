"""
app/services/hub_logger.py
--------------------------
Posts structured admin log entries to the Admin Logs forum topic
inside the Verification Hub.

All admin actions (invite generation, subscription changes, bans, etc.)
must write here AND to audit_logs MongoDB collection (dual-write, §22).

Usage:
    from app.services.hub_logger import write_admin_log
    await write_admin_log(
        action_type="INVITE GENERATED",
        performed_by=admin_id,
        target_user_id=user_id,
        detail="plan=1month chat=-100xyz token=abc expires=...",
    )
"""

from __future__ import annotations

import asyncio
from datetime import datetime, timezone
from typing import Optional

from pyrogram.errors import FloodWait

from app.config import settings
from app.utils.logger import get_logger

logger = get_logger(__name__)

_MAX_FLOOD_RETRIES = 3
_FLOOD_BUFFER = 2


async def write_admin_log(
    action_type: str,
    performed_by: Optional[int],
    target_user_id: Optional[int] = None,
    detail: str = "",
) -> None:
    """
    Post a structured entry to the Admin Logs topic in the Verification Hub.

    Reads admin_logs_topic_id from hub_config collection at call time so
    topic ID changes take effect without restart.

    Non-raising: all exceptions are logged. A failed admin log write must
    never abort the calling operation.

    Args:
        action_type:     Uppercase label, e.g. "INVITE GENERATED".
        performed_by:    Telegram user_id of the acting admin (None = system).
        target_user_id:  Telegram user_id being acted upon (None if N/A).
        detail:          Free-text action-specific context.
    """
    try:
        from app.bot.client import get_bot
        from app.core.database import DatabaseManager

        bot = get_bot()
        db = DatabaseManager.get_db()

        # Resolve Admin Logs topic ID from hub_config
        topic_doc = await db["hub_config"].find_one({"key": "admin_logs_topic_id"})
        topic_id: Optional[int] = int(topic_doc["value"]) if topic_doc and topic_doc.get("value") else None

        if not topic_id:
            # Fallback to settings
            topic_id = getattr(settings, "HUB_TOPIC_ADMIN_LOGS", 0) or None

        if not topic_id:
            logger.warning(
                "write_admin_log: admin_logs_topic_id not configured — skipping",
                extra={"ctx_action": action_type},
            )
            return

        hub_id = settings.VERIFICATION_GROUP_ID
        if not hub_id:
            return

        now_str = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")

        text = (
            f"<b>[{action_type}]</b>\n"
            f"Admin     : {performed_by or 'System'}\n"
            f"Target ID : {target_user_id or 'N/A'}\n"
            f"Detail    : {detail}\n"
            f"Time      : {now_str}"
        )

        for attempt in range(_MAX_FLOOD_RETRIES):
            try:
                await bot.send_message(
                    chat_id=hub_id,
                    text=text,
                    parse_mode="html",
                    message_thread_id=topic_id,
                )
                return
            except FloodWait as fw:
                wait = int(fw.value) + _FLOOD_BUFFER
                logger.warning(
                    "write_admin_log: FloodWait",
                    extra={"ctx_wait": wait, "ctx_attempt": attempt + 1},
                )
                await asyncio.sleep(wait)
            except Exception as exc:
                logger.error(
                    "write_admin_log: Telegram send failed",
                    extra={
                        "ctx_action": action_type,
                        "ctx_error": str(exc),
                        "ctx_attempt": attempt + 1,
                        "ctx_hub_id": hub_id,
                        "ctx_topic_id": topic_id,
                    },
                    exc_info=True,  # Full traceback visible in Railway logs
                )
                return  # non-fatal

    except Exception as exc:
        logger.error(
            "write_admin_log: unexpected failure",
            extra={"ctx_action": action_type, "ctx_error": str(exc)},
        )
