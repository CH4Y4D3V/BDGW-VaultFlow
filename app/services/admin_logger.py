from __future__ import annotations

import asyncio
from datetime import datetime, timezone
from typing import Optional

from pyrogram.client import Client
from pyrogram.enums import ParseMode

from app.config import settings
from app.utils.logger import get_logger

logger = get_logger(__name__)

class AdminLogger:
    """
    Standardized logger for the Hub's Admin Logs thread.
    Follows Section 28 format.
    """

    async def log(
        self,
        client: Client,
        action: str,
        admin_id: int,
        admin_name: str,
        target_user_id: Optional[int] = None,
        target_user_name: Optional[str] = None,
        details: Optional[str] = None,
    ) -> None:
        """Posts a formatted log entry to the Admin Logs thread."""
        if not settings.HUB_TOPIC_ADMIN_LOGS:
            return

        now_str = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
        
        text = (
            f"📜 <b>{action.upper()}</b>\n\n"
            f"<b>Admin:</b> {admin_name}\n"
            f"<b>Admin ID:</b> <code>{admin_id}</code>\n"
        )
        
        if target_user_name:
            text += f"<b>Target User:</b> {target_user_name}\n"
        if target_user_id:
            text += f"<b>Target User ID:</b> <code>{target_user_id}</code>\n"
            
        text += f"<b>Timestamp:</b> {now_str}"
        
        if details:
            text += f"\n\n<b>Details:</b>\n{details}"

        try:
            await client.send_message(
                chat_id=settings.VERIFICATION_GROUP_ID,
                text=text,
                message_thread_id=settings.HUB_TOPIC_ADMIN_LOGS,
                parse_mode=ParseMode.HTML
            )
        except Exception as e:
            logger.error(
                "write_admin_log: Telegram send failed",
                extra={
                    "ctx_action": action,
                    "ctx_error": str(e),
                    "ctx_hub_id": settings.VERIFICATION_GROUP_ID,
                    "ctx_topic_id": settings.HUB_TOPIC_ADMIN_LOGS,
                },
                exc_info=True,
            )

_admin_logger: Optional[AdminLogger] = None

def get_admin_logger() -> AdminLogger:
    global _admin_logger
    if _admin_logger is None:
        _admin_logger = AdminLogger()
    return _admin_logger
