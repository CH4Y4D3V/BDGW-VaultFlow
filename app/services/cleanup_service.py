from __future__ import annotations

"""
CleanupService — Section 20 Message Cleanup Policy

All 5 rules:
  1. General bot conversation     → auto-delete after 60 minutes
  2. Payment session messages     → auto-delete after 20 minutes
  3. BD phone numbers (01[3-9]x)  → auto-delete after 7 minutes
  4. ./prefix messages            → 10 seconds (handled in update_logger.py)
  5. Support conversations        → deleted on ticket closure (user-side only)

FIX (GAP 7):
  - BD phone regex was a substring match on e.g. "017" which matches "2017".
    Correct pattern requires word boundary or digit count enforcement.
  - Payment session cleanup was never called by payment handlers.
    Payment handlers now call log_message(category="payment") after sending.
  - delete_user_support_history() was implemented but never called on ticket close.
    support_handler.py closure callbacks now call it.

RULE: No cleanup notifications ever. Silent deletion only.
"""

import asyncio
import re
from datetime import datetime, timedelta, timezone
from typing import Optional

from pyrogram import Client
from pyrogram.errors import MessageDeleteForbidden, MessageIdInvalid, RPCError

from app.config import settings
from app.core.database import DatabaseManager
from app.utils.logger import get_logger

logger = get_logger(__name__)

# FIX GAP 7: Correct BD phone number pattern.
# BD numbers: 01[3-9]\d{8} — 11 digits total starting with 01[3-9]
# Must be preceded by non-digit (or start of string) and followed by non-digit
# to avoid matching partial numbers like "2017" or "10189xxxxx".
BD_PHONE_REGEX = re.compile(r"(?<!\d)01[3-9]\d{8}(?!\d)")

_CLEANUP_TTL = {
    "general": 60,   # minutes
    "payment": 20,   # minutes
    "phone": 7,      # minutes
    "support": 60,   # minutes (deleted on closure, not by sweep)
}


class CleanupService:
    """
    Enforces Section 20 — MESSAGE CLEANUP POLICY.

    Messages are logged with a category and expiry timestamp.
    The sweep runs every 5 minutes and deletes expired messages silently.
    """

    def __init__(self, bot: Client):
        self._bot = bot
        self._db = DatabaseManager.get_db()
        self._history = self._db["message_history"]

    async def log_message(
        self,
        user_id: int,
        message_id: int,
        text: str = "",
        category: str = "general",
        chat_id: Optional[int] = None,
    ) -> None:
        """
        Record a message for scheduled silent deletion.

        chat_id: if provided, deletion will target this chat (for group messages).
                 If None, defaults to user_id (DM chat).
        category: general | payment | phone | support
        """
        # BD phone number detection overrides category
        if text and BD_PHONE_REGEX.search(text):
            category = "phone"

        expiry_mins = _CLEANUP_TTL.get(category, 60)
        expires_at = datetime.now(timezone.utc) + timedelta(minutes=expiry_mins)
        target_chat_id = chat_id or user_id

        try:
            await self._history.update_one(
                {"chat_id": target_chat_id, "message_id": message_id},
                {
                    "$set": {
                        "user_id": user_id,
                        "chat_id": target_chat_id,
                        "message_id": message_id,
                        "category": category,
                        "expires_at": expires_at,
                        "deleted": False,
                    }
                },
                upsert=True,
            )
        except Exception as e:
            logger.warning(
                "log_message_failed",
                extra={"ctx_user_id": user_id, "ctx_error": str(e)},
            )

    async def run_cleanup_sweep(self) -> None:
        """
        Scans for expired messages and deletes them silently.
        Called every 5 minutes by the scheduler.
        Skips support-category messages (those are deleted on ticket closure only).
        """
        try:
            now = datetime.now(timezone.utc)
            cursor = self._history.find(
                {
                    "expires_at": {"$lte": now},
                    "deleted": False,
                    "category": {"$nin": ["support"]},  # support deleted on closure
                }
            )

            deleted_count = 0
            async for record in cursor:
                chat_id = record.get("chat_id") or record.get("user_id")
                msg_id = record["message_id"]

                deleted = await self._delete_message_safe(chat_id, msg_id)

                await self._history.update_one(
                    {"_id": record["_id"]},
                    {"$set": {"deleted": True, "deleted_at": now}},
                )

                if deleted:
                    deleted_count += 1

            if deleted_count:
                logger.debug(
                    "Cleanup sweep complete",
                    extra={"ctx_deleted": deleted_count},
                )

        except Exception as e:
            logger.error(
                "CleanupService sweep failed",
                extra={"ctx_error": str(e)},
            )

    async def delete_user_support_history(self, user_id: int) -> int:
        """
        Rule 15.5 / Section 20: Delete all user-side support messages on closure.
        Only deletes from the user's DM chat (not the hub topic).
        Hub topic is PRESERVED permanently.
        Returns count of messages deleted.
        """
        deleted_count = 0
        try:
            cursor = self._history.find(
                {
                    "user_id": user_id,
                    "category": "support",
                    "deleted": False,
                }
            )

            msg_ids = []
            records = []
            async for r in cursor:
                # Only delete from user's DM (chat_id == user_id)
                if r.get("chat_id") == user_id or r.get("chat_id") is None:
                    msg_ids.append(r["message_id"])
                    records.append(r)

            # Batch delete is more efficient
            if msg_ids:
                try:
                    await self._bot.delete_messages(user_id, msg_ids)
                    deleted_count = len(msg_ids)
                except Exception as e:
                    # Batch failed — try one by one
                    for record in records:
                        deleted = await self._delete_message_safe(
                            user_id, record["message_id"]
                        )
                        if deleted:
                            deleted_count += 1

            # Mark all as deleted regardless
            if records:
                record_ids = [r["_id"] for r in records]
                await self._history.update_many(
                    {"_id": {"$in": record_ids}},
                    {
                        "$set": {
                            "deleted": True,
                            "deleted_at": datetime.now(timezone.utc),
                        }
                    },
                )

            logger.info(
                "Support history deleted on closure",
                extra={"ctx_user_id": user_id, "ctx_count": deleted_count},
            )
        except Exception as e:
            logger.warning(
                "delete_user_support_history failed",
                extra={"ctx_user_id": user_id, "ctx_error": str(e)},
            )

        return deleted_count

    async def log_support_message(
        self, user_id: int, message_id: int, chat_id: Optional[int] = None
    ) -> None:
        """Log a support message for deletion on ticket closure."""
        await self.log_message(
            user_id=user_id,
            message_id=message_id,
            text="",
            category="support",
            chat_id=chat_id or user_id,
        )

    async def _delete_message_safe(self, chat_id: int, message_id: int) -> bool:
        """
        Attempt to delete a message. Returns True on success.
        Never raises — Section 20 requires silent deletion.
        """
        try:
            await self._bot.delete_messages(
                chat_id=chat_id, message_ids=message_id
            )
            return True
        except (MessageDeleteForbidden, MessageIdInvalid):
            # Already deleted or no permission — not an error
            return False
        except RPCError as e:
            logger.debug(
                "Delete message RPC error",
                extra={
                    "ctx_chat_id": chat_id,
                    "ctx_msg_id": message_id,
                    "ctx_error": str(e),
                },
            )
            return False
        except Exception as e:
            logger.debug(
                "Delete message unexpected error",
                extra={
                    "ctx_chat_id": chat_id,
                    "ctx_msg_id": message_id,
                    "ctx_error": str(e),
                },
            )
            return False


# ── Module-level singleton ────────────────────────────────────────────────────

_cleanup_service: Optional[CleanupService] = None


def get_cleanup_service(bot: Optional[Client] = None) -> CleanupService:
    global _cleanup_service
    if _cleanup_service is None:
        if bot is None:
            from app.bot.client import get_bot
            bot = get_bot()
        _cleanup_service = CleanupService(bot)
    return _cleanup_service
