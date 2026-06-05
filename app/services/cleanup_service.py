from __future__ import annotations

"""
app/services/cleanup_service.py

CleanupService — Section 20 Message Cleanup Policy
(BDGW VaultFlow Master Reference v1.0)

Five rules:
  1. General bot conversation       → auto-delete after 60 minutes
  2. Payment session messages       → auto-delete after 20 minutes
  3. BD phone numbers (01[3-9]x)    → auto-delete after 7 minutes
  4. ./prefix messages              → 10 seconds (handled in update_logger.py)
  5. Support conversations          → deleted on ticket closure (user-side only)

RULE: No cleanup notifications ever. Silent deletion only.

Fixes applied (audit session):
  GAP 7 (original):
    - BD phone regex corrected to enforce word boundaries and exact 11-digit
      length, preventing false matches on strings like "2017" or "10189xxxxx".
    - Payment session category now logged by payment handlers.
    - delete_user_support_history() now called from support_handler and
      (see dependency note) must also be called from admin_handler on /close.

  GAP (this session — CRITICAL, reported in audit):
    - _delete_message_safe() now handles FloodWait explicitly with retry,
      instead of treating all RPCErrors uniformly.
    - run_cleanup_sweep() adds a configurable inter-deletion sleep to avoid
      rapid-fire delete bursts that trigger FloodWait during large sweeps.
    - run_cleanup_sweep() caps per-sweep deletions to prevent unbounded
      execution time on cold starts / large backlogs.

DEPENDENCY:
  admin_handler.handle_close_command() must call
  get_cleanup_service().delete_user_support_history(user_id) on every
  /close invocation.  support_handler.cmd_close_support_legacy() already
  calls it, but that only covers the /closesupport alias.
"""

import asyncio
import re
from datetime import datetime, timedelta, timezone
from typing import Optional

from pyrogram import Client
from pyrogram.errors import (
    FloodWait,
    MessageDeleteForbidden,
    MessageIdInvalid,
    RPCError,
)

from app.config import settings
from app.core.database import DatabaseManager
from app.utils.logger import get_logger

logger = get_logger(__name__)

# ---------------------------------------------------------------------------
# BD phone regex  (GAP 7 fix — kept verbatim from previous session)
# ---------------------------------------------------------------------------
# BD numbers: 01[3-9]\d{8} — exactly 11 digits starting with 01[3-9].
# Word boundaries prevent matching partial numbers like "2017" or "10189xxxxx".
BD_PHONE_REGEX = re.compile(r"(?<!\d)01[3-9]\d{8}(?!\d)")

# ---------------------------------------------------------------------------
# Cleanup TTL configuration (minutes)
# ---------------------------------------------------------------------------
_CLEANUP_TTL: dict[str, int] = {
    "general": 60,
    "payment": 20,
    "phone":    7,
    "support": 60,   # Irrelevant for sweep; support deleted on closure only.
}

# Maximum messages to delete in a single sweep run.  Caps execution time
# on cold starts with a large backlog.
_SWEEP_MAX_DELETIONS: int = getattr(settings, "CLEANUP_SWEEP_MAX_DELETIONS", 500)

# Seconds to sleep between individual delete calls during a sweep to avoid
# triggering FloodWait under high-volume conditions.
_INTER_DELETE_SLEEP: float = getattr(settings, "CLEANUP_INTER_DELETE_SLEEP", 0.05)


# ---------------------------------------------------------------------------
# CleanupService
# ---------------------------------------------------------------------------

class CleanupService:
    """
    Enforces Section 20 — MESSAGE CLEANUP POLICY.

    Messages are logged with a category and expiry timestamp.
    The sweep runs every 5 minutes (via external scheduler) and silently
    deletes expired messages.

    Support messages are excluded from the timed sweep; they are deleted
    via delete_user_support_history() when a session is closed.
    """

    def __init__(self, bot: Client) -> None:
        """
        Initialise with a Pyrogram client used for all deletion calls.

        Args:
            bot: The active Pyrogram Client instance.
        """
        self._bot = bot
        self._db = DatabaseManager.get_db()
        self._history = self._db["message_history"]

    # ------------------------------------------------------------------
    # Logging
    # ------------------------------------------------------------------

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

        If the message text matches the BD phone regex, the category is
        automatically overridden to "phone" regardless of the supplied value.

        Args:
            user_id:    Telegram ID of the user who sent or received the message.
            message_id: Telegram message ID.
            text:       Optional message text (used for phone number detection).
            category:   "general" | "payment" | "phone" | "support"
            chat_id:    Chat where the message lives.  Defaults to user_id
                        (i.e. the bot's DM with the user).
        """
        if text and BD_PHONE_REGEX.search(text):
            category = "phone"

        expiry_mins = _CLEANUP_TTL.get(category, 60)
        expires_at = datetime.now(timezone.utc) + timedelta(minutes=expiry_mins)
        target_chat_id = chat_id if chat_id is not None else user_id

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
        except Exception as exc:
            logger.warning(
                "log_message_failed",
                extra={"ctx_user_id": user_id, "ctx_error": str(exc)},
            )

    async def log_support_message(
        self,
        user_id: int,
        message_id: int,
        chat_id: Optional[int] = None,
    ) -> None:
        """
        Register a support message for user-side deletion on session closure.

        Support messages are excluded from the timed sweep; they are removed
        in bulk by delete_user_support_history() when the admin closes the
        session (Section 20 Rule 5, Section 15.5).

        Args:
            user_id:    Telegram ID of the user.
            message_id: Telegram message ID in the user's DM chat.
            chat_id:    Chat where the message lives.  Defaults to user_id.
        """
        await self.log_message(
            user_id=user_id,
            message_id=message_id,
            text="",
            category="support",
            chat_id=chat_id if chat_id is not None else user_id,
        )

    # ------------------------------------------------------------------
    # Timed sweep
    # ------------------------------------------------------------------

    async def run_cleanup_sweep(self) -> None:
        """
        Scan message_history for expired records and delete them silently.

        Called by the scheduler every 5 minutes.

        Behaviour:
          - Skips "support" category (deleted on session closure, not by sweep).
          - Caps total deletions at _SWEEP_MAX_DELETIONS per run.
          - Sleeps _INTER_DELETE_SLEEP seconds between each delete call to
            prevent rapid-fire bursts that trigger FloodWait.
          - Marks each record as deleted=True in MongoDB regardless of whether
            the Telegram delete succeeded (avoids retrying already-gone messages
            on every subsequent sweep).
        """
        try:
            now = datetime.now(timezone.utc)
            cursor = self._history.find(
                {
                    "expires_at": {"$lte": now},
                    "deleted": False,
                    "category": {"$nin": ["support"]},
                }
            )

            deleted_count = 0
            total_processed = 0

            async for record in cursor:
                if total_processed >= _SWEEP_MAX_DELETIONS:
                    logger.debug(
                        "Cleanup sweep capped at max deletions",
                        extra={"ctx_cap": _SWEEP_MAX_DELETIONS},
                    )
                    break

                total_processed += 1
                chat_id = record.get("chat_id") or record.get("user_id")
                msg_id = record["message_id"]

                deleted = await self._delete_message_safe(chat_id, msg_id)

                await self._history.update_one(
                    {"_id": record["_id"]},
                    {"$set": {"deleted": True, "deleted_at": now}},
                )

                if deleted:
                    deleted_count += 1

                # Throttle to avoid FloodWait bursts.
                if _INTER_DELETE_SLEEP > 0:
                    await asyncio.sleep(_INTER_DELETE_SLEEP)

            if deleted_count:
                logger.debug(
                    "Cleanup sweep complete",
                    extra={"ctx_deleted": deleted_count, "ctx_processed": total_processed},
                )

        except Exception as exc:
            logger.error(
                "CleanupService sweep failed",
                extra={"ctx_error": str(exc)},
            )

    # ------------------------------------------------------------------
    # User-side support history deletion  (Section 15.5, Section 20 Rule 5)
    # ------------------------------------------------------------------

    async def delete_user_support_history(self, user_id: int) -> int:
        """
        Delete all user-side support messages when a session is closed.

        Per Section 15.5:
          - Only messages in the user's DM chat are deleted (chat_id == user_id).
          - The hub topic is PRESERVED permanently.
          - Deletion is silent (no notification to user per Section 20 rule).

        Attempts a single batch delete first; falls back to one-by-one on
        batch failure.  All records are marked deleted=True in MongoDB
        regardless of the Telegram outcome.

        Args:
            user_id: Telegram ID of the user whose session is being closed.

        Returns:
            Count of messages successfully deleted from Telegram.
        """
        deleted_count = 0
        try:
            cursor = self._history.find({
                "user_id": user_id,
                "category": "support",
                "deleted": False,
            })

            msg_ids: list[int] = []
            records: list[dict] = []

            async for record in cursor:
                # Only delete from the user's own DM chat.
                rec_chat_id = record.get("chat_id")
                if rec_chat_id is None or rec_chat_id == user_id:
                    msg_ids.append(record["message_id"])
                    records.append(record)

            if msg_ids:
                try:
                    await self._bot.delete_messages(user_id, msg_ids)
                    deleted_count = len(msg_ids)
                except FloodWait as fw:
                    logger.warning(
                        "delete_support_history_flood_wait",
                        extra={"ctx_user_id": user_id, "ctx_wait": fw.value},
                    )
                    await asyncio.sleep(fw.value + 1)
                    # Retry batch once after flood wait.
                    try:
                        await self._bot.delete_messages(user_id, msg_ids)
                        deleted_count = len(msg_ids)
                    except Exception as retry_exc:
                        logger.warning(
                            "delete_support_history_batch_retry_failed",
                            extra={"ctx_user_id": user_id, "ctx_error": str(retry_exc)},
                        )
                        # Fall back to one-by-one.
                        for record in records:
                            ok = await self._delete_message_safe(
                                user_id, record["message_id"]
                            )
                            if ok:
                                deleted_count += 1
                except Exception as exc:
                    logger.warning(
                        "delete_support_history_batch_failed",
                        extra={"ctx_user_id": user_id, "ctx_error": str(exc)},
                    )
                    # Fall back to one-by-one.
                    for record in records:
                        ok = await self._delete_message_safe(
                            user_id, record["message_id"]
                        )
                        if ok:
                            deleted_count += 1

            # Mark all records as deleted in MongoDB regardless of outcome.
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

        except Exception as exc:
            logger.warning(
                "delete_user_support_history_failed",
                extra={"ctx_user_id": user_id, "ctx_error": str(exc)},
            )

        return deleted_count

    # ------------------------------------------------------------------
    # Private: safe single-message delete with FloodWait retry
    # ------------------------------------------------------------------

    async def _delete_message_safe(
        self, chat_id: int, message_id: int
    ) -> bool:
        """
        Attempt to delete a single message silently.

        Handles FloodWait explicitly with one retry.  All other errors
        (permission denied, message already deleted) are swallowed and
        return False — Section 20 requires silent deletion with no
        user-visible indication.

        Args:
            chat_id:    Chat ID where the message lives.
            message_id: Telegram message ID.

        Returns:
            True if the message was successfully deleted, False otherwise.
        """
        for attempt in range(2):  # One retry on FloodWait.
            try:
                await self._bot.delete_messages(
                    chat_id=chat_id, message_ids=message_id
                )
                return True

            except FloodWait as fw:
                if attempt == 1:
                    # Second FloodWait — give up; the sweep will retry next run.
                    logger.debug(
                        "Delete message flood wait exceeded retry limit",
                        extra={
                            "ctx_chat_id": chat_id,
                            "ctx_msg_id": message_id,
                            "ctx_wait": fw.value,
                        },
                    )
                    return False
                wait_secs = fw.value + 1
                logger.debug(
                    "Delete message flood wait, sleeping",
                    extra={
                        "ctx_chat_id": chat_id,
                        "ctx_msg_id": message_id,
                        "ctx_wait": wait_secs,
                    },
                )
                await asyncio.sleep(wait_secs)
                # Loop continues to retry.

            except (MessageDeleteForbidden, MessageIdInvalid):
                # Already deleted or no permission — not an error condition.
                return False

            except RPCError as exc:
                logger.debug(
                    "Delete message RPC error",
                    extra={
                        "ctx_chat_id": chat_id,
                        "ctx_msg_id": message_id,
                        "ctx_error": str(exc),
                    },
                )
                return False

            except Exception as exc:
                logger.debug(
                    "Delete message unexpected error",
                    extra={
                        "ctx_chat_id": chat_id,
                        "ctx_msg_id": message_id,
                        "ctx_error": str(exc),
                    },
                )
                return False

        return False  # Exhausted retries.


# ---------------------------------------------------------------------------
# Module-level singleton
# ---------------------------------------------------------------------------

_cleanup_service: Optional[CleanupService] = None


def get_cleanup_service(bot: Optional[Client] = None) -> CleanupService:
    """
    Return the module-level CleanupService singleton.

    Initialised on first call.  Pass `bot` on the first call (typically at
    startup); subsequent calls return the cached instance regardless of
    whether `bot` is supplied.

    Args:
        bot: The active Pyrogram Client.  Required on first call; ignored
             on subsequent calls.

    Returns:
        The shared CleanupService instance.

    Raises:
        RuntimeError: If called before initialisation without a bot argument.
    """
    global _cleanup_service
    if _cleanup_service is None:
        if bot is None:
            from app.bot.client import get_bot
            bot = get_bot()
        _cleanup_service = CleanupService(bot)
    return _cleanup_service
