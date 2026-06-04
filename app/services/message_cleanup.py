"""
message_cleanup.py
──────────────────
Background scheduler: auto-deletes old tracked user-facing messages.

FIX (Bug #2): Previous code did _RETENTION_HOURS_FOR_QUERY = max(1, _RETENTION_SECONDS // 3600).
With a 30-minute retention (1800s): 1800 // 3600 = 0 → max(1, 0) = 1.
db.get_stale_messages(older_than_hours=1) never matched messages newer than 1 hour,
so the scheduler was effectively a no-op for any retention under 60 minutes.

ROOT CAUSE: The scheduler computed a UTC cutoff in Python but then discarded it
in favour of an integer hours value floored to 1. The fix: compute the cutoff
datetime directly in Python (using the exact _RETENTION_SECONDS value) and pass
it to db.get_stale_messages(older_than=cutoff). This removes the integer-division
rounding entirely and makes the query precise to the second.

REQUIRED DB CHANGE: Update get_stale_messages() in database/repository.py to
accept `older_than: datetime` instead of `older_than_hours: int`.

  Before:
    async def get_stale_messages(self, older_than_hours: int) -> list[dict]:
        cutoff = datetime.now(timezone.utc) - timedelta(hours=older_than_hours)
        ...WHERE created_at < :cutoff...

  After:
    async def get_stale_messages(self, older_than: datetime) -> list[dict]:
        ...WHERE created_at < :cutoff...   # pass older_than directly as :cutoff
"""

import asyncio
import logging
import os
from datetime import datetime, timedelta, timezone
from typing import Dict, List

from aiogram import Bot
from aiogram.exceptions import TelegramBadRequest

from database.repository import Database

log = logging.getLogger("message_cleanup")

_CHECK_INTERVAL = int(os.getenv("MESSAGE_CLEANUP_INTERVAL_SECONDS", "900"))

_raw_minutes = os.getenv("MESSAGE_CLEANUP_MINUTES", "").strip()
_raw_hours = os.getenv("MESSAGE_CLEANUP_HOURS", "").strip()

if _raw_minutes:
    _RETENTION_SECONDS = max(60, int(_raw_minutes)) * 60
elif _raw_hours:
    _RETENTION_SECONDS = max(1, int(_raw_hours)) * 3600
else:
    _RETENTION_SECONDS = 30 * 60  # default: 30 minutes

_ALREADY_DELETED = frozenset({
    "message to delete not found",
    "message can't be deleted",
    "message cant be deleted",
    "message is too old",
})


async def _run_cleanup(bot: Bot, db: Database) -> None:
    # FIX: compute the cutoff datetime precisely in Python.
    # No integer-division rounding, no forced 1-hour floor.
    cutoff = datetime.now(timezone.utc) - timedelta(seconds=_RETENTION_SECONDS)
    stale = await db.get_stale_messages(older_than=cutoff)

    log.info("[CLEANUP] Found %d stale message(s) to delete.", len(stale))

    by_user: Dict[int, List[int]] = {}
    for msg in stale:
        by_user.setdefault(msg["user_id"], []).append(msg["message_id"])

    total_deleted = 0
    total_skipped = 0

    for user_id, msg_ids in by_user.items():
        confirmed_deleted: List[int] = []

        for msg_id in msg_ids:
            try:
                await bot.delete_message(chat_id=user_id, message_id=msg_id)
                confirmed_deleted.append(msg_id)
                total_deleted += 1
            except TelegramBadRequest as exc:
                lowered = str(exc).lower()
                if any(p in lowered for p in _ALREADY_DELETED):
                    # Already gone — still remove from tracking table.
                    confirmed_deleted.append(msg_id)
                    total_deleted += 1
                else:
                    total_skipped += 1
                    log.debug(
                        "[CLEANUP] Skipping msg %d for user %d: %s",
                        msg_id, user_id, exc,
                    )
            except Exception as exc:
                total_skipped += 1
                log.debug(
                    "[CLEANUP] Error deleting msg %d user %d: %s",
                    msg_id, user_id, exc,
                )

            await asyncio.sleep(0.05)

        if confirmed_deleted:
            await db.mark_user_messages_deleted(user_id, confirmed_deleted)

    log.info(
        "[CLEANUP] Done. Deleted=%d | Skipped=%d",
        total_deleted, total_skipped,
    )


async def message_cleanup_scheduler(bot: Bot, db: Database) -> None:
    cutoff_repr = f"{_RETENTION_SECONDS}s"
    if _RETENTION_SECONDS >= 3600 and _RETENTION_SECONDS % 3600 == 0:
        cutoff_repr = f"{_RETENTION_SECONDS // 3600}h"
    elif _RETENTION_SECONDS >= 60 and _RETENTION_SECONDS % 60 == 0:
        cutoff_repr = f"{_RETENTION_SECONDS // 60}m"

    log.info(
        "[CLEANUP] Scheduler started. Interval=%ds | Retention=%s",
        _CHECK_INTERVAL, cutoff_repr,
    )

    while True:
        try:
            await asyncio.sleep(_CHECK_INTERVAL)
            await _run_cleanup(bot, db)
        except asyncio.CancelledError:
            log.info("[CLEANUP] Scheduler cancelled — shutting down.")
            break
        except Exception as exc:
            log.error("[CLEANUP] Unexpected error: %s", exc, exc_info=True)
            await asyncio.sleep(60)