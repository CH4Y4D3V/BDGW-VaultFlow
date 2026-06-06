"""
message_cleanup.py
──────────────────
Background scheduler: auto-deletes old tracked user-facing messages.
"""

import asyncio
import logging
import os
from datetime import datetime, timedelta, timezone
from typing import Dict, List

from pyrogram import Client
from pyrogram.errors import BadRequest

from app.core.database import DatabaseManager

log = logging.getLogger("message_cleanup")

_CHECK_INTERVAL = int(os.getenv("MESSAGE_CLEANUP_INTERVAL_SECONDS", "300"))

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


async def _run_cleanup(client: Client) -> None:
    db = DatabaseManager.get_db()
    cutoff = datetime.now(timezone.utc) - timedelta(seconds=_RETENTION_SECONDS)
    
    stale_cursor = db["message_tracker"].find({
        "is_deleted": False,
        "created_at": {"$lt": cutoff}
    })
    stale = await stale_cursor.to_list(length=None)

    if not stale:
        return

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
                await client.delete_messages(chat_id=user_id, message_ids=msg_id)
                confirmed_deleted.append(msg_id)
                total_deleted += 1
            except BadRequest as exc:
                lowered = str(exc).lower()
                if any(p in lowered for p in _ALREADY_DELETED):
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
            await db["message_tracker"].update_many(
                {"user_id": user_id, "message_id": {"$in": confirmed_deleted}},
                {"$set": {"is_deleted": True, "deleted_at": datetime.now(timezone.utc)}}
            )

    log.info(
        "[CLEANUP] Done. Deleted=%d | Skipped=%d",
        total_deleted, total_skipped,
    )


async def message_cleanup_scheduler(client: Client) -> None:
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
            await _run_cleanup(client)
        except asyncio.CancelledError:
            log.info("[CLEANUP] Scheduler cancelled — shutting down.")
            break
        except Exception as exc:
            log.error("[CLEANUP] Unexpected error: %s", exc, exc_info=True)
            await asyncio.sleep(60)
