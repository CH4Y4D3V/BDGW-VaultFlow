import asyncio
import logging
import os
from datetime import datetime, timezone

from pyrogram import Client
from pyrogram.errors import BadRequest

from app.core.database import DatabaseManager

log = logging.getLogger("chat_cleanup")


def _cleanup_interval_hours() -> int:
    raw = os.getenv("CHAT_CLEANUP_INTERVAL_HOURS", "")
    try:
        return max(1, int(raw)) if raw else 12
    except ValueError:
        return 12


async def _wipe_user_dm(client: Client, user_id: int) -> tuple[int, int]:
    db = DatabaseManager.get_db()
    
    # Using direct Motor query
    messages_cursor = db["message_tracker"].find({"user_id": user_id, "is_deleted": False})
    messages = await messages_cursor.to_list(length=None)
    
    if not messages:
        return 0, 0

    deleted, failed = 0, 0
    msg_ids = [m["message_id"] for m in messages]
    
    try:
        await client.delete_messages(chat_id=user_id, message_ids=msg_ids)
        deleted = len(msg_ids)
    except Exception as exc:
        log.warning(
            "[CLEANUP] Bulk delete failed for user %d: %s. Trying individual...",
            user_id, exc,
        )
        for msg_id in msg_ids:
            try:
                await client.delete_messages(chat_id=user_id, message_ids=msg_id)
                deleted += 1
            except BadRequest:
                failed += 1
            except Exception:
                failed += 1

    await db["message_tracker"].update_many(
        {"user_id": user_id, "message_id": {"$in": msg_ids}},
        {"$set": {"is_deleted": True, "deleted_at": datetime.now(timezone.utc)}}
    )
    return deleted, failed


async def chat_cleanup_loop(client: Client) -> None:
    interval_hours = _cleanup_interval_hours()
    interval_seconds = interval_hours * 3600

    log.info("[CLEANUP] Scheduled DM wipe started. Interval: every %dh.", interval_hours)

    while True:
        try:
            await asyncio.sleep(interval_seconds)
            
            db = DatabaseManager.get_db()
            user_ids = await db["users"].distinct("_id")
            
            total_deleted = 0
            total_users = 0

            for user_id in user_ids:
                deleted, failed = await _wipe_user_dm(client, user_id)
                if deleted or failed:
                    total_users += 1
                    total_deleted += deleted
                    log.debug(
                        "[CLEANUP] User %d: %d deleted, %d failed",
                        user_id, deleted, failed,
                    )

            log.info(
                "[CLEANUP] Cycle complete. %d messages wiped across %d users.",
                total_deleted, total_users,
            )

        except asyncio.CancelledError:
            log.info("[CLEANUP] Cancelled — shutting down.")
            break
        except Exception as exc:
            log.error("[CLEANUP] Unexpected error: %s", exc, exc_info=True)
            await asyncio.sleep(300)
