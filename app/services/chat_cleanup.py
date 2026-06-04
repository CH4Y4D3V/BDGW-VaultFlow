# services/chat_cleanup.py

import asyncio
import logging
import os

from aiogram import Bot
from aiogram.exceptions import TelegramBadRequest
from database.repository import Database
from config import BotConfig

log = logging.getLogger("chat_cleanup")


def _cleanup_interval_hours() -> int:
    raw = os.getenv("CHAT_CLEANUP_INTERVAL_HOURS", "")
    try:
        return max(1, int(raw)) if raw else 12
    except ValueError:
        return 12


async def _wipe_user_dm(bot: Bot, db: Database, user_id: int) -> tuple[int, int]:
    msg_ids = await db.get_tracked_message_ids(user_id)
    if not msg_ids:
        return 0, 0

    deleted, failed = 0, 0
    for msg_id in msg_ids:
        try:
            await bot.delete_message(chat_id=user_id, message_id=msg_id)
            deleted += 1
        except TelegramBadRequest:
            failed += 1
        except Exception as exc:
            log.warning(
                "[CLEANUP] Could not delete msg %d for user %d: %s",
                msg_id, user_id, exc,
            )
            failed += 1

    await db.clear_tracked_messages(user_id)
    return deleted, failed


async def chat_cleanup_loop(bot: Bot, db: Database, settings: BotConfig) -> None:
    interval_hours = _cleanup_interval_hours()
    interval_seconds = interval_hours * 3600

    log.info("[CLEANUP] Scheduled DM wipe started. Interval: every %dh.", interval_hours)

    while True:
        try:
            await asyncio.sleep(interval_seconds)

            user_ids = await db.get_all_user_ids()
            total_deleted = 0
            total_users = 0

            for user_id in user_ids:
                deleted, failed = await _wipe_user_dm(bot, db, user_id)
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