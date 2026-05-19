"""
One-time backfill: reads existing messages from VAULT_CHANNEL_ID and inserts
them into the vault collection so the scheduler can distribute them.

Usage:
    python scripts/backfill_vault.py
"""

import asyncio
from datetime import datetime, timezone

from pymongo import UpdateOne
from pymongo.errors import BulkWriteError
from pyrogram import Client

from app.config import settings
from app.core.database import DatabaseManager
from app.core.models import ModerationState

BACKFILL_PLAN = [
    {"dest": "nsfw",    "limit": 80},
    {"dest": "premium", "limit": 50},
]


async def backfill(bot: Client, dest: str, limit: int) -> None:
    db = DatabaseManager.get_db()
    vault = db[settings.VAULT_COLLECTION]
    now = datetime.now(timezone.utc)

    operations = []
    count = 0

    print(f"\n[{dest.upper()}] Reading up to {limit} messages from vault channel {settings.VAULT_CHANNEL_ID}")

    async for msg in bot.get_chat_history(settings.VAULT_CHANNEL_ID, limit=limit):
        if not msg.media:
            continue

        try:
            media = getattr(msg, msg.media.value, None)
        except Exception:
            media = None

        file_id = getattr(media, "file_id", None) if media else None
        file_unique_id = getattr(media, "file_unique_id", None) if media else None
        media_type_str = msg.media.value if msg.media else "text"
        content_id = f"{settings.VAULT_CHANNEL_ID}_{msg.id}_{dest}"

        operations.append(UpdateOne(
            {"content_id": content_id},
            {
                "$setOnInsert": {
                    "content_id": content_id,
                    "source_chat_id": str(settings.VAULT_CHANNEL_ID),
                    "message_id": msg.id,
                    "media_group_id": msg.media_group_id,
                    "media_type": media_type_str,
                    "file_id": file_id,
                    "file_unique_id": file_unique_id,
                    "caption": msg.caption or msg.text or "",
                    "created_at": msg.date or now,
                    "usage_count": 0,
                    "last_posted_at": None,
                    "cooldown_until": None,
                    "submitter_user_id": None,
                },
                "$set": {
                    "moderation_destination": dest,
                    "status": ModerationState.QUEUED.value,
                    "distribution_state": "pending",
                    "vault_message_id": msg.id,
                    "vault_channel_id": str(settings.VAULT_CHANNEL_ID),
                    "updated_at": now,
                    "metadata": {
                        "has_spoiler": getattr(media, "has_spoiler", False) if media else False,
                        "date": msg.date.isoformat() if msg.date else None,
                    },
                },
            },
            upsert=True,
        ))
        count += 1

        if len(operations) >= 100:
            await _flush(vault, operations)
            operations.clear()
            print(f"  [{dest.upper()}] flushed batch, total so far: {count}")

    if operations:
        await _flush(vault, operations)

    print(f"  [{dest.upper()}] Done. {count} media messages queued for distribution.")


async def _flush(vault, operations):
    try:
        result = await vault.bulk_write(operations, ordered=False)
        print(f"  upserted={result.upserted_count} matched={result.matched_count}")
    except BulkWriteError as e:
        print(f"  bulk write partial error: {e.details['nInserted']} inserted")


async def main():
    await DatabaseManager.connect()

    bot = Client(
        name=settings.SESSION_NAME,
        bot_token=settings.BOT_TOKEN,
        api_id=settings.API_ID,
        api_hash=settings.API_HASH,
    )
    await bot.start()

    for plan in BACKFILL_PLAN:
        await backfill(bot, plan["dest"], plan["limit"])

    await bot.stop()
    await DatabaseManager.disconnect()
    print("\nBackfill complete. Scheduler will pick up content on next cycle (≤60s).")


if __name__ == "__main__":
    asyncio.run(main())