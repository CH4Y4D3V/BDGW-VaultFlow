"""
One-time backfill (and re-runnable top-up): reads ALL media from VAULT_CHANNEL_ID
and inserts them into the vault MongoDB collection so the scheduler can distribute them.

RULES:
  - Reads EVERYTHING from the vault channel — no hardcoded message limit.
  - Upserts are idempotent: re-running never creates duplicates.
  - If VAULT_CHANNEL_ID changes in .env, re-run — old content keeps distributing,
    new channel content gets added alongside it.
  - Daily posting rate is controlled exclusively by DAILY_CAP_NSFW / DAILY_CAP_PREMIUM
    in your .env — not by anything in this script.
  - Distribution order: oldest message first (chronological, top-to-bottom).

REQUIREMENT:
  Must run as a USER client (your personal Telegram account), NOT the bot.
  Bots cannot call messages.GetHistory — hard Telegram API restriction.
  Use the same API_ID and API_HASH from your .env — just don't pass bot_token.

FIRST RUN:
  Pyrogram will prompt for your phone number and the OTP Telegram sends you.
  A session file 'backfill_user_session.session' is created locally.
  Delete it after the backfill is complete if you want.

USAGE:
  $env:PYTHONPATH = "."; py scripts/backfill_vault.py

CONFIGURE BELOW:
  Set DEST to "nsfw", "premium", or "both".
  If your vault channel contains content for only one destination, set that one.
  If it contains content for both, set "both" — each message will be registered
  for both destinations (the daily cap still controls how many post per day).
"""

import asyncio
import os
import sys
from datetime import datetime, timezone

from pymongo import UpdateOne
from pymongo.errors import BulkWriteError
from pyrogram import Client

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from app.config import settings
from app.core.database import DatabaseManager
from app.core.models import ModerationState

# ── SET THIS ──────────────────────────────────────────────────────────────────
# "nsfw"    → all vault content goes to NSFW destination
# "premium" → all vault content goes to PREMIUM destination
# "both"    → register each item for both destinations
DEST = "nsfw"
# ─────────────────────────────────────────────────────────────────────────────


async def backfill_dest(client: Client, dest: str) -> None:
    db = DatabaseManager.get_db()
    vault = db[settings.VAULT_COLLECTION]
    now = datetime.now(timezone.utc)

    operations = []
    total_media = 0
    total_skipped = 0

    print(f"\n[{dest.upper()}] Reading ALL messages from vault channel {settings.VAULT_CHANNEL_ID}")
    print(f"[{dest.upper()}] This may take a while for large channels...")

    # limit=0 means no limit — reads entire channel history
    async for msg in client.get_chat_history(settings.VAULT_CHANNEL_ID, limit=0):
        if not msg.media:
            total_skipped += 1
            continue

        try:
            media = getattr(msg, msg.media.value, None)
        except Exception:
            media = None

        file_id        = getattr(media, "file_id",        None)  if media else None
        file_unique_id = getattr(media, "file_unique_id", None)  if media else None
        media_type_str = msg.media.value if msg.media else "text"

        # content_id includes vault_channel_id so if the channel changes,
        # old IDs are untouched and new channel content gets new IDs.
        content_id = f"{settings.VAULT_CHANNEL_ID}_{msg.id}_{dest}"

        operations.append(UpdateOne(
            {"content_id": content_id},
            {
                "$setOnInsert": {
                    "content_id":       content_id,
                    "source_chat_id":   str(settings.VAULT_CHANNEL_ID),
                    "message_id":       msg.id,
                    "media_group_id":   msg.media_group_id,
                    "media_type":       media_type_str,
                    "file_id":          file_id,
                    "file_unique_id":   file_unique_id,
                    "caption":          msg.caption or msg.text or "",
                    # msg.date is already timezone-aware from Pyrogram
                    "created_at":       msg.date or now,
                    "usage_count":      0,
                    "last_posted_at":   None,
                    "cooldown_until":   None,
                    "submitter_user_id": None,
                },
                "$set": {
                    "moderation_destination": dest,
                    "status":                 ModerationState.QUEUED.value,
                    # distribution_state drives the scheduler query
                    "distribution_state":     "pending",
                    "vault_message_id":       msg.id,
                    "vault_channel_id":       str(settings.VAULT_CHANNEL_ID),
                    "updated_at":             now,
                    "metadata": {
                        "has_spoiler": getattr(media, "has_spoiler", False) if media else False,
                        "date":        msg.date.isoformat() if msg.date else None,
                    },
                },
            },
            upsert=True,
        ))
        total_media += 1

        # Flush every 100 to avoid large in-memory batches
        if len(operations) >= 100:
            await _flush(vault, operations, dest)
            operations.clear()
            print(f"  [{dest.upper()}] ...{total_media} media processed so far")

    # Flush remainder
    if operations:
        await _flush(vault, operations, dest)

    print(
        f"\n[{dest.upper()}] ✅ Complete.\n"
        f"  Media registered : {total_media}\n"
        f"  Non-media skipped: {total_skipped}\n"
        f"  Daily cap        : {_daily_cap(dest)} posts/day\n"
        f"  Est. days to post all: "
        f"{'∞ (cap=0?)' if not _daily_cap(dest) else round(total_media / _daily_cap(dest), 1)}"
    )


def _daily_cap(dest: str) -> int:
    if dest == "nsfw":
        return getattr(settings, "DAILY_CAP_NSFW", 75)
    if dest == "premium":
        return getattr(settings, "DAILY_CAP_PREMIUM", 140)
    return 0


async def _flush(vault, operations: list, dest: str) -> None:
    try:
        result = await vault.bulk_write(operations, ordered=False)
        if result.upserted_count:
            print(f"  [{dest.upper()}] inserted {result.upserted_count} new, "
                  f"matched {result.matched_count} existing (skipped)")
    except BulkWriteError as e:
        inserted = e.details.get("nInserted", "?")
        print(f"  [{dest.upper()}] partial write — {inserted} inserted (duplicates silently skipped)")


async def main() -> None:
    print("=" * 60)
    print("  VaultFlow Backfill Tool")
    print("=" * 60)
    print(f"  Vault channel : {settings.VAULT_CHANNEL_ID}")
    print(f"  Destination(s): {DEST}")
    print(f"  MongoDB       : {settings.MONGO_URI.split('@')[-1]}")
    print("=" * 60)

    print("\nConnecting to MongoDB...")
    await DatabaseManager.connect()
    print("MongoDB connected.\n")

    # User client — no bot_token
    client = Client(
        name="backfill_user_session",
        api_id=settings.API_ID,
        api_hash=settings.API_HASH,
    )

    print("Starting Pyrogram user client...")
    print("You will be prompted for your phone number and OTP on first run.\n")
    await client.start()
    me = await client.get_me()
    print(f"Logged in as: {me.first_name} (@{me.username}  id={me.id})\n")

    if DEST == "both":
        await backfill_dest(client, "nsfw")
        await backfill_dest(client, "premium")
    else:
        await backfill_dest(client, DEST)

    await client.stop()
    await DatabaseManager.disconnect()

    print("\n" + "=" * 60)
    print("  Backfill complete.")
    print("  Scheduler picks up queued content within 60 seconds.")
    print("  Daily cap in .env controls posting rate.")
    print("  You can delete 'backfill_user_session.session' now.")
    print("=" * 60)


if __name__ == "__main__":
    asyncio.run(main())
