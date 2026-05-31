"""
One-time backfill script to fix vault documents where `vault_message_id` is None.

PROBLEM:
  A previous bug caused content to be archived to the vault collection without
  storing the `vault_message_id` of the message copy in the VAULT_CHANNEL_ID.
  This makes it impossible to refresh the media's file_reference, leading to
  `FILE_REFERENCE_EXPIRED` errors for any downstream processing.

SOLUTION:
  This script iterates through the entire message history of the VAULT_CHANNEL_ID
  using manual chunked pagination (100 messages per request) with explicit
  FloodWait handling so it never crashes mid-run.

  For each message containing media, it extracts the `file_unique_id`. It then
  finds a document in the `vault` collection that has the same `file_unique_id`
  but where `vault_message_id` is `null`. It then updates that document with the
  correct `message.id` from the vault channel.

REQUIREMENT:
  - Must run as a USER client (your personal Telegram account), NOT the bot.
    Bots cannot call messages.GetHistory, a hard Telegram API restriction.
  - The user account must be a MEMBER of the VAULT_CHANNEL_ID before running.

FIRST RUN:
  Pyrogram will prompt for your phone number, password (if any), and OTP.
  A session file `backfill_user.session` will be created.
  On subsequent runs it reuses the saved session — no prompt needed.

USAGE:
  $env:PYTHONPATH = "."; python scripts/backfill_vault_message_ids.py
"""

import asyncio
import os
import sys

from pymongo import UpdateOne, operations
from pymongo.errors import BulkWriteError
from pyrogram import Client
from pyrogram.errors import FloodWait

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from app.config import settings
from app.utils.logger import get_logger

logger = get_logger(__name__)
from app.core.database import DatabaseManager

CHUNK_SIZE = 100        # messages per GetHistory request
BULK_BATCH = 200        # MongoDB bulk_write threshold
FLOOD_PAD  = 2          # extra seconds added to FloodWait sleep


async def main() -> None:
    logger.info("=" * 60)
    logger.info("  Vault Message ID Backfill Tool")
    logger.info("=" * 60)
    logger.info(f"  Vault Channel : {settings.VAULT_CHANNEL_ID}")
    logger.info(f"  MongoDB       : {settings.MONGO_URI.split('@')[-1]}")
    logger.info("=" * 60)

    if not settings.VAULT_CHANNEL_ID:
        logger.info("\n❌ VAULT_CHANNEL_ID is not set in your environment. Aborting.")
        return

    logger.info("\nConnecting to MongoDB...")
    await DatabaseManager.connect()
    db = DatabaseManager.get_db()
    vault_collection = db[settings.VAULT_COLLECTION]
    logger.info("MongoDB connected.\n")

    client = Client(
        name="backfill_user",
        api_id=settings.API_ID,
        api_hash=settings.API_HASH,
    )

    logger.info("Starting Pyrogram user client...")
    logger.info("You may be prompted for your phone number and OTP on first run.\n")
    await client.start()
    me = await client.get_me()
    logger.info(f"Logged in as: {me.first_name} (@{me.username} id={me.id})\n")

    try:
        vault_chat_id = int(settings.VAULT_CHANNEL_ID)
        chat = await client.get_chat(vault_chat_id)
        logger.info(f"Successfully accessed vault channel: '{chat.title}'\n")
    except Exception as e:
        logger.info(f"\n❌ Could not access VAULT_CHANNEL_ID ({settings.VAULT_CHANNEL_ID}).")
        logger.info(f"   Error: {e}")
        logger.info("   Ensure the logged-in user is a member of the channel.")
        await client.stop()
        await DatabaseManager.disconnect()
        return

    logger.info("Starting backfill process. This may take a while for large channels...")
    logger.info("FloodWait errors will be handled automatically — do not interrupt.\n")

    operations    = []
    processed     = 0
    updated       = 0
    offset_id     = 0   # walk backwards from newest; 0 = start from top

    while True:
        chunk = []
        retrying = True

        while retrying:
            retrying = False
            try:
                async for msg in client.get_chat_history(
                    vault_chat_id,
                    limit=CHUNK_SIZE,
                    offset_id=offset_id
                ):
                    chunk.append(msg)
            except FloodWait as e:
                wait = e.value + FLOOD_PAD
                logger.info(f"  [FloodWait] Sleeping {wait}s — will resume from same offset...")
                await asyncio.sleep(wait)
                chunk = []
                retrying = True

        # If after retries the chunk is still empty, we are done.
        if not chunk:
            break

        for message in chunk:
            processed += 1
            if processed % 500 == 0:
                logger.info(f"  ...scanned {processed} messages, found {updated} matches...")

            media = None
            if message.media:
                try:
                    media = getattr(message, message.media.value, None)
                except Exception:
                    continue

            if not media or not hasattr(media, "file_unique_id"):
                continue

            operations.append(UpdateOne(
                {"file_unique_id": media.file_unique_id, "vault_message_id": None},
                {"$set": {
                    "vault_message_id": message.id,
                    "vault_channel_id": str(vault_chat_id)
                }}
            ))

            if len(operations) >= BULK_BATCH:
                try:
                    result = await vault_collection.bulk_write(operations, ordered=False)
                    updated += result.modified_count
                except BulkWriteError as bwe:
                    updated += bwe.details.get("nModified", 0)
                operations.clear()

        offset_id = chunk[-1].id

        if len(chunk) < CHUNK_SIZE:
            break

    # ── Final flush for any remaining operations ──────────────────────────
    if operations:
        try:
            result = await vault_collection.bulk_write(operations, ordered=False)
            updated += result.modified_count
        except BulkWriteError as bwe:
            updated += bwe.details.get("nModified", 0)

    logger.info("\n" + "=" * 60)
    logger.info("✅ Backfill Complete.")
    logger.info(f"  Total messages scanned : {processed}")
    logger.info(f"  Documents updated      : {updated}")
    logger.info("=" * 60)

    await client.stop()
    await DatabaseManager.disconnect()


if __name__ == "__main__":
    asyncio.run(main())
