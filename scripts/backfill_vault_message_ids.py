"""
One-time backfill script to fix vault documents where `vault_message_id` is None.

PROBLEM:
  A previous bug caused content to be archived to the vault collection without
  storing the `vault_message_id` of the message copy in the VAULT_CHANNEL_ID.
  This makes it impossible to refresh the media's file_reference, leading to
  `FILE_REFERENCE_EXPIRED` errors for any downstream processing.

SOLUTION:
  This script iterates through the entire message history of the VAULT_CHANNEL_ID.
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

USAGE:
  $env:PYTHONPATH = "."; py scripts/backfill_vault_message_ids.py
"""

import asyncio
import os
import sys

from pymongo import UpdateOne
from pymongo.errors import BulkWriteError
from pyrogram import Client

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from app.config import settings
from app.core.database import DatabaseManager


async def main() -> None:
    print("=" * 60)
    print("  Vault Message ID Backfill Tool")
    print("=" * 60)
    print(f"  Vault Channel : {settings.VAULT_CHANNEL_ID}")
    print(f"  MongoDB       : {settings.MONGO_URI.split('@')[-1]}")
    print("=" * 60)

    if not settings.VAULT_CHANNEL_ID:
        print("\n❌ VAULT_CHANNEL_ID is not set in your environment. Aborting.")
        return

    print("\nConnecting to MongoDB...")
    await DatabaseManager.connect()
    db = DatabaseManager.get_db()
    vault_collection = db[settings.VAULT_COLLECTION]
    print("MongoDB connected.\n")

    client = Client(
        name="backfill_user",
        api_id=settings.API_ID,
        api_hash=settings.API_HASH,
    )

    print("Starting Pyrogram user client...")
    print("You may be prompted for your phone number and OTP on first run.\n")
    await client.start()
    me = await client.get_me()
    print(f"Logged in as: {me.first_name} (@{me.username} id={me.id})\n")

    try:
        vault_chat_id = int(settings.VAULT_CHANNEL_ID)
        chat = await client.get_chat(vault_chat_id)
        print(f"Successfully accessed vault channel: '{chat.title}'")
    except Exception as e:
        print(f"\n❌ Could not access VAULT_CHANNEL_ID ({settings.VAULT_CHANNEL_ID}).")
        print(f"   Error: {e}")
        print("   Ensure the logged-in user is a member of the channel.")
        await client.stop()
        await DatabaseManager.disconnect()
        return

    print("\nStarting backfill process. This may take a while for large channels...")
    operations = []
    processed_count = 0
    updated_count = 0

    async for message in client.get_chat_history(vault_chat_id, limit=0):
        processed_count += 1
        if processed_count % 500 == 0:
            print(f"  ...scanned {processed_count} messages, found {updated_count} matches to update...")

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
            {"$set": {"vault_message_id": message.id, "vault_channel_id": str(vault_chat_id)}}
        ))

        if len(operations) >= 200:
            try:
                result = await vault_collection.bulk_write(operations)
                updated_count += result.modified_count
            except BulkWriteError as bwe:
                updated_count += bwe.details.get("nModified", 0)
            operations.clear()

    if operations:
        try:
            result = await vault_collection.bulk_write(operations)
            updated_count += result.modified_count
        except BulkWriteError as bwe:
            updated_count += bwe.details.get("nModified", 0)

    print("\n" + "=" * 60)
    print("✅ Backfill Complete.")
    print(f"  Total messages scanned: {processed_count}")
    print(f"  Documents updated: {updated_count}")
    print("=" * 60)

    await client.stop()
    await DatabaseManager.disconnect()

if __name__ == "__main__":
    asyncio.run(main())