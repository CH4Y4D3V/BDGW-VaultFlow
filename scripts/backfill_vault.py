"""
One-time backfill (and re-runnable top-up): reads ALL media from the appropriate
source channel and inserts them into the vault MongoDB collection.

CHANNEL ROUTING (from .env):
  DEST = "nsfw"    → reads from VAULT_CHANNEL_ID
  DEST = "premium" → reads from PREMIUM_CHANNEL_ID
  DEST = "both"    → reads VAULT_CHANNEL_ID for nsfw, PREMIUM_CHANNEL_ID for premium

RULES:
  - Reads EVERYTHING from each channel — no hardcoded message limit.
  - Upserts are idempotent: re-running never creates duplicates.
  - content_id = {channel_id}_{msg_id}_{dest} — nsfw and premium never collide.
  - Daily posting rate is controlled by DAILY_CAP_NSFW / DAILY_CAP_PREMIUM in .env.
  - Distribution order: oldest message first (chronological).

REQUIREMENT:
  Must run as a USER client (your personal Telegram account), NOT the bot.
  Bots cannot call messages.GetHistory — hard Telegram API restriction.
  The user account must be a MEMBER of both channels before running.

FIRST RUN:
  Pyrogram prompts for phone number + OTP. Session saved as
  'backfill_user_session.session'. Delete it after backfill if you want.

USAGE:
  $env:PYTHONPATH = "."; py scripts/backfill_vault.py
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
# "nsfw"    → reads VAULT_CHANNEL_ID   → registers as nsfw
# "premium" → reads PREMIUM_CHANNEL_ID → registers as premium
# "both"    → runs both sequentially (nsfw first, then premium)
DEST = "premium"
# ─────────────────────────────────────────────────────────────────────────────


async def backfill_dest(client: Client, dest: str, channel_id: int) -> None:
    """
    Backfill one destination from its resolved channel_id.
    channel_id must already be resolved via get_chat() before calling this.
    """
    db = DatabaseManager.get_db()
    vault = db[settings.VAULT_COLLECTION]
    now = datetime.now(timezone.utc)

    operations = []
    total_media = 0
    total_skipped = 0

    print(f"\n[{dest.upper()}] Reading ALL messages from channel {channel_id}")
    print(f"[{dest.upper()}] This may take a while for large channels...")

    # limit=0 means no limit — reads entire channel history
    async for msg in client.get_chat_history(channel_id, limit=0):
        if not msg.media:
            total_skipped += 1
            continue

        try:
            media = getattr(msg, msg.media.value, None)
        except Exception:
            media = None

        file_id        = getattr(media, "file_id",        None) if media else None
        file_unique_id = getattr(media, "file_unique_id", None) if media else None
        media_type_str = msg.media.value if msg.media else "text"

        # content_id is scoped to channel + message + dest
        # nsfw and premium never collide even if sourced from the same channel
        content_id = f"{channel_id}_{msg.id}_{dest}"

        operations.append(UpdateOne(
            {"content_id": content_id},
            {
                "$setOnInsert": {
                    "content_id":        content_id,
                    "source_chat_id":    str(channel_id),
                    "source_message_id": msg.id,
                    "media_group_id":    msg.media_group_id,
                    "media_type":        media_type_str,
                    "file_id":           file_id,
                    "file_unique_id":    file_unique_id,
                    "caption":           msg.caption or msg.text or "",
                    "created_at":        msg.date or now,
                    "usage_count":       0,
                    "last_posted_at":    None,
                    "cooldown_until":    None,
                    "submitter_user_id": None,
                },
                "$set": {
                    "source_chat_id":       str(channel_id),
                    "source_message_id":    msg.id,
                    "moderation_destination": dest,
                    "status":                 ModerationState.QUEUED.value,
                    "distribution_state":     "pending",
                    "vault_message_id":       msg.id,
                    "vault_channel_id":       str(channel_id),
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

    cap = _daily_cap(dest)
    print(
        f"\n[{dest.upper()}] ✅ Complete.\n"
        f"  Media registered : {total_media}\n"
        f"  Non-media skipped: {total_skipped}\n"
        f"  Daily cap        : {cap} posts/day\n"
        f"  Est. days to post all: "
        f"{'∞ (cap=0?)' if not cap else round(total_media / cap, 1)}"
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


async def _resolve_channel(client: Client, raw_id, label: str) -> int | None:
    """
    Cast raw_id to int and force-resolve the peer via get_chat().
    Returns the resolved int ID, or None on failure.
    Passing a string "-100xxx" to Pyrogram routes through phone/username
    resolution and always fails — we must cast to int first.
    """
    try:
        channel_id = int(raw_id)
    except (TypeError, ValueError):
        print(f"❌ [{label}] Invalid channel ID in .env: {raw_id!r}")
        return None

    print(f"  Resolving {label} ({channel_id})...", end=" ", flush=True)
    try:
        chat = await client.get_chat(channel_id)
        print(f"✅  '{chat.title}' (type={chat.type})")
        return chat.id  # use Pyrogram's confirmed ID (now cached in session)
    except Exception as e:
        print(f"\n  ❌ Cannot resolve {label} ({channel_id}): {e}")
        print(f"     Make sure the logged-in account is a MEMBER of this channel.")
        return None


async def main() -> None:
    print("=" * 60)
    print("  VaultFlow Backfill Tool")
    print("=" * 60)
    print(f"  Destination(s)  : {DEST}")
    print(f"  NSFW channel    : {settings.VAULT_CHANNEL_ID}  (VAULT_CHANNEL_ID)")
    print(f"  Premium channel : {settings.PREMIUM_CHANNEL_ID}  (PREMIUM_CHANNEL_ID)")
    print(f"  MongoDB         : {settings.MONGO_URI.split('@')[-1]}")
    print("=" * 60)

    if DEST not in ("nsfw", "premium", "both"):
        print(f'\n❌ Invalid DEST={DEST!r}. Must be "nsfw", "premium", or "both".')
        return

    print("\nConnecting to MongoDB...")
    await DatabaseManager.connect()
    print("MongoDB connected.\n")

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

    # ✅ FIX: get_dialogs() is async generator — must iterate, not await
    print("Syncing dialogs (this caches all your channels)...")
    async for dialog in client.get_dialogs():
        title = (dialog.chat.title or "").lower()
        if "premium" in title or "vault" in title or "wild" in title:
            print(f"  Found: '{dialog.chat.title}' → id={dialog.chat.id}")
    print("Dialogs synced.\n")

    if DEST in ("nsfw", "both"):
        nsfw_id = await _resolve_channel(client, settings.VAULT_CHANNEL_ID, "VAULT_CHANNEL_ID")
        if nsfw_id is None:
            await client.stop()
            await DatabaseManager.disconnect()
            return

    if DEST in ("premium", "both"):
        premium_id = await _resolve_channel(client, settings.PREMIUM_CHANNEL_ID, "PREMIUM_CHANNEL_ID")
        if premium_id is None:
            await client.stop()
            await DatabaseManager.disconnect()
            return

    # ── Run backfill(s) ──────────────────────────────────────────────────────
    if DEST == "nsfw":
        await backfill_dest(client, "nsfw", nsfw_id)
    elif DEST == "premium":
        await backfill_dest(client, "premium", premium_id)
    elif DEST == "both":
        await backfill_dest(client, "nsfw",    nsfw_id)
        await backfill_dest(client, "premium", premium_id)

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