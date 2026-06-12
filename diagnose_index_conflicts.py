"""
Read-only diagnostic for the two unique-index creation failures seen at boot:

    [err] Index creation failed for collection: 'user_topics'
    [err] Index creation error for payments

Both indexes (user_topics.user_id unique, payments.txid unique-sparse) are
almost certainly failing because of pre-existing duplicate values in the
live data (E11000 DuplicateKeyError during index build). This script makes
NO writes — it only reports what's duplicated so you can decide how to
clean it up.

Usage:
    python diagnose_index_conflicts.py

Requires MONGO_URI / MONGO_DB_NAME env vars (same as the bot's .env).
"""
import asyncio
import os

from motor.motor_asyncio import AsyncIOMotorClient


async def main() -> None:
    uri = os.environ["MONGO_URI"]
    db_name = os.environ.get("MONGO_DB_NAME", "vaultflow")

    client = AsyncIOMotorClient(uri)
    db = client[db_name]

    print(f"Connected to database: {db_name}\n")

    # ── user_topics: unique index on user_id ────────────────────────────────
    print("=" * 60)
    print("user_topics — duplicate user_id values")
    print("=" * 60)
    pipeline = [
        {"$group": {"_id": "$user_id", "count": {"$sum": 1}, "doc_ids": {"$push": "$_id"}}},
        {"$match": {"count": {"$gt": 1}}},
        {"$sort": {"count": -1}},
        {"$limit": 25},
    ]
    dupes = await db["user_topics"].aggregate(pipeline).to_list(length=25)
    if not dupes:
        print("No duplicate user_id values found. (Index failure may be a "
              "different cause — check the raw JSON log 'ctx_error' field.)")
    else:
        total = await db["user_topics"].count_documents({})
        print(f"Total documents: {total}")
        print(f"Distinct user_ids with duplicates: {len(dupes)} (showing up to 25)\n")
        for d in dupes:
            print(f"  user_id={d['_id']}  count={d['count']}  doc_ids={d['doc_ids']}")

    # Also check for docs missing user_id entirely (would collide on null)
    missing = await db["user_topics"].count_documents({"user_id": {"$exists": False}})
    null_count = await db["user_topics"].count_documents({"user_id": None})
    print(f"\nDocs missing user_id field: {missing}")
    print(f"Docs with user_id == null: {null_count}")

    # ── payments: unique-sparse index on txid ───────────────────────────────
    print("\n" + "=" * 60)
    print("payments — duplicate txid values (non-null)")
    print("=" * 60)
    pipeline = [
        {"$match": {"txid": {"$ne": None, "$exists": True}}},
        {"$group": {"_id": "$txid", "count": {"$sum": 1}, "doc_ids": {"$push": "$_id"}}},
        {"$match": {"count": {"$gt": 1}}},
        {"$sort": {"count": -1}},
        {"$limit": 25},
    ]
    dupes = await db["payments"].aggregate(pipeline).to_list(length=25)
    if not dupes:
        print("No duplicate non-null txid values found. (Index failure may be "
              "a different cause — check the raw JSON log 'ctx_error' field.)")
    else:
        total = await db["payments"].count_documents({})
        print(f"Total documents: {total}")
        print(f"Distinct txids with duplicates: {len(dupes)} (showing up to 25)\n")
        for d in dupes:
            print(f"  txid={d['_id']!r}  count={d['count']}  doc_ids={d['doc_ids']}")

    # Empty-string txid is NOT excluded by sparse — flag it separately
    empty_count = await db["payments"].count_documents({"txid": ""})
    print(f"\nDocs with txid == '' (empty string, NOT excluded by sparse): {empty_count}")

    client.close()


if __name__ == "__main__":
    asyncio.run(main())
