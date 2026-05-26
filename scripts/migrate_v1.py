import asyncio
import hashlib
from datetime import datetime, timezone
from motor.motor_asyncio import AsyncIOMotorClient
from app.config import settings

async def migrate():
    print("Starting BDGW VaultFlow Schema Migration (v1)...")
    client = AsyncIOMotorClient(settings.MONGO_URI)
    db = client[settings.MONGO_DB_NAME]
    queue = db[settings.QUEUE_COLLECTION]
    vault = db[settings.VAULT_COLLECTION]

    # 1. Update all jobs to schema_version 1
    result = await queue.update_many(
        {"schema_version": {"$exists": False}},
        {"$set": {"schema_version": 1, "migration_version": 0}}
    )
    print(f"Updated {result.modified_count} jobs to schema_version 1")

    # 2. Fix content_id and references
    cursor = queue.find({"status": {"$in": ["pending", "watermarking", "ready", "locked", "processing"]}})
    async for job in cursor:
        job_id = job["_id"]
        meta = job.get("metadata", {})
        
        source_chat_id = meta.get("source_chat_id")
        source_msg_id = meta.get("source_message_id") or job.get("source_message_id")
        
        if not source_chat_id or not source_msg_id:
            print(f"Job {job_id} missing source refs. Quarantining.")
            await queue.update_one({"_id": job_id}, {"$set": {"status": "quarantine", "quarantine_reason": "missing_source_refs"}})
            continue

        # Deterministic content_id
        raw = f"{source_chat_id}:{source_msg_id}:none"
        new_content_id = hashlib.sha256(raw.encode()).hexdigest()
        
        # Check vault reference
        vault_msg_id = job.get("vault_message_id")
        if not vault_msg_id:
            # Try to find in vault
            v_doc = await vault.find_one({"source_chat_id": str(source_chat_id), "source_message_id": source_msg_id})
            if v_doc:
                vault_msg_id = v_doc.get("vault_message_id")
        
        if not vault_msg_id:
            print(f"Job {job_id} missing vault ref. Quarantining.")
            await queue.update_one({"_id": job_id}, {"$set": {"status": "quarantine", "quarantine_reason": "unrecoverable_vault_reference"}})
            continue

        # Update job
        await queue.update_one(
            {"_id": job_id},
            {
                "$set": {
                    "content_id": new_content_id,
                    "vault_message_id": vault_msg_id,
                    "vault_chat_id": int(settings.VAULT_CHANNEL_ID),
                    "updated_at": datetime.now(timezone.utc)
                }
            }
        )

    print("Migration complete.")

if __name__ == "__main__":
    asyncio.run(migrate())
