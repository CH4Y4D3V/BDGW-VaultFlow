from datetime import datetime, timezone
from typing import Optional
from motor.motor_asyncio import AsyncIOMotorClient, AsyncIOMotorDatabase
from pymongo import IndexModel, ASCENDING, DESCENDING
from app.config import settings
from app.core.logger import get_logger

logger = get_logger(__name__)


class DataMigrationManager:
    """
    Handles database stabilization and migration auditing during startup.
    Ensures legacy data is repaired or quarantined before strict indexes are applied.
    """

    @classmethod
    async def stabilize_queue(cls, db: AsyncIOMotorDatabase) -> None:
        """
        Audit and stabilize the queue collection to prevent index creation failures.
        """
        logger.info("MIGRATION: Starting queue stabilization audit...")
        
        queue = db[settings.QUEUE_COLLECTION]
        quarantine = db[settings.QUARANTINE_COLLECTION]
        
        # 1. Detect and Quarantine NULL Vault References in Active Jobs
        # These are the primary cause of E11000 duplicate key errors on unique indexes.
        active_statuses = ["pending", "processing", "locked", "watermarking", "ready", "delivering"]
        
        invalid_ref_query = {
            "status": {"$in": active_statuses},
            "$or": [
                {"vault_chat_id": None},
                {"vault_message_id": None},
                {"vault_chat_id": 0},
                {"vault_message_id": 0}
            ]
        }
        
        invalid_count = await queue.count_documents(invalid_ref_query)
        if invalid_count > 0:
            logger.warning(
                "MIGRATION: Detected active jobs with null/invalid vault references. Quarantining...",
                extra={"ctx_collection": settings.QUEUE_COLLECTION, "ctx_count": invalid_count}
            )
            
            # Move to quarantine before deletion to preserve history
            cursor = queue.find(invalid_ref_query)
            async for doc in cursor:
                doc["quarantine_reason"] = "migration_null_vault_reference"
                doc["quarantined_at"] = datetime.now(timezone.utc)
                doc["original_collection"] = settings.QUEUE_COLLECTION
                await quarantine.insert_one(doc)
            
            # Purge from active queue so index creation can proceed
            result = await queue.delete_many(invalid_ref_query)
            logger.info(
                "MIGRATION: Removed invalid jobs from active queue.",
                extra={"ctx_collection": settings.QUEUE_COLLECTION, "ctx_deleted": result.deleted_count}
            )

        # 2. Resolve Content ID Duplicates in Active Queue
        # Enforces uniqueness for content_id across all active states.
        pipeline = [
            {"$match": {"status": {"$in": active_statuses}}},
            {"$group": {"_id": "$content_id", "count": {"$sum": 1}, "ids": {"$push": "$_id"}}},
            {"$match": {"count": {"$gt": 1}}}
        ]
        
        content_dups = await queue.aggregate(pipeline).to_list(length=None)
        if content_dups:
            logger.warning(
                "MIGRATION: Detected duplicate active content groups. Resolving...",
                extra={"ctx_collection": settings.QUEUE_COLLECTION, "ctx_dups": len(content_dups)}
            )
            for dup in content_dups:
                # Keep the first one, quarantine others
                ids_to_quarantine = dup["ids"][1:]
                for doc_id in ids_to_quarantine:
                    doc = await queue.find_one({"_id": doc_id})
                    if doc:
                        doc["quarantine_reason"] = "migration_duplicate_content_id"
                        doc["quarantined_at"] = datetime.now(timezone.utc)
                        doc["original_collection"] = settings.QUEUE_COLLECTION
                        await quarantine.insert_one(doc)
                        await queue.delete_one({"_id": doc_id})
            logger.info(
                "MIGRATION: Resolved content_id conflicts.",
                extra={"ctx_collection": settings.QUEUE_COLLECTION, "ctx_resolved": len(content_dups)}
            )

        logger.info("MIGRATION: Queue stabilization complete.")

    @classmethod
    async def stabilize_vault(cls, db: AsyncIOMotorDatabase) -> None:
        """
        Audit and stabilize the vault collection before index creation.
        """
        logger.info("MIGRATION: Starting vault stabilization audit...")
        
        vault = db[settings.VAULT_COLLECTION]
        quarantine = db[settings.QUARANTINE_COLLECTION]
        
        # Detect and Quarantine NULL Vault References in Vault items
        # E11000 duplicate key error { vault_chat_id: null, vault_message_id: null }
        invalid_vault_query = {
            "$or": [
                {"vault_chat_id": None},
                {"vault_message_id": None}
            ]
        }
        
        invalid_count = await vault.count_documents(invalid_vault_query)
        if invalid_count > 0:
            logger.warning(
                "MIGRATION: Detected vault items with null references. Quarantining...",
                extra={"ctx_collection": settings.VAULT_COLLECTION, "ctx_count": invalid_count}
            )
            
            cursor = vault.find(invalid_vault_query)
            async for doc in cursor:
                doc["quarantine_reason"] = "migration_null_vault_reference"
                doc["quarantined_at"] = datetime.now(timezone.utc)
                doc["original_collection"] = settings.VAULT_COLLECTION
                await quarantine.insert_one(doc)
            
            result = await vault.delete_many(invalid_vault_query)
            logger.info(
                "MIGRATION: Removed invalid vault items.",
                extra={"ctx_collection": settings.VAULT_COLLECTION, "ctx_deleted": result.deleted_count}
            )

        logger.info("MIGRATION: Vault stabilization complete.")


class DatabaseManager:
    _client: Optional[AsyncIOMotorClient] = None
    _db: Optional[AsyncIOMotorDatabase] = None
    _initialized: bool = False
    _transactions_supported: bool = False

    @classmethod
    async def connect(cls) -> None:
        if cls._initialized:
            return

        logger.info("Starting MongoDB connection process...")
        try:
            client = AsyncIOMotorClient(
                settings.MONGO_URI,
                serverSelectionTimeoutMS=5000,
                maxPoolSize=settings.MONGO_MAX_POOL_SIZE,
                minPoolSize=settings.MONGO_MIN_POOL_SIZE,
                retryWrites=True,
            )
            cls._client = client
            cls._db = client[settings.MONGO_DB_NAME]

            # â”€â”€ Connection Test (FATAL if fails) â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
            logger.debug("Pinging MongoDB admin database...")
            await client.admin.command("ping")
            logger.info("MongoDB socket connection established", extra={"ctx_db": settings.MONGO_DB_NAME})

            # â”€â”€ Capabilities Audit â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
            try:
                # Check for replica set status (needed for transactions)
                status = await client.admin.command("replSetGetStatus")
                logger.info("MongoDB replica set detected", extra={"ctx_set_name": status.get("set")})
                cls._transactions_supported = True
            except Exception:
                logger.warning("MongoDB replica set NOT detected â€” transactions will be disabled/unavailable")
                cls._transactions_supported = False

        except Exception as e:
            logger.exception("FATAL: MongoDB connection or authentication failed")
            raise e

        # â”€â”€ MIGRATION STABILIZATION (Non-FATAL) â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
        # CRITICAL: Audit and clean legacy data BEFORE creating strict indexes.
        try:
            await DataMigrationManager.stabilize_queue(cls._db)
            await DataMigrationManager.stabilize_vault(cls._db)
        except Exception as e:
            logger.error(
                "MIGRATION: Data stabilization audit failed â€” attempting to proceed to index creation",
                extra={"ctx_error": str(e)}
            )

        # â”€â”€ Schema Verification (Non-FATAL) â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
        logger.debug("Initiating index verification/creation phase...")
        try:
            await cls._ensure_indexes()
            logger.info("All MongoDB indexes verified/created")
        except Exception as e:
            logger.error(
                "DEGRADED: Index verification failed. Application will boot with missing/stale indexes.",
                extra={"ctx_error": str(e)}
            )

        # â”€â”€ Referral System Indexes (Non-FATAL) â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
        # ── Referral System Indexes (Non-FATAL) ──────────────────────────────
            try:
                from app.referral.repository import ReferralRepository
                ref_repo = ReferralRepository(cls._db)
                try:
                    await ref_repo.create_indexes()
                except Exception as e:
                    logger.error(
                        "non_core_index_setup_failed",
                        extra={"ctx_collection": "referral", "ctx_error": repr(e)},
                        exc_info=True
                    )

                from app.payments.repository import PaymentRepository
                payment_repo = PaymentRepository(cls._db)
                try:
                    await payment_repo.create_indexes()
                except Exception as e:
                    logger.error(
                        "non_core_index_setup_failed",
                        extra={"ctx_collection": "payments", "ctx_error": repr(e)},
                        exc_info=True
                    )
            logger.info("MongoDB initialization complete (Connection + Indices)")
        except Exception as e:
            logger.error(
                "non_core_index_setup_failed",
                extra={"ctx_error": repr(e)},
                exc_info=True
            )

        cls._initialized = True
    @classmethod
    def transactions_supported(cls) -> bool:
        return cls._transactions_supported

    @classmethod
    async def disconnect(cls) -> None:
        client = cls._client
        if client is not None:
            client.close()
            cls._client = None
            cls._db = None
            cls._initialized = False
            logger.info("MongoDB connection closed")

    @classmethod
    def get_db(cls) -> AsyncIOMotorDatabase:
        db = cls._db
        if db is None:
            raise RuntimeError("Database not initialized. Call DatabaseManager.connect() first.")
        return db

    @classmethod
    async def _ensure_indexes(cls) -> None:
        db = cls.get_db()

        async def _safe_create(collection_name: str, indexes: list[IndexModel]) -> None:
            logger.debug(f"Verifying indexes for collection: {collection_name}")
            
            # â”€â”€ RC-11: Surgical Index Reconciliation â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
            # Specifically for unique indexes which often change definitions
            target_indexes = ["unique_active_content", "vault_ref_unique", "vault_message_unique"]
            
            try:
                existing_indexes = await db[collection_name].list_indexes().to_list(length=100)
                for target_name in target_indexes:
                    found_index = next((idx for idx in existing_indexes if idx["name"] == target_name), None)
                    if found_index:
                        expected = next((idx for idx in indexes if idx.document["name"] == target_name), None)
                        if expected:
                            expected_doc = expected.document
                            
                            # Compare critical options: unique, sparse, partialFilterExpression
                            mismatch = False
                            if found_index.get("unique") != expected_doc.get("unique"): mismatch = True
                            if found_index.get("sparse") != expected_doc.get("sparse"): mismatch = True
                            if found_index.get("partialFilterExpression") != expected_doc.get("partialFilterExpression"): mismatch = True
                            
                            if mismatch:
                                logger.warning(
                                    f"Index definition mismatch for {target_name}. Surgically recreating...",
                                    extra={
                                        "ctx_collection": collection_name,
                                        "ctx_index": target_name,
                                        "ctx_actual": found_index,
                                        "ctx_expected": expected_doc
                                    }
                                )
                                await db[collection_name].drop_index(target_name)
            except Exception as e:
                logger.warning(
                    f"Surgical index audit failed for {collection_name} (non-fatal)",
                    extra={"ctx_error": str(e)}
                )

            try:
                await db[collection_name].create_indexes(indexes)
                logger.debug(f"Successfully verified indexes for {collection_name}")
            except Exception as e:
                # RC-10 FIX: Catch other index specification mismatches.
                error_str = str(e)
                if "already exists with different options" in error_str or "IndexOptionsConflict" in error_str:
                    logger.warning(
                        f"Index conflict detected in {collection_name}. Attempting full collection recovery...",
                        extra={"ctx_collection": collection_name, "ctx_error": error_str}
                    )
                    try:
                        await db[collection_name].drop_indexes()
                        await db[collection_name].create_indexes(indexes)
                        logger.info(f"Successfully recovered and recreated indexes for {collection_name}")
                        return
                    except Exception as secondary_e:
                        logger.error(
                            f"Index recovery failed for {collection_name}",
                            extra={"ctx_collection": collection_name, "ctx_error": str(secondary_e)}
                        )
                        raise secondary_e
                
                logger.error(
                    f"Index creation failed for collection: {collection_name}",
                    extra={"ctx_collection": collection_name, "ctx_error": str(e)}
                )
                raise e

        # â”€â”€ Queue â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
        await _safe_create(settings.QUEUE_COLLECTION, [
            IndexModel(
                [("content_id", ASCENDING)],
                name="unique_active_content",
                unique=True,
                partialFilterExpression={
                    "status": {"$in": ["pending", "processing", "locked", "watermarking", "ready", "delivering"]}
                },
            ),
            IndexModel(
                [("delivery_key", ASCENDING)],
                name="delivery_key_unique",
                unique=True,
                sparse=True,
                background=True,
            ),
            IndexModel(
                [("vault_chat_id", ASCENDING), ("vault_message_id", ASCENDING)],
                name="vault_ref_unique",
                unique=True,
                background=True,
                partialFilterExpression={
                    "status": {"$in": ["pending", "processing", "locked", "watermarking", "ready", "delivering"]},
                    "vault_chat_id": {"$gt": 0},
                    "vault_message_id": {"$gt": 0}
                },
            ),
            IndexModel(
                [("media_group_id", ASCENDING)],
                name="queue_media_group",
                background=True,
                sparse=True,
            ),
            IndexModel(
                [("status", ASCENDING), ("locked_by", ASCENDING), ("priority", DESCENDING)],
                name="status_locked_by_priority",
                background=True,
            ),
            IndexModel(
                [("status", ASCENDING), ("priority", DESCENDING), ("execute_after", ASCENDING)],
                name="status_priority_execute",
                background=True,
            ),
            IndexModel(
                [("content_id", ASCENDING), ("status", ASCENDING)],
                name="content_status",
                background=True,
            ),
            IndexModel(
                [("locked_by", ASCENDING), ("locked_at", ASCENDING)],
                name="lock_recovery",
                background=True,
                sparse=True,
            ),
            IndexModel(
                [("source_channel_id", ASCENDING), ("created_at", DESCENDING)],
                name="channel_recency",
                background=True,
            ),
            IndexModel(
                [("status", ASCENDING), ("retry_count", ASCENDING)],
                name="retry_tracking",
                background=True,
            ),
            IndexModel(
                [("created_at", ASCENDING)],
                name="created_ttl",
                background=True,
                expireAfterSeconds=604800,
                partialFilterExpression={"status": {"$in": ["completed"]}},
            ),
            IndexModel(
                [("status", ASCENDING), ("locked_at", ASCENDING)],
                name="stale_lock_sweep",
                background=True,
            ),
            IndexModel(
                [("metadata.submitter_user_id", ASCENDING), ("created_at", DESCENDING)],
                name="user_queue_lookup",
                background=True,
            ),
        ])

        # â”€â”€ Vault â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
        await _safe_create(settings.VAULT_COLLECTION, [
            IndexModel([("content_id", ASCENDING)], name="vault_content_unique", unique=True),
            IndexModel(
                [("vault_chat_id", ASCENDING), ("vault_message_id", ASCENDING)],
                name="vault_message_unique",
                unique=True,
                background=True,
                sparse=True,
            ),
            IndexModel(
                [("media_group_id", ASCENDING)],
                name="vault_media_group",
                background=True,
                sparse=True,
            ),
            IndexModel(
                [("status", ASCENDING), ("moderation_destination", ASCENDING), ("created_at", ASCENDING)],
                name="vault_dist_query",
                background=True,
            ),
            IndexModel(
                [("file_unique_id", ASCENDING)],
                name="vault_file_unique",
                background=True,
                sparse=True,
            ),
            # M3 checksum/cooldown/submitter indexes
            IndexModel(
                [("checksum", ASCENDING)],
                name="vault_checksum_unique",
                unique=True,
                sparse=True,
                background=True,
            ),
            IndexModel(
                [("distribution_state", ASCENDING), ("cooldown_until", ASCENDING)],
                name="vault_state_cooldown",
                background=True,
            ),
            IndexModel(
                [("submitter_user_id", ASCENDING)],
                name="vault_submitter",
                background=True,
                sparse=True,
            ),
        ])

        # â”€â”€ Pending submissions â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
        await _safe_create(settings.PENDING_COLLECTION, [
            IndexModel([("key", ASCENDING)], name="pending_key_unique", unique=True),
            IndexModel(
                [("expires_at", ASCENDING)],
                name="pending_expiry_ttl",
                expireAfterSeconds=0,
            ),
            IndexModel(
                [("submitter_user_id", ASCENDING)],
                name="pending_by_submitter",
                background=True,
            ),
        ])

        # â”€â”€ Subscriptions â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
        await _safe_create("subscriptions", [
            IndexModel([("user_id", ASCENDING)], name="sub_user_unique", unique=True),
            IndexModel(
                [("status", ASCENDING), ("expires_at", ASCENDING)],
                name="sub_status_expiry",
                background=True,
            ),
            IndexModel(
                [("status", ASCENDING), ("grace_until", ASCENDING)],
                name="sub_status_grace",
                background=True,
            ),
            IndexModel(
                [("plan", ASCENDING), ("status", ASCENDING)],
                name="sub_plan_status",
                background=True,
            ),
            IndexModel(
                [("updated_at", DESCENDING)],
                name="sub_updated",
                background=True,
            ),
        ])

        # â”€â”€ Memberships â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
        await _safe_create("memberships", [
            IndexModel(
                [("user_id", ASCENDING), ("chat_id", ASCENDING)],
                name="membership_unique",
                unique=True,
            ),
            IndexModel(
                [("chat_id", ASCENDING), ("status", ASCENDING)],
                name="membership_chat_status",
                background=True,
            ),
            IndexModel(
                [("user_id", ASCENDING), ("status", ASCENDING)],
                name="membership_user_status",
                background=True,
            ),
            IndexModel(
                [("chat_id", ASCENDING), ("last_verified", ASCENDING)],
                name="membership_stale_sweep",
                background=True,
            ),
        ])

        # â”€â”€ Invites â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
        await _safe_create("invites", [
            IndexModel([("token", ASCENDING)], name="invite_token_unique", unique=True),
            IndexModel(
                [("created_by", ASCENDING), ("status", ASCENDING)],
                name="invite_creator",
                background=True,
            ),
            IndexModel(
                [("chat_id", ASCENDING), ("status", ASCENDING)],
                name="invite_chat_status",
                background=True,
            ),
            IndexModel(
                [("status", ASCENDING), ("expires_at", ASCENDING)],
                name="invite_expiry_sweep",
                background=True,
                sparse=True,
            ),
            IndexModel(
                [("intended_user_id", ASCENDING), ("chat_id", ASCENDING), ("status", ASCENDING)],
                name="invite_intended_user",
                background=True,
            ),
        ])

        # â”€â”€ Activity â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
        await _safe_create("activity", [
            IndexModel(
                [("user_id", ASCENDING), ("timestamp", DESCENDING)],
                name="activity_user_time",
                background=True,
            ),
            IndexModel(
                [("chat_id", ASCENDING), ("timestamp", DESCENDING)],
                name="activity_chat_time",
                background=True,
                sparse=True,
            ),
            IndexModel(
                [("action", ASCENDING), ("timestamp", DESCENDING)],
                name="activity_action_time",
                background=True,
            ),
            IndexModel(
                [("timestamp", ASCENDING)],
                name="activity_ttl",
                background=True,
                expireAfterSeconds=7776000,
            ),
        ])

        # â”€â”€ Bot config â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
        await _safe_create("bot_config", [
            IndexModel([("key", ASCENDING)], name="config_key_unique", unique=True),
        ])

        # â”€â”€ User topics â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
        await _safe_create("user_topics", [
            IndexModel(
                [("user_id", ASCENDING), ("topic_type", ASCENDING)],
                name="user_topic_unique",
                unique=True,
                background=True,
            ),
            IndexModel(
                [("topic_id", ASCENDING)],
                name="user_topic_id_lookup",
                background=True,
            ),
        ])

        # â”€â”€ Support messages â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
        await _safe_create("support_messages", [
            IndexModel(
                [("topic_id", ASCENDING)],
                name="support_msg_topic",
                background=True,
            ),
            IndexModel(
                [("user_id", ASCENDING)],
                name="support_msg_user",
                background=True,
            ),
            IndexModel(
                [("user_id", ASCENDING), ("direction", ASCENDING), ("created_at", DESCENDING)],
                name="support_user_direction",
                background=True,
            ),
            IndexModel(
                [("hub_message_id", ASCENDING), ("direction", ASCENDING)],
                name="support_hub_msg_unique",
                unique=True,
                sparse=True,
            ),
        ])

        # â”€â”€ Moderation audit â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
        await _safe_create("moderation_audit", [
            IndexModel(
                [("performed_by", ASCENDING), ("timestamp", DESCENDING)],
                name="audit_by_admin",
                background=True,
            ),
            IndexModel(
                [("content_id", ASCENDING)],
                name="audit_by_content",
                background=True,
                sparse=True,
            ),
            IndexModel(
                [("target_user_id", ASCENDING)],
                name="audit_by_target_user",
                background=True,
                sparse=True,
            ),
        ])

        # â”€â”€ Consent records â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
        await _safe_create("consent_records", [
            IndexModel(
                [("user_id", ASCENDING), ("record_type", ASCENDING), ("is_active", ASCENDING)],
                name="consent_user_type_active",
                background=True,
            ),
        ])

        # â”€â”€ Creator profiles â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
        await _safe_create("creator_profiles", [
            IndexModel(
                [("user_id", ASCENDING)],
                name="creator_profile_user_unique",
                unique=True,
            ),
            IndexModel(
                [("status", ASCENDING)],
                name="creator_by_status",
                background=True,
            ),
        ])

        # â”€â”€ M3: submissions â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
        await _safe_create("submissions", [
            IndexModel(
                [("status", ASCENDING), ("created_at", ASCENDING)],
                name="submissions_status_created",
                background=True,
            ),
            IndexModel([("user_id", ASCENDING)], name="submissions_user", background=True),
        ])

        # â”€â”€ M3: takedown_requests â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
        await _safe_create("takedown_requests", [
            IndexModel(
                [("content_id", ASCENDING), ("status", ASCENDING)],
                name="takedown_content_status",
                background=True,
            ),
            IndexModel([("reported_by", ASCENDING)], name="takedown_reporter", background=True),
            IndexModel([("created_at", DESCENDING)], name="takedown_recency", background=True),
            IndexModel(
                [("status", ASCENDING), ("created_at", DESCENDING)],
                name="takedown_pending_sweep",
                background=True,
            ),
        ])

        # â”€â”€ M3: floodwait_tracking â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
        await _safe_create("floodwait_tracking", [
            IndexModel(
                [("target_id", ASCENDING), ("recorded_at", DESCENDING)],
                name="fw_target_time",
                background=True,
            ),
            IndexModel(
                [("recorded_at", ASCENDING)],
                name="fw_ttl",
                background=True,
                expireAfterSeconds=2592000,
            ),
        ])

        # â”€â”€ M3: distribution_jobs â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
        await _safe_create("distribution_jobs", [
            IndexModel([("content_id", ASCENDING)], name="distjob_content", background=True),
            IndexModel(
                [("status", ASCENDING), ("created_at", ASCENDING)],
                name="distjob_status_created",
                background=True,
            ),
        ])

        logger.info("All MongoDB indexes verified/created")


async def get_database() -> AsyncIOMotorDatabase:
    return DatabaseManager.get_db()

