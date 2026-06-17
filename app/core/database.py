"""
app/core/database.py
────────────────────────────────────────────────────────────────────────────
MongoDB connection manager and index-guarantee layer for BDGW VaultFlow.

Responsibilities
  • Open / close the Motor client (singleton).
  • Detect replica-set capability (enables multi-document transactions).
  • Run DataMigrationManager stabilization passes before indexes are applied.
  • Call _ensure_indexes() to idempotently create / reconcile every index
    for every collection defined in the Master Reference (Section 25A).
  • Expose get_db() for repository layer consumption.

Spec coverage: Section 25A (all 20 collections), Section 8 (TXID uniqueness),
               Section 24 (Motor async driver), Section 25 (restart safety).
────────────────────────────────────────────────────────────────────────────
"""

from datetime import datetime, timezone
from typing import Optional

from motor.motor_asyncio import AsyncIOMotorClient, AsyncIOMotorDatabase
from pymongo import IndexModel, ASCENDING, DESCENDING
from app.config import settings
from app.utils.logger import get_logger

logger = get_logger(__name__)

# ---------------------------------------------------------------------------
# Index names whose definitions may change across deployments and therefore
# require surgical drop-and-recreate when a definition mismatch is detected.
# ---------------------------------------------------------------------------
_SURGICAL_INDEX_NAMES: frozenset[str] = frozenset({
    "unique_active_content",
    "vault_ref_unique",
    "vault_message_unique",
    "support_hub_msg_unique",
})


# ═══════════════════════════════════════════════════════════════════════════
#  DataMigrationManager
# ═══════════════════════════════════════════════════════════════════════════

class DataMigrationManager:
    """
    Handles database stabilization and migration auditing during startup.

    Must run BEFORE strict unique indexes are applied.  Detects and
    quarantines documents that would violate the new indexes (null vault
    references, duplicate active content groups) so that index creation
    succeeds cleanly on every deployment.
    """

    @classmethod
    async def stabilize_queue(cls, db: AsyncIOMotorDatabase) -> None:
        """
        Audit the queue collection for integrity violations and quarantine
        any documents that would block unique-index creation.

        Checks:
          1. Active jobs with null / zero vault references.
          2. Duplicate active content groups (keeps oldest, quarantines rest).
        """
        logger.info("MIGRATION: Starting queue stabilization audit...")

        queue = db[settings.QUEUE_COLLECTION]
        quarantine = db[settings.QUARANTINE_COLLECTION]

        active_statuses = [
            "pending", "processing", "locked",
            "watermarking", "ready", "delivering",
        ]

        # ── Pass 1: null vault references ──────────────────────────────────
        invalid_ref_query = {
            "status": {"$in": active_statuses},
            "$or": [
                {"vault_chat_id": None},
                {"vault_message_id": None},
                {"vault_chat_id": 0},
                {"vault_message_id": 0},
            ],
        }

        invalid_count = await queue.count_documents(invalid_ref_query)
        if invalid_count > 0:
            logger.warning(
                "MIGRATION: Detected active jobs with null/invalid vault "
                "references. Quarantining...",
                extra={
                    "ctx_collection": settings.QUEUE_COLLECTION,
                    "ctx_count": invalid_count,
                },
            )
            cursor = queue.find(invalid_ref_query)
            async for doc in cursor:
                doc["quarantine_reason"] = "migration_null_vault_reference"
                doc["quarantined_at"] = datetime.now(timezone.utc)
                doc["original_collection"] = settings.QUEUE_COLLECTION
                try:
                    await quarantine.insert_one(doc)
                except Exception:
                    # Already quarantined (duplicate _id); safe to skip.
                    pass
            result = await queue.delete_many(invalid_ref_query)
            logger.info(
                "MIGRATION: Removed invalid jobs from active queue.",
                extra={
                    "ctx_collection": settings.QUEUE_COLLECTION,
                    "ctx_deleted": result.deleted_count,
                },
            )

        # ── Pass 2: duplicate active content groups ─────────────────────────
        pipeline = [
            {"$match": {"status": {"$in": active_statuses}}},
            {
                "$group": {
                    "_id": "$content_id",
                    "count": {"$sum": 1},
                    "ids": {"$push": "$_id"},
                }
            },
            {"$match": {"count": {"$gt": 1}}},
        ]
        content_dups = await queue.aggregate(pipeline).to_list(length=None)
        if content_dups:
            logger.warning(
                "MIGRATION: Detected duplicate active content groups. "
                "Resolving...",
                extra={
                    "ctx_collection": settings.QUEUE_COLLECTION,
                    "ctx_dups": len(content_dups),
                },
            )
            for dup in content_dups:
                # Keep the first entry; quarantine all others.
                ids_to_quarantine = dup["ids"][1:]
                for doc_id in ids_to_quarantine:
                    doc = await queue.find_one({"_id": doc_id})
                    if doc:
                        doc["quarantine_reason"] = "migration_duplicate_content_id"
                        doc["quarantined_at"] = datetime.now(timezone.utc)
                        doc["original_collection"] = settings.QUEUE_COLLECTION
                        try:
                            await quarantine.insert_one(doc)
                        except Exception:
                            pass
                        await queue.delete_one({"_id": doc_id})
            logger.info(
                "MIGRATION: Resolved content_id conflicts.",
                extra={
                    "ctx_collection": settings.QUEUE_COLLECTION,
                    "ctx_resolved": len(content_dups),
                },
            )

        logger.info("MIGRATION: Queue stabilization complete.")

    @classmethod
    async def stabilize_vault(cls, db: AsyncIOMotorDatabase) -> None:
        """
        Audit the vault collection for null vault references and quarantine
        any documents that would block unique-index creation.
        """
        logger.info("MIGRATION: Starting vault stabilization audit...")

        vault = db[settings.VAULT_COLLECTION]
        quarantine = db[settings.QUARANTINE_COLLECTION]

        invalid_vault_query = {
            "$or": [
                {"vault_chat_id": None},
                {"vault_message_id": None},
            ]
        }

        invalid_count = await vault.count_documents(invalid_vault_query)
        if invalid_count > 0:
            logger.warning(
                "MIGRATION: Detected vault items with null references. "
                "Quarantining...",
                extra={
                    "ctx_collection": settings.VAULT_COLLECTION,
                    "ctx_count": invalid_count,
                },
            )
            cursor = vault.find(invalid_vault_query)
            async for doc in cursor:
                doc["quarantine_reason"] = "migration_null_vault_reference"
                doc["quarantined_at"] = datetime.now(timezone.utc)
                doc["original_collection"] = settings.VAULT_COLLECTION
                try:
                    await quarantine.insert_one(doc)
                except Exception:
                    pass
            result = await vault.delete_many(invalid_vault_query)
            logger.info(
                "MIGRATION: Removed invalid vault items.",
                extra={
                    "ctx_collection": settings.VAULT_COLLECTION,
                    "ctx_deleted": result.deleted_count,
                },
            )

        logger.info("MIGRATION: Vault stabilization complete.")


# ═══════════════════════════════════════════════════════════════════════════
#  DatabaseManager
# ═══════════════════════════════════════════════════════════════════════════

class DatabaseManager:
    """
    Singleton Motor client and database accessor.

    Usage:
        await DatabaseManager.connect()   # called once at startup
        db = DatabaseManager.get_db()     # used by repository layer
        await DatabaseManager.disconnect()
    """

    _client: Optional[AsyncIOMotorClient] = None
    _db: Optional[AsyncIOMotorDatabase] = None
    _initialized: bool = False
    _transactions_supported: bool = False

    # ── Lifecycle ──────────────────────────────────────────────────────────

    @classmethod
    async def connect(cls) -> None:
        """
        Open the Motor connection, probe for replica-set support, run data
        migration passes, and ensure every required MongoDB index exists.

        Idempotent: does nothing if already initialized.
        Raises on fatal connection failure; non-fatal sub-steps are logged
        but do not prevent startup.
        """
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

            logger.debug("Pinging MongoDB admin database...")
            await client.admin.command("ping")
            logger.info(
                "MongoDB socket connection established",
                extra={"ctx_db": settings.MONGO_DB_NAME},
            )

            try:
                status = await client.admin.command("replSetGetStatus")
                logger.info(
                    "MongoDB replica set detected",
                    extra={"ctx_set_name": status.get("set")},
                )
                cls._transactions_supported = True
            except Exception:
                logger.warning(
                    "MongoDB replica set NOT detected — transactions disabled"
                )
                cls._transactions_supported = False

        except Exception as e:
            logger.exception("FATAL: MongoDB connection or authentication failed")
            raise

        # ── Data migration / stabilization (non-fatal) ─────────────────────
        try:
            await DataMigrationManager.stabilize_queue(cls._db)
            await DataMigrationManager.stabilize_vault(cls._db)
        except Exception as e:
            logger.error(
                "MIGRATION: Data stabilization audit failed — attempting to proceed",
                extra={"ctx_error": str(e)},
            )

        # ── Index verification / creation ───────────────────────────────────
        logger.debug("Initiating index verification/creation phase...")
        try:
            await cls._ensure_indexes()
            logger.info("All MongoDB indexes verified/created successfully.")
        except Exception as e:
            logger.error(
                "DEGRADED: Index verification failed. Application will boot "
                "with missing/stale indexes.",
                extra={"ctx_error": str(e)},
            )

        cls._initialized = True

    @classmethod
    def transactions_supported(cls) -> bool:
        """Return True if the connected MongoDB deployment supports transactions."""
        return cls._transactions_supported

    @classmethod
    async def disconnect(cls) -> None:
        """
        Close the Motor client and reset all singleton state.

        Safe to call even if not initialized.
        """
        client = cls._client
        if client is not None:
            client.close()
        cls._client = None
        cls._db = None
        cls._initialized = False
        cls._transactions_supported = False
        logger.info("MongoDB connection closed.")

    @classmethod
    def get_db(cls) -> AsyncIOMotorDatabase:
        """
        Return the active Motor database instance.

        Raises RuntimeError if connect() has not been called.
        """
        db = cls._db
        if db is None:
            raise RuntimeError(
                "Database not initialized. Call DatabaseManager.connect() first."
            )
        return db

    # ── Private: index management ──────────────────────────────────────────

    @classmethod
    async def _ensure_indexes(cls) -> None:
        """
        Create or reconcile all MongoDB indexes required by the Master
        Reference (Section 25A).

        Every collection defined in Section 25A is covered here.
        This method is the SINGLE authoritative source for index definitions.
        External repository create_indexes() calls are not relied upon for
        critical fraud-prevention indexes (e.g. txid_registry).

        Idempotent: safe to call on every startup.
        """
        db = cls.get_db()

        async def _safe_create(
            collection_name: str,
            indexes: list[IndexModel],
        ) -> None:
            """
            Create indexes on `collection_name`, performing surgical
            drop-and-recreate for any known mutable index whose definition
            has changed since the last deployment.

            Never raises; logs errors but allows startup to continue so that
            partial index coverage is always better than a hard crash.
            """
            logger.debug(
                f"Verifying indexes for collection: {collection_name}"
            )

            # ── Surgical reconciliation for mutable indexes ─────────────────
            try:
                existing_indexes = await db[collection_name].list_indexes().to_list(
                    length=100
                )
                for idx in existing_indexes:
                    idx_name = idx.get("name", "")
                    if idx_name not in _SURGICAL_INDEX_NAMES:
                        continue
                    expected = next(
                        (
                            m for m in indexes
                            if m.document.get("name") == idx_name
                        ),
                        None,
                    )
                    if expected is None:
                        continue

                    expected_doc = expected.document
                    mismatch = (
                        idx.get("unique") != expected_doc.get("unique")
                        or bool(idx.get("sparse")) != bool(expected_doc.get("sparse"))
                        or idx.get("partialFilterExpression")
                        != expected_doc.get("partialFilterExpression")
                    )
                    if mismatch:
                        logger.warning(
                            f"Index definition mismatch for '{idx_name}' on "
                            f"'{collection_name}'. Surgically recreating...",
                            extra={
                                "ctx_collection": collection_name,
                                "ctx_index": idx_name,
                            },
                        )
                        try:
                            await db[collection_name].drop_index(idx_name)
                        except Exception as drop_err:
                            logger.warning(
                                f"Could not drop mismatched index '{idx_name}' "
                                f"(non-fatal)",
                                extra={"ctx_error": str(drop_err)},
                            )
            except Exception as e:
                logger.warning(
                    f"Surgical index audit failed for '{collection_name}' "
                    f"(non-fatal)",
                    extra={"ctx_error": str(e)},
                )

            # ── Main create_indexes call ────────────────────────────────────
            try:
                await db[collection_name].create_indexes(indexes)
                logger.debug(
                    f"Indexes verified for collection: {collection_name}"
                )
            except Exception as e:
                error_str = str(e).lower()
                error_code = getattr(e, "code", None)
                # Broaden the conflict detection to handle various MongoDB error
                # messages AND OperationFailure codes:
                #   85 = IndexOptionsConflict  (index exists under the same name
                #                                with different options)
                #   86 = IndexKeySpecsConflict (an index on the same key pattern
                #                                already exists under a DIFFERENT
                #                                name — this happens whenever a
                #                                prior deployment/manual script
                #                                created an index under an older
                #                                name and this code now expects a
                #                                renamed index, e.g.
                #                                user_topics_topic_id (old) vs
                #                                user_topics_topic_id_lookup (new))
                is_conflict = (
                    error_code in (85, 86)
                    or any(msg in error_str for msg in [
                        "already exists with different options",
                        "indexoptionsconflict",
                        "already exists with the same name",
                        "already exists with a different name",
                        "indexkeyspecsconflict",
                        "equivalent index already exists",
                        "duplicate index",
                    ])
                )

                if is_conflict:
                    logger.warning(
                        f"Index conflict detected in '{collection_name}'. "
                        f"Attempting full collection recovery (drop and recreate)...",
                        extra={
                            "ctx_collection": collection_name,
                            "ctx_error": str(e),
                        },
                    )
                    try:
                        await db[collection_name].drop_indexes()
                        await db[collection_name].create_indexes(indexes)
                        logger.info(
                            f"Successfully recovered and recreated indexes "
                            f"for '{collection_name}'."
                        )
                        return
                    except Exception as secondary_e:
                        logger.error(
                            f"Index recovery failed for '{collection_name}'",
                            extra={
                                "ctx_collection": collection_name,
                                "ctx_error": str(secondary_e),
                            },
                        )
                        return  # Non-fatal: log and continue startup.

                logger.error(
                    f"Index creation failed for collection: '{collection_name}'",
                    extra={
                        "ctx_collection": collection_name,
                        "ctx_error": str(e),
                    },
                )
                # Non-fatal: log and continue so other collections proceed.

        # ══════════════════════════════════════════════════════════════════
        # Section 25A.1 — users
        # ══════════════════════════════════════════════════════════════════
        await _safe_create("users", [
            IndexModel(
                [("referral_code", ASCENDING)],
                name="users_referral_code_unique",
                unique=True,
                sparse=True,
            ),
            IndexModel(
                [("username", ASCENDING)],
                name="users_username",
                background=True,
                sparse=True,
            ),
            IndexModel(
                [("is_banned", ASCENDING)],
                name="users_is_banned",
                background=True,
            ),
            IndexModel(
                [("is_premium", ASCENDING)],
                name="users_is_premium",
                background=True,
            ),
        ])

        # ══════════════════════════════════════════════════════════════════
        # Section 25A.2 — user_topics
        # Unique index on user_id (not compound with topic_type — the spec
        # defines ONE topic per user with no topic_type field).
        # ══════════════════════════════════════════════════════════════════
        await _safe_create("user_topics", [
            IndexModel(
                [("user_id", ASCENDING)],
                name="user_topics_user_id_unique",
                unique=True,
            ),
            IndexModel(
                [("topic_id", ASCENDING)],
                name="user_topics_topic_id_lookup",
                background=True,
            ),
        ])

        # ══════════════════════════════════════════════════════════════════
        # Section 25A.3 — subscriptions
        # NOTE: user_id is unique per spec Section 7.2.
        # ══════════════════════════════════════════════════════════════════
        await _safe_create("subscriptions", [
            IndexModel(
                [("user_id", ASCENDING)],
                name="sub_user_id_unique",
                unique=True,
                background=True,
            ),
            IndexModel(
                [("status", ASCENDING), ("expires_at", ASCENDING)],
                name="sub_status_expiry",
                background=True,
            ),
            IndexModel(
                [("status", ASCENDING), ("grace_until", ASCENDING)],
                name="sub_status_grace",
                background=True,
                sparse=True,
            ),
            IndexModel(
                [("package_id", ASCENDING), ("status", ASCENDING)],
                name="sub_package_status",
                background=True,
            ),
            IndexModel(
                [("expires_at", ASCENDING)],
                name="sub_expires_at",
                background=True,
            ),
        ])

        # ══════════════════════════════════════════════════════════════════
        # Section 25A.4 — payment_sessions
        # One active session per user (unique on user_id).
        # ══════════════════════════════════════════════════════════════════
        await _safe_create("payment_sessions", [
            IndexModel(
                [("user_id", ASCENDING)],
                name="psession_user_id_unique",
                unique=True,
            ),
            IndexModel(
                [("expires_at", ASCENDING)],
                name="psession_expires_at",
                background=True,
            ),
            IndexModel(
                [("status", ASCENDING)],
                name="psession_status",
                background=True,
            ),
        ])

        # ══════════════════════════════════════════════════════════════════
        # Section 25A.5 — payment_history
        # ══════════════════════════════════════════════════════════════════
        await _safe_create("payment_history", [
            IndexModel(
                [("user_id", ASCENDING)],
                name="phist_user_id",
                background=True,
            ),
            IndexModel(
                [("txid", ASCENDING)],
                name="phist_txid_unique",
                unique=True,
                sparse=True,
            ),
            IndexModel(
                [("status", ASCENDING)],
                name="phist_status",
                background=True,
            ),
            IndexModel(
                [("reviewed_at", DESCENDING)],
                name="phist_reviewed_at",
                background=True,
                sparse=True,
            ),
        ])

        # ══════════════════════════════════════════════════════════════════
        # Section 25A.6 — txid_registry  ← CRITICAL FRAUD-PREVENTION INDEX
        #
        # This unique index is the DB-level guarantee that no TXID can be
        # accepted twice, even under concurrent write pressure.  It was
        # previously missing from _ensure_indexes and relied solely on
        # TXIDRepository.create_indexes() — which could fail or be skipped,
        # leaving the platform with no duplicate-payment protection.
        #
        # This index MUST be created here, unconditionally, at startup.
        # Spec: Section 8 (TXID Protection), Section 25A.6.
        # ══════════════════════════════════════════════════════════════════
        await _safe_create("txid_registry", [
            IndexModel(
                [("txid", ASCENDING)],
                name="txid_registry_txid_unique",
                unique=True,
            ),
            IndexModel(
                [("user_id", ASCENDING)],
                name="txid_registry_user_id",
                background=True,
            ),
            IndexModel(
                [("registered_at", DESCENDING)],
                name="txid_registry_registered_at",
                background=True,
            ),
        ])

        # ══════════════════════════════════════════════════════════════════
        # Section 25A.7 — invites
        # ══════════════════════════════════════════════════════════════════
        await _safe_create("invites", [
            IndexModel(
                [("user_id", ASCENDING)],
                name="invites_user_id",
                background=True,
            ),
            IndexModel(
                [("subscription_id", ASCENDING)],
                name="invites_subscription_id",
                background=True,
            ),
            IndexModel(
                [("expires_at", ASCENDING)],
                name="invites_expires_at",
                background=True,
            ),
            IndexModel(
                [("used", ASCENDING)],
                name="invites_used",
                background=True,
            ),
        ])

        # ══════════════════════════════════════════════════════════════════
        # Section 25A.8 — support_sessions
        # ══════════════════════════════════════════════════════════════════
        await _safe_create("support_sessions", [
            IndexModel(
                [("user_id", ASCENDING)],
                name="ssession_user_id",
                background=True,
            ),
            IndexModel(
                [("status", ASCENDING)],
                name="ssession_status",
                background=True,
            ),
            IndexModel(
                [("user_id", ASCENDING), ("status", ASCENDING)],
                name="ssession_user_status",
                background=True,
            ),
            IndexModel(
                [("accepted_by", ASCENDING)],
                name="ssession_accepted_by",
                background=True,
                sparse=True,
            ),
        ])

        # ══════════════════════════════════════════════════════════════════
        # Section 25A.9 — content_submissions
        # ══════════════════════════════════════════════════════════════════
        await _safe_create("content_submissions", [
            IndexModel(
                [("user_id", ASCENDING)],
                name="csub_user_id",
                background=True,
            ),
            IndexModel(
                [("media_hash", ASCENDING)],
                name="csub_media_hash",
                background=True,
                sparse=True,
            ),
            IndexModel(
                [("status", ASCENDING)],
                name="csub_status",
                background=True,
            ),
            IndexModel(
                [("submitted_at", DESCENDING)],
                name="csub_submitted_at",
                background=True,
            ),
            IndexModel(
                [("media_group_id", ASCENDING)],
                name="csub_media_group_id",
                background=True,
                sparse=True,
            ),
        ])

        # ══════════════════════════════════════════════════════════════════
        # Section 25A.10 — content_fingerprints
        # Unique index on media_hash prevents duplicate content submissions.
        # ══════════════════════════════════════════════════════════════════
        await _safe_create("content_fingerprints", [
            IndexModel(
                [("media_hash", ASCENDING)],
                name="cfp_media_hash_unique",
                unique=True,
            ),
            IndexModel(
                [("submission_id", ASCENDING)],
                name="cfp_submission_id",
                background=True,
            ),
            IndexModel(
                [("registered_at", DESCENDING)],
                name="cfp_registered_at",
                background=True,
            ),
        ])

        # ══════════════════════════════════════════════════════════════════
        # Section 25A.11 — vault_items  (settings-driven collection name)
        # ══════════════════════════════════════════════════════════════════
        await _safe_create(settings.VAULT_COLLECTION, [
            IndexModel(
                [("submission_id", ASCENDING)],
                name="vault_submission_id_unique",
                unique=True,
            ),
            IndexModel(
                [("vault_chat_id", ASCENDING), ("vault_message_id", ASCENDING)],
                name="vault_message_unique",
                unique=True,
                background=True,
                partialFilterExpression={
                    "vault_chat_id": {"$gt": 0},
                    "vault_message_id": {"$gt": 0},
                },
            ),
            IndexModel(
                [("user_id", ASCENDING)],
                name="vault_user_id",
                background=True,
            ),
            IndexModel(
                [("vault_type", ASCENDING)],
                name="vault_type",
                background=True,
            ),
            IndexModel(
                [("vault_type", ASCENDING), ("last_posted_at", ASCENDING)],
                name="vault_type_last_posted",
                background=True,
                sparse=True,
            ),
            IndexModel(
                [("media_hash", ASCENDING)],
                name="vault_media_hash",
                background=True,
                sparse=True,
            ),
            IndexModel(
                [("approved_at", DESCENDING)],
                name="vault_approved_at",
                background=True,
            ),
        ])

        # ══════════════════════════════════════════════════════════════════
        # Section 25A.12 — queue_jobs  (settings-driven collection name)
        # ══════════════════════════════════════════════════════════════════
        await _safe_create(settings.QUEUE_COLLECTION, [
            IndexModel(
                [("vault_id", ASCENDING)],
                name="queue_vault_id",
                background=True,
            ),
            IndexModel(
                [("vault_chat_id", ASCENDING), ("vault_message_id", ASCENDING)],
                name="vault_ref_unique",
                unique=True,
                background=True,
                partialFilterExpression={
                    "status": {
                        "$in": [
                            "pending", "processing", "locked",
                            "watermarking", "ready", "delivering",
                        ]
                    },
                    "vault_chat_id": {"$gt": 0},
                    "vault_message_id": {"$gt": 0},
                },
            ),
            IndexModel(
                [("status", ASCENDING), ("vault_type", ASCENDING), ("scheduled_at", ASCENDING)],
                name="queue_status_type_scheduled",
                background=True,
            ),
            IndexModel(
                [("status", ASCENDING), ("retry_count", ASCENDING)],
                name="queue_retry_tracking",
                background=True,
            ),
            IndexModel(
                [("status", ASCENDING), ("locked_by", ASCENDING), ("priority", DESCENDING)],
                name="queue_status_locked_priority",
                background=True,
                sparse=True,
            ),
            IndexModel(
                [("locked_by", ASCENDING), ("locked_at", ASCENDING)],
                name="queue_lock_recovery",
                background=True,
                sparse=True,
            ),
            IndexModel(
                [("created_at", ASCENDING)],
                name="queue_completed_ttl",
                background=True,
                expireAfterSeconds=604800,   # 7 days
                partialFilterExpression={"status": {"$in": ["completed"]}},
            ),
            IndexModel(
                [("media_group_id", ASCENDING)],
                name="queue_media_group",
                background=True,
                sparse=True,
            ),
        ])

        # ══════════════════════════════════════════════════════════════════
        # Section 25A.13 — dead_letters
        # ══════════════════════════════════════════════════════════════════
        await _safe_create("dead_letters", [
            IndexModel(
                [("job_id", ASCENDING)],
                name="dl_job_id",
                background=True,
            ),
            IndexModel(
                [("vault_id", ASCENDING)],
                name="dl_vault_id",
                background=True,
            ),
            IndexModel(
                [("vault_type", ASCENDING)],
                name="dl_vault_type",
                background=True,
            ),
            IndexModel(
                [("reviewed", ASCENDING)],
                name="dl_reviewed",
                background=True,
            ),
            IndexModel(
                [("created_at", DESCENDING)],
                name="dl_created_at",
                background=True,
            ),
        ])

        # ══════════════════════════════════════════════════════════════════
        # Section 25A.14 — referrals
        # ══════════════════════════════════════════════════════════════════
        await _safe_create("referrals", [
            IndexModel(
                [("referrer_user_id", ASCENDING)],
                name="ref_referrer_user_id",
                background=True,
            ),
            IndexModel(
                [("referred_user_id", ASCENDING)],
                # NOTE: name must match app/referral/repository.py's
                # ReferralRepository.create_indexes() index name
                # ("unique_referral_user"). Same key pattern with a
                # different index name causes IndexOptionsConflict (code 85)
                # at runtime, and the reconciliation drop in
                # ReferralRepository looks for this exact name.
                name="unique_referral_user",
                unique=True,
            ),
            IndexModel(
                [("points_awarded", ASCENDING)],
                name="ref_points_awarded",
                background=True,
            ),
            IndexModel(
                [("status", ASCENDING)],
                name="ref_status",
                background=True,
            ),
        ])

        # ══════════════════════════════════════════════════════════════════
        # Section 25A.15 — punishments
        # ══════════════════════════════════════════════════════════════════
        await _safe_create("punishments", [
            IndexModel(
                [("user_id", ASCENDING)],
                name="pun_user_id",
                background=True,
            ),
            IndexModel(
                [("user_id", ASCENDING), ("type", ASCENDING), ("active", ASCENDING)],
                name="pun_user_type_active",
                background=True,
            ),
            IndexModel(
                [("issued_by", ASCENDING)],
                name="pun_issued_by",
                background=True,
            ),
            IndexModel(
                [("active", ASCENDING)],
                name="pun_active",
                background=True,
            ),
        ])

        # ══════════════════════════════════════════════════════════════════
        # Section 25A.16 — takedown_requests
        # ══════════════════════════════════════════════════════════════════
        await _safe_create("takedown_requests", [
            IndexModel(
                [("user_id", ASCENDING)],
                name="tdown_user_id",
                background=True,
            ),
            IndexModel(
                [("status", ASCENDING)],
                name="tdown_status",
                background=True,
            ),
            IndexModel(
                [("user_id", ASCENDING), ("status", ASCENDING)],
                name="tdown_user_status",
                background=True,
            ),
            IndexModel(
                [("submitted_at", DESCENDING)],
                name="tdown_submitted_at",
                background=True,
            ),
            IndexModel(
                [("reviewed_by", ASCENDING)],
                name="tdown_reviewed_by",
                background=True,
                sparse=True,
            ),
        ])

        # ══════════════════════════════════════════════════════════════════
        # Section 25A.17 — audit_logs
        # ══════════════════════════════════════════════════════════════════
        await _safe_create("audit_logs", [
            IndexModel(
                [("timestamp", DESCENDING)],
                name="audit_timestamp",
                background=True,
            ),
            IndexModel(
                [("action", ASCENDING)],
                name="audit_action",
                background=True,
            ),
            IndexModel(
                [("target_user_id", ASCENDING), ("timestamp", DESCENDING)],
                name="audit_target_user_time",
                background=True,
                sparse=True,
            ),
            IndexModel(
                [("admin_user_id", ASCENDING), ("timestamp", DESCENDING)],
                name="audit_admin_user_time",
                background=True,
                sparse=True,
            ),
            # TTL: keep audit logs for 2 years (63072000 seconds).
            IndexModel(
                [("timestamp", ASCENDING)],
                name="audit_ttl",
                background=True,
                expireAfterSeconds=63072000,
            ),
        ])

        # ══════════════════════════════════════════════════════════════════
        # Section 25A.18 — admins
        # ══════════════════════════════════════════════════════════════════
        await _safe_create("admins", [
            IndexModel(
                [("user_id", ASCENDING)],
                name="admins_user_id_unique",
                unique=True,
            ),
            IndexModel(
                [("role", ASCENDING)],
                name="admins_role",
                background=True,
            ),
            IndexModel(
                [("is_active", ASCENDING)],
                name="admins_is_active",
                background=True,
            ),
        ])

        # ══════════════════════════════════════════════════════════════════
        # Section 25A.19 — hub_config
        # Key/value store for Admin Logs topic ID, channel IDs, group IDs.
        # ══════════════════════════════════════════════════════════════════
        await _safe_create("hub_config", [
            IndexModel(
                [("key", ASCENDING)],
                name="hub_config_key_unique",
                unique=True,
            ),
        ])

        # ══════════════════════════════════════════════════════════════════
        # Additional operational collections (not in 25A schema but used
        # by existing DataMigrationManager and sub-systems)
        # ══════════════════════════════════════════════════════════════════

        # ── Pending submissions (album buffer / pre-moderation) ─────────────
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

        # ── Memberships (premium group tracking) ───────────────────────────
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

        # ── Support messages (two-way bridge records) ───────────────────────
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
            # partialFilterExpression replaces sparse=True:
            # Only documents with a real Telegram message ID participate in
            # the unique constraint; null/missing values are excluded.
            IndexModel(
                [("hub_message_id", ASCENDING), ("direction", ASCENDING)],
                name="support_hub_msg_unique",
                unique=True,
                partialFilterExpression={
                    "hub_message_id": {"$exists": True, "$gt": 0}
                },
            ),
        ])

        # ── Activity / event log (TTL: 90 days) ────────────────────────────
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
                expireAfterSeconds=7776000,  # 90 days
            ),
        ])

        # ── Floodwait tracking (TTL: 30 days) ──────────────────────────────
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
                expireAfterSeconds=2592000,  # 30 days
            ),
        ])

        # ── Bot config (legacy — superseded by hub_config) ──────────────────
        await _safe_create("bot_config", [
            IndexModel([("key", ASCENDING)], name="bot_config_key_unique", unique=True),
        ])

        # ── Quarantine (receives migrated broken documents) ─────────────────
        await _safe_create(settings.QUARANTINE_COLLECTION, [
            IndexModel(
                [("quarantined_at", DESCENDING)],
                name="quarantine_time",
                background=True,
            ),
            IndexModel(
                [("original_collection", ASCENDING)],
                name="quarantine_origin",
                background=True,
            ),
            IndexModel(
                [("quarantine_reason", ASCENDING)],
                name="quarantine_reason",
                background=True,
            ),
        ])

        # ══════════════════════════════════════════════════════════════════
        # Distributed Lock Service collection
        # ══════════════════════════════════════════════════════════════════
        await _safe_create("locks", [
            IndexModel(
                [("lock_key", ASCENDING)],
                name="lock_key_unique",
                unique=True,
            ),
            IndexModel(
                [("expires_at", ASCENDING)],
                name="locks_expiry_ttl",
                expireAfterSeconds=0,
            ),
        ])

        logger.info("_ensure_indexes: all collections processed.")


# ═══════════════════════════════════════════════════════════════════════════
#  Module-level convenience accessor
# ═══════════════════════════════════════════════════════════════════════════

async def get_database() -> AsyncIOMotorDatabase:
    """
    FastAPI / dependency-injection compatible accessor.

    Returns the active Motor database.  Assumes DatabaseManager.connect()
    has already been called during application startup.
    """
    return DatabaseManager.get_db()
