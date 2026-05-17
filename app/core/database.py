from typing import Optional
from motor.motor_asyncio import AsyncIOMotorClient, AsyncIOMotorDatabase
from pymongo import IndexModel, ASCENDING, DESCENDING
from app.config import settings
from app.core.logger import get_logger

logger = get_logger(__name__)


class DatabaseManager:
    _client: Optional[AsyncIOMotorClient] = None
    _db: Optional[AsyncIOMotorDatabase] = None
    _initialized: bool = False

    @classmethod
    async def connect(cls) -> None:
        if cls._initialized:
            return

        client = AsyncIOMotorClient(
            settings.MONGO_URI,
            serverSelectionTimeoutMS=5000,
            maxPoolSize=settings.MONGO_MAX_POOL_SIZE,
            minPoolSize=settings.MONGO_MIN_POOL_SIZE,
            retryWrites=True,
        )
        cls._client = client
        cls._db = client[settings.MONGO_DB_NAME]

        await client.admin.command("ping")
        logger.info("MongoDB connection established", extra={"ctx_db": settings.MONGO_DB_NAME})

        await cls._ensure_indexes()
        cls._initialized = True

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

        # ── Queue collection ──────────────────────────────────────────────────
        queue_indexes = [
            IndexModel(
                [("content_id", ASCENDING)],
                name="unique_active_content",
                unique=True,
                partialFilterExpression={
                    "status": {"$in": ["pending", "processing", "locked", "watermarking"]}
                },
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
                expireAfterSeconds=604800,  # 7-day auto-cleanup for completed jobs
                partialFilterExpression={"status": {"$in": ["completed"]}},
            ),
            IndexModel(
                [("status", ASCENDING), ("locked_at", ASCENDING)],
                name="stale_lock_sweep",
                background=True,
            ),
        ]
        await db[settings.QUEUE_COLLECTION].create_indexes(queue_indexes)

        # ── Dead letter collection ─────────────────────────────────────────────
        dlq_indexes = [
            IndexModel([("original_job_id", ASCENDING)], name="original_job", unique=True),
            IndexModel([("dead_at", DESCENDING)], name="dead_recency", background=True),
            IndexModel([("source_channel_id", ASCENDING)], name="dlq_channel", background=True),
        ]
        await db[settings.DEAD_LETTER_COLLECTION].create_indexes(dlq_indexes)

        # ── Distributed locks ──────────────────────────────────────────────────
        lock_indexes = [
            IndexModel([("lock_key", ASCENDING)], name="lock_key_unique", unique=True),
            IndexModel([("expires_at", ASCENDING)], name="lock_expiry_ttl", expireAfterSeconds=0),
        ]
        await db[settings.LOCK_COLLECTION].create_indexes(lock_indexes)

        # ── Metrics ───────────────────────────────────────────────────────────
        metrics_indexes = [
            IndexModel([("collected_at", DESCENDING)], name="metrics_recency", background=True),
            IndexModel(
                [("collected_at", ASCENDING)],
                name="metrics_ttl",
                expireAfterSeconds=2592000,  # 30-day retention
            ),
        ]
        await db[settings.METRICS_COLLECTION].create_indexes(metrics_indexes)

        # ── Vault collection ───────────────────────────────────────────────────
        vault_indexes = [
            IndexModel([("content_id", ASCENDING)], name="vault_content_unique", unique=True),
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
            IndexModel([("vault_message_id", ASCENDING)], name="vault_msg_id", background=True, sparse=True),
        ]
        await db[settings.VAULT_COLLECTION].create_indexes(vault_indexes)

        # ── Pending submissions ────────────────────────────────────────────────
        # TTL index auto-expires unactioned submissions after 48 h so orphans self-clean.
        pending_indexes = [
            IndexModel([("key", ASCENDING)], name="pending_key_unique", unique=True),
            IndexModel(
                [("expires_at", ASCENDING)],
                name="pending_expiry_ttl",
                expireAfterSeconds=0,  # MongoDB removes doc when expires_at passes
            ),
        ]
        await db[settings.PENDING_COLLECTION].create_indexes(pending_indexes)

        logger.info("All MongoDB indexes verified/created")


async def get_database() -> AsyncIOMotorDatabase:
    return DatabaseManager.get_db()