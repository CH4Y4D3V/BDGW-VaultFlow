import asyncio
from typing import Optional
from motor.motor_asyncio import AsyncIOMotorClient, AsyncIOMotorDatabase
from pymongo import IndexModel, ASCENDING, DESCENDING
from pymongo.errors import CollectionInvalid
from app.config import settings
from app.utils.logger import get_logger

logger = get_logger(__name__)


class DatabaseManager:
    _client: Optional[AsyncIOMotorClient] = None
    _db: Optional[AsyncIOMotorDatabase] = None
    _initialized: bool = False

    @classmethod
    async def connect(cls) -> None:
        if cls._initialized:
            return

        cls._client = AsyncIOMotorClient(
            settings.MONGO_URI,
            serverSelectionTimeoutMS=5000,
            maxPoolSize=20,
            minPoolSize=2,
            retryWrites=True,
        )

        cls._db = cls._client[settings.MONGO_DB_NAME]

        await cls._client.admin.command("ping")
        logger.info("MongoDB connection established", extra={"ctx_db": settings.MONGO_DB_NAME})

        await cls._ensure_indexes()
        cls._initialized = True

    @classmethod
    async def disconnect(cls) -> None:
        if cls._client:
            cls._client.close()
            cls._client = None
            cls._db = None
            cls._initialized = False
            logger.info("MongoDB connection closed")

    @classmethod
    def get_db(cls) -> AsyncIOMotorDatabase:
        if cls._db is None:
            raise RuntimeError("Database not initialized. Call DatabaseManager.connect() first.")
        return cls._db

    @classmethod
    async def _ensure_indexes(cls) -> None:
        db = cls._db

        # Queue collection indexes
        queue_indexes = [
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
                expireAfterSeconds=604800,  # 7-day auto-cleanup for completed
                partialFilterExpression={"status": {"$in": ["completed"]}},
            ),
        ]
        await db[settings.QUEUE_COLLECTION].create_indexes(queue_indexes)

        # Dead letter collection indexes
        dlq_indexes = [
            IndexModel([("original_job_id", ASCENDING)], name="original_job", unique=True),
            IndexModel([("dead_at", DESCENDING)], name="dead_recency", background=True),
            IndexModel([("source_channel_id", ASCENDING)], name="dlq_channel", background=True),
        ]
        await db[settings.DEAD_LETTER_COLLECTION].create_indexes(dlq_indexes)

        # Distributed locks indexes
        lock_indexes = [
            IndexModel([("lock_key", ASCENDING)], name="lock_key_unique", unique=True),
            IndexModel(
                [("expires_at", ASCENDING)],
                name="lock_expiry_ttl",
                expireAfterSeconds=0,
            ),
        ]
        await db[settings.LOCK_COLLECTION].create_indexes(lock_indexes)

        # Metrics indexes
        metrics_indexes = [
            IndexModel([("collected_at", DESCENDING)], name="metrics_recency", background=True),
            IndexModel(
                [("collected_at", ASCENDING)],
                name="metrics_ttl",
                expireAfterSeconds=2592000,  # 30-day retention
            ),
        ]
        await db[settings.METRICS_COLLECTION].create_indexes(metrics_indexes)

        logger.info("All MongoDB indexes verified/created")


async def get_database() -> AsyncIOMotorDatabase:
    return DatabaseManager.get_db()
