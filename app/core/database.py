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

        # ── Queue ─────────────────────────────────────────────────────────────
        await db[settings.QUEUE_COLLECTION].create_indexes([
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
                expireAfterSeconds=604800,
                partialFilterExpression={"status": {"$in": ["completed"]}},
            ),
            IndexModel(
                [("status", ASCENDING), ("locked_at", ASCENDING)],
                name="stale_lock_sweep",
                background=True,
            ),
        ])

        # ── Dead letter ───────────────────────────────────────────────────────
        await db[settings.DEAD_LETTER_COLLECTION].create_indexes([
            IndexModel([("original_job_id", ASCENDING)], name="original_job", unique=True),
            IndexModel([("dead_at", DESCENDING)], name="dead_recency", background=True),
            IndexModel([("source_channel_id", ASCENDING)], name="dlq_channel", background=True),
        ])

        # ── Distributed locks ─────────────────────────────────────────────────
        await db[settings.LOCK_COLLECTION].create_indexes([
            IndexModel([("lock_key", ASCENDING)], name="lock_key_unique", unique=True),
            IndexModel([("expires_at", ASCENDING)], name="lock_expiry_ttl", expireAfterSeconds=0),
        ])

        # ── Metrics ───────────────────────────────────────────────────────────
        await db[settings.METRICS_COLLECTION].create_indexes([
            IndexModel([("collected_at", DESCENDING)], name="metrics_recency", background=True),
            IndexModel(
                [("collected_at", ASCENDING)],
                name="metrics_ttl",
                expireAfterSeconds=2592000,
            ),
        ])

        # ── Vault ─────────────────────────────────────────────────────────────
        await db[settings.VAULT_COLLECTION].create_indexes([
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
            IndexModel(
                [("vault_message_id", ASCENDING)],
                name="vault_msg_id",
                background=True,
                sparse=True,
            ),
        ])

        # ── Pending submissions ───────────────────────────────────────────────
        await db[settings.PENDING_COLLECTION].create_indexes([
            IndexModel([("key", ASCENDING)], name="pending_key_unique", unique=True),
            IndexModel(
                [("expires_at", ASCENDING)],
                name="pending_expiry_ttl",
                expireAfterSeconds=0,
            ),
        ])

        # ── Subscriptions ─────────────────────────────────────────────────────
        await db["subscriptions"].create_indexes([
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

        # ── Memberships ───────────────────────────────────────────────────────
        await db["memberships"].create_indexes([
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

        # ── Invites ───────────────────────────────────────────────────────────
        await db["invites"].create_indexes([
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
        ])

        # ── Activity ──────────────────────────────────────────────────────────
        await db["activity"].create_indexes([
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
                expireAfterSeconds=7776000,  # 90-day retention
            ),
        ])

        # ── Bot config (rules, welcome messages, etc.) ────────────────────────
        await db["bot_config"].create_indexes([
            IndexModel([("key", ASCENDING)], name="config_key_unique", unique=True),
        ])

        # ── User topics ───────────────────────────────────────────────────────
        # Bug 9: unique compound index prevents duplicate (user_id, topic_type) pairs;
        # single topic_id index supports get_user_by_topic() lookups.
        await db["user_topics"].create_indexes([
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

        # ── Support messages ──────────────────────────────────────────────────
        # Bug 9: topic_id and user_id indexes support the support message routing queries.
        await db["support_messages"].create_indexes([
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
        ])

        # ── Moderation audit ──────────────────────────────────────────────────
        # Bug 9: (performed_by, timestamp DESC) for admin action history queries;
        # content_id and target_user_id for content/user dispute lookups.
        await db["moderation_audit"].create_indexes([
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

        # ── Consent records ───────────────────────────────────────────────────
        # Bug 9: compound index on (user_id, record_type, is_active) matches the
        # exact query pattern in ConsentService.get_active_consent().
        await db["consent_records"].create_indexes([
            IndexModel(
                [("user_id", ASCENDING), ("record_type", ASCENDING), ("is_active", ASCENDING)],
                name="consent_user_type_active",
                background=True,
            ),
        ])

        # ── Creator profiles ──────────────────────────────────────────────────
        # Bug 9: unique index on user_id — one profile per user, enforced at DB level.
        await db["creator_profiles"].create_indexes([
            IndexModel(
                [("user_id", ASCENDING)],
                name="creator_profile_user_unique",
                unique=True,
            ),
        ])

        logger.info("All MongoDB indexes verified/created")


async def get_database() -> AsyncIOMotorDatabase:
    return DatabaseManager.get_db()