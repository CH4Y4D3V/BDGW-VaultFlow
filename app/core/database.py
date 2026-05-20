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
            # FIX 11: index on submitter_user_id for admin queries
            IndexModel(
                [("submitter_user_id", ASCENDING)],
                name="pending_by_submitter",
                background=True,
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
            # FIX 10: index on intended_user_id for efficient identity verification queries
            IndexModel(
                [("intended_user_id", ASCENDING), ("chat_id", ASCENDING), ("status", ASCENDING)],
                name="invite_intended_user",
                background=True,
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
            # FIX 11: compound index for user+direction queries
            IndexModel(
                [("user_id", ASCENDING), ("direction", ASCENDING), ("created_at", DESCENDING)],
                name="support_user_direction",
                background=True,
            ),
            # FIX 11: unique sparse index on hub_message_id + direction
            IndexModel(
                [("hub_message_id", ASCENDING), ("direction", ASCENDING)],
                name="support_hub_msg_unique",
                unique=True,
                sparse=True,
            ),
        ])

        # ── Moderation audit ──────────────────────────────────────────────────
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
        await db["consent_records"].create_indexes([
            IndexModel(
                [("user_id", ASCENDING), ("record_type", ASCENDING), ("is_active", ASCENDING)],
                name="consent_user_type_active",
                background=True,
            ),
        ])

        # ── Creator profiles ──────────────────────────────────────────────────
        await db["creator_profiles"].create_indexes([
            IndexModel(
                [("user_id", ASCENDING)],
                name="creator_profile_user_unique",
                unique=True,
            ),
            # FIX 11: index on status for admin queries (active/suspended/banned creators)
            IndexModel(
                [("status", ASCENDING)],
                name="creator_by_status",
                background=True,
            ),
        ])

        # ── M3: submissions collection ────────────────────────────────────────
        await db["submissions"].create_indexes([
            IndexModel(
                [("status", ASCENDING), ("created_at", ASCENDING)],
                name="submissions_status_created",
                background=True,
            ),
            IndexModel([("user_id", ASCENDING)], name="submissions_user", background=True),
        ])

        # ── M3: takedown_requests collection ─────────────────────────────────
        await db["takedown_requests"].create_indexes([
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

        # ── M3: floodwait_tracking collection ─────────────────────────────────
        await db["floodwait_tracking"].create_indexes([
            IndexModel(
                [("target_id", ASCENDING), ("recorded_at", DESCENDING)],
                name="fw_target_time",
                background=True,
            ),
            IndexModel(
                [("recorded_at", ASCENDING)],
                name="fw_ttl",
                background=True,
                expireAfterSeconds=2592000,  # 30-day retention
            ),
        ])

        # ── M3: distribution_jobs (metrics history alias) ─────────────────────
        await db["distribution_jobs"].create_indexes([
            IndexModel([("content_id", ASCENDING)], name="distjob_content", background=True),
            IndexModel(
                [("status", ASCENDING), ("created_at", ASCENDING)],
                name="distjob_status_created",
                background=True,
            ),
        ])

        # ── M3: vault_items additional indexes (checksum, cooldown, submitter) ─
        await db[settings.VAULT_COLLECTION].create_indexes([
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

        logger.info("All MongoDB indexes verified/created")


async def get_database() -> AsyncIOMotorDatabase:
    return DatabaseManager.get_db()
