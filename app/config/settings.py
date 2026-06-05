from __future__ import annotations

import json
from typing import List

from pydantic import Field, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=True,
        extra="ignore",
    )

    # ── Pyrogram ──────────────────────────────────────────────────────────────
    API_ID: int
    API_HASH: str
    BOT_TOKEN: str
    BOT_USERNAME: str = ""
    SESSION_NAME: str = "vaultflow_bot"
    SESSION_DIR: str = "./sessions"
    MAX_CONCURRENT_TRANSMISSIONS: int = 10

    # ── MongoDB ───────────────────────────────────────────────────────────────
    MONGO_URI: str
    MONGO_DB_NAME: str = "vaultflow"
    MONGO_MAX_POOL_SIZE: int = 20
    MONGO_MIN_POOL_SIZE: int = 5
    QUEUE_COLLECTION: str = "queue"
    DEAD_LETTER_COLLECTION: str = "dead_letters"
    LOCK_COLLECTION: str = "locks"
    METRICS_COLLECTION: str = "metrics"
    VAULT_COLLECTION: str = "vault"
    CHANNEL_CONFIG_COLLECTION: str = "channel_config"
    PENDING_COLLECTION: str = "pending_submissions"
    SCHEDULER_JOBS_COLLECTION: str = "scheduler_jobs"
    QUARANTINE_COLLECTION: str = "quarantine"

    # ── Redis ─────────────────────────────────────────────────────────────────
    REDIS_URL: str = "redis://localhost:6379/0"

    # ── Channels ──────────────────────────────────────────────────────────────
    VERIFICATION_GROUP_ID: int
    VAULT_CHANNEL_ID: int
    NSFW_VAULT_CHANNEL_ID: int = 0
    PREMIUM_VAULT_CHANNEL_ID: int = 0
    NSFW_GROUP_ID: int = 0
    PREMIUM_GROUP_ID: int = 0
    LOG_CHANNEL_ID: int = 0
    MAIN_CHANNEL_ID: int = 0

    # ── Destination display names ─────────────────────────────────────────────
    NSFW_DISPLAY_NAME: str = "BD GONE WILD"
    PREMIUM_DISPLAY_NAME: str = "BD GONE WILD ✦ PREMIUM"

    # ── Access Control ────────────────────────────────────────────────────────
    # Only OWNER_ID and ADMIN_IDS are used. All admins have full access.
    OWNER_ID: int = Field(..., description="Telegram user_id of the bot owner. Required.")
    ADMIN_IDS: List[int] = Field(default_factory=list)

    # ── Legacy role lists (kept for backward compat, not used in logic) ───────
    SUDO_IDS: List[int] = Field(default_factory=list)
    MODERATOR_IDS: List[int] = Field(default_factory=list)
    SUPPORT_ADMIN_IDS: List[int] = Field(default_factory=list)
    PAYMENT_ADMIN_IDS: List[int] = Field(default_factory=list)
    SCHEDULER_ADMIN_IDS: List[int] = Field(default_factory=list)

    # ── Worker Pools ──────────────────────────────────────────────────────────
    DISPATCHER_WORKER_COUNT: int = 4
    WATERMARK_WORKER_COUNT: int = 2
    WORKER_BATCH_SIZE: int = 5
    WORKER_POLL_INTERVAL: float = 2.0

    # ── Scheduler & Fairness ──────────────────────────────────────────────────
    SCHEDULER_INTERVAL_SECONDS: int = 60
    MAX_JOBS_PER_CYCLE: int = 100
    RANDOMIZE_POSTING_WINDOW: int = 300
    REPOST_PREVENTION_HOURS: int = 168

    # ── Queue deadline for moderator-queued content ───────────────────────────
    QUEUE_DEADLINE_HOURS: int = 24

    # ── Retries & Backoff ─────────────────────────────────────────────────────
    MAX_RETRY_ATTEMPTS: int = 3
    RETRY_BASE_DELAY: float = 5.0
    RETRY_MAX_DELAY: float = 3600.0
    RETRY_JITTER_RANGE: float = 2.0

    # ── Distributed Locks ─────────────────────────────────────────────────────
    LOCK_TTL_SECONDS: int = 300
    LOCK_RETRY_ATTEMPTS: int = 5
    LOCK_RETRY_DELAY: float = 1.0
    STALE_LOCK_THRESHOLD_SECONDS: int = 600

    # ── Rate Limits & Flood Protection ────────────────────────────────────────
    GLOBAL_RATE_LIMIT_PER_MIN: int = 30
    PER_TARGET_RATE_LIMIT_PER_MIN: int = 10
    FLOODWAIT_EXTRA_BUFFER: int = 2
    FLOODWAIT_MAX_WAIT: int = 86400
    FLOOD_MAX_REQUESTS: int = 5
    FLOOD_WINDOW_SECONDS: int = 60

    # ── Media Group ───────────────────────────────────────────────────────────
    MEDIA_GROUP_TIMEOUT_SECONDS: float = 3.0
    MEDIA_GROUP_TIMEOUT: float = 3.0
    MEDIA_GROUP_MAX_SIZE: int = 10

    # ── Media Processing ──────────────────────────────────────────────────────
    PROCESSED_MEDIA_DIR: str = "./processed"
    WATERMARK_CACHE_DIR: str = "./watermark_cache"
    FFMPEG_TIMEOUT: float = 120.0

    # ── Watermark toggle ─────────────────────────────────────────────────────
    WATERMARK_ENABLED: bool = True  # Set True only when logo assets exist

    # ── Watermark assets — per-destination logos ──────────────────────────────
    WATERMARK_LOGO_PATH_NSFW: str = "./assets/watermarks/nsfw_logo.png"
    WATERMARK_LOGO_PATH_PREMIUM: str = "./assets/watermarks/premium_logo.png"
    WATERMARK_LOGO_PATH: str = "./assets/watermarks/nsfw_logo.png"

    WATERMARK_TEXT_NSFW: str = "BD GONE WILD"
    WATERMARK_TEXT_PREMIUM: str = "BD GONE WILD ✦ PREMIUM"
    WATERMARK_FONT_PATH: str = "./assets/fonts/Montserrat-SemiBold.ttf"

    # Accepted values: BOTTOM_RIGHT, BOTTOM_LEFT, TOP_RIGHT, TOP_LEFT, CENTER
    WATERMARK_POSITION: str = "BOTTOM_RIGHT"
    WATERMARK_OPACITY: float = 107
    WATERMARK_SCALE: float = 0.040
    WATERMARK_ROTATION: int = 0

    # ── Verification Hub Topics ───────────────────────────────────────────────
    HUB_TOPIC_PAYMENTS: int = 0
    HUB_TOPIC_CONTENT_REVIEW: int = 0
    HUB_TOPIC_SUPPORT: int = 0
    HUB_TOPIC_TAKEDOWN: int = 0
    HUB_TOPIC_USER_MOD: int = 0
    HUB_TOPIC_BROADCASTS: int = 0
    HUB_TOPIC_AUDIT: int = 0

    # ── Vault ─────────────────────────────────────────────────────────────────
    VAULT_IMMUTABLE: bool = True

    # ── Subscriptions ─────────────────────────────────────────────────────────
    GRACE_PERIOD_DAYS: int = 3

    # ── Invite security ───────────────────────────────────────────────────────
    INVITE_EXPIRY_MINUTES: int = 30

    # ── Daily distribution caps ───────────────────────────────────────────────
    DAILY_CAP_NSFW: int = 75
    DAILY_CAP_PREMIUM: int = 140

    # ── Runtime ───────────────────────────────────────────────────────────────
    DEBUG: bool = False
    LOG_LEVEL: str = "INFO"
    LOG_FORMAT: str = "JSON"

    @field_validator(
        "ADMIN_IDS", "SUDO_IDS", "MODERATOR_IDS",
        "SUPPORT_ADMIN_IDS", "PAYMENT_ADMIN_IDS", "SCHEDULER_ADMIN_IDS",
        mode="before",
    )
    @classmethod
    def _parse_id_lists(cls, v: str | List[int]) -> List[int]:
        if isinstance(v, str):
            return json.loads(v)
        return v

    @field_validator("LOG_LEVEL", mode="before")
    @classmethod
    def _normalise_log_level(cls, v: str) -> str:
        return v.upper()

    @field_validator("WATERMARK_POSITION", mode="before")
    @classmethod
    def _normalise_watermark_position(cls, v: str) -> str:
        v = v.upper()
        allowed = {"BOTTOM_RIGHT", "BOTTOM_LEFT", "TOP_RIGHT", "TOP_LEFT", "CENTER"}
        if v not in allowed:
            raise ValueError(
                f"WATERMARK_POSITION must be one of {allowed}, got: {v!r}"
            )
        return v


settings = Settings()  # type: ignore