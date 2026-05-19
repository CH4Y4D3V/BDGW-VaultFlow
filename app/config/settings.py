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

    # ── Redis ─────────────────────────────────────────────────────────────────
    REDIS_URL: str = "redis://localhost:6379/0"

    # ── Channels ──────────────────────────────────────────────────────────────
    VERIFICATION_GROUP_ID: int
    VAULT_CHANNEL_ID: int
    NSFW_GROUP_ID: int = 0
    PREMIUM_GROUP_ID: int = 0
    LOG_CHANNEL_ID: int = 0

    # ── Destination display names ─────────────────────────────────────────────
    NSFW_DISPLAY_NAME: str = "𝐁𝐃 𝐆𝐎𝐍𝐄 𝐖𝐈𝐋𝐃 𝐕𝐈𝐃𝐄𝐎"
    PREMIUM_DISPLAY_NAME: str = "𝐁𝐃 𝐆𝐎𝐍𝐄 𝐖𝐈𝐋𝐃 ✦ 𝐏𝐑𝐄𝐌𝐈𝐔𝐌"

    # ── Access Control ────────────────────────────────────────────────────────
    OWNER_ID: int = 0
    ADMIN_IDS: List[int] = Field(default_factory=list)
    SUDO_IDS: List[int] = Field(default_factory=list)

    # ── Granular role lists ───────────────────────────────────────────────────
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

    # ── Watermark assets — per-destination logos ──────────────────────────────
    WATERMARK_LOGO_PATH_NSFW: str = "./assets/watermarks/nsfw_logo.png"
    WATERMARK_LOGO_PATH_PREMIUM: str = "./assets/watermarks/premium_logo.png"
    WATERMARK_LOGO_PATH: str = "./assets/watermarks/nsfw_logo.png"

    WATERMARK_TEXT_NSFW: str = "𝐁𝐃 𝐆𝐎𝐍𝐄 𝐖𝐈𝐋𝐃 𝐕𝐈𝐃𝐄𝐎"
    WATERMARK_TEXT_PREMIUM: str = "𝐁𝐃 𝐆𝐎𝐍𝐄 𝐖𝐈𝐋𝐃 ✦ 𝐏𝐑𝐄𝐌𝐈𝐔𝐌"
    WATERMARK_FONT_PATH: str = "./assets/fonts/Montserrat-SemiBold.ttf"

    WATERMARK_POSITION: str = "BOTTOM_RIGHT"
    WATERMARK_OPACITY: float = 0.8
    WATERMARK_SCALE: float = 0.15

    # ── Vault ─────────────────────────────────────────────────────────────────
    VAULT_IMMUTABLE: bool = True

    # ── Subscriptions ─────────────────────────────────────────────────────────
    GRACE_PERIOD_DAYS: int = 3

    # ── Invite security ───────────────────────────────────────────────────────
    INVITE_EXPIRY_MINUTES: int = 30

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


settings = Settings()  # type: ignore