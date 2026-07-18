from __future__ import annotations

"""
settings.py — Application configuration via pydantic-settings.

All values are loaded from environment variables or a .env file.
No secrets or IDs should appear here — this file is the schema only.

WATERMARK_OPACITY note (spec §13):
  Photos use opacity 90 (out of 255) → 90/255 ≈ 0.353 as a 0-1 float.
  Videos use opacity 110–130 (out of 255) → mid-point 120/255 ≈ 0.471.
  The previous default of 107 was a raw 0-255 integer stored as a float,
  which caused inconsistent behaviour: photo watermark divided by 255
  (correct), video watermark used the raw value in an ffmpeg drawtext
  filter as a decimal (wrong — ffmpeg expects 0.0–1.0 for alpha).

  WATERMARK_OPACITY is now defined as a normalised 0.0–1.0 float.
  Default: 0.42 (≈ 107/255 — preserves the intended visual level).
  A validator enforces the 0.0–1.0 range at startup so misconfiguration
  fails loudly rather than silently producing invisible or fully-opaque
  watermarks.

  Callers that previously did `float(opacity)/255` must remove the division.
  Callers that passed the raw value directly to ffmpeg drawtext are now correct.
"""

import json
from typing import List

from pydantic import Field, field_validator, AliasChoices
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
    HUB_SUPERGROUP_ID: int = 0  # Fallback for hub_config
    VAULT_CHANNEL_ID: int
    NSFW_VAULT_CHANNEL_ID: int = 0
    PREMIUM_VAULT_CHANNEL_ID: int = 0
    NSFW_GROUP_ID: int = 0
    PREMIUM_GROUP_ID: int = 0
    # FIX L5-010: PREMIUM_CHANNEL_ID referenced in 7 files via getattr but not defined.
    # Aliased to PREMIUM_GROUP_ID as the canonical group/channel for premium members.
    PREMIUM_CHANNEL_ID: int = Field(
        default=0,
        validation_alias=AliasChoices("PREMIUM_CHANNEL_ID", "PREMIUM_GROUP_ID"),
    )
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
    # FIX: was 168 (7 days). With a small vault pool (a handful of items per
    # destination in early operation), a full week before ANY item becomes
    # repostable meant the entire distribution pool could go silent for days
    # once the initial batch was exhausted — logged every cycle as "All
    # content was recently posted; nothing to schedule" — which is
    # indistinguishable from a broken pipeline to anyone watching the logs,
    # even though fairness.py itself was working exactly as designed.
    # Reduced to 24h: still prevents the same item appearing twice in one
    # day, but lets the vault-fill/backfill mechanism cycle through a small
    # pool responsively instead of freezing for a week at a time. Increase
    # this once the vault has enough content that daily repeats would look
    # spammy to subscribers.
    REPOST_PREVENTION_HOURS: int = 24

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

    # ── Watermark toggle ──────────────────────────────────────────────────────
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

    # ── WATERMARK_OPACITY ─────────────────────────────────────────────────────
    # Normalised 0.0–1.0 float used consistently across all watermark processors.
    #
    # Spec §13 target levels (as 0-1 floats):
    #   Photo : opacity 90  → 90/255  ≈ 0.353
    #   Video : opacity 110–130 → mid-point 120/255 ≈ 0.471
    #
    # Default 0.42 approximates the old raw value of 107 (107/255 ≈ 0.420)
    # while now being unambiguous across Pillow (putalpha) and ffmpeg (drawtext
    # alpha parameter, which expects 0.0–1.0).
    #
    # Migration note for callers:
    #   - photo_watermark.py: REMOVE the `/255` division — value is already normalised.
    #   - ffmpeg_processor.py: use the value directly for drawtext alpha.
    #
    WATERMARK_OPACITY: float = 0.42

    # FIX: was 0.040 (4% of image width = ~43px on a 1080px image, invisible
    # at normal viewing size). The comment in ffmpeg_processor.py said "15%"
    # but the actual constant contradicted it. Changed to 0.15 to match the
    # intended visible scale. Operators can override via WATERMARK_SCALE env var.
    WATERMARK_SCALE: float = 0.15
    WATERMARK_ROTATION: int = 0

    # ── Verification Hub Topics ───────────────────────────────────────────────
    HUB_TOPIC_ADMIN_LOGS: int = 0
    HUB_TOPIC_AUDIT: int = 0

    # ── Vault ─────────────────────────────────────────────────────────────────
    VAULT_IMMUTABLE: bool = True

    # ── Subscriptions ─────────────────────────────────────────────────────────
    GRACE_PERIOD_DAYS: int = 3

    # Minimum hours between consecutive re-invite DMs sent to the same
    # subscriber by MembershipReconciliationWorker (Section 26, runs every
    # 6h). Prevents accumulating a fresh single-use invite link every cycle
    # for a subscriber who has not yet clicked a previous one, or for any
    # other edge case producing a persistent "not in group" false positive.
    RECONCILE_REINVITE_COOLDOWN_HOURS: int = 24

    # Minutes a vault item may sit at distribution_state="pending_delivery"
    # with no matching active-status queue job before provider.py's
    # self-heal pass (fetch_distribution_content) releases it automatically.
    # Must be long enough that a job's brief window between being enqueued
    # and appearing in a query can never be mistaken for an orphan.
    ORPHAN_GRACE_PERIOD_MINUTES: int = 15

    # ── Invite security ───────────────────────────────────────────────────────
    INVITE_EXPIRY_MINUTES: int = 30

    # ── Daily distribution caps ───────────────────────────────────────────────
    DAILY_CAP_NSFW: int = Field(default=75, validation_alias=AliasChoices('DAILY_CAP_NSFW', 'NSFW_DAILY_COUNT'))
    DAILY_CAP_PREMIUM: int = Field(default=140, validation_alias=AliasChoices('DAILY_CAP_PREMIUM', 'PREMIUM_DAILY_COUNT'))

    # ── Daily submission caps ─────────────────────────────────────────────────
    PREMIUM_DAILY_SUBMISSION_LIMIT: int = 20
    FREE_DAILY_SUBMISSION_LIMIT: int = 5

    # ── Runtime ───────────────────────────────────────────────────────────────
    DEBUG: bool = False
    LOG_LEVEL: str = "INFO"
    LOG_FORMAT: str = "JSON"

    # ── Validators ────────────────────────────────────────────────────────────

    @field_validator(
        "ADMIN_IDS", "SUDO_IDS", "MODERATOR_IDS",
        "SUPPORT_ADMIN_IDS", "PAYMENT_ADMIN_IDS", "SCHEDULER_ADMIN_IDS",
        mode="before",
    )
    @classmethod
    def _parse_id_lists(cls, v: str | List[int]) -> List[int]:
        """Parse JSON-encoded list strings from environment variables."""
        if isinstance(v, str):
            return json.loads(v)
        return v

    @field_validator("LOG_LEVEL", mode="before")
    @classmethod
    def _normalise_log_level(cls, v: str) -> str:
        """Normalise log level to uppercase."""
        return v.upper()

    @field_validator("WATERMARK_POSITION", mode="before")
    @classmethod
    def _normalise_watermark_position(cls, v: str) -> str:
        """Validate and normalise watermark position to uppercase."""
        v = v.upper()
        allowed = {"BOTTOM_RIGHT", "BOTTOM_LEFT", "TOP_RIGHT", "TOP_LEFT", "CENTER"}
        if v not in allowed:
            raise ValueError(
                f"WATERMARK_POSITION must be one of {allowed}, got: {v!r}"
            )
        return v

    @field_validator("WATERMARK_OPACITY", mode="before")
    @classmethod
    def _validate_watermark_opacity(cls, v: float | int | str) -> float:
        """
        Validate WATERMARK_OPACITY is in the 0.0–1.0 range.

        Rejects the old-style 0-255 integer values (e.g., 107) at startup
        with a clear error message rather than silently producing wrong output.

        If you previously set WATERMARK_OPACITY=107, divide by 255 and update
        your .env: WATERMARK_OPACITY=0.42
        """
        try:
            val = float(v)
        except (TypeError, ValueError):
            raise ValueError(f"WATERMARK_OPACITY must be a float, got: {v!r}")

        if val > 1.0:
            raise ValueError(
                f"WATERMARK_OPACITY must be in the range 0.0–1.0 (normalised). "
                f"Got {val!r}, which looks like a 0-255 value. "
                f"Divide by 255 and update your .env. "
                f"Example: WATERMARK_OPACITY={val / 255:.3f}"
            )
        if val < 0.0:
            raise ValueError(
                f"WATERMARK_OPACITY must be >= 0.0, got: {val!r}"
            )
        return val


settings = Settings()  # type: ignore
