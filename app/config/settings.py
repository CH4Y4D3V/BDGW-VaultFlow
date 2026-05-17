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
    )

    # ── Pyrogram ──────────────────────────────────────────────────────────────
    API_ID: int
    API_HASH: str
    BOT_TOKEN: str
    SESSION_NAME: str = "vaultflow_bot"
    SESSION_DIR: str = "./sessions"

    # ── MongoDB ───────────────────────────────────────────────────────────────
    MONGO_URI: str
    MONGO_DB_NAME: str = "vaultflow"
    MONGO_MAX_POOL_SIZE: int = 20
    MONGO_MIN_POOL_SIZE: int = 5

    # ── Channels ──────────────────────────────────────────────────────────────
    VERIFICATION_CHANNEL_ID: int
    VAULT_CHANNEL_ID: int
    LOG_CHANNEL_ID: int = 0

    # ── Access Control ────────────────────────────────────────────────────────
    ADMIN_IDS: List[int] = Field(default_factory=list)

    # ── Media Group ───────────────────────────────────────────────────────────
    MEDIA_GROUP_TIMEOUT: float = 3.0
    MEDIA_GROUP_MAX_SIZE: int = 10

    # ── Flood Protection ──────────────────────────────────────────────────────
    FLOOD_MAX_REQUESTS: int = 5
    FLOOD_WINDOW_SECONDS: int = 60

    # ── Vault ─────────────────────────────────────────────────────────────────
    VAULT_IMMUTABLE: bool = True

    # ── External Module Integration ───────────────────────────────────────────
    CONTENT_ROUTING_ENABLED: bool = True

    # ── Runtime ───────────────────────────────────────────────────────────────
    DEBUG: bool = False
    LOG_LEVEL: str = "INFO"

    @field_validator("ADMIN_IDS", mode="before")
    @classmethod
    def _parse_admin_ids(cls, v: str | List[int]) -> List[int]:
        if isinstance(v, str):
            return json.loads(v)
        return v

    @field_validator("LOG_LEVEL", mode="before")
    @classmethod
    def _normalise_log_level(cls, v: str) -> str:
        return v.upper()


settings = Settings()