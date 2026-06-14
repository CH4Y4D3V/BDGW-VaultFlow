from __future__ import annotations

from datetime import datetime, timezone
from typing import Optional
from pydantic import BaseModel, Field


class User(BaseModel):
    """
    User domain model.

    Maps to the ``users`` collection (Section 25A.1).
    The ``_id`` field in MongoDB is the Telegram user_id (int).

    FIX L5-009: Added ``is_premium`` field which is indexed in
    ``app/core/database.py`` (``users_is_premium`` index) but was
    previously absent from the model, causing KeyError on documents
    that include the field and inconsistent upsert behaviour.
    """

    user_id: int = Field(..., alias="_id")
    username: Optional[str] = None
    full_name: str = ""
    join_date: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    onboarded: bool = False
    is_banned: bool = False
    is_muted: bool = False
    # FIX L5-009: field was missing — indexed in DB but absent from model
    is_premium: bool = False
    trust_score: int = 0
    fraud_score: int = 0
    referral_code: str = ""
    referred_by: Optional[int] = None
    referral_points: int = 0
    warn_count: int = 0

    # Punishment details
    ban_reason: Optional[str] = None
    mute_reason: Optional[str] = None
    mute_until: Optional[datetime] = None

    # Audit timestamps
    created_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    updated_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))

    class Config:
        populate_by_name = True

    def to_dict(self) -> dict:
        return self.model_dump(by_alias=True)

    @classmethod
    def from_dict(cls, data: dict) -> "User":
        return cls(**data)
