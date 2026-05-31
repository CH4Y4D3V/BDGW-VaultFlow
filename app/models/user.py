from __future__ import annotations

from datetime import datetime, timezone
from typing import Optional
from pydantic import BaseModel, Field


class User(BaseModel):
    user_id: int = Field(..., alias="_id")
    username: Optional[str] = None
    full_name: str
    join_date: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    onboarded: bool = False
    is_banned: bool = False
    is_muted: bool = False
    trust_score: int = 0
    fraud_score: int = 0
    referral_code: str
    referred_by: Optional[int] = None
    referral_points: int = 0
    warn_count: int = 0
    
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
