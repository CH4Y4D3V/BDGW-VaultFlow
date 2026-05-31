from __future__ import annotations

from datetime import datetime, timezone
from enum import Enum
from typing import Optional
from pydantic import BaseModel, Field


class TakedownStatus(str, Enum):
    PENDING = "PENDING"
    APPROVED = "APPROVED"
    REJECTED = "REJECTED"


class TakedownRequest(BaseModel):
    takedown_id: str = Field(..., alias="_id")
    user_id: int
    reason: str
    content_link: str
    status: TakedownStatus = TakedownStatus.PENDING
    submitted_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    reviewed_at: Optional[datetime] = None
    reviewed_by: Optional[int] = None
    rejection_reason: Optional[str] = None
    support_ticket_id: Optional[str] = None

    class Config:
        populate_by_name = True

    def to_dict(self) -> dict:
        return self.model_dump(by_alias=True)

    @classmethod
    def from_dict(cls, data: dict) -> "TakedownRequest":
        return cls(**data)
