from __future__ import annotations

from datetime import datetime, timezone
from enum import Enum
from typing import Optional
from pydantic import BaseModel, Field


class SupportStatus(str, Enum):
    PENDING = "PENDING"
    ACTIVE = "ACTIVE"
    CLOSED = "CLOSED"


class SupportTicket(BaseModel):
    ticket_id: str = Field(..., alias="_id")
    user_id: int
    hub_topic_id: int
    subject: str
    status: SupportStatus = SupportStatus.PENDING
    assigned_to: Optional[int] = None
    source: str = "SUPPORT_MENU"  # e.g., SUPPORT_MENU, TAKEDOWN_REJECTION
    created_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    updated_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    closed_at: Optional[datetime] = None
    closure_summary: Optional[str] = None

    class Config:
        populate_by_name = True

    def to_dict(self) -> dict:
        return self.model_dump(by_alias=True)

    @classmethod
    def from_dict(cls, data: dict) -> "SupportTicket":
        return cls(**data)
