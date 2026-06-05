from __future__ import annotations

from datetime import datetime, timezone
from enum import Enum
from typing import Optional
from pydantic import BaseModel, Field


class PaymentStatus(str, Enum):
    WAITING_PAYMENT_DETAILS = "waiting_payment_details"
    REQUESTED = "requested"
    PENDING_DETAILS = "pending_details"
    AWAITING_PAYMENT = "awaiting_payment"
    WAITING_TXID = "waiting_txid"
    WAITING_SCREENSHOT = "waiting_screenshot"
    SUBMITTED = "submitted"
    UNDER_REVIEW = "under_review"
    APPROVED = "approved"
    REJECTED = "rejected"
    EXPIRED = "expired"
    CANCELLED = "cancelled"
    PROCESSING = "processing"


class PaymentSession(BaseModel):
    id: str = Field(..., alias="_id")
    user_id: int
    plan_id: str
    locked_amount: float
    points_used: int = 0
    currency: str = "BDT"
    status: PaymentStatus = PaymentStatus.WAITING_PAYMENT_DETAILS
    payment_method: Optional[str] = None
    txid: Optional[str] = None
    screenshot_file_id: Optional[str] = None
    topic_id: Optional[int] = None
    
    created_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    updated_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    expires_at: Optional[datetime] = None
    
    locked_at: Optional[datetime] = None
    approved_at: Optional[datetime] = None
    approved_by: Optional[int] = None
    
    rejection_reason: Optional[str] = None
    rejected_at: Optional[datetime] = None
    rejected_by: Optional[int] = None

    class Config:
        populate_by_name = True

    def to_dict(self) -> dict:
        return self.model_dump(by_alias=True)

    @classmethod
    def from_dict(cls, data: dict) -> "PaymentSession":
        return cls(**data)


class TXIDRegistry(BaseModel):
    txid: str = Field(..., alias="_id")
    user_id: int
    payment_id: str
    verified_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))

    class Config:
        populate_by_name = True

    def to_dict(self) -> dict:
        return self.model_dump(by_alias=True)

    @classmethod
    def from_dict(cls, data: dict) -> "TXIDRegistry":
        return cls(**data)
