from __future__ import annotations

from datetime import datetime, timezone
from enum import Enum
from typing import Optional
from pydantic import BaseModel, Field


class PaymentStatus(str, Enum):
    WAITING_PAYMENT_DETAILS = "WAITING_PAYMENT_DETAILS"
    AWAITING_PAYMENT = "AWAITING_PAYMENT"
    UNDER_REVIEW = "UNDER_REVIEW"
    APPROVED = "APPROVED"
    REJECTED = "REJECTED"
    EXPIRED = "EXPIRED"
    CANCELLED = "CANCELLED"
    PROCESSING = "PROCESSING"


class PaymentSession(BaseModel):
    id: str = Field(..., alias="_id")
    user_id: int
    subscription_id: Optional[str] = None
    package_id: str
    method: str
    txid: Optional[str] = None
    amount: float
    status: PaymentStatus = PaymentStatus.WAITING_PAYMENT_DETAILS
    session_active: bool = True
    session_started_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    proof_screenshot_file_id: Optional[str] = None
    approved_by: Optional[int] = None
    rejected_by: Optional[int] = None
    rejection_reason: Optional[str] = None
    
    # Audit timestamps
    created_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    updated_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    expires_at: Optional[datetime] = None

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
