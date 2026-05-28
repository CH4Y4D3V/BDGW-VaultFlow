from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum
from typing import Optional


class PaymentStatus(str, Enum):
    WAITING_PAYMENT_DETAILS = "waiting_payment_details"
    REQUESTED = "requested"
    PENDING_DETAILS = "pending_details"
    AWAITING_PAYMENT = "awaiting_payment"
    WAITING_SCREENSHOT = "waiting_screenshot"
    SUBMITTED = "submitted"
    UNDER_REVIEW = "under_review"
    APPROVED = "approved"
    REJECTED = "rejected"
    EXPIRED = "expired"
    CANCELLED = "cancelled"
    PROCESSING = "processing"


@dataclass
class PaymentSession:
    id: str
    user_id: int
    plan_id: str
    locked_amount: float
    currency: str = "BDT"
    status: PaymentStatus = PaymentStatus.WAITING_PAYMENT_DETAILS
    payment_method: Optional[str] = None
    txid: Optional[str] = None
    screenshot_file_id: Optional[str] = None
    topic_id: Optional[int] = None
    
    created_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    updated_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    expires_at: Optional[datetime] = None
    
    locked_at: Optional[datetime] = None
    approved_at: Optional[datetime] = None
    approved_by: Optional[int] = None
    
    rejection_reason: Optional[str] = None
    rejected_at: Optional[datetime] = None
    rejected_by: Optional[int] = None

    def to_dict(self) -> dict:
        return {
            "_id": self.id,
            "user_id": self.user_id,
            "plan_id": self.plan_id,
            "locked_amount": self.locked_amount,
            "currency": self.currency,
            "status": self.status.value,
            "payment_method": self.payment_method,
            "txid": self.txid,
            "screenshot_file_id": self.screenshot_file_id,
            "topic_id": self.topic_id,
            "created_at": self.created_at,
            "updated_at": self.updated_at,
            "expires_at": self.expires_at,
            "locked_at": self.locked_at,
            "approved_at": self.approved_at,
            "approved_by": self.approved_by,
            "rejection_reason": self.rejection_reason,
            "rejected_at": self.rejected_at,
            "rejected_by": self.rejected_by,
        }

    @classmethod
    def from_dict(cls, data: dict) -> PaymentSession:
        return cls(
            id=data["_id"],
            user_id=data["user_id"],
            plan_id=data["plan_id"],
            locked_amount=data["locked_amount"],
            currency=data.get("currency", "BDT"),
            status=PaymentStatus(data["status"]),
            payment_method=data.get("payment_method"),
            txid=data.get("txid"),
            screenshot_file_id=data.get("screenshot_file_id"),
            topic_id=data.get("topic_id"),
            created_at=data["created_at"],
            updated_at=data["updated_at"],
            expires_at=data.get("expires_at"),
            locked_at=data.get("locked_at"),
            approved_at=data.get("approved_at"),
            approved_by=data.get("approved_by"),
            rejection_reason=data.get("rejection_reason"),
            rejected_at=data.get("rejected_at"),
            rejected_by=data.get("rejected_by"),
        )
