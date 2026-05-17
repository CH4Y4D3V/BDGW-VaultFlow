from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum
from typing import Optional


class InviteStatus(str, Enum):
    ACTIVE = "active"
    EXPIRED = "expired"
    EXHAUSTED = "exhausted"
    REVOKED = "revoked"


@dataclass
class Invite:
    token: str
    created_by: int
    chat_id: int
    max_uses: int
    uses_remaining: int
    created_at: datetime
    status: InviteStatus
    plan_grant: Optional[str] = None
    expires_at: Optional[datetime] = None
    telegram_link: Optional[str] = None
    notes: Optional[str] = None
    used_by: list[int] = field(default_factory=list)
    revoked_by: Optional[int] = None
    revoked_at: Optional[datetime] = None

    @property
    def is_valid(self) -> bool:
        if self.status != InviteStatus.ACTIVE:
            return False
        if self.uses_remaining <= 0:
            return False
        if self.expires_at and datetime.now(timezone.utc) > self.expires_at:
            return False
        return True

    def to_dict(self) -> dict:
        return {
            "token": self.token,
            "created_by": self.created_by,
            "chat_id": self.chat_id,
            "plan_grant": self.plan_grant,
            "max_uses": self.max_uses,
            "uses_remaining": self.uses_remaining,
            "expires_at": self.expires_at,
            "created_at": self.created_at,
            "status": self.status.value,
            "telegram_link": self.telegram_link,
            "used_by": self.used_by,
            "notes": self.notes,
            "revoked_by": self.revoked_by,
            "revoked_at": self.revoked_at,
        }

    @classmethod
    def from_dict(cls, data: dict) -> "Invite":
        return cls(
            token=data["token"],
            created_by=data["created_by"],
            chat_id=data["chat_id"],
            plan_grant=data.get("plan_grant"),
            max_uses=data["max_uses"],
            uses_remaining=data["uses_remaining"],
            expires_at=data.get("expires_at"),
            created_at=data["created_at"],
            status=InviteStatus(data["status"]),
            telegram_link=data.get("telegram_link"),
            used_by=data.get("used_by", []),
            notes=data.get("notes"),
            revoked_by=data.get("revoked_by"),
            revoked_at=data.get("revoked_at"),
        )