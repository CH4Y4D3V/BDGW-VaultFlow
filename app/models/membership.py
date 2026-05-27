from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from enum import Enum
from typing import Optional


class MembershipStatus(str, Enum):
    ACTIVE = "active"
    PENDING = "pending"
    REMOVED = "removed"
    KICKED = "kicked"


class ChatType(str, Enum):
    PREMIUM = "premium"
    NSFW = "nsfw"
    PUBLIC = "public"


@dataclass
class Membership:
    user_id: int
    chat_id: int
    chat_type: ChatType
    status: MembershipStatus
    joined_at: datetime
    last_verified: datetime
    removed_at: Optional[datetime] = None
    removed_reason: Optional[str] = None

    @property
    def is_active(self) -> bool:
        return self.status == MembershipStatus.ACTIVE

    def to_dict(self) -> dict:
        return {
            "user_id": self.user_id,
            "chat_id": self.chat_id,
            "chat_type": self.chat_type.value,
            "status": self.status.value,
            "joined_at": self.joined_at,
            "last_verified": self.last_verified,
            "removed_at": self.removed_at,
            "removed_reason": self.removed_reason,
        }

    @classmethod
    def from_dict(cls, data: dict) -> "Membership":
        now = datetime.now()
        return cls(
            user_id=data["user_id"],
            chat_id=data["chat_id"],
            chat_type=ChatType(data.get("chat_type", ChatType.PUBLIC)),
            status=MembershipStatus(data.get("status", MembershipStatus.REMOVED)),
            joined_at=data.get("joined_at") or now,
            last_verified=data.get("last_verified") or now,
            removed_at=data.get("removed_at"),
            removed_reason=data.get("removed_reason"),
        )