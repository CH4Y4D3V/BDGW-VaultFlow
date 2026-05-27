from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Any, Optional


class ActivityAction(str, Enum):
    # Membership
    JOIN = "join"
    LEAVE = "leave"
    KICK = "kick"
    BAN = "ban"
    UNBAN = "unban"

    # Content
    UPLOAD = "upload"
    INTERACTION = "interaction"

    # Subscription lifecycle
    SUBSCRIPTION_GRANT = "subscription_grant"
    SUBSCRIPTION_REVOKE = "subscription_revoke"
    SUBSCRIPTION_EXPIRE = "subscription_expire"
    SUBSCRIPTION_GRACE = "subscription_grace"

    # Invite lifecycle
    INVITE_CREATE = "invite_create"
    INVITE_USE = "invite_use"
    INVITE_REVOKE = "invite_revoke"
    INVITE_EXPIRE = "invite_expire"

    # Permission
    PERMISSION_DENIED = "permission_denied"

    # System
    AUDIT = "audit"
    RECONCILE = "reconcile"


@dataclass
class Activity:
    user_id: int
    action: ActivityAction
    timestamp: datetime
    chat_id: Optional[int] = None
    performed_by: Optional[int] = None
    metadata: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict:
        return {
            "user_id": self.user_id,
            "action": self.action.value,
            "timestamp": self.timestamp,
            "chat_id": self.chat_id,
            "performed_by": self.performed_by,
            "metadata": self.metadata,
        }

    @classmethod
    def from_dict(cls, data: dict) -> "Activity":
        return cls(
            user_id=data["user_id"],
            action=ActivityAction(data.get("action", ActivityAction.AUDIT)),
            timestamp=data.get("timestamp") or datetime.now(),
            chat_id=data.get("chat_id"),
            performed_by=data.get("performed_by"),
            metadata=data.get("metadata", {}),
        )