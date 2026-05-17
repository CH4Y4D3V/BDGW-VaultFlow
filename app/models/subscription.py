from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from enum import Enum
from typing import Optional


class SubscriptionStatus(str, Enum):
    ACTIVE = "active"
    GRACE = "grace"
    EXPIRED = "expired"
    BANNED = "banned"


class Plan(str, Enum):
    FREE = "free"
    PREMIUM = "premium"
    NSFW = "nsfw"
    ADMIN = "admin"
    SUDO = "sudo"
    OWNER = "owner"


PLAN_HIERARCHY: dict[Plan, int] = {
    Plan.FREE: 10,
    Plan.PREMIUM: 40,
    Plan.NSFW: 60,
    Plan.ADMIN: 80,
    Plan.SUDO: 90,
    Plan.OWNER: 100,
}


def plan_rank(plan: "Plan | str") -> int:
    try:
        return PLAN_HIERARCHY[Plan(plan)]
    except (ValueError, KeyError):
        return 0


@dataclass
class Subscription:
    user_id: int
    plan: Plan
    status: SubscriptionStatus
    started_at: datetime
    expires_at: Optional[datetime]
    grace_until: Optional[datetime]
    created_at: datetime
    updated_at: datetime
    notes: Optional[str] = None
    granted_by: Optional[int] = None

    # ── State predicates ──────────────────────────────────────────────────────

    @property
    def is_active(self) -> bool:
        return self.status == SubscriptionStatus.ACTIVE

    @property
    def is_expired(self) -> bool:
        return self.status == SubscriptionStatus.EXPIRED

    @property
    def is_in_grace(self) -> bool:
        return self.status == SubscriptionStatus.GRACE

    @property
    def is_banned(self) -> bool:
        return self.status == SubscriptionStatus.BANNED

    @property
    def is_lifetime(self) -> bool:
        return self.expires_at is None

    # ── Time helpers ──────────────────────────────────────────────────────────

    @property
    def remaining_days(self) -> Optional[int]:
        if self.expires_at is None:
            return None
        delta = self.expires_at - datetime.utcnow()
        return max(0, delta.days)

    @property
    def grace_remaining_days(self) -> Optional[int]:
        if self.grace_until is None:
            return None
        delta = self.grace_until - datetime.utcnow()
        return max(0, delta.days)

    # ── Access control ────────────────────────────────────────────────────────

    @property
    def rank(self) -> int:
        return plan_rank(self.plan)

    def has_access(self, required_plan: "Plan | str") -> bool:
        if self.is_banned:
            return False
        return self.rank >= plan_rank(required_plan)

    # ── Serialisation ─────────────────────────────────────────────────────────

    def to_dict(self) -> dict:
        return {
            "user_id": self.user_id,
            "plan": self.plan.value,
            "status": self.status.value,
            "started_at": self.started_at,
            "expires_at": self.expires_at,
            "grace_until": self.grace_until,
            "created_at": self.created_at,
            "updated_at": self.updated_at,
            "notes": self.notes,
            "granted_by": self.granted_by,
        }

    @classmethod
    def from_dict(cls, data: dict) -> "Subscription":
        return cls(
            user_id=data["user_id"],
            plan=Plan(data["plan"]),
            status=SubscriptionStatus(data["status"]),
            started_at=data["started_at"],
            expires_at=data.get("expires_at"),
            grace_until=data.get("grace_until"),
            created_at=data["created_at"],
            updated_at=data["updated_at"],
            notes=data.get("notes"),
            granted_by=data.get("granted_by"),
        )