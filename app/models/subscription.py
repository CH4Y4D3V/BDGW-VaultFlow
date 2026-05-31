from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum
from typing import Optional


class SubscriptionStatus(str, Enum):
    ACTIVE = "ACTIVE"
    EXPIRED = "EXPIRED"
    CANCELLED = "CANCELLED"
    PENDING = "PENDING"
    GRACE = "GRACE"
    BANNED = "BANNED"


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
    subscription_id: str
    user_id: int
    package_id: str
    started_at: Optional[datetime]
    expires_at: Optional[datetime]
    status: SubscriptionStatus
    created_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    updated_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    
    # Legacy/Compatibility fields
    grace_until: Optional[datetime] = None
    plan: Optional[Plan] = None
    metadata: dict = field(default_factory=dict)

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
        delta = self.expires_at - datetime.now(timezone.utc)
        return max(0, delta.days)

    # ── Access control ────────────────────────────────────────────────────────

    @property
    def rank(self) -> int:
        return plan_rank(self.plan or self.package_id)

    def has_access(self, required_plan: "Plan | str") -> bool:
        if self.is_banned:
            return False
        return self.rank >= plan_rank(required_plan)

    # ── Serialisation ─────────────────────────────────────────────────────────

    def to_dict(self) -> dict:
        return {
            "_id": self.subscription_id,
            "subscription_id": self.subscription_id,
            "user_id": self.user_id,
            "package_id": self.package_id,
            "started_at": self.started_at,
            "expires_at": self.expires_at,
            "status": self.status.value,
            "created_at": self.created_at,
            "updated_at": self.updated_at,
            "grace_until": self.grace_until,
            "plan": self.plan.value if self.plan else self.package_id,
            "metadata": self.metadata,
        }

    @classmethod
    def from_dict(cls, data: dict) -> "Subscription":
        return cls(
            subscription_id=str(data.get("_id", data.get("subscription_id"))),
            user_id=data["user_id"],
            package_id=data.get("package_id", data.get("plan", "unknown")),
            started_at=data.get("started_at"),
            expires_at=data.get("expires_at"),
            status=SubscriptionStatus(data.get("status", SubscriptionStatus.PENDING)),
            created_at=data.get("created_at", datetime.now(timezone.utc)),
            updated_at=data.get("updated_at", datetime.now(timezone.utc)),
            grace_until=data.get("grace_until"),
            plan=Plan(data.get("plan", Plan.FREE)) if data.get("plan") else None,
            metadata=data.get("metadata", {}),
        )
