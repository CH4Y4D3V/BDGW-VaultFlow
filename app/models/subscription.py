"""
app/models/subscription.py

Subscription domain model for the BDGW VaultFlow platform.

Notes on design decisions:
  - SubscriptionStatus values are uppercase strings to match MongoDB storage.
    Any worker querying MongoDB directly MUST use `.value` when building
    filter dicts (e.g. {"status": SubscriptionStatus.ACTIVE.value}).
  - Plan enum mixes subscription plans (FREE/PREMIUM/NSFW) with elevated
    internal roles (ADMIN/SUDO/OWNER). The role-tier plans never expire and
    are excluded from all lifecycle workers. This is intentional — the rank
    hierarchy is used uniformly for access control.
  - GRACE and BANNED are platform extensions not listed in the spec's minimal
    status set; they are required for lifecycle management and enforcement.
"""

from __future__ import annotations

import math
from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum
from typing import Optional


# ---------------------------------------------------------------------------
# Enums
# ---------------------------------------------------------------------------


class SubscriptionStatus(str, Enum):
    """Lifecycle states for a user's subscription.

    All values are stored as uppercase strings in MongoDB.
    Workers querying the DB directly must use `.value`.
    """

    ACTIVE = "ACTIVE"
    EXPIRED = "EXPIRED"
    CANCELLED = "CANCELLED"
    PENDING = "PENDING"
    # Platform extensions (not in spec Section 7.2 minimal set):
    GRACE = "GRACE"    # Subscription has expired but grace period is active.
    BANNED = "BANNED"  # User is platform-banned; all access denied.


class Plan(str, Enum):
    """Access tiers used for both subscription plans and internal role grants.

    FREE / PREMIUM / NSFW  — purchasable subscription plans (Section 7.1).
    ADMIN / SUDO / OWNER   — internal role grants that never expire and are
                             excluded from lifecycle workers.
    """

    FREE = "free"
    PREMIUM = "premium"
    NSFW = "nsfw"
    ADMIN = "admin"
    SUDO = "sudo"
    OWNER = "owner"


# Numeric rank for access-level comparison.
# Higher number = higher privilege.
PLAN_HIERARCHY: dict[Plan, int] = {
    Plan.FREE: 10,
    Plan.PREMIUM: 40,
    Plan.NSFW: 60,
    Plan.ADMIN: 80,
    Plan.SUDO: 90,
    Plan.OWNER: 100,
}


def plan_rank(plan: "Plan | str") -> int:
    """Return the numeric rank for *plan*.

    Accepts either a ``Plan`` enum member or a raw string value.
    Returns 0 for unrecognised values so that unknown plans are treated
    as the lowest possible privilege level (fail-safe deny).

    Args:
        plan: A ``Plan`` enum member or a string matching a ``Plan`` value.

    Returns:
        Integer rank; 0 if the plan string is not recognised.
    """
    try:
        return PLAN_HIERARCHY[Plan(plan)]
    except (ValueError, KeyError):
        return 0


# ---------------------------------------------------------------------------
# Dataclass
# ---------------------------------------------------------------------------


@dataclass
class Subscription:
    """Represents a single user subscription record.

    Fields mirror the ``subscriptions`` collection schema defined in
    Section 7.2 / Section 25A of the master reference, plus legacy/compat
    fields added during the MongoDB migration.

    The ``plan`` field is the canonical access-tier indicator. When absent
    (legacy documents), ``package_id`` is used as a fallback for rank
    calculation.
    """

    subscription_id: str
    user_id: int
    package_id: str
    started_at: Optional[datetime]
    expires_at: Optional[datetime]
    status: SubscriptionStatus
    created_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    updated_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))

    # Legacy / compatibility fields kept for the migration period.
    grace_until: Optional[datetime] = None
    plan: Optional[Plan] = None
    metadata: dict = field(default_factory=dict)

    # ── State predicates ──────────────────────────────────────────────────────

    @property
    def is_active(self) -> bool:
        """True when the subscription is in ACTIVE state."""
        return self.status == SubscriptionStatus.ACTIVE

    @property
    def is_expired(self) -> bool:
        """True when the subscription is in EXPIRED state."""
        return self.status == SubscriptionStatus.EXPIRED

    @property
    def is_in_grace(self) -> bool:
        """True when the subscription is in the GRACE period."""
        return self.status == SubscriptionStatus.GRACE

    @property
    def is_banned(self) -> bool:
        """True when the user has been platform-banned."""
        return self.status == SubscriptionStatus.BANNED

    @property
    def is_lifetime(self) -> bool:
        """True when ``expires_at`` is None, indicating a non-expiring grant."""
        return self.expires_at is None

    # ── Time helpers ──────────────────────────────────────────────────────────

    @property
    def remaining_days(self) -> Optional[int]:
        """Number of whole days remaining until expiry.

        Returns ``None`` for lifetime subscriptions.
        Returns ``0`` if the subscription has already passed its expiry
        timestamp (clock-based, not status-based).

        Uses ``math.ceil`` so that 23 h 59 m remaining reports as 1 day,
        which is the user-facing expectation for notification copy.
        """
        if self.expires_at is None:
            return None
        delta = self.expires_at - datetime.now(timezone.utc)
        total_seconds = delta.total_seconds()
        if total_seconds <= 0:
            return 0
        return math.ceil(total_seconds / 86_400)

    # ── Access control ────────────────────────────────────────────────────────

    @property
    def rank(self) -> int:
        """Numeric privilege rank of this subscription.

        Uses ``plan`` if set; falls back to ``package_id`` for legacy records.
        Unrecognised package IDs return rank 0 (safest default).
        """
        return plan_rank(self.plan or self.package_id)

    def has_access(self, required_plan: "Plan | str") -> bool:
        """Return True if this subscription grants access to *required_plan*.

        Access is denied if:
          - The user is banned (regardless of rank).
          - The subscription is expired or cancelled.
          - The subscription rank is below the required rank.

        GRACE period users retain read access so they can be notified and
        renew, but callers that require full access should check ``is_active``
        separately for write/join operations.

        Args:
            required_plan: The minimum ``Plan`` tier (or its string value)
                           needed for the operation.

        Returns:
            True if access is permitted.
        """
        if self.is_banned:
            return False
        if self.status in (SubscriptionStatus.EXPIRED, SubscriptionStatus.CANCELLED):
            return False
        return self.rank >= plan_rank(required_plan)

    # ── Serialisation ─────────────────────────────────────────────────────────

    def to_dict(self) -> dict:
        """Serialise to a MongoDB-compatible document dict.

        ``_id`` is intentionally NOT included here. The repository layer
        uses ``user_id`` as the upsert filter key and lets MongoDB manage
        ``_id``. Including ``_id`` in a ``$set`` payload causes a
        ``WriteError: Mod on _id not allowed`` on documents that already
        have an ``_id``.
        """
        return {
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
        """Deserialise from a MongoDB document dict.

        Handles both current-schema documents (with ``subscription_id``,
        ``package_id``) and legacy documents written by ``SubscriptionService``
        (which use ``user_id`` as the sole key and store only ``plan``,
        ``status``, ``expires_at``).

        Safe defaults are applied for any missing field so that legacy reads
        never raise ``KeyError``.

        Args:
            data: Raw document dict from MongoDB.

        Returns:
            A ``Subscription`` instance.

        Raises:
            ValueError: Only if ``user_id`` is missing — that field is the
                        canonical identity key and cannot be defaulted.
        """
        # ── Resolve plan / package_id ─────────────────────────────────────
        # Legacy documents store "plan"; current documents store "package_id".
        # Prefer "plan" (more specific), fall back to "package_id", then "free".
        plan_raw: str = (
            data.get("plan")
            or data.get("package_id")
            or "free"
        )
        plan_enum: Optional[Plan]
        try:
            plan_enum = Plan(plan_raw)
        except ValueError:
            # Unrecognised plan string (e.g. a legacy "1_month_premium" slug).
            # Default to FREE so the user is not silently denied access checks;
            # the caller should log and investigate if rank matters here.
            plan_enum = Plan.FREE

        # ── Resolve subscription_id ───────────────────────────────────────
        # Current documents have "subscription_id"; legacy ones may only have
        # MongoDB's "_id". Fall back to str(user_id) as last resort so we
        # always have a non-None identifier.
        raw_id = data.get("subscription_id") or data.get("_id")
        subscription_id: str = str(raw_id) if raw_id is not None else str(data["user_id"])

        # ── Resolve status ────────────────────────────────────────────────
        # Must use the string .value as the default, NOT the enum member,
        # because SubscriptionStatus(enum_member) raises ValueError.
        raw_status: str = data.get("status", SubscriptionStatus.PENDING.value)
        try:
            status = SubscriptionStatus(raw_status)
        except ValueError:
            # Unknown status in DB — treat as PENDING (safest for lifecycle).
            status = SubscriptionStatus.PENDING

        return cls(
            subscription_id=subscription_id,
            user_id=data["user_id"],  # Mandatory — raises KeyError if absent.
            package_id=str(plan_raw),
            started_at=data.get("started_at"),
            expires_at=data.get("expires_at"),
            status=status,
            created_at=data.get("created_at", datetime.now(timezone.utc)),
            updated_at=data.get("updated_at", datetime.now(timezone.utc)),
            grace_until=data.get("grace_until"),
            plan=plan_enum,
            metadata=data.get("metadata", {}),
        )
