from __future__ import annotations

import uuid
from datetime import datetime, timedelta, timezone
from typing import Optional

from app.config import settings
from app.models.subscription import Plan, Subscription, SubscriptionStatus, plan_rank
from app.repositories.subscription_repository import SubscriptionRepository
from app.core.logger import get_logger

logger = get_logger(__name__)


class SubscriptionService:
    def __init__(self) -> None:
        self._repo = SubscriptionRepository()

    # ── Reads ─────────────────────────────────────────────────────────────

    async def get_subscription(self, user_id: int) -> Optional[Subscription]:
        return await self._repo.get_by_user_id(user_id)

    async def get_effective_plan(self, user_id: int) -> Plan:
        if user_id == settings.OWNER_ID:
            return Plan.OWNER
        if user_id in settings.SUDO_IDS:
            return Plan.SUDO
        sub = await self._repo.get_by_user_id(user_id)
        if not sub:
            return Plan.FREE
        if sub.status in (SubscriptionStatus.EXPIRED, SubscriptionStatus.BANNED):
            return Plan.FREE
        return sub.plan or Plan.FREE

    async def has_access(self, user_id: int, required_plan: Plan) -> bool:
        effective = await self.get_effective_plan(user_id)
        return plan_rank(effective) >= plan_rank(required_plan)

    async def get_stats(self) -> dict:
        return await self._repo.get_stats()

    async def get_paginated(
        self,
        status: Optional[SubscriptionStatus] = None,
        plan: Optional[Plan] = None,
        limit: int = 50,
        skip: int = 0,
    ) -> tuple[list[Subscription], int]:
        return await self._repo.get_paginated(status, plan, limit, skip)

    # ── Lifecycle mutations ───────────────────────────────────────────────

    async def grant(
        self,
        user_id: int,
        plan: Plan,
        duration_days: Optional[int],
        granted_by: int,
        notes: Optional[str] = None,
    ) -> Subscription:
        """
        Grant or extend a subscription.

        FIX A-08: Subscription dataclass requires subscription_id and
        package_id as the first positional fields. The previous implementation
        omitted both, causing TypeError on every grant() call and breaking
        every subscription activation.

        Fix: generate subscription_id via uuid4, derive package_id from
        plan.value so the dataclass constructor receives all required fields.
        """
        now = datetime.now(timezone.utc)
        existing = await self._repo.get_by_user_id(user_id)

        expires_at: Optional[datetime] = None
        grace_until: Optional[datetime] = None

        if duration_days is not None:
            if (
                existing
                and existing.is_active
                and existing.plan == plan
                and existing.expires_at
            ):
                base = max(existing.expires_at, now)
                exp = base + timedelta(days=duration_days)
            else:
                exp = now + timedelta(days=duration_days)

            expires_at = exp
            grace_until = exp + timedelta(days=settings.GRACE_PERIOD_DAYS)

        merged_metadata = {
            **(existing.metadata if existing and existing.metadata else {}),
            "granted_by": granted_by,
            "notes": notes or "",
        }

        # FIX A-08: provide subscription_id and package_id that the
        # Subscription dataclass requires as positional arguments.
        sub = Subscription(
            subscription_id=str(uuid.uuid4()),          # was missing → TypeError
            user_id=user_id,
            package_id=plan.value,                      # was missing → TypeError
            started_at=now,
            expires_at=expires_at,
            status=SubscriptionStatus.ACTIVE,
            created_at=existing.created_at if existing else now,
            updated_at=now,
            grace_until=grace_until,
            plan=plan,
            metadata=merged_metadata,
        )
        await self._repo.upsert(sub)
        logger.info(
            "Subscription granted",
            extra={
                "ctx_user_id": user_id,
                "ctx_plan": plan.value,
                "ctx_duration_days": duration_days,
                "ctx_granted_by": granted_by,
            },
        )
        return sub

    async def revoke(self, user_id: int, revoked_by: int) -> Optional[Subscription]:
        sub = await self._repo.get_by_user_id(user_id)
        if not sub:
            return None
        now = datetime.now(timezone.utc)
        sub.status = SubscriptionStatus.EXPIRED
        sub.expires_at = now
        sub.grace_until = now
        sub.plan = Plan.FREE
        sub.updated_at = now
        await self._repo.upsert(sub)
        logger.info("Subscription revoked", extra={"ctx_user_id": user_id, "ctx_revoked_by": revoked_by})
        return sub

    async def set_grace(self, sub: Subscription) -> Subscription:
        now = datetime.now(timezone.utc)
        sub.status = SubscriptionStatus.GRACE
        sub.updated_at = now
        await self._repo.upsert(sub)
        logger.info("Subscription → grace", extra={"ctx_user_id": sub.user_id})
        return sub

    async def expire(self, sub: Subscription) -> Subscription:
        now = datetime.now(timezone.utc)
        previous_plan = sub.plan.value if sub.plan else "unknown"
        sub.status = SubscriptionStatus.EXPIRED
        sub.plan = Plan.FREE
        sub.updated_at = now
        await self._repo.upsert(sub)
        logger.info(
            "Subscription fully expired",
            extra={"ctx_user_id": sub.user_id, "ctx_previous_plan": previous_plan},
        )
        return sub

    async def check_and_update_status(self, sub: Subscription) -> Subscription:
        if sub.is_lifetime:
            return sub
        now = datetime.now(timezone.utc)
        if sub.is_active and sub.expires_at and sub.expires_at <= now:
            return await self.set_grace(sub)
        if sub.is_in_grace and sub.grace_until and sub.grace_until <= now:
            return await self.expire(sub)
        return sub

    async def update_subscription(self, sub: Subscription) -> None:
        sub.updated_at = datetime.now(timezone.utc)
        await self._repo.upsert(sub)

    # ── Worker helpers ────────────────────────────────────────────────────

    async def get_newly_expired(self) -> list[Subscription]:
        return await self._repo.get_newly_expired()

    async def get_grace_expired(self) -> list[Subscription]:
        return await self._repo.get_grace_expired()

    async def get_expiring_soon(self, within_hours: int = 24) -> list[Subscription]:
        return await self._repo.get_expiring_soon(within_hours)
