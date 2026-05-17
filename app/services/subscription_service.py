from __future__ import annotations

from datetime import datetime, timedelta
from typing import Optional

from app.config.settings import settings
from app.models.subscription import Plan, Subscription, SubscriptionStatus, plan_rank
from app.repositories.subscription_repository import SubscriptionRepository
from app.utils.logger import get_logger

logger = get_logger(__name__)


class SubscriptionService:
    def __init__(self) -> None:
        self._repo = SubscriptionRepository()

    # ── Reads ─────────────────────────────────────────────────────────────────

    async def get_subscription(self, user_id: int) -> Optional[Subscription]:
        return await self._repo.get_by_user_id(user_id)

    async def get_effective_plan(self, user_id: int) -> Plan:
        """Return the highest plan the user currently holds, including hardcoded roles."""
        if user_id == settings.OWNER_ID:
            return Plan.OWNER
        if user_id in settings.SUDO_IDS:
            return Plan.SUDO
        sub = await self._repo.get_by_user_id(user_id)
        if not sub:
            return Plan.FREE
        if sub.status in (SubscriptionStatus.EXPIRED, SubscriptionStatus.BANNED):
            return Plan.FREE
        return sub.plan

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

    # ── Lifecycle mutations ───────────────────────────────────────────────────

    async def grant(
        self,
        user_id: int,
        plan: Plan,
        duration_days: Optional[int],
        granted_by: int,
        notes: Optional[str] = None,
    ) -> Subscription:
        """Grant or extend a subscription.

        If the user already has an active non-lifetime subscription of the same
        plan, we extend from the current expiry rather than overwriting it.
        """
        now = datetime.utcnow()
        existing = await self._repo.get_by_user_id(user_id)

        if duration_days is not None:
            if (
                existing
                and existing.is_active
                and existing.plan == plan
                and existing.expires_at
            ):
                base = max(existing.expires_at, now)
                expires_at: Optional[datetime] = base + timedelta(days=duration_days)
            else:
                expires_at = now + timedelta(days=duration_days)
            grace_until: Optional[datetime] = expires_at + timedelta(
                days=settings.GRACE_PERIOD_DAYS
            )
        else:
            # Lifetime
            expires_at = None
            grace_until = None

        sub = Subscription(
            user_id=user_id,
            plan=plan,
            status=SubscriptionStatus.ACTIVE,
            started_at=now,
            expires_at=expires_at,
            grace_until=grace_until,
            created_at=existing.created_at if existing else now,
            updated_at=now,
            notes=notes,
            granted_by=granted_by,
        )
        await self._repo.upsert(sub)
        logger.info(
            "Subscription granted",
            user_id=user_id,
            plan=plan.value,
            duration_days=duration_days,
            granted_by=granted_by,
        )
        return sub

    async def revoke(self, user_id: int, revoked_by: int) -> Optional[Subscription]:
        """Immediately expire and downgrade a subscription."""
        sub = await self._repo.get_by_user_id(user_id)
        if not sub:
            return None
        now = datetime.utcnow()
        sub.status = SubscriptionStatus.EXPIRED
        sub.expires_at = now
        sub.grace_until = now
        sub.plan = Plan.FREE
        sub.updated_at = now
        await self._repo.upsert(sub)
        logger.info("Subscription revoked", user_id=user_id, revoked_by=revoked_by)
        return sub

    async def set_grace(self, sub: Subscription) -> Subscription:
        now = datetime.utcnow()
        sub.status = SubscriptionStatus.GRACE
        sub.updated_at = now
        await self._repo.upsert(sub)
        logger.info("Subscription → grace", user_id=sub.user_id)
        return sub

    async def expire(self, sub: Subscription) -> Subscription:
        now = datetime.utcnow()
        previous_plan = sub.plan.value
        sub.status = SubscriptionStatus.EXPIRED
        sub.plan = Plan.FREE
        sub.updated_at = now
        await self._repo.upsert(sub)
        logger.info(
            "Subscription fully expired",
            user_id=sub.user_id,
            previous_plan=previous_plan,
        )
        return sub

    async def check_and_update_status(self, sub: Subscription) -> Subscription:
        """Advance sub through state machine based on wall-clock time."""
        if sub.is_lifetime:
            return sub
        now = datetime.utcnow()
        if sub.is_active and sub.expires_at and sub.expires_at <= now:
            return await self.set_grace(sub)
        if sub.is_in_grace and sub.grace_until and sub.grace_until <= now:
            return await self.expire(sub)
        return sub

    # ── Worker helpers ────────────────────────────────────────────────────────

    async def get_newly_expired(self) -> list[Subscription]:
        return await self._repo.get_newly_expired()

    async def get_grace_expired(self) -> list[Subscription]:
        return await self._repo.get_grace_expired()

    async def get_expiring_soon(self, within_hours: int = 24) -> list[Subscription]:
        return await self._repo.get_expiring_soon(within_hours)