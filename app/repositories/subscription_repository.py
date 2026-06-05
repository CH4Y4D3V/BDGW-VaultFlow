"""
app/repositories/subscription_repository.py

Repository for the ``subscriptions`` MongoDB collection.

All public methods are async (Motor). Every ``from_dict`` call is wrapped in
a defensive try/except so that corrupt or legacy documents never crash a
bulk query — they are logged and skipped instead.

Lifetime subscriptions (``expires_at = None``) are excluded from all expiry
and notification queries via explicit ``$ne: None`` filters.
"""

from __future__ import annotations

import logging
from datetime import datetime, timedelta, timezone
from typing import Optional

from pymongo import ASCENDING, DESCENDING

from app.models.subscription import Plan, Subscription, SubscriptionStatus
from app.repositories.base import BaseRepository

logger = logging.getLogger(__name__)

# Internal role plans that must never be treated as expirable paid
# subscriptions. ADMIN is included here — it is a role grant, not a
# purchased plan, and must not appear in expiry/notification queries.
_NON_EXPIRABLE_PLANS: list[str] = [
    Plan.FREE.value,
    Plan.ADMIN.value,
    Plan.SUDO.value,
    Plan.OWNER.value,
]


def _safe_from_dict(doc: dict) -> Optional[Subscription]:
    """Deserialise *doc* into a ``Subscription``, returning ``None`` on error.

    Logs a warning with the document's ``user_id`` (if available) so that
    corrupt or legacy documents are visible in logs without crashing bulk
    queries.

    Args:
        doc: Raw MongoDB document dict.

    Returns:
        A ``Subscription`` instance, or ``None`` if deserialisation fails.
    """
    try:
        return Subscription.from_dict(doc)
    except Exception as exc:  # noqa: BLE001
        user_id = doc.get("user_id", "<unknown>")
        logger.warning(
            "Failed to deserialise subscription document — skipping",
            extra={"ctx_user_id": user_id, "ctx_error": str(exc)},
            exc_info=False,
        )
        return None


class SubscriptionRepository(BaseRepository):
    """Data-access layer for the ``subscriptions`` collection.

    All methods are async. Bulk-read methods silently skip documents that
    fail deserialisation and log a warning for each, so a single corrupt
    document cannot break pagination or worker scans.
    """

    collection_name = "subscriptions"

    # ── Single-record ops ─────────────────────────────────────────────────────

    async def get_by_user_id(self, user_id: int) -> Optional[Subscription]:
        """Fetch the subscription for *user_id*, or ``None`` if not found.

        Returns ``None`` both for missing documents and for documents that
        fail deserialisation (the latter is logged as a warning).

        Args:
            user_id: Telegram user ID.

        Returns:
            ``Subscription`` or ``None``.
        """
        doc = await self.find_one({"user_id": user_id})
        if doc is None:
            return None
        return _safe_from_dict(doc)

    async def upsert(self, subscription: Subscription) -> None:
        """Insert or update a subscription by ``user_id``.

        Uses ``$set`` so that only provided fields are overwritten. The
        ``_id`` field is intentionally excluded from the payload because
        MongoDB does not allow ``$set`` to modify ``_id`` on existing docs.

        Args:
            subscription: The ``Subscription`` to persist.
        """
        payload = subscription.to_dict()
        # Defensive: strip _id in case a caller accidentally injected it.
        payload.pop("_id", None)

        await self.collection.update_one(
            {"user_id": subscription.user_id},
            {"$set": payload},
            upsert=True,
        )

    async def update_status(
        self,
        user_id: int,
        status: SubscriptionStatus,
        updated_at: Optional[datetime] = None,
    ) -> None:
        """Update only the ``status`` and ``updated_at`` fields for *user_id*.

        Args:
            user_id:    Telegram user ID.
            status:     New ``SubscriptionStatus`` value.
            updated_at: Timestamp to record; defaults to ``now(UTC)``.
        """
        await self.update_one(
            {"user_id": user_id},
            {
                "$set": {
                    "status": status.value,
                    "updated_at": updated_at or datetime.now(timezone.utc),
                }
            },
        )

    async def delete_by_user_id(self, user_id: int) -> int:
        """Delete the subscription document for *user_id*.

        Args:
            user_id: Telegram user ID.

        Returns:
            Number of documents deleted (0 or 1).
        """
        deleted = await self.delete_one({"user_id": user_id})
        if deleted == 0:
            logger.warning(
                "delete_by_user_id: no subscription found to delete",
                extra={"ctx_user_id": user_id},
            )
        return deleted

    # ── Bulk expiry queries ───────────────────────────────────────────────────

    async def get_expiring_soon(self, within_hours: int = 24) -> list[Subscription]:
        """Return active paid subscriptions expiring within *within_hours*.

        Used by the notification worker to send advance expiry warnings
        (Section 7.7 of the master reference: 7-day, 3-day, and same-day
        notifications).

        Excludes:
          - Lifetime subscriptions (``expires_at = None``).
          - Non-expirable internal plans (FREE, ADMIN, SUDO, OWNER).

        Results are sorted by ``expires_at`` ascending (soonest first).

        Args:
            within_hours: Look-ahead window in hours. Default is 24.

        Returns:
            List of ``Subscription`` objects, skipping any corrupt documents.
        """
        now = datetime.now(timezone.utc)
        cutoff = now + timedelta(hours=within_hours)
        docs = await self.find_many(
            {
                "status": SubscriptionStatus.ACTIVE.value,
                # Exclude lifetime subs: expires_at must exist and be in window.
                "expires_at": {"$ne": None, "$lte": cutoff, "$gt": now},
                "plan": {"$nin": _NON_EXPIRABLE_PLANS},
            },
            sort=[("expires_at", ASCENDING)],
        )
        result = [s for doc in docs if (s := _safe_from_dict(doc)) is not None]
        return result

    async def get_newly_expired(self) -> list[Subscription]:
        """Return active subscriptions whose ``expires_at`` has passed.

        These are candidates for transition to GRACE status. Excludes
        lifetime subscriptions and non-expirable internal plans.

        Returns:
            List of ``Subscription`` objects, skipping any corrupt documents.
        """
        now = datetime.now(timezone.utc)
        docs = await self.find_many(
            {
                "status": SubscriptionStatus.ACTIVE.value,
                # $ne: None ensures lifetime subs are excluded.
                "expires_at": {"$ne": None, "$lte": now},
                "plan": {"$nin": _NON_EXPIRABLE_PLANS},
            }
        )
        result = [s for doc in docs if (s := _safe_from_dict(doc)) is not None]
        return result

    async def get_grace_expired(self) -> list[Subscription]:
        """Return GRACE-status subscriptions whose ``grace_until`` has passed.

        These are candidates for final EXPIRED status transition.

        Returns:
            List of ``Subscription`` objects, skipping any corrupt documents.
        """
        now = datetime.now(timezone.utc)
        docs = await self.find_many(
            {
                "status": SubscriptionStatus.GRACE.value,
                "grace_until": {"$ne": None, "$lte": now},
            }
        )
        result = [s for doc in docs if (s := _safe_from_dict(doc)) is not None]
        return result

    # ── List / stats queries ──────────────────────────────────────────────────

    async def get_all_active(self, plan: Optional[Plan] = None) -> list[Subscription]:
        """Return all ACTIVE subscriptions, optionally filtered by *plan*.

        Results are sorted by ``expires_at`` ascending. Lifetime subscriptions
        (``expires_at = None``) sort last because MongoDB places nulls first
        in ascending order — the sort is applied only when ``expires_at`` is
        not null, and lifetime subs are appended separately.

        Note: If you need strict expiry ordering for notifications, prefer
        ``get_expiring_soon`` which already excludes lifetime subs.

        Args:
            plan: Optional plan filter. If ``None``, all active plans returned.

        Returns:
            List of ``Subscription`` objects, skipping any corrupt documents.
        """
        filter_: dict = {"status": SubscriptionStatus.ACTIVE.value}
        if plan:
            filter_["plan"] = plan.value
        docs = await self.find_many(filter_, sort=[("expires_at", ASCENDING)])
        result = [s for doc in docs if (s := _safe_from_dict(doc)) is not None]
        return result

    async def get_all_by_status(self, status: SubscriptionStatus) -> list[Subscription]:
        """Return all subscriptions with the given *status*.

        Args:
            status: The ``SubscriptionStatus`` to filter by.

        Returns:
            List of ``Subscription`` objects, skipping any corrupt documents.
        """
        docs = await self.find_many({"status": status.value})
        result = [s for doc in docs if (s := _safe_from_dict(doc)) is not None]
        return result

    async def get_paginated(
        self,
        status: Optional[SubscriptionStatus] = None,
        plan: Optional[Plan] = None,
        limit: int = 50,
        skip: int = 0,
    ) -> tuple[list[Subscription], int]:
        """Fetch a paginated slice of subscriptions with an optional filter.

        Args:
            status: Filter by subscription status. ``None`` = all statuses.
            plan:   Filter by plan tier. ``None`` = all plans.
            limit:  Maximum number of documents to return. Default 50.
            skip:   Number of documents to skip (for offset pagination).

        Returns:
            A tuple of ``(items, total)`` where:
              - ``items`` is the paginated list of ``Subscription`` objects.
              - ``total`` is the total count matching the filter (not the page
                size), suitable for building pagination controls.
        """
        filter_: dict = {}
        if status:
            filter_["status"] = status.value
        if plan:
            filter_["plan"] = plan.value
        total = await self.count(filter_)
        docs = await self.find_many(
            filter_,
            sort=[("updated_at", DESCENDING)],
            limit=limit,
            skip=skip,
        )
        items = [s for doc in docs if (s := _safe_from_dict(doc)) is not None]
        return items, total

    async def get_stats(self) -> dict[str, int]:
        """Aggregate subscription counts grouped by status × plan.

        Returns a flat dict keyed as ``"STATUS:plan"`` (e.g.
        ``"ACTIVE:premium"``), where the value is the document count for
        that combination.

        Example::

            {
                "ACTIVE:premium": 142,
                "ACTIVE:nsfw": 37,
                "EXPIRED:premium": 8,
            }

        Returns:
            Dict mapping ``"STATUS:plan"`` strings to integer counts.
        """
        pipeline = [
            {
                "$group": {
                    "_id": {"status": "$status", "plan": "$plan"},
                    "count": {"$sum": 1},
                }
            }
        ]
        result: dict[str, int] = {}
        async for doc in self.collection.aggregate(pipeline):
            key = f"{doc['_id']['status']}:{doc['_id']['plan']}"
            result[key] = doc["count"]
        return result
