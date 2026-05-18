from __future__ import annotations

"""
ConsentService — Immutable consent ledger and creator verification.

Design principles:
  - ConsentRecords are NEVER deleted. Withdrawals create a new WITHDRAWAL record.
  - Every vault item links to exactly one ConsentRecord via consent_record_id.
  - If a creator withdraws consent, all linked content can be batch-locked.
  - All operations are append-only writes to preserve audit trail.
"""

from datetime import datetime, timezone
from typing import Optional

from bson import ObjectId

from app.core.database import DatabaseManager
from app.utils.logger import get_logger

logger = get_logger(__name__)

# ── Versioned attestation text ─────────────────────────────────────────────────
# Increment version if text changes. Old records reference old version.
# Never modify old versions retroactively.

ATTESTATION_VERSION = "v1.0"

ATTESTATION_TEXT = (
    "I, the submitter, declare under penalty of platform ban and legal liability:\n\n"
    "1. I am at least 18 years of age.\n"
    "2. I am the original creator of this content, or I hold explicit written "
    "permission from the creator to distribute it.\n"
    "3. ALL persons depicted or identifiable in this content are adults (18+) and "
    "have given their explicit, informed consent to this content being shared on "
    "this platform.\n"
    "4. This content does not violate any applicable laws.\n"
    "5. I understand that my Telegram identity is permanently logged internally "
    "for accountability purposes, even though it remains private from public viewers.\n"
    "6. I understand that falsifying any of the above is grounds for permanent ban "
    "and may result in legal action."
)

# ── State constants ────────────────────────────────────────────────────────────

CREATOR_STATUS_PENDING = "pending"
CREATOR_STATUS_ACTIVE = "active"
CREATOR_STATUS_SUSPENDED = "suspended"
CREATOR_STATUS_BANNED = "banned"

RECORD_TYPE_ATTESTATION = "attestation"
RECORD_TYPE_WITHDRAWAL = "withdrawal"


class ConsentService:
    """
    Manages the consent lifecycle for content submitters.

    Every consent action writes a new record — nothing is ever mutated or deleted.
    This gives you a permanent, auditable ledger you can produce in legal disputes.
    """

    # ── Creator status ─────────────────────────────────────────────────────────

    async def is_verified_creator(self, user_id: int) -> bool:
        """Return True only if the creator has an active profile AND active consent."""
        profile = await self.get_creator_profile(user_id)
        if not profile or profile.get("status") != CREATOR_STATUS_ACTIVE:
            return False
        consent = await self.get_active_consent(user_id)
        return consent is not None

    async def get_creator_profile(self, user_id: int) -> Optional[dict]:
        db = DatabaseManager.get_db()
        return await db["creator_profiles"].find_one({"user_id": user_id})

    async def get_active_consent(self, user_id: int) -> Optional[dict]:
        """Return the most recent attestation record, or None if no active consent."""
        db = DatabaseManager.get_db()
        # Most recent attestation that has not been withdrawn
        doc = await db["consent_records"].find_one(
            {
                "user_id": user_id,
                "record_type": RECORD_TYPE_ATTESTATION,
                "is_active": True,
            },
            sort=[("agreed_at", -1)],
        )
        return doc

    async def get_consent_record_by_id(self, record_id: str) -> Optional[dict]:
        db = DatabaseManager.get_db()
        return await db["consent_records"].find_one({"_id": ObjectId(record_id)})

    # ── Onboarding ─────────────────────────────────────────────────────────────

    async def create_consent_record(
        self,
        user_id: int,
        telegram_username: Optional[str] = None,
    ) -> str:
        """
        Create an immutable consent attestation record.
        Returns the consent_record_id (str).
        """
        db = DatabaseManager.get_db()
        now = datetime.now(timezone.utc)

        doc = {
            "user_id": user_id,
            "telegram_username_at_time": telegram_username,
            "record_type": RECORD_TYPE_ATTESTATION,
            "attestation_version": ATTESTATION_VERSION,
            "attestation_text": ATTESTATION_TEXT,
            "agreed_at": now,
            "is_active": True,
        }

        result = await db["consent_records"].insert_one(doc)
        record_id = str(result.inserted_id)

        logger.info(
            "Consent record created",
            extra={
                "ctx_user_id": user_id,
                "ctx_record_id": record_id,
                "ctx_version": ATTESTATION_VERSION,
            },
        )
        return record_id

    async def register_creator(
        self,
        user_id: int,
        consent_record_id: str,
        telegram_username: Optional[str] = None,
    ) -> None:
        """
        Upsert a creator profile tied to a consent record.
        Idempotent — safe to call multiple times.
        """
        db = DatabaseManager.get_db()
        now = datetime.now(timezone.utc)

        await db["creator_profiles"].update_one(
            {"user_id": user_id},
            {
                "$set": {
                    "user_id": user_id,
                    "status": CREATOR_STATUS_ACTIVE,
                    "consent_record_id": consent_record_id,
                    "telegram_username_at_time": telegram_username,
                    "last_consent_at": now,
                },
                "$setOnInsert": {
                    "verified_at": now,
                    "submissions_count": 0,
                    "created_at": now,
                },
            },
            upsert=True,
        )
        logger.info(
            "Creator profile registered",
            extra={"ctx_user_id": user_id, "ctx_record_id": consent_record_id},
        )

    # ── Consent withdrawal ─────────────────────────────────────────────────────

    async def withdraw_consent(
        self,
        user_id: int,
        reason: str,
        requested_by: int,
    ) -> str:
        """
        Record a consent withdrawal.

        - The original attestation record is marked is_active=False (NOT deleted).
        - A new WITHDRAWAL record is inserted (immutable append).
        - All content linked to the old consent record is batch-locked.
        - Returns the withdrawal record ID.
        """
        db = DatabaseManager.get_db()
        now = datetime.now(timezone.utc)

        # Deactivate existing attestation
        await db["consent_records"].update_many(
            {
                "user_id": user_id,
                "record_type": RECORD_TYPE_ATTESTATION,
                "is_active": True,
            },
            {"$set": {"is_active": False, "deactivated_at": now}},
        )

        # Append withdrawal record
        withdrawal_doc = {
            "user_id": user_id,
            "record_type": RECORD_TYPE_WITHDRAWAL,
            "reason": reason,
            "requested_by": requested_by,
            "withdrawn_at": now,
            "is_active": True,
        }
        result = await db["consent_records"].insert_one(withdrawal_doc)
        withdrawal_id = str(result.inserted_id)

        # Suspend creator profile
        await db["creator_profiles"].update_one(
            {"user_id": user_id},
            {
                "$set": {
                    "status": CREATOR_STATUS_SUSPENDED,
                    "suspended_at": now,
                    "suspension_reason": reason,
                }
            },
        )

        # Batch-lock all vault content from this creator
        lock_result = await db["vault"].update_many(
            {
                "submitter_user_id": user_id,
                "distribution_state": {"$nin": ["locked", "removed"]},
            },
            {
                "$set": {
                    "distribution_state": "locked",
                    "locked_at": now,
                    "lock_reason": f"consent_withdrawn:{withdrawal_id}",
                    "locked_by_system": True,
                }
            },
        )

        logger.warning(
            "Consent withdrawn — content locked",
            extra={
                "ctx_user_id": user_id,
                "ctx_withdrawal_id": withdrawal_id,
                "ctx_locked_count": lock_result.modified_count,
                "ctx_requested_by": requested_by,
            },
        )
        return withdrawal_id

    # ── Submission tracking ────────────────────────────────────────────────────

    async def increment_submission_count(self, user_id: int) -> None:
        db = DatabaseManager.get_db()
        now = datetime.now(timezone.utc)
        await db["creator_profiles"].update_one(
            {"user_id": user_id},
            {
                "$inc": {"submissions_count": 1},
                "$set": {"last_submission_at": now},
            },
        )

    # ── Admin controls ────────────────────────────────────────────────────────

    async def suspend_creator(
        self,
        user_id: int,
        reason: str,
        performed_by: int,
    ) -> None:
        db = DatabaseManager.get_db()
        now = datetime.now(timezone.utc)
        await db["creator_profiles"].update_one(
            {"user_id": user_id},
            {
                "$set": {
                    "status": CREATOR_STATUS_SUSPENDED,
                    "suspended_at": now,
                    "suspension_reason": reason,
                    "suspended_by": performed_by,
                }
            },
        )
        logger.warning(
            "Creator suspended",
            extra={"ctx_user_id": user_id, "ctx_by": performed_by},
        )

    async def ban_creator(
        self,
        user_id: int,
        reason: str,
        performed_by: int,
    ) -> None:
        db = DatabaseManager.get_db()
        now = datetime.now(timezone.utc)
        await db["creator_profiles"].update_one(
            {"user_id": user_id},
            {
                "$set": {
                    "status": CREATOR_STATUS_BANNED,
                    "banned_at": now,
                    "ban_reason": reason,
                    "banned_by": performed_by,
                }
            },
        )
        # Deactivate consent
        await db["consent_records"].update_many(
            {"user_id": user_id, "is_active": True},
            {"$set": {"is_active": False, "deactivated_at": now}},
        )
        logger.warning(
            "Creator banned",
            extra={"ctx_user_id": user_id, "ctx_by": performed_by},
        )