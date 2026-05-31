from __future__ import annotations

import asyncio
from datetime import datetime, timezone
from typing import Optional

from pymongo import ReturnDocument  # FIX BUG E: was missing
from pyrogram import Client
from pyrogram.enums import ChatMemberStatus
from pyrogram.errors import FloodWait, UserNotParticipant

from app.referral.repository import ReferralRepository
from app.referral.models import ReferralStatus
from app.utils.logger import get_logger

logger = get_logger(__name__)


class ReferralService:
    def __init__(self, repository: ReferralRepository, bot: Client) -> None:
        self._repo = repository
        self._bot = bot

    async def register_referral(self, referrer_id: int, referred_id: int) -> bool:
        """
        Register a pending referral. Referral is only credited after the referred
        user joins the main channel (Section 16).
        """
        if referrer_id == referred_id:
            logger.warning(
                "referral_rejected",
                extra={
                    "ctx_referrer_id": referrer_id,
                    "ctx_referred_id": referred_id,
                    "ctx_reason": "self_referral",
                },
            )
            return False

        existing = await self._repo.get_referral_by_referred(referred_id)
        if existing:
            logger.warning(
                "referral_rejected",
                extra={
                    "ctx_referrer_id": referrer_id,
                    "ctx_referred_id": referred_id,
                    "ctx_reason": "already_referred",
                },
            )
            return False

        success = await self._repo.create_pending(referrer_id, referred_id)
        if not success:
            return False

        await self._repo.upsert_wallet(referrer_id)
        logger.info(
            "referral_pending",
            extra={"ctx_referrer_id": referrer_id, "ctx_referred_id": referred_id},
        )
        return True

    async def check_membership(self, user_id: int, channel_id: int) -> bool:
        """Verify user is an active member of the channel (Section 16 — join verified)."""
        try:
            member = await self._bot.get_chat_member(channel_id, user_id)
            return member.status in (
                ChatMemberStatus.MEMBER,
                ChatMemberStatus.ADMINISTRATOR,
                ChatMemberStatus.OWNER,
            )
        except UserNotParticipant:
            return False
        except FloodWait as e:
            await asyncio.sleep(e.value + 2)
            try:
                member = await self._bot.get_chat_member(channel_id, user_id)
                return member.status in (
                    ChatMemberStatus.MEMBER,
                    ChatMemberStatus.ADMINISTRATOR,
                    ChatMemberStatus.OWNER,
                )
            except Exception:
                return False
        except Exception as e:
            logger.warning(
                "referral_membership_check_failed",
                extra={"ctx_user_id": user_id, "ctx_error": str(e)},
            )
            return False

    async def qualify_pending_referrals(self, channel_id: int) -> int:
        """
        Hourly sweep: qualify pending referrals older than 24h whose referred
        user has joined the channel. Awards 1 point per qualified referral.
        """
        pending = await self._repo.get_pending_older_than(24)
        qualified_count = 0

        for ref in pending:
            referred_id = ref["referred_user_id"]
            referrer_id = ref["referrer_user_id"]

            if await self.check_membership(referred_id, channel_id):
                success = await self._repo.qualify_referral(referred_id)
                if success:
                    # Section 16: 1 referral = 5 points (Flow M)
                    await self._repo.increment_balance(referrer_id, 5)
                    qualified_count += 1
                    logger.info(
                        "referral_qualified",
                        extra={
                            "ctx_referrer_id": referrer_id,
                            "ctx_referred_id": referred_id,
                        },
                    )

            await asyncio.sleep(0.05)  # Avoid hammering Telegram API

        return qualified_count

    async def reward_approved_content(self, referred_id: int) -> None:
        """
        Section 16: Every 3 approved pieces from a referred user = 1 point for referrer.
        FIX BUG E: ReturnDocument now imported — this method no longer raises NameError.
        """
        ref = await self._repo.get_referral_by_referred(referred_id)
        if not ref or ref["status"] != ReferralStatus.QUALIFIED:
            return

        referrer_id = ref["referrer_user_id"]

        from app.core.database import DatabaseManager
        db = DatabaseManager.get_db()

        # Atomically increment approved_content_count and check if divisible by 3
        updated_ref = await db["referrals"].find_one_and_update(
            {"referred_user_id": referred_id},
            {"$inc": {"approved_content_count": 1}},
            return_document=ReturnDocument.AFTER,  # FIX BUG E: now works
        )

        if updated_ref and updated_ref.get("approved_content_count", 0) % 3 == 0:
            await self._repo.increment_balance(referrer_id, 1)
            logger.info(
                "referral_content_reward",
                extra={
                    "ctx_referrer_id": referrer_id,
                    "ctx_referred_id": referred_id,
                    "ctx_approved_count": updated_ref["approved_content_count"],
                },
            )

    async def handle_member_left(self, user_id: int) -> None:
        """
        When a referred user leaves the channel, invalidate their referral
        and decrement the referrer's points (Section 16 — active referrals).
        """
        ref = await self._repo.get_referral_by_referred(user_id)
        if ref and ref["status"] == ReferralStatus.QUALIFIED:
            referrer_id = ref["referrer_user_id"]
            await self._repo.invalidate_referral(user_id)
            await self._repo.decrement_balance(referrer_id)
            logger.info(
                "referral_invalidated",
                extra={
                    "ctx_user_id": user_id,
                    "ctx_referrer_id": referrer_id,
                },
            )

    async def handle_member_rejoined(
        self, user_id: int, channel_id: int
    ) -> None:
        """
        When a previously invalidated referred user rejoins, reactivate
        the referral and restore the referrer's point.
        """
        ref = await self._repo.get_referral_by_referred(user_id)
        if ref and ref["status"] == ReferralStatus.INVALIDATED:
            if await self.check_membership(user_id, channel_id):
                referrer_id = ref["referrer_user_id"]
                await self._repo.reactivate_referral(user_id)
                await self._repo.increment_balance(referrer_id, 1)
                logger.info(
                    "referral_reactivated",
                    extra={
                        "ctx_user_id": user_id,
                        "ctx_referrer_id": referrer_id,
                    },
                )

    async def refund_points(self, user_id: int, points: int) -> bool:
        """Restore points to user wallet (e.g. on payment session expiry)."""
        if points <= 0:
            return True
        await self._repo.increment_balance(user_id, points)
        logger.info(
            "points_refunded",
            extra={"ctx_user_id": user_id, "ctx_points": points},
        )
        return True

    async def get_wallet(self, user_id: int) -> Optional[dict]:
        return await self._repo.get_wallet(user_id)

    async def snapshot_discount(
        self, user_id: int, points_to_use: int, plan_price: int
    ) -> dict:
        """Lock points for a payment discount. Raises ValueError if insufficient."""
        wallet = await self._repo.get_wallet(user_id)
        if not wallet or wallet["points_balance"] < points_to_use:
            raise ValueError("Insufficient referral balance")
        if points_to_use > plan_price:
            raise ValueError("Cannot discount more than plan price")

        success = await self._repo.deduct_points(user_id, points_to_use)
        if not success:
            raise ValueError("Insufficient referral balance (concurrent deduction)")

        return {
            "original_price": plan_price,
            "discount_applied": points_to_use,
            "final_price": plan_price - points_to_use,
            "snapshotted_at": datetime.now(timezone.utc).isoformat(),
        }
