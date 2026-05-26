from __future__ import annotations
import asyncio
from datetime import datetime, timezone
from typing import Optional, List
import structlog
from pyrogram import Client
from pyrogram.enums import ChatMemberStatus
from pyrogram.errors import UserNotParticipant, FloodWait
from app.referral.repository import ReferralRepository
from app.referral.models import ReferralStatus

logger = structlog.get_logger(__name__)

class ReferralService:
    def __init__(self, repository: ReferralRepository, bot: Client):
        self._repo = repository
        self._bot = bot

    async def register_referral(self, referrer_id: int, referred_id: int) -> bool:
        # 1. referrer_id == referred_id -> return False (self-referral)
        if referrer_id == referred_id:
            logger.warning("referral_rejected", referrer_id=referrer_id, referred_id=referred_id, reason="self_referral")
            return False

        # 2. Check if referred_id already has a referral in DB -> return False
        existing = await self._repo.get_referral_by_referred(referred_id)
        if existing:
            logger.warning("referral_rejected", referrer_id=referrer_id, referred_id=referred_id, reason="already_referred")
            return False

        # 3. Create pending referral via repository.create_pending()
        success = await self._repo.create_pending(referrer_id, referred_id)
        if not success:
            logger.warning("referral_rejected", referrer_id=referrer_id, referred_id=referred_id, reason="db_insert_failed")
            return False

        # 4. Ensure referrer wallet exists via repository.upsert_wallet()
        await self._repo.upsert_wallet(referrer_id)
        
        logger.info("referral_pending", referrer_id=referrer_id, referred_id=referred_id)
        return True

    async def check_membership(self, user_id: int, channel_id: int) -> bool:
        try:
            member = await self._bot.get_chat_member(channel_id, user_id)
            return member.status in [
                ChatMemberStatus.MEMBER,
                ChatMemberStatus.ADMINISTRATOR,
                ChatMemberStatus.OWNER
            ]
        except UserNotParticipant:
            return False
        except FloodWait as e:
            await asyncio.sleep(e.value)
            # Retry once
            try:
                member = await self._bot.get_chat_member(channel_id, user_id)
                return member.status in [ChatMemberStatus.MEMBER, ChatMemberStatus.ADMINISTRATOR, ChatMemberStatus.OWNER]
            except Exception:
                return False
        except Exception:
            return False

    async def qualify_pending_referrals(self, channel_id: int) -> int:
        # Get all pending referrals older than 24 hours
        pending = await self._repo.get_pending_older_than(24)
        qualified_count = 0
        
        for ref in pending:
            referred_id = ref["referred_user_id"]
            referrer_id = ref["referrer_user_id"]
            
            # Check channel membership
            if await self.check_membership(referred_id, channel_id):
                # If member: repository.qualify_referral(), repository.increment_balance(referrer_id, +1)
                success = await self._repo.qualify_referral(referred_id)
                if success:
                    await self._repo.increment_balance(referrer_id, 1)
                    qualified_count += 1
            
            await asyncio.sleep(0.1)  # Rate safety
            
        return qualified_count

    async def handle_member_left(self, user_id: int) -> None:
        # Get referral where referred_user_id=user_id and status=QUALIFIED
        ref = await self._repo.get_referral_by_referred(user_id)
        if ref and ref["status"] == ReferralStatus.QUALIFIED:
            referrer_id = ref["referrer_user_id"]
            # If found: repository.invalidate_referral(user_id), repository.decrement_balance(referrer_id)
            await self._repo.invalidate_referral(user_id)
            await self._repo.decrement_balance(referrer_id)
            logger.info("referral_invalidated", user_id=user_id, referrer_id=referrer_id)

    async def handle_member_rejoined(self, user_id: int, channel_id: int) -> None:
        # Get referral where referred_user_id=user_id and status=INVALIDATED
        ref = await self._repo.get_referral_by_referred(user_id)
        if ref and ref["status"] == ReferralStatus.INVALIDATED:
            # If found: verify channel membership via check_membership()
            if await self.check_membership(user_id, channel_id):
                # If confirmed member: repository.reactivate_referral(user_id), repository.increment_balance(referrer_id, +1)
                referrer_id = ref["referrer_user_id"]
                await self._repo.reactivate_referral(user_id)
                await self._repo.increment_balance(referrer_id, 1)
                logger.info("referral_reactivated", user_id=user_id, referrer_id=referrer_id)

    async def get_wallet(self, user_id: int) -> Optional[dict]:
        return await self._repo.get_wallet(user_id)

    async def snapshot_discount(self, user_id: int, points_to_use: int, plan_price: int) -> dict:
        wallet = await self._repo.get_wallet(user_id)
        if not wallet or wallet["points_balance"] < points_to_use:
            raise ValueError('Insufficient referral balance')
        
        if points_to_use > plan_price:
            raise ValueError('Cannot discount more than plan price')

        # Deduct via repository.deduct_points(user_id, points_to_use)
        success = await self._repo.deduct_points(user_id, points_to_use)
        if not success:
            raise ValueError('Insufficient referral balance')

        return {
            'original_price': plan_price,
            'discount_applied': points_to_use,
            'final_price': plan_price - points_to_use,
            'snapshotted_at': datetime.now(timezone.utc).isoformat()
        }
