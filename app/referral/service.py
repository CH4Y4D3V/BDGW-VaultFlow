from __future__ import annotations
from pyrogram import Client
from datetime import datetime, timezone, timedelta
from app.referral.db import ReferralRepository
from app.referral.membership import is_channel_member
from app.utils.logger import get_logger

logger = get_logger(__name__)

class ReferralService:
    def __init__(self):
        self.repo = ReferralRepository()

    async def handle_referral_start(self, bot: Client, referrer_id: int, referred_id: int):
        # 1. Anti-Abuse: Block self-referral
        if referrer_id == referred_id:
            logger.warning("Referral blocked: Self-referral attempt", extra={"user": referred_id})
            return False, "You cannot refer yourself."

        # 2. Anti-Abuse: Block if already referred
        existing = await self.repo.get_referral_by_referred(referred_id)
        if existing:
            return False, "You have already been referred or are an existing user."

        # 3. Check Channel Membership
        if not await is_channel_member(bot, referred_id):
            return False, "You must join our main channel first to qualify for referrals."

        # 4. Create Pending Referral (24h qualification period begins)
        await self.repo.create_pending_referral(referrer_id, referred_id)
        await self.repo.upsert_wallet(referrer_id)
        return True, "Referral recorded! Points will be awarded after 24 hours of active membership."

    async def validate_pending_referrals(self, bot: Client):
        """Job 1: Award points to referrers after 24h of membership."""
        now = datetime.now(timezone.utc)
        threshold = now - timedelta(hours=24)
        
        cursor = self.repo.referrals.find({"status": "pending", "created_at": {"$lt": threshold}})
        async for ref in cursor:
            referred_id = ref["referred_user_id"]
            referrer_id = ref["referrer_user_id"]
            
            # Re-verify membership still active
            if await is_channel_member(bot, referred_id):
                await self.repo.activate_referral(referred_id)
                await self.repo.increment_wallet_points(referrer_id, 1)
                logger.info("Referral activated", extra={"referrer": referrer_id, "referred": referred_id})
            else:
                # User left before 24h
                await self.repo.invalidate_referral(referred_id)
                logger.info("Referral failed: User left before 24h", extra={"referred": referred_id})

    async def check_and_sync_active_referrals(self, bot: Client):
        """Job 2: Dynamic points - deduct on leave, restore on rejoin."""
        cursor = self.repo.referrals.find({"status": {"$in": ["active", "inactive"]}})
        async for ref in cursor:
            referred_id = ref["referred_user_id"]
            referrer_id = ref["referrer_user_id"]
            current_status = ref["status"]
            
            is_member = await is_channel_member(bot, referred_id)
            
            if current_status == "active" and not is_member:
                # User left: Invalidate and deduct point
                await self.repo.invalidate_referral(referred_id)
                await self.repo.increment_wallet_points(referrer_id, -1)
                logger.info("Referral invalidated: User left", extra={"referrer": referrer_id, "referred": referred_id})
                
            elif current_status == "inactive" and is_member:
                # User rejoined: Restore and grant point back
                await self.repo.restore_referral(referred_id)
                await self.repo.increment_wallet_points(referrer_id, 1)
                logger.info("Referral restored: User rejoined", extra={"referrer": referrer_id, "referred": referred_id})
