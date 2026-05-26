from __future__ import annotations
import asyncio
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from app.referral.service import ReferralService
from pyrogram import Client

class ReferralScheduler:
    def __init__(self, bot: Client):
        self._bot = bot
        self._service = ReferralService()
        self._scheduler = AsyncIOScheduler(timezone="UTC")
        self._running = False

    async def start(self):
        if self._running:
            return
            
        # Ensure DB indexes exist
        await self._service.repo.create_indexes()

        # Job 1: Validate pending referrals (Every hour)
        self._scheduler.add_job(
            self._service.validate_pending_referrals,
            "interval",
            hours=1,
            args=[self._bot],
            id="referral_validation",
            replace_existing=True
        )

        # Job 2: Recheck active members (Every 6 hours)
        self._scheduler.add_job(
            self._service.check_and_sync_active_referrals,
            "interval",
            hours=6,
            args=[self._bot],
            id="referral_sync",
            replace_existing=True
        )

        self._scheduler.start()
        self._running = True

    async def stop(self):
        if self._running:
            self._scheduler.shutdown()
            self._running = False
