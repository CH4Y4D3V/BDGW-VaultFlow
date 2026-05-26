from __future__ import annotations
import structlog
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.interval import IntervalTrigger
from app.referral.service import ReferralService

logger = structlog.get_logger(__name__)

class ReferralScheduler:
    def __init__(self, service: ReferralService, scheduler: AsyncIOScheduler, channel_id: int):
        self._service = service
        self._scheduler = scheduler
        self._channel_id = channel_id

    def register_jobs(self) -> None:
        # Register one APScheduler job
        self._scheduler.add_job(
            self._qualification_job,
            trigger=IntervalTrigger(hours=1),
            id='referral_qualification_sweep',
            misfire_grace_time=300,
            max_instances=1,
            replace_existing=True
        )

    async def _qualification_job(self) -> None:
        # THIN. No business logic.
        count = await self._service.qualify_pending_referrals(self._channel_id)
        logger.info('referral_qualification_sweep', qualified_count=count)
