from __future__ import annotations

from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.interval import IntervalTrigger

from app.referral.service import ReferralService
from app.utils.logger import get_logger

logger = get_logger(__name__)


class ReferralScheduler:
    def __init__(self, service: ReferralService, scheduler: AsyncIOScheduler, channel_id: int) -> None:
        self._service = service
        self._scheduler = scheduler
        self._channel_id = channel_id

    def register_jobs(self) -> None:
        self._scheduler.add_job(
            self._qualification_job,
            trigger=IntervalTrigger(hours=1),
            id='referral_qualification_sweep',
            misfire_grace_time=300,
            max_instances=1,
            replace_existing=True,
        )
        logger.info('Referral qualification sweep job registered')

    async def _qualification_job(self) -> None:
        try:
            count = await self._service.qualify_pending_referrals(self._channel_id)
            logger.info('referral_qualification_sweep', extra={'ctx_qualified_count': count})
        except Exception as e:
            logger.error('referral_qualification_sweep failed', extra={'ctx_error': str(e)}, exc_info=True)

    async def stop(self) -> None:
        logger.info('ReferralScheduler stop called')
