from __future__ import annotations

import asyncio
from typing import Optional

from pyrogram import Client

from app.services.cleanup_service import get_cleanup_service
from app.utils.logger import get_logger

logger = get_logger(__name__)

_SWEEP_INTERVAL_SECONDS = 300  # 5 minutes


class CleanupWorker:
    """
    Background worker that triggers the Message Cleanup sweep.
    Enforces Section 20 policies:
    - 60min general conversation
    - 20min payment session messages
    - 7min phone number messages
    """

    def __init__(self) -> None:
        self._bot: Optional[Client] = None
        self._running = False
        self._task: Optional[asyncio.Task] = None

    async def start(self, bot: Client) -> None:
        if self._running:
            return
        self._bot = bot
        self._running = True
        self._task = asyncio.create_task(
            self._run_loop(), name="cleanup-worker"
        )
        logger.info("Cleanup worker started")

    async def stop(self) -> None:
        self._running = False
        if self._task and not self._task.done():
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
        logger.info("Cleanup worker stopped")

    async def _run_loop(self) -> None:
        service = get_cleanup_service(self._bot)
        while self._running:
            try:
                await service.run_cleanup_sweep()
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error("Cleanup sweep unhandled error", exc_info=e)
            
            try:
                await asyncio.sleep(_SWEEP_INTERVAL_SECONDS)
            except asyncio.CancelledError:
                break
