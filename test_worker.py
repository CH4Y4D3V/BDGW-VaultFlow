import asyncio
import logging
from app.config import settings
from motor.motor_asyncio import AsyncIOMotorClient
from app.repositories.queue_repository import QueueRepository
from app.watermark.dispatcher_worker import DispatcherWorker
from app.core.logger import setup_logging
from app.distribution.flood_wait import FloodWaitHandler
from app.distribution.lock_service import DistributedLockService
from app.distribution.dispatcher import DistributionDispatcher
from app.bot.client import get_bot

async def run_worker():
    setup_logging(level="DEBUG", debug=True)
    client = AsyncIOMotorClient(settings.MONGO_URI)
    db = client[settings.MONGO_DB_NAME]
    queue_repo = QueueRepository(db)
    lock_service = DistributedLockService(db, "test_worker")
    flood = FloodWaitHandler()
    bot = get_bot()
    dispatcher = DistributionDispatcher(queue_repo, flood, bot)
    
    worker = DispatcherWorker("test_worker", queue_repo, lock_service, dispatcher, flood)
    worker._running = True
    await worker._run_loop()

if __name__ == "__main__":
    asyncio.run(run_worker())
