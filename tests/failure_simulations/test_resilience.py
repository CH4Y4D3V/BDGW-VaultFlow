import pytest
import asyncio
from datetime import datetime, timezone, timedelta
from motor.motor_asyncio import AsyncIOMotorClient
from app.config import settings
from app.scheduler.scheduler import DistributionScheduler
from app.repositories.queue_repository import QueueRepository

@pytest.fixture
async def db():
    client = AsyncIOMotorClient(settings.MONGO_URI)
    db = client[settings.MONGO_DB_NAME + "_fail_test"]
    yield db
    await client.drop_database(settings.MONGO_DB_NAME + "_fail_test")

@pytest.mark.asyncio
async def test_scheduler_singleton_lock(db):
    # 1. Start scheduler A
    sched_a = DistributionScheduler(db, lambda: [])
    await sched_a.start()
    assert sched_a._started is True
    
    # 2. Start scheduler B
    sched_b = DistributionScheduler(db, lambda: [])
    await sched_b.start()
    assert sched_b._started is False, "Scheduler B should have failed to acquire lock"
    
    # 3. Stop A, then B should be able to start
    await sched_a.stop()
    await sched_b.start()
    assert sched_b._started is True
    await sched_b.stop()

@pytest.mark.asyncio
async def test_startup_integrity_scan(db):
    repo = QueueRepository(db)
    queue = db[settings.QUEUE_COLLECTION]
    
    # Simulate orphaned LOCKED jobs
    await queue.insert_one({
        "status": "locked",
        "locked_by": "dead_worker",
        "locked_at": datetime.now(timezone.utc) - timedelta(hours=2),
        "content_id": "orphaned_1",
        "vault_chat_id": 123,
        "vault_message_id": 456
    })
    
    # Simulate job with missing vault ref
    await queue.insert_one({
        "status": "pending",
        "content_id": "corrupt_1",
        "vault_chat_id": None,
        "vault_message_id": None
    })
    
    # Run scan
    sched = DistributionScheduler(db, lambda: [])
    await sched._run_startup_integrity_scan()
    
    # Verify
    job1 = await queue.find_one({"content_id": "orphaned_1"})
    assert job1["status"] == "pending", "Orphaned job should be reset to pending"
    assert job1["locked_by"] is None
    
    job2 = await queue.find_one({"content_id": "corrupt_1"})
    assert job2["status"] == "quarantine", "Corrupt job should be moved to quarantine"
    assert job2["quarantine_reason"] == "missing_vault_references"
