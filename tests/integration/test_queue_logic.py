import pytest
import asyncio
from datetime import datetime, timezone, timedelta
from motor.motor_asyncio import AsyncIOMotorClient
from app.config import settings
from app.core.models import QueueJob, JobStatus, MediaType, WatermarkState
from app.repositories.queue_repository import QueueRepository
from app.core.exceptions import DuplicateJobError

@pytest.fixture
async def db():
    client = AsyncIOMotorClient(settings.MONGO_URI)
    db = client[settings.MONGO_DB_NAME + "_test"]
    yield db
    await client.drop_database(settings.MONGO_DB_NAME + "_test")

@pytest.fixture
def repo(db):
    return QueueRepository(db)

@pytest.mark.asyncio
async def test_atomic_album_claiming(repo, db):
    # Setup: 4 jobs in an album
    album_id = "test_album_123"
    for i in range(4):
        job = QueueJob(
            schema_version=1,
            content_id=f"content_{i}",
            source_channel_id="src",
            vault_chat_id=1,
            vault_message_id=100 + i,
            media_group_id=album_id,
            target_channel_ids=["target"],
            media_type=MediaType.PHOTO,
            album_sequence_index=i
        )
        await repo.enqueue(job)

    # Simulate concurrent claiming
    worker_a = "worker_a"
    worker_b = "worker_b"
    
    # Worker A claims next
    claimed_a = await repo.claim_next(worker_a, batch_size=1)
    
    # Worker B claims next
    claimed_b = await repo.claim_next(worker_b, batch_size=1)

    # Verification
    assert len(claimed_a) == 4, "Worker A should have claimed the whole album"
    assert len(claimed_b) == 0, "Worker B should have found nothing left"
    
    # Check DB state
    cursor = db[settings.QUEUE_COLLECTION].find({"media_group_id": album_id})
    async for doc in cursor:
        assert doc["locked_by"] == worker_a
        assert doc["status"] == JobStatus.LOCKED

@pytest.mark.asyncio
async def test_delivery_idempotency(repo):
    job_id = "507f1f77bcf86cd799439011"
    target_id = "target_1"
    
    # 1. Acquire lock
    acquired = await repo.acquire_delivery_lock(job_id, target_id)
    assert acquired is True
    
    # 2. Try to acquire again (same worker or different)
    acquired_again = await repo.acquire_delivery_lock(job_id, target_id)
    assert acquired_again is False, "Lock should prevent duplicate delivery"
    
    # 3. Release and re-acquire
    await repo.release_delivery_lock(job_id, target_id)
    acquired_final = await repo.acquire_delivery_lock(job_id, target_id)
    assert acquired_final is True

@pytest.mark.asyncio
async def test_watermark_atomic_swap(repo, db):
    album_id = "album_wm_test"
    # Setup album
    for i in range(2):
        job = QueueJob(
            schema_version=1,
            content_id=f"wm_content_{i}",
            source_channel_id="src",
            vault_chat_id=1,
            vault_message_id=200 + i,
            media_group_id=album_id,
            target_channel_ids=["target"],
            media_type=MediaType.PHOTO,
            album_sequence_index=i,
            status=JobStatus.WATERMARKING
        )
        await repo.enqueue(job)

    # New vault refs from processed files
    new_refs = [
        {"album_sequence_index": 0, "vault_message_id": 999},
        {"album_sequence_index": 1, "vault_message_id": 1000}
    ]
    
    await repo.swap_album_vault_references(album_id, new_refs)
    
    # Verify updates
    cursor = db[settings.QUEUE_COLLECTION].find({"media_group_id": album_id})
    async for doc in cursor:
        if doc["album_sequence_index"] == 0:
            assert doc["vault_message_id"] == 999
        else:
            assert doc["vault_message_id"] == 1000
        assert doc["status"] == JobStatus.PENDING
        assert doc["watermark_state"] == WatermarkState.COMPLETED
