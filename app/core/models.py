from datetime import datetime
from enum import Enum
from typing import Optional, List, Dict

from pydantic import BaseModel, Field


class MediaType(str, Enum):
    PHOTO = "photo"
    VIDEO = "video"
    TEXT = "text"
    DOCUMENT = "document"


class JobStatus(str, Enum):
    PENDING = "pending"
    LOCKED = "locked"
    PROCESSING = "processing"
    WATERMARKING = "watermarking"
    READY = "ready"
    DELIVERING = "delivering"
    COMPLETED = "completed"
    FAILED = "failed"
    DEAD = "dead"
    QUARANTINE = "quarantine"


class WatermarkState(str, Enum):
    PENDING = "pending"
    PROCESSING = "processing"
    UPLOADED = "uploaded"
    COMPLETED = "completed"
    FAILED = "failed"


class ModerationState(str, Enum):
    PENDING = "pending"
    APPROVED = "approved"
    QUEUED = "queued"
    REJECTED = "rejected"
    POSTED = "posted"
    FAILED = "failed"
    QUARANTINE = "quarantine"


class ModerationDestination(str, Enum):
    PENDING = "pending"
    NSFW = "nsfw"
    PREMIUM = "premium"


class DistributionPriority(int, Enum):
    LOW = 0
    NORMAL = 1
    HIGH = 2
    MODERATED = 3


class DistributionResult(BaseModel):
    job_id: str
    target_id: str
    success: bool
    delivered_at: Optional[datetime] = None
    error: Optional[str] = None
    floodwait_seconds: Optional[int] = None


class WatermarkPosition(str, Enum):
    BOTTOM_RIGHT = "BOTTOM_RIGHT"
    BOTTOM_LEFT = "BOTTOM_LEFT"
    TOP_RIGHT = "TOP_RIGHT"
    TOP_LEFT = "TOP_LEFT"
    CENTER = "CENTER"


class QueueMetrics(BaseModel):
    pending_count: int = 0
    processing_count: int = 0
    completed_count: int = 0
    failed_count: int = 0
    dead_count: int = 0


# Alias for backwards compatibility or expected naming convention
QueueState = JobStatus


class QueueJob(BaseModel):
    id: Optional[str] = Field(None, alias="_id")

    # ─────────────────────────────
    # Versioning
    # ─────────────────────────────
    schema_version: int = 1
    migration_version: int = 0

    # ─────────────────────────────
    # Stable identifiers
    # ─────────────────────────────

    content_id: str

    source_channel_id: str
    source_message_id: Optional[int] = None

    vault_chat_id: int
    vault_message_id: int

    media_group_id: Optional[str] = None

    # ─────────────────────────────
    # Distribution
    # ─────────────────────────────

    target_channel_ids: List[str]
    delivery_key: Optional[str] = None  # "{job_id}:{target_id}" for idempotency
    album_delivery_batch_id: Optional[str] = None  # for sequential fallback tracking

    # ─────────────────────────────
    # Media
    # ─────────────────────────────

    media_type: MediaType

    media_file_id: Optional[str] = None  # METADATA ONLY - NEVER USE FOR DELIVERY
    media_unique_id: Optional[str] = None

    media_path: Optional[str] = None
    caption: Optional[str] = None

    # ─────────────────────────────
    # Queue execution
    # ─────────────────────────────

    priority: DistributionPriority = DistributionPriority.NORMAL

    status: JobStatus = JobStatus.PENDING

    retry_count: int = 0
    max_retries: int = 3

    execute_after: Optional[datetime] = None
    queue_deadline: Optional[datetime] = None

    # ─────────────────────────────
    # Delivery state
    # ─────────────────────────────

    delivered_targets: List[str] = Field(default_factory=list)

    failed_targets: Dict[str, str] = Field(default_factory=dict)

    locked_by: Optional[str] = None
    locked_at: Optional[datetime] = None

    completed_at: Optional[datetime] = None
    failed_at: Optional[datetime] = None

    # ─────────────────────────────
    # Watermark tracking
    # ─────────────────────────────

    watermark_required: bool = False
    watermark_config: Optional[dict] = None
    watermark_generation_id: Optional[str] = None
    album_sequence_index: Optional[int] = None
    watermark_state: WatermarkState = WatermarkState.PENDING

    # ─────────────────────────────
    # Metadata
    # ─────────────────────────────

    metadata: dict = Field(default_factory=dict)