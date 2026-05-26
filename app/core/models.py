from datetime import datetime
from enum import Enum
from typing import Optional, List

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
    COMPLETED = "completed"
    FAILED = "failed"
    DEAD = "dead"


class ModerationState(str, Enum):
    PENDING = "pending"
    APPROVED = "approved"
    QUEUED = "queued"
    REJECTED = "rejected"
    POSTED = "posted"
    FAILED = "failed"


class ModerationDestination(str, Enum):
    NSFW = "nsfw"
    PREMIUM = "premium"


class DistributionPriority(int, Enum):
    LOW = 0
    NORMAL = 1
    HIGH = 2
    MODERATED = 3


class WatermarkPosition(str, Enum):
    BOTTOM_RIGHT = "BOTTOM_RIGHT"
    BOTTOM_LEFT = "BOTTOM_LEFT"
    TOP_RIGHT = "TOP_RIGHT"
    TOP_LEFT = "TOP_LEFT"
    CENTER = "CENTER"


class QueueJob(BaseModel):
    id: Optional[str] = Field(None, alias="_id")

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

    # ─────────────────────────────
    # Media
    # ─────────────────────────────

    media_type: MediaType

    media_file_id: Optional[str] = None
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

    failed_targets: List[dict] = Field(default_factory=list)

    locked_by: Optional[str] = None
    locked_at: Optional[datetime] = None

    completed_at: Optional[datetime] = None
    failed_at: Optional[datetime] = None

    # ─────────────────────────────
    # Watermark
    # ─────────────────────────────

    watermark_required: bool = False
    watermark_config: Optional[dict] = None

    # ─────────────────────────────
    # Metadata
    # ─────────────────────────────

    metadata: dict = Field(default_factory=dict)