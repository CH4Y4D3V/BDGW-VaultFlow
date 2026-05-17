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


class DistributionPriority(int, Enum):
    LOW = 0
    NORMAL = 1
    HIGH = 2


class WatermarkPosition(str, Enum):
    BOTTOM_RIGHT = "BOTTOM_RIGHT"
    BOTTOM_LEFT = "BOTTOM_LEFT"
    TOP_RIGHT = "TOP_RIGHT"
    TOP_LEFT = "TOP_LEFT"
    CENTER = "CENTER"


class QueueJob(BaseModel):
    id: Optional[str] = Field(None, alias="_id")
    content_id: str
    source_channel_id: str
    target_channel_ids: List[str]
    media_type: MediaType
    media_file_id: Optional[str] = None
    media_path: Optional[str] = None
    caption: Optional[str] = None
    priority: DistributionPriority = DistributionPriority.NORMAL
    status: JobStatus = JobStatus.PENDING
    max_retries: int = 3
    execute_after: Optional[datetime] = None
    watermark_required: bool = False
    watermark_config: Optional[dict] = None
    metadata: dict = Field(default_factory=dict)


class DeadLetterJob(BaseModel):
    original_job_id: str
    content_id: str
    source_channel_id: str
    target_channel_ids: List[str]
    failure_reason: str
    retry_history: List[dict] = Field(default_factory=list)
    final_error: str
    dead_at: datetime
    metadata: dict = Field(default_factory=dict)


class QueueMetrics(BaseModel):
    pending_count: int = 0
    processing_count: int = 0
    completed_count: int = 0
    failed_count: int = 0
    dead_count: int = 0


class DistributionResult(BaseModel):
    job_id: str
    target_id: str
    success: bool
    error: Optional[str] = None
    floodwait_seconds: Optional[int] = None
    delivered_at: Optional[datetime] = None