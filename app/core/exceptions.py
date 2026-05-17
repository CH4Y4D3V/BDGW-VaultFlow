class VaultFlowBaseError(Exception):
    """Base for all VaultFlow distribution engine errors."""


class QueueLockError(VaultFlowBaseError):
    """Failed to acquire distributed lock."""


class StaleLockError(VaultFlowBaseError):
    """Lock is stale and was forcefully released."""


class JobNotFoundError(VaultFlowBaseError):
    """Referenced job does not exist in the queue."""


class MaxRetriesExceededError(VaultFlowBaseError):
    """Job has exhausted all retry attempts."""


class FloodWaitError(VaultFlowBaseError):
    """Telegram FloodWait encountered during dispatch."""

    def __init__(self, seconds: int, message: str = ""):
        self.seconds = seconds
        super().__init__(message or f"FloodWait: must wait {seconds}s")


class WatermarkProcessingError(VaultFlowBaseError):
    """FFmpeg watermark processing failed."""


class FFmpegTimeoutError(WatermarkProcessingError):
    """FFmpeg process exceeded allowed timeout."""


class FFmpegNotFoundError(WatermarkProcessingError):
    """FFmpeg binary not found on system."""


class MediaFileNotFoundError(VaultFlowBaseError):
    """Source media file does not exist."""


class DispatcherError(VaultFlowBaseError):
    """Generic dispatcher failure."""


class RateLimitExceededError(VaultFlowBaseError):
    """Internal rate limit exceeded; caller should back off."""


class DuplicateJobError(VaultFlowBaseError):
    """Job for this content/target combination already exists."""
