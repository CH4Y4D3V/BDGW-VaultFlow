class VaultFlowBaseError(Exception):
    pass


class RetryableError(VaultFlowBaseError):
    pass


class PermanentJobError(VaultFlowBaseError):
    pass


class RetryableDeliveryError(RetryableError):
    pass


class PermanentDeliveryError(PermanentJobError):
    pass


class QueueLockError(RetryableError):
    pass


class StaleLockError(RetryableError):
    pass


class JobNotFoundError(PermanentJobError):
    pass


class MaxRetriesExceededError(PermanentJobError):
    pass


class FloodWaitError(RetryableDeliveryError):
    def __init__(self, seconds: int, message: str = ""):
        self.seconds = seconds
        super().__init__(message or f"FloodWait: wait {seconds}s")


class WatermarkProcessingError(PermanentJobError):
    pass


class FFmpegTimeoutError(WatermarkProcessingError):
    pass


class FFmpegNotFoundError(WatermarkProcessingError):
    pass


class MediaFileNotFoundError(PermanentJobError):
    pass


class DispatcherError(RetryableDeliveryError):
    pass


class RateLimitExceededError(RetryableDeliveryError):
    pass


class DuplicateJobError(PermanentJobError):
    pass


class VaultReferenceMissingError(PermanentJobError):
    pass


class InvalidQueueJobError(PermanentJobError):
    pass


class VaultMessageDeletedError(PermanentJobError):
    pass


class ConsistencyViolationError(PermanentJobError):
    """Raised when a data consistency rule is violated (e.g. duplicate delivery prevented)."""
    pass


class APIDegradationError(RetryableDeliveryError):
    """Raised when Telegram API is showing signs of degradation (e.g. copy_media_group failure)."""
    pass


class QuarantineError(PermanentJobError):
    """Raised when a job is unrecoverable and must be moved to quarantine."""
    pass