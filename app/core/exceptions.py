class VaultFlowBaseError(Exception):
    pass


class RetryableError(VaultFlowBaseError):
    pass


class PermanentJobError(VaultFlowBaseError):
    pass


class QueueLockError(RetryableError):
    pass


class StaleLockError(RetryableError):
    pass


class JobNotFoundError(PermanentJobError):
    pass


class MaxRetriesExceededError(PermanentJobError):
    pass


class FloodWaitError(RetryableError):
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


class DispatcherError(RetryableError):
    pass


class RateLimitExceededError(RetryableError):
    pass


class DuplicateJobError(PermanentJobError):
    pass


class VaultReferenceMissingError(PermanentJobError):
    pass


class InvalidQueueJobError(PermanentJobError):
    pass


class VaultMessageDeletedError(PermanentJobError):
    pass