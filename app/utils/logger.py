# Canonical logger is app.core.logger.
# This module re-exports everything so existing imports don't need to change.
from app.core.logger import (  # noqa: F401
    get_logger,
    setup_logging,
    set_correlation_id,
    reset_correlation_id,
    LogContext,
)

__all__ = [
    "get_logger",
    "setup_logging",
    "set_correlation_id",
    "reset_correlation_id",
    "LogContext",
]