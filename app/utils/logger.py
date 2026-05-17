import logging
import json
import sys
import traceback
from datetime import datetime, timezone
from typing import Any, Optional
from app.config import settings


class StructuredFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        log_obj: dict[str, Any] = {
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
            "module": record.module,
            "function": record.funcName,
            "line": record.lineno,
        }

        if record.exc_info:
            log_obj["exception"] = {
                "type": record.exc_info[0].__name__ if record.exc_info[0] else None,
                "message": str(record.exc_info[1]) if record.exc_info[1] else None,
                "traceback": traceback.format_exception(*record.exc_info),
            }

        for key, value in record.__dict__.items():
            if key.startswith("ctx_"):
                log_obj[key[4:]] = value

        return json.dumps(log_obj, default=str)


def get_logger(name: str) -> logging.Logger:
    logger = logging.getLogger(name)

    if logger.handlers:
        return logger

    logger.setLevel(getattr(logging, settings.LOG_LEVEL.upper(), logging.INFO))

    handler = logging.StreamHandler(sys.stdout)

    if settings.LOG_FORMAT == "json":
        handler.setFormatter(StructuredFormatter())
    else:
        handler.setFormatter(
            logging.Formatter(
                "%(asctime)s [%(levelname)s] %(name)s %(funcName)s:%(lineno)d — %(message)s"
            )
        )

    logger.addHandler(handler)
    logger.propagate = False
    return logger


class LogContext:
    """Context manager to add structured fields to log records."""

    def __init__(self, logger: logging.Logger, **context_fields):
        self.logger = logger
        self.context_fields = context_fields
        self._old_factory = logging.getLogRecordFactory()

    def __enter__(self):
        fields = self.context_fields

        def record_factory(*args, **kwargs):
            record = self._old_factory(*args, **kwargs)
            for k, v in fields.items():
                setattr(record, f"ctx_{k}", v)
            return record

        logging.setLogRecordFactory(record_factory)
        return self

    def __exit__(self, *args):
        logging.setLogRecordFactory(self._old_factory)
