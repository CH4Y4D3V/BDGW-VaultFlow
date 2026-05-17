from __future__ import annotations

import json
import logging
import contextvars
import sys
import traceback
from datetime import datetime, timezone
from typing import Any, Dict

# ── Correlation ID context var ────────────────────────────────────────────────

_correlation_id_ctx: contextvars.ContextVar[str] = contextvars.ContextVar(
    "correlation_id", default=""
)


def set_correlation_id(cid: str) -> contextvars.Token:
    return _correlation_id_ctx.set(cid)


def reset_correlation_id(token: contextvars.Token) -> None:
    _correlation_id_ctx.reset(token)


# ── JSON formatter ────────────────────────────────────────────────────────────

class _JSONFormatter(logging.Formatter):
    _RESERVED: frozenset = frozenset({
        "args", "created", "exc_info", "exc_text", "filename",
        "funcName", "id", "levelname", "levelno", "lineno", "message",
        "module", "msecs", "msg", "name", "pathname", "process",
        "processName", "relativeCreated", "stack_info", "thread", "threadName",
    })

    def format(self, record: logging.LogRecord) -> str:
        log_record: Dict[str, Any] = {
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
            "module": record.module,
            "function": record.funcName,
            "line": record.lineno,
            "pid": record.process,
        }

        cid = _correlation_id_ctx.get()
        if cid:
            log_record["correlation_id"] = cid

        if record.exc_info:
            exc_type, exc_value, exc_tb = record.exc_info
            log_record["exception"] = {
                "type": exc_type.__name__ if exc_type else None,
                "message": str(exc_value),
                "traceback": traceback.format_exception(exc_type, exc_value, exc_tb),
            }

        extras = {
            k: v
            for k, v in record.__dict__.items()
            if k not in self._RESERVED and not k.startswith("_")
        }
        if extras:
            log_record["context"] = extras

        return json.dumps(log_record, ensure_ascii=False, default=str)


# ── Public API ────────────────────────────────────────────────────────────────

def setup_logging(level: str = "INFO", debug: bool = False) -> None:
    """
    Configure the root logger. Call once at process startup in main_bot.py.
    Clears any previously attached handlers to avoid duplicate output.
    """
    root = logging.getLogger()
    root.handlers.clear()

    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(_JSONFormatter())
    root.addHandler(handler)

    effective_level = logging.DEBUG if debug else getattr(logging, level, logging.INFO)
    root.setLevel(effective_level)

    # Suppress noisy third-party loggers
    for name in ("pyrogram", "motor", "pymongo", "asyncio", "apscheduler"):
        logging.getLogger(name).setLevel(logging.WARNING)


def get_logger(name: str) -> logging.Logger:
    """Return a namespaced logger. All app loggers live under vaultflow.*"""
    return logging.getLogger(f"vaultflow.{name}")


class LogContext:
    """
    Context manager that injects structured key=value pairs into every log
    record emitted within its scope.

    Usage:
        with LogContext(logger, job_id=job_id, target=target_id):
            logger.info("Processing job")   # record has ctx_job_id, ctx_target
    """

    def __init__(self, logger: logging.Logger, **context_fields: Any):
        self._logger = logger
        self._fields = context_fields
        self._old_factory = logging.getLogRecordFactory()

    def __enter__(self) -> "LogContext":
        fields = self._fields

        def _factory(*args: Any, **kwargs: Any) -> logging.LogRecord:
            record = self._old_factory(*args, **kwargs)
            for k, v in fields.items():
                setattr(record, f"ctx_{k}", v)
            return record

        logging.setLogRecordFactory(_factory)
        return self

    def __exit__(self, *_: Any) -> None:
        logging.setLogRecordFactory(self._old_factory)