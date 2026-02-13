"""Structured logging helpers for query visualization."""
from __future__ import annotations

import json
import logging
from datetime import datetime, timezone
from typing import Any, Dict
from uuid import uuid4


_LOGGER_NAME = "query_visualization"


def get_logger() -> logging.Logger:
    """Return a shared logger instance."""
    logger = logging.getLogger(_LOGGER_NAME)
    if logger.handlers:
        return logger

    logger.setLevel(logging.INFO)
    handler = logging.StreamHandler()
    formatter = logging.Formatter(
        "%(asctime)s | %(levelname)s | %(name)s | %(message)s")
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    return logger


def new_request_id() -> str:
    """Generate a request id to trace a single pipeline execution."""
    return f"qv-{uuid4().hex[:12]}"


def log_event(
    event: str,
    payload: Dict[str, Any] | None = None,
    *,
    level: str = "info",
) -> None:
    """Write one structured log event in JSON format."""
    logger = get_logger()
    data: Dict[str, Any] = {
        "event": event,
        "ts": datetime.now(timezone.utc).isoformat(),
    }
    if payload:
        data.update(payload)

    message = json.dumps(data, ensure_ascii=False, default=str)
    writer = getattr(logger, level.lower(), logger.info)
    writer("%s", message)
