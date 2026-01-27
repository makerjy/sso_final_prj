"""에이전트 파이프라인용 로깅 유틸."""
from __future__ import annotations

import logging
from typing import Any, Dict


_LOGGER_NAME = "query_visualization"


def get_logger() -> logging.Logger:
    """모듈 공통 로거 반환."""
    logger = logging.getLogger(_LOGGER_NAME)
    if logger.handlers:
        return logger

    logger.setLevel(logging.INFO)
    handler = logging.StreamHandler()
    formatter = logging.Formatter(
        "%(asctime)s | %(levelname)s | %(name)s | %(message)s"
    )
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    return logger


def log_event(event: str, payload: Dict[str, Any] | None = None) -> None:
    """이벤트 로그 기록."""
    logger = get_logger()
    if payload is None:
        logger.info("%s", event)
        return

    # 너무 길어지지 않도록 키만 간단히 출력
    keys = ", ".join(payload.keys())
    logger.info("%s | keys=%s", event, keys)
