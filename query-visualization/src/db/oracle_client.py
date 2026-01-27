"""Oracle DB SQL 실행 유틸.

- 환경변수 로딩/DSN 구성은 config/db_config.py에서 담당한다.
- 이 모듈은 SQL 실행(조회/변경)에만 집중한다.
"""
from __future__ import annotations

import os
from contextlib import contextmanager
from typing import Any, Dict, List, Optional

import oracledb

from src.config.db_config import get_oracle_config


def _init_oracle_client() -> None:
    """ORACLE_LIB_DIR가 있으면 thick client 초기화."""
    lib_dir = os.getenv("ORACLE_LIB_DIR")
    if lib_dir:
        # idempotent; will raise if called twice with different params
        oracledb.init_oracle_client(lib_dir=lib_dir)


@contextmanager
def get_connection():
    """Oracle 연결을 생성해서 컨텍스트로 반환."""
    _init_oracle_client()
    params = get_oracle_config()
    conn = oracledb.connect(**params)
    try:
        yield conn
    finally:
        conn.close()


def fetch_all(
    sql: str,
    params: Optional[Dict[str, Any]] = None,
    fetch_size: int = 1000,
) -> List[Dict[str, Any]]:
    """SELECT 실행 후 rows를 list[dict]로 반환."""
    with get_connection() as conn:
        cur = conn.cursor()
        cur.arraysize = fetch_size
        cur.execute(sql, params or {})
        columns = [col[0].lower() for col in cur.description]
        rows = cur.fetchall()
        return [dict(zip(columns, row)) for row in rows]


def execute(
    sql: str,
    params: Optional[Dict[str, Any]] = None,
) -> int:
    """DML 실행 후 영향받은 row count 반환."""
    with get_connection() as conn:
        cur = conn.cursor()
        cur.execute(sql, params or {})
        conn.commit()
        return cur.rowcount
