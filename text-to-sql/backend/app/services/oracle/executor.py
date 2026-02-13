from __future__ import annotations

import re
from typing import Any

from fastapi import HTTPException

from app.core.config import get_settings
from app.services.oracle.connection import acquire_connection


def _sanitize_sql(sql: str) -> str:
    return sql.strip().rstrip(";")


def _apply_row_cap(sql: str, row_cap: int) -> str:
    return f"SELECT * FROM ({sql}) WHERE ROWNUM <= :row_cap"


def execute_sql(sql: str) -> dict[str, Any]:
    settings = get_settings()
    text = _sanitize_sql(sql)
    # Keep executor policy aligned with precheck_sql:
    # allow plain SELECT and CTE-based read-only queries (WITH ... SELECT ...).
    if not re.match(r"^\s*(select|with)\b", text, re.IGNORECASE):
        raise HTTPException(status_code=400, detail="Only SELECT queries are allowed")
    if re.match(r"^\s*with\b", text, re.IGNORECASE) and not re.search(r"\bselect\b", text, re.IGNORECASE):
        raise HTTPException(status_code=400, detail="CTE query must include SELECT")

    conn = acquire_connection()
    try:
        try:
            conn.call_timeout = settings.db_timeout_sec * 1000
        except Exception:
            pass
        cur = conn.cursor()
        if settings.oracle_default_schema:
            schema = settings.oracle_default_schema.strip()
            if re.fullmatch(r"[A-Za-z0-9_$#]+", schema):
                cur.execute(f"ALTER SESSION SET CURRENT_SCHEMA = {schema}")
        capped_sql = _apply_row_cap(text, settings.row_cap)
        cur.execute(capped_sql, row_cap=settings.row_cap)
        columns = [d[0] for d in cur.description] if cur.description else []
        rows = cur.fetchmany(settings.row_cap)
        cur.close()
        return {
            "columns": columns,
            "rows": rows,
            "row_count": len(rows),
            "row_cap": settings.row_cap,
        }
    except Exception as exc:  # pragma: no cover - depends on driver
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    finally:
        conn.close()
