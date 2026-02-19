from __future__ import annotations

import json
import re
from typing import Any
from pathlib import Path

from fastapi import HTTPException

from app.core.config import get_settings
from app.core.paths import project_path
from app.services.oracle.connection import acquire_connection
from app.services.runtime.settings_store import load_connection_settings

_FROM_JOIN_TABLE_WITH_SCHEMA_RE = re.compile(
    r"\b(from|join)\s+(\"?[A-Za-z0-9_$#]+\"?)\s*\.\s*(\"?[A-Za-z0-9_$#]+\"?)",
    re.IGNORECASE,
)


def _sanitize_sql(sql: str) -> str:
    return sql.strip().rstrip(";")


def _load_metadata_owner() -> str:
    path: Path = project_path("var/metadata/schema_catalog.json")
    if not path.exists():
        return ""
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        return ""
    return str((data or {}).get("owner") or "").strip()


def _is_ora_00942(exc: Exception) -> bool:
    return "ORA-00942" in str(exc).upper()


def _valid_schema_name(schema: str) -> bool:
    return bool(re.fullmatch(r"[A-Za-z0-9_$#]+", schema))


def _normalize_identifier(identifier: str) -> str:
    value = str(identifier or "").strip()
    if len(value) >= 2 and value[0] == '"' and value[-1] == '"':
        value = value[1:-1]
    return value.strip().upper()


def _strip_non_target_schema_prefixes(
    sql: str,
    *,
    target_schemas: set[str],
) -> tuple[str, bool]:
    normalized_targets = {
        _normalize_identifier(schema)
        for schema in target_schemas
        if _valid_schema_name(str(schema or "").strip())
    }
    changed = False

    def _replace(match: re.Match[str]) -> str:
        nonlocal changed
        keyword = match.group(1)
        schema = match.group(2)
        table = match.group(3)
        if _normalize_identifier(schema) in normalized_targets:
            return match.group(0)
        changed = True
        return f"{keyword} {table}"

    rewritten = _FROM_JOIN_TABLE_WITH_SCHEMA_RE.sub(_replace, sql)
    return rewritten, changed


def execute_sql(sql: str) -> dict[str, Any]:
    settings = get_settings()
    overrides = load_connection_settings()
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
        session_schema = str(
            overrides.get("defaultSchema")
            or settings.oracle_default_schema
            or ""
        ).strip()
        fallback_schema = _load_metadata_owner()

        def _run_once(schema_name: str, sql_text: str) -> dict[str, Any]:
            if schema_name and _valid_schema_name(schema_name):
                schema_cur = conn.cursor()
                try:
                    schema_cur.execute(f"ALTER SESSION SET CURRENT_SCHEMA = {schema_name}")
                finally:
                    schema_cur.close()

            # Best-effort full result count for UI badges.
            total_count: int | None = None
            count_cur = conn.cursor()
            try:
                count_cur.execute(f"SELECT COUNT(*) FROM ({sql_text})")
                count_row = count_cur.fetchone()
                if count_row and len(count_row) > 0 and count_row[0] is not None:
                    total_count = int(count_row[0])
            except Exception:
                total_count = None
            finally:
                count_cur.close()

            run_cur = conn.cursor()
            try:
                run_cur.execute(sql_text)
                columns = [d[0] for d in run_cur.description] if run_cur.description else []
                rows = run_cur.fetchall()
                return {
                    "columns": columns,
                    "rows": rows,
                    "row_count": len(rows),
                    "row_cap": None,
                    "total_count": total_count,
                }
            finally:
                run_cur.close()

        try:
            return _run_once(session_schema, text)
        except Exception as exc:
            # If default schema is stale/misconfigured, retry once with
            # the metadata owner inferred during table sync. Also handle
            # stale schema prefixes baked into SQL (e.g. old_owner.TABLE).
            last_exc: Exception = exc
            if (
                _is_ora_00942(exc)
                and fallback_schema
                and fallback_schema.upper() != session_schema.upper()
            ):
                try:
                    return _run_once(fallback_schema, text)
                except Exception as retry_exc:
                    last_exc = retry_exc
            if _is_ora_00942(last_exc):
                rewritten_sql, changed = _strip_non_target_schema_prefixes(
                    text,
                    target_schemas={session_schema, fallback_schema},
                )
                if changed:
                    try:
                        rewrite_schema = fallback_schema or session_schema
                        return _run_once(rewrite_schema, rewritten_sql)
                    except Exception as rewrite_exc:
                        raise HTTPException(status_code=400, detail=str(rewrite_exc)) from rewrite_exc
            raise HTTPException(status_code=400, detail=str(last_exc)) from last_exc
    except Exception as exc:  # pragma: no cover - depends on driver
        if isinstance(exc, HTTPException):
            raise
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    finally:
        conn.close()
