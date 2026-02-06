from __future__ import annotations

import re
from fastapi import HTTPException

from app.core.config import get_settings
from app.services.runtime.settings_store import load_table_scope


_WRITE_KEYWORDS = re.compile(r"\b(delete|update|insert|merge|drop|alter|truncate)\b", re.IGNORECASE)
_TABLE_REF = re.compile(r"\b(from|join)\s+([A-Za-z0-9_.$#\"]+)", re.IGNORECASE)
_CTE_REF = re.compile(r"(?:with|,)\s*([A-Za-z0-9_]+)\s+as\s*\(", re.IGNORECASE)


def _extract_table_names(sql: str) -> list[str]:
    tables: list[str] = []
    for _, raw in _TABLE_REF.findall(sql):
        name = raw.strip().strip('"').strip()
        name = re.sub(r"[(),]", "", name)
        if "." in name:
            name = name.split(".")[-1]
        if name:
            tables.append(name)
    return tables


def precheck_sql(sql: str) -> None:
    text = sql.strip()
    if not text:
        raise HTTPException(status_code=400, detail="Empty SQL")
    if _WRITE_KEYWORDS.search(text):
        raise HTTPException(status_code=403, detail="Write operations are not allowed")
    if not re.match(r"^\s*select\b", text, re.IGNORECASE):
        raise HTTPException(status_code=400, detail="Only SELECT queries are allowed")

    settings = get_settings()
    join_count = len(re.findall(r"\bjoin\b", text, re.IGNORECASE))
    if join_count > settings.max_db_joins:
        raise HTTPException(status_code=400, detail="Join limit exceeded")

    has_where = re.search(r"\bwhere\b", text, re.IGNORECASE) is not None
    if not has_where:
        raise HTTPException(status_code=403, detail="WHERE clause required")

    allowed_tables = {name.lower() for name in load_table_scope() if name}
    if allowed_tables:
        cte_names = {name.lower() for name in _CTE_REF.findall(text)}
        found_tables = [t for t in _extract_table_names(text) if t.lower() not in cte_names]
        disallowed = [t for t in found_tables if t.lower() not in allowed_tables]
        if disallowed:
            raise HTTPException(
                status_code=403,
                detail=f"Table not allowed: {', '.join(sorted(set(disallowed)))}",
            )
