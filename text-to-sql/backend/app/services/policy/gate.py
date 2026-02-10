from __future__ import annotations

import re
from fastapi import HTTPException

from app.core.config import get_settings
from app.services.runtime.settings_store import load_table_scope


_WRITE_KEYWORDS = re.compile(r"\b(delete|update|insert|merge|drop|alter|truncate)\b", re.IGNORECASE)
_TABLE_REF = re.compile(r"\b(from|join)\s+([A-Za-z0-9_.$#\"]+)", re.IGNORECASE)
_CTE_REF = re.compile(r"(?:with|,)\s*([A-Za-z0-9_]+)\s+as\s*\(", re.IGNORECASE)
_AGG_FN_RE = re.compile(r"\b(count|avg|sum|min|max)\s*\(", re.IGNORECASE)

_WHERE_OPTIONAL_QUESTION_HINTS = (
    "count",
    "how many",
    "number of",
    "distribution",
    "trend",
    "compare",
    "comparison",
    "average",
    "mean",
    "median",
    "ratio",
    "rate",
    "top",
    "most",
    "least",
    "summary",
    "aggregate",
    "분포",
    "추이",
    "비교",
    "평균",
    "중앙",
    "비율",
    "건수",
    "통계",
    "요약",
    "상위",
    "하위",
    "몇 명",
    "몇건",
    "트렌드",
)


def _check(name: str, passed: bool, message: str) -> dict[str, str | bool]:
    return {"name": name, "passed": passed, "message": message}


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


def _can_skip_where(question: str | None, sql: str) -> bool:
    if not question:
        return False
    q = question.lower()
    if not any(hint in q for hint in _WHERE_OPTIONAL_QUESTION_HINTS):
        return False
    has_aggregate_shape = bool(_AGG_FN_RE.search(sql)) or bool(re.search(r"\bgroup\s+by\b", sql, re.IGNORECASE))
    return has_aggregate_shape


def precheck_sql(sql: str, question: str | None = None) -> dict[str, object]:
    text = sql.strip()
    if not text:
        raise HTTPException(status_code=400, detail="Empty SQL")
    checks: list[dict[str, str | bool]] = []

    if _WRITE_KEYWORDS.search(text):
        checks.append(_check("Read-only", False, "Write keyword detected"))
        raise HTTPException(status_code=403, detail="Write operations are not allowed")
    checks.append(_check("Read-only", True, "No write keyword detected"))

    # Allow SELECT and CTE-based read-only queries (WITH ... SELECT ...).
    # Write keywords are already blocked by _WRITE_KEYWORDS above.
    statement_ok = bool(re.match(r"^\s*(select|with)\b", text, re.IGNORECASE))
    checks.append(_check("Statement type", statement_ok, "SELECT/CTE only"))
    if not statement_ok:
        raise HTTPException(status_code=400, detail="Only SELECT queries are allowed")
    if re.match(r"^\s*with\b", text, re.IGNORECASE):
        cte_has_select = bool(re.search(r"\bselect\b", text, re.IGNORECASE))
        checks.append(_check("CTE", cte_has_select, "WITH clause includes SELECT"))
        if not cte_has_select:
            raise HTTPException(status_code=400, detail="CTE query must include SELECT")

    settings = get_settings()
    join_count = len(re.findall(r"\bjoin\b", text, re.IGNORECASE))
    join_ok = join_count <= settings.max_db_joins
    checks.append(_check("Join limit", join_ok, f"{join_count}/{settings.max_db_joins} joins"))
    if join_count > settings.max_db_joins:
        raise HTTPException(status_code=400, detail="Join limit exceeded")

    has_where = re.search(r"\bwhere\b", text, re.IGNORECASE) is not None
    where_optional = _can_skip_where(question, text)
    where_ok = has_where or where_optional
    where_message = "WHERE clause present" if has_where else "Aggregate question: WHERE optional"
    checks.append(_check("WHERE rule", where_ok, where_message))
    if not has_where and not where_optional:
        raise HTTPException(status_code=403, detail="WHERE clause required")

    allowed_tables = {name.lower() for name in load_table_scope() if name}
    if allowed_tables:
        cte_names = {name.lower() for name in _CTE_REF.findall(text)}
        found_tables = [t for t in _extract_table_names(text) if t.lower() not in cte_names]
        disallowed = [t for t in found_tables if t.lower() not in allowed_tables]
        scope_ok = not disallowed
        if scope_ok:
            checks.append(_check("Table scope", True, f"{len(found_tables)} table references allowed"))
        else:
            checks.append(_check("Table scope", False, f"Disallowed: {', '.join(sorted(set(disallowed)))}"))
        if disallowed:
            raise HTTPException(
                status_code=403,
                detail=f"Table not allowed: {', '.join(sorted(set(disallowed)))}",
            )
    else:
        checks.append(_check("Table scope", True, "No table scope restriction"))

    return {"passed": True, "checks": checks}
