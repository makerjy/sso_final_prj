from __future__ import annotations

from typing import Iterable
import re


_COUNT_RE = re.compile(r"^Count rows in ([A-Za-z0-9_]+) \(sampled\)$", re.IGNORECASE)
_SAMPLE_RE = re.compile(r"^Show sample ([A-Za-z0-9_]+) rows with (.+)$", re.IGNORECASE)
_DISTINCT_RE = re.compile(
    r"^List distinct values of ([A-Za-z0-9_]+) in ([A-Za-z0-9_]+) \(sample\)$",
    re.IGNORECASE,
)
_IDENT_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_$#]*$")


def _parse_columns(text: str) -> list[str]:
    cleaned = re.sub(r"\s+and\s+", ",", text.strip(), flags=re.IGNORECASE)
    cols = [c.strip() for c in cleaned.split(",") if c.strip()]
    if not cols:
        return []
    if any(not _IDENT_RE.fullmatch(c) for c in cols):
        return []
    return cols


def _first(items: Iterable[str]) -> str | None:
    for item in items:
        return item
    return None


def postprocess_sql(question: str, sql: str) -> tuple[str, list[str]]:
    rules: list[str] = []
    q = question.strip()

    match = _COUNT_RE.match(q)
    if match:
        table = match.group(1)
        rules.append("count_rows_sampled_template")
        return f"SELECT COUNT(*) AS cnt FROM {table} WHERE ROWNUM <= 1000", rules

    match = _DISTINCT_RE.match(q)
    if match:
        col = match.group(1)
        table = match.group(2)
        rules.append("distinct_sample_template")
        return f"SELECT DISTINCT {col} FROM {table} WHERE {col} IS NOT NULL AND ROWNUM <= 50", rules

    match = _SAMPLE_RE.match(q)
    if match:
        table = match.group(1)
        cols = _parse_columns(match.group(2))
        first = _first(cols)
        if cols and first:
            cols_sql = ", ".join(cols)
            rules.append("sample_rows_template")
            return (
                f"SELECT {cols_sql} FROM {table} WHERE {first} IS NOT NULL AND ROWNUM <= 100",
                rules,
            )

    return sql, rules
