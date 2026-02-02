from __future__ import annotations

import re
from fastapi import HTTPException

from app.core.config import get_settings


_WRITE_KEYWORDS = re.compile(r"\b(delete|update|insert|merge|drop|alter|truncate)\b", re.IGNORECASE)


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
