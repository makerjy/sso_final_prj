from __future__ import annotations

from datetime import datetime
from typing import Any

from fastapi import APIRouter, Query

from app.core.config import get_settings
from app.services.logging_store.store import read_events


router = APIRouter()


def _format_ts(ts: int | None) -> str:
    if not ts:
        return ""
    try:
        return datetime.fromtimestamp(ts).strftime("%Y-%m-%d %H:%M:%S")
    except Exception:
        return ""


def _safe_int(value: Any, default: int = 0) -> int:
    try:
        return int(value)
    except Exception:
        return default


def _format_duration(duration_ms: int | None) -> str:
    if duration_ms is None:
        return "0.00초"
    try:
        return f"{duration_ms / 1000:.2f}초"
    except Exception:
        return "0.00초"


def _normalize_terms(items: Any) -> list[dict[str, str]]:
    if not isinstance(items, list):
        return []
    normalized: list[dict[str, str]] = []
    for item in items:
        if isinstance(item, dict):
            term = str(item.get("term") or item.get("name") or "").strip()
            version = str(item.get("version") or "").strip()
            if term:
                normalized.append({"term": term, "version": version})
        elif isinstance(item, str) and item.strip():
            normalized.append({"term": item.strip(), "version": ""})
    return normalized


def _normalize_metrics(items: Any) -> list[dict[str, str]]:
    if not isinstance(items, list):
        return []
    normalized: list[dict[str, str]] = []
    for item in items:
        if isinstance(item, dict):
            name = str(item.get("name") or item.get("metric") or item.get("term") or "").strip()
            version = str(item.get("version") or "").strip()
            if name:
                normalized.append({"name": name, "version": version})
        elif isinstance(item, str) and item.strip():
            normalized.append({"name": item.strip(), "version": ""})
    return normalized


def _event_to_log(event: dict[str, Any], fallback_id: int) -> dict[str, Any]:
    ts = _safe_int(event.get("ts") or 0)
    user = event.get("user") if isinstance(event.get("user"), dict) else {}
    user_name = str(user.get("name") or "사용자")
    user_role = str(user.get("role") or "연구원")
    question = str(event.get("question") or "직접 SQL 실행")
    sql = str(event.get("sql") or "")
    status = str(event.get("status") or "success")
    duration_ms = event.get("duration_ms")
    rows_returned = _safe_int(event.get("rows_returned") or 0)

    log = {
        "id": str(event.get("id") or event.get("qid") or f"audit-{fallback_id}"),
        "timestamp": _format_ts(ts),
        "ts": ts,
        "user": {"name": user_name, "role": user_role},
        "query": {"original": question, "sql": sql},
        "appliedTerms": _normalize_terms(event.get("applied_terms")),
        "appliedMetrics": _normalize_metrics(event.get("applied_metrics")),
        "execution": {
            "duration": _format_duration(duration_ms if isinstance(duration_ms, int) else None),
            "rowsReturned": rows_returned,
            "status": status,
        },
    }

    summary = event.get("result_summary")
    download_url = event.get("result_download_url")
    if summary or download_url:
        log["resultSnapshot"] = {
            "summary": str(summary or ""),
            "downloadUrl": str(download_url or ""),
        }

    return log


@router.get("/logs")
def audit_logs(limit: int = Query(200, ge=1, le=2000)):
    settings = get_settings()
    events = read_events(settings.events_log_path)
    audit_events = [event for event in events if event.get("type") == "audit"]

    audit_events.sort(key=lambda item: _safe_int(item.get("ts") or 0), reverse=True)

    total = len(audit_events)
    success_count = sum(1 for event in audit_events if event.get("status") == "success")
    today = datetime.now().date()
    today_count = 0
    user_names: set[str] = set()
    for event in audit_events:
        ts = _safe_int(event.get("ts") or 0)
        if ts:
            try:
                if datetime.fromtimestamp(ts).date() == today:
                    today_count += 1
            except Exception:
                pass
        user = event.get("user")
        if isinstance(user, dict):
            name = user.get("name")
            if name:
                user_names.add(str(name))

    success_rate = round((success_count / total) * 100, 1) if total else 0.0

    sliced = audit_events[:limit]
    logs = [_event_to_log(event, idx) for idx, event in enumerate(sliced, start=1)]

    return {
        "logs": logs,
        "stats": {
            "total": total,
            "today": today_count,
            "active_users": len(user_names),
            "success_rate": success_rate,
        },
    }
