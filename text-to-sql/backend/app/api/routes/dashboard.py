from __future__ import annotations

from typing import Any

from fastapi import APIRouter, Query
from pydantic import BaseModel

from app.services.runtime.state_store import get_state_store
from app.services.runtime.user_scope import scoped_state_key

router = APIRouter()


class MetricItem(BaseModel):
    label: str
    value: str
    trend: str | None = None


class PreviewData(BaseModel):
    columns: list[str] = []
    rows: list[list[Any]] = []
    row_count: int = 0
    row_cap: int | None = None


class DashboardQuery(BaseModel):
    id: str
    title: str
    description: str
    query: str
    lastRun: str
    schedule: str | None = None
    isPinned: bool = False
    category: str
    folderId: str | None = None
    preview: PreviewData | None = None
    metrics: list[MetricItem] = []
    chartType: str


class DashboardFolder(BaseModel):
    id: str
    name: str
    tone: str | None = None
    createdAt: str | None = None


class DashboardPayload(BaseModel):
    user: str | None = None
    queries: list[DashboardQuery] | None = None
    folders: list[DashboardFolder] | None = None


class SaveQueryPayload(BaseModel):
    user: str | None = None
    question: str
    sql: str
    metadata: dict[str, Any] | None = None


def _dashboard_key(user: str | None) -> str:
    return scoped_state_key("dashboard::queries", user)


@router.get("/queries")
def get_queries(user: str | None = Query(default=None)):
    store = get_state_store()
    if not store.enabled:
        return {"queries": [], "folders": [], "detail": "MongoDB is not configured"}
    key = _dashboard_key(user)
    value = store.get(key) or {}
    queries = value.get("queries", []) if isinstance(value, dict) else []
    folders = value.get("folders", []) if isinstance(value, dict) else []
    return {"queries": queries, "folders": folders}


@router.post("/queries")
def save_queries(payload: DashboardPayload):
    store = get_state_store()
    if not store.enabled:
        return {"ok": False, "detail": "MongoDB is not configured"}
    key = _dashboard_key(payload.user)
    existing = store.get(key) or {}
    existing_queries = existing.get("queries", []) if isinstance(existing, dict) else []
    existing_folders = existing.get("folders", []) if isinstance(existing, dict) else []

    queries = []
    for item in payload.queries or []:
        if hasattr(item, "model_dump"):
            queries.append(item.model_dump())
        else:
            queries.append(item.dict())

    folders = []
    for item in payload.folders or []:
        if hasattr(item, "model_dump"):
            folders.append(item.model_dump())
        else:
            folders.append(item.dict())

    next_queries = queries if payload.queries is not None else existing_queries
    next_folders = folders if payload.folders is not None else existing_folders
    store.set(key, {"queries": next_queries, "folders": next_folders})
    return {"ok": True, "count": len(next_queries), "folders": len(next_folders)}


@router.post("/saveQuery")
def save_query(payload: SaveQueryPayload):
    store = get_state_store()
    if not store.enabled:
        return {"ok": False, "detail": "MongoDB is not configured"}

    key = _dashboard_key(payload.user)
    existing = store.get(key) or {}
    existing_queries = existing.get("queries", []) if isinstance(existing, dict) else []
    existing_folders = existing.get("folders", []) if isinstance(existing, dict) else []

    metadata = payload.metadata or {}
    entry = metadata.get("entry") if isinstance(metadata, dict) else None
    new_folder = metadata.get("new_folder") if isinstance(metadata, dict) else None
    if not isinstance(entry, dict):
        entry = {
            "id": f"dashboard-{len(existing_queries) + 1}",
            "title": payload.question,
            "description": "Query result summary",
            "query": payload.sql,
            "lastRun": "just now",
            "isPinned": True,
            "category": "all",
            "metrics": [
                {"label": "rows", "value": str(metadata.get("row_count", 0))},
                {"label": "columns", "value": str(metadata.get("column_count", 0))},
            ],
            "chartType": "bar",
        }

    next_folders = list(existing_folders)
    if isinstance(new_folder, dict):
        folder_id = str(new_folder.get("id", "")).strip()
        folder_name = str(new_folder.get("name", "")).strip()
        if folder_id and folder_name:
            exists = any(str(item.get("id", "")).strip() == folder_id for item in next_folders if isinstance(item, dict))
            if not exists:
                next_folders.append(
                    {
                        "id": folder_id,
                        "name": folder_name,
                        "tone": str(new_folder.get("tone", "")).strip() or None,
                        "createdAt": str(new_folder.get("createdAt", "")).strip() or None,
                    }
                )

    next_queries = [entry, *existing_queries]
    store.set(key, {"queries": next_queries, "folders": next_folders})
    return {"ok": True, "count": len(next_queries), "folders": len(next_folders)}
