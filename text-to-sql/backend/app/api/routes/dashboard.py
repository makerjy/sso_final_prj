from __future__ import annotations

from fastapi import APIRouter
from typing import Any
from pydantic import BaseModel

from app.services.runtime.state_store import get_state_store

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
    queries: list[DashboardQuery] | None = None
    folders: list[DashboardFolder] | None = None


@router.get("/queries")
def get_queries():
    store = get_state_store()
    if not store.enabled:
        return {"queries": [], "folders": [], "detail": "MongoDB is not configured"}
    value = store.get("dashboard::queries") or {}
    queries = value.get("queries", []) if isinstance(value, dict) else []
    folders = value.get("folders", []) if isinstance(value, dict) else []
    return {"queries": queries, "folders": folders}


@router.post("/queries")
def save_queries(payload: DashboardPayload):
    store = get_state_store()
    if not store.enabled:
        return {"ok": False, "detail": "MongoDB is not configured"}
    existing = store.get("dashboard::queries") or {}
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
    store.set("dashboard::queries", {"queries": next_queries, "folders": next_folders})
    return {"ok": True, "count": len(next_queries), "folders": len(next_folders)}
