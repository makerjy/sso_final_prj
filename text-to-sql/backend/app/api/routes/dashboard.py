from __future__ import annotations

from fastapi import APIRouter
from pydantic import BaseModel

from app.services.runtime.state_store import get_state_store

router = APIRouter()


class MetricItem(BaseModel):
    label: str
    value: str
    trend: str | None = None


class DashboardQuery(BaseModel):
    id: str
    title: str
    description: str
    query: str
    lastRun: str
    schedule: str | None = None
    isPinned: bool = False
    category: str
    metrics: list[MetricItem] = []
    chartType: str


class DashboardPayload(BaseModel):
    queries: list[DashboardQuery] = []


@router.get("/queries")
def get_queries():
    store = get_state_store()
    if not store.enabled:
        return {"queries": [], "detail": "MongoDB is not configured"}
    value = store.get("dashboard::queries") or {}
    queries = value.get("queries", []) if isinstance(value, dict) else []
    return {"queries": queries}


@router.post("/queries")
def save_queries(payload: DashboardPayload):
    store = get_state_store()
    if not store.enabled:
        return {"ok": False, "detail": "MongoDB is not configured"}
    queries = []
    for item in payload.queries:
        if hasattr(item, "model_dump"):
            queries.append(item.model_dump())
        else:
            queries.append(item.dict())
    store.set("dashboard::queries", {"queries": queries})
    return {"ok": True, "count": len(queries)}
