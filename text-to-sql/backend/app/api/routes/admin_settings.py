from __future__ import annotations

from fastapi import APIRouter
from pydantic import BaseModel
from app.core.config import get_settings
from app.services.oracle.connection import reset_pool
from app.services.oracle.metadata_extractor import extract_metadata
from app.services.runtime.settings_store import (
    load_connection_settings as fetch_connection_settings,
    load_table_scope as fetch_table_scope,
    save_connection_settings as persist_connection_settings,
    save_table_scope as persist_table_scope,
)

router = APIRouter()


class ConnectionSettings(BaseModel):
    host: str
    port: str
    database: str
    username: str
    password: str | None = None
    sslMode: str | None = None
    defaultSchema: str | None = None


class TableScopeSettings(BaseModel):
    selected_ids: list[str] = []


@router.get("/connection")
def get_connection_settings():
    return fetch_connection_settings() or {}


@router.post("/connection")
def save_connection_settings(req: ConnectionSettings):
    previous = fetch_connection_settings() or {}
    payload = req.model_dump(exclude_none=True)
    persist_connection_settings(payload)
    reset_pool()

    settings = get_settings()
    owner = str(
        payload.get("defaultSchema")
        or previous.get("defaultSchema")
        or settings.oracle_default_schema
        or ""
    ).strip()

    if not owner:
        return {
            "ok": True,
            "metadata_synced": False,
            "owner": None,
            "reason": "ORACLE_DEFAULT_SCHEMA is not configured",
        }

    try:
        sync_result = extract_metadata(owner)
    except Exception as exc:
        detail = str(getattr(exc, "detail", exc)).strip()
        return {
            "ok": True,
            "metadata_synced": False,
            "owner": owner.upper(),
            "reason": detail or "metadata sync failed",
        }

    return {
        "ok": True,
        "metadata_synced": True,
        "owner": owner.upper(),
        "tables": int(sync_result.get("tables") or 0),
    }


@router.get("/table-scope")
def get_table_scope():
    selected_ids = fetch_table_scope()
    return {"selected_ids": selected_ids}


@router.post("/table-scope")
def save_table_scope(req: TableScopeSettings):
    persist_table_scope(req.selected_ids)
    return {"ok": True}
