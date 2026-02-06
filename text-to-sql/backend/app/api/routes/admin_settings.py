from __future__ import annotations

from fastapi import APIRouter
from pydantic import BaseModel
from app.services.oracle.connection import reset_pool
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


class TableScopeSettings(BaseModel):
    selected_ids: list[str] = []


@router.get("/connection")
def get_connection_settings():
    return fetch_connection_settings() or {}


@router.post("/connection")
def save_connection_settings(req: ConnectionSettings):
    persist_connection_settings(req.model_dump())
    reset_pool()
    return {"ok": True}


@router.get("/table-scope")
def get_table_scope():
    selected_ids = fetch_table_scope()
    return {"selected_ids": selected_ids}


@router.post("/table-scope")
def save_table_scope(req: TableScopeSettings):
    persist_table_scope(req.selected_ids)
    return {"ok": True}
