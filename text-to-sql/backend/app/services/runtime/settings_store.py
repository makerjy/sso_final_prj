from __future__ import annotations

from pathlib import Path
from typing import Any
import json

from app.core.paths import project_path
from app.services.runtime.state_store import get_state_store


BASE_PATH = project_path("var/metadata")
CONNECTION_PATH = BASE_PATH / "connection_settings.json"
TABLE_SCOPE_PATH = BASE_PATH / "table_scope.json"


def _load_json(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        return {}
    if isinstance(data, dict):
        return data
    return {}


def _save_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=True), encoding="utf-8")


def load_connection_settings() -> dict[str, Any]:
    store = get_state_store()
    data = store.get("connection_settings") if store.enabled else None
    if isinstance(data, dict) and data:
        return data
    return _load_json(CONNECTION_PATH)


def save_connection_settings(payload: dict[str, Any]) -> None:
    store = get_state_store()
    if store.enabled and store.set("connection_settings", payload):
        return
    _save_json(CONNECTION_PATH, payload)


def load_table_scope() -> list[str]:
    store = get_state_store()
    data = store.get("table_scope") if store.enabled else None
    if not isinstance(data, dict):
        data = _load_json(TABLE_SCOPE_PATH)
    raw = data.get("selected_ids", [])
    if not isinstance(raw, list):
        return []
    return [str(item) for item in raw if isinstance(item, (str, int))]


def save_table_scope(selected_ids: list[str]) -> None:
    payload = {"selected_ids": selected_ids}
    store = get_state_store()
    if store.enabled and store.set("table_scope", payload):
        return
    _save_json(TABLE_SCOPE_PATH, payload)
