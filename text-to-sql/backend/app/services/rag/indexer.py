from __future__ import annotations

from pathlib import Path
from typing import Any
import json

from app.services.rag.mongo_store import MongoStore


def _load_json(path: Path) -> Any:
    if not path.exists():
        return None
    return json.loads(path.read_text(encoding="utf-8"))


def _load_jsonl(path: Path) -> list[dict[str, Any]]:
    if not path.exists():
        return []
    items: list[dict[str, Any]] = []
    for line in path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line:
            continue
        try:
            items.append(json.loads(line))
        except json.JSONDecodeError:
            continue
    return items


def _schema_docs(schema_catalog: dict[str, Any]) -> list[dict[str, Any]]:
    docs = []
    tables = schema_catalog.get("tables", {})
    for table_name, entry in tables.items():
        columns = entry.get("columns", [])
        pk = entry.get("primary_keys", [])
        col_text = ", ".join([f"{c['name']}:{c['type']}" for c in columns])
        pk_text = ", ".join(pk)
        text = f"Table {table_name}. Columns: {col_text}. Primary keys: {pk_text}."
        docs.append({
            "id": f"schema::{table_name}",
            "text": text,
            "metadata": {"type": "schema", "table": table_name},
        })
    return docs


def _glossary_docs(items: list[dict[str, Any]]) -> list[dict[str, Any]]:
    docs = []
    for idx, item in enumerate(items):
        term = item.get("term") or item.get("key") or item.get("name") or ""
        desc = item.get("desc") or item.get("definition") or item.get("value") or ""
        text = f"Glossary: {term} = {desc}".strip()
        docs.append({
            "id": f"glossary::{idx}",
            "text": text,
            "metadata": {"type": "glossary", "term": term},
        })
    return docs


def _example_docs(items: list[dict[str, Any]]) -> list[dict[str, Any]]:
    docs = []
    for idx, item in enumerate(items):
        question = item.get("question", "")
        sql = item.get("sql", "")
        text = f"Question: {question}\nSQL: {sql}".strip()
        docs.append({
            "id": f"example::{idx}",
            "text": text,
            "metadata": {"type": "example"},
        })
    return docs


def _template_docs(items: list[dict[str, Any]]) -> list[dict[str, Any]]:
    docs = []
    for idx, item in enumerate(items):
        name = item.get("name", f"template_{idx}")
        sql = item.get("sql", "")
        text = f"Template: {name}\nSQL: {sql}".strip()
        docs.append({
            "id": f"template::{idx}",
            "text": text,
            "metadata": {"type": "template", "name": name},
        })
    return docs


def reindex(metadata_dir: str = "var/metadata") -> dict[str, int]:
    base = Path(metadata_dir)
    schema_catalog = _load_json(base / "schema_catalog.json") or {"tables": {}}
    glossary_items = _load_jsonl(base / "glossary_docs.jsonl")
    example_items = _load_jsonl(base / "sql_examples.jsonl")
    template_items = _load_jsonl(base / "join_templates.jsonl")
    template_items.extend(_load_jsonl(base / "sql_templates.jsonl"))

    docs: list[dict[str, Any]] = []
    docs.extend(_schema_docs(schema_catalog))
    docs.extend(_glossary_docs(glossary_items))
    docs.extend(_example_docs(example_items))
    docs.extend(_template_docs(template_items))

    store = MongoStore()
    store.upsert_documents(docs)

    return {
        "schema_docs": len(_schema_docs(schema_catalog)),
        "glossary_docs": len(glossary_items),
        "sql_examples_docs": len(example_items),
        "join_templates_docs": len(template_items),
    }
