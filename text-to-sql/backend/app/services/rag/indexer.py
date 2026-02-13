from __future__ import annotations

from pathlib import Path
from typing import Any
import json

from app.services.rag.mongo_store import MongoStore
from app.services.runtime.column_value_store import load_column_value_rows


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


def _template_docs(items: list[dict[str, Any]], kind: str = "generic") -> list[dict[str, Any]]:
    docs = []
    for idx, item in enumerate(items):
        name = item.get("name", f"template_{idx}")
        sql = item.get("sql", "")
        text = f"Template: {name}\nSQL: {sql}".strip()
        docs.append({
            "id": f"template::{idx}",
            "text": text,
            "metadata": {"type": "template", "name": name, "kind": kind},
        })
    return docs


def _diagnosis_map_docs(items: list[dict[str, Any]]) -> list[dict[str, Any]]:
    docs = []
    for idx, item in enumerate(items):
        if not isinstance(item, dict):
            continue
        term = str(item.get("term") or "").strip()
        if not term:
            continue
        aliases_raw = item.get("aliases") or []
        aliases = [str(alias).strip() for alias in aliases_raw if str(alias).strip()] if isinstance(aliases_raw, list) else []
        prefixes_raw = item.get("icd_prefixes") or item.get("prefixes") or []
        prefixes = [str(prefix).strip().upper() for prefix in prefixes_raw if str(prefix).strip()] if isinstance(prefixes_raw, list) else []
        if not prefixes:
            continue
        alias_text = ", ".join(aliases) if aliases else "-"
        prefix_text = ", ".join(f"{prefix}%" for prefix in prefixes)
        text = (
            f"Diagnosis mapping: {term}. "
            f"Aliases: {alias_text}. "
            f"ICD_CODE prefixes: {prefix_text}. "
            "Use DIAGNOSES_ICD.ICD_CODE LIKE '<prefix>%'. "
            "If prefixes mix alphabetic and numeric forms, pair with ICD_VERSION "
            "(10 for alphabetic prefixes, 9 for numeric prefixes)."
        )
        docs.append({
            "id": f"diagnosis_map::{idx}",
            "text": text,
            "metadata": {"type": "diagnosis_map", "term": term},
        })
    return docs


def _procedure_map_docs(items: list[dict[str, Any]]) -> list[dict[str, Any]]:
    docs = []
    for idx, item in enumerate(items):
        if not isinstance(item, dict):
            continue
        term = str(item.get("term") or "").strip()
        if not term:
            continue
        aliases_raw = item.get("aliases") or []
        aliases = [str(alias).strip() for alias in aliases_raw if str(alias).strip()] if isinstance(aliases_raw, list) else []
        prefixes_raw = item.get("icd_prefixes") or item.get("prefixes") or []
        prefixes = [str(prefix).strip().upper() for prefix in prefixes_raw if str(prefix).strip()] if isinstance(prefixes_raw, list) else []
        if not prefixes:
            continue
        alias_text = ", ".join(aliases) if aliases else "-"
        prefix_text = ", ".join(f"{prefix}%" for prefix in prefixes)
        text = (
            f"Procedure mapping: {term}. "
            f"Aliases: {alias_text}. "
            f"ICD_CODE prefixes: {prefix_text}. "
            "Use PROCEDURES_ICD.ICD_CODE LIKE '<prefix>%'. "
            "If prefixes mix alphabetic and numeric forms, pair with ICD_VERSION "
            "(10 for alphabetic prefixes, 9 for numeric prefixes)."
        )
        docs.append({
            "id": f"procedure_map::{idx}",
            "text": text,
            "metadata": {"type": "procedure_map", "term": term},
        })
    return docs


def _column_value_docs(items: list[dict[str, Any]]) -> list[dict[str, Any]]:
    docs = []
    for idx, item in enumerate(items):
        if not isinstance(item, dict):
            continue
        table = str(item.get("table") or "").strip().upper()
        column = str(item.get("column") or "").strip().upper()
        value = str(item.get("value") or "").strip()
        description = str(item.get("description") or "").strip()
        sheet = str(item.get("sheet") or "").strip()
        if not table or not column or not value:
            continue
        if description:
            text = (
                f"Column value hint: {table}.{column} includes '{value}'. "
                f"Meaning: {description}. "
                "Prefer exact value filtering when this concept appears in user intent."
            )
        else:
            text = (
                f"Column value hint: {table}.{column} includes '{value}'. "
                "Prefer exact value filtering when this concept appears in user intent."
            )
        docs.append({
            "id": f"column_value::{idx}",
            "text": text,
            "metadata": {
                "type": "column_value",
                "table": table,
                "column": column,
                "value": value,
                "sheet": sheet,
            },
        })
    return docs


def _label_intent_docs(items: list[dict[str, Any]]) -> list[dict[str, Any]]:
    docs = []
    for idx, item in enumerate(items):
        if not isinstance(item, dict):
            continue
        name = str(item.get("name") or item.get("id") or f"label_intent_{idx}").strip()
        if not name:
            continue
        table = str(item.get("table") or "D_ITEMS").strip().upper() or "D_ITEMS"
        event_table = str(item.get("event_table") or "PROCEDUREEVENTS").strip().upper() or "PROCEDUREEVENTS"
        question_any_raw = item.get("question_any") or []
        anchor_terms_raw = item.get("anchor_terms") or []
        required_terms_raw = item.get("required_terms_with_anchor") or []
        exclude_terms_raw = item.get("exclude_terms_with_anchor") or []

        question_any = [str(token).strip() for token in question_any_raw if str(token).strip()] if isinstance(question_any_raw, list) else []
        anchor_terms = [str(token).strip().upper() for token in anchor_terms_raw if str(token).strip()] if isinstance(anchor_terms_raw, list) else []
        required_terms = [str(token).strip().upper() for token in required_terms_raw if str(token).strip()] if isinstance(required_terms_raw, list) else []
        exclude_terms = [str(token).strip().upper() for token in exclude_terms_raw if str(token).strip()] if isinstance(exclude_terms_raw, list) else []

        if not anchor_terms:
            continue

        question_text = ", ".join(question_any) if question_any else "-"
        anchor_text = ", ".join(anchor_terms)
        required_text = ", ".join(required_terms) if required_terms else "-"
        exclude_text = ", ".join(exclude_terms) if exclude_terms else "-"
        text = (
            f"Label intent profile: {name}. "
            f"Question cues: {question_text}. "
            f"Use {event_table} joined with {table} for LABEL-based filtering. "
            f"Anchor LABEL keywords: {anchor_text}. "
            f"Required-with-anchor keywords: {required_text}. "
            f"Exclude keywords: {exclude_text}."
        )
        docs.append({
            "id": f"label_intent::{idx}",
            "text": text,
            "metadata": {"type": "label_intent", "name": name, "table": table, "event_table": event_table},
        })
    return docs


def reindex(metadata_dir: str = "var/metadata") -> dict[str, int]:
    base = Path(metadata_dir)
    schema_catalog = _load_json(base / "schema_catalog.json") or {"tables": {}}
    glossary_items = _load_jsonl(base / "glossary_docs.jsonl")
    example_items = _load_jsonl(base / "sql_examples.jsonl")
    join_template_items = _load_jsonl(base / "join_templates.jsonl")
    sql_template_items = _load_jsonl(base / "sql_templates.jsonl")
    diagnosis_map_items = _load_jsonl(base / "diagnosis_icd_map.jsonl")
    procedure_map_items = _load_jsonl(base / "procedure_icd_map.jsonl")
    label_intent_items = _load_jsonl(base / "label_intent_profiles.jsonl")
    column_value_items = load_column_value_rows()

    docs: list[dict[str, Any]] = []
    docs.extend(_schema_docs(schema_catalog))
    docs.extend(_glossary_docs(glossary_items))
    docs.extend(_diagnosis_map_docs(diagnosis_map_items))
    docs.extend(_procedure_map_docs(procedure_map_items))
    docs.extend(_label_intent_docs(label_intent_items))
    docs.extend(_column_value_docs(column_value_items))
    docs.extend(_example_docs(example_items))
    docs.extend(_template_docs(join_template_items, kind="join"))
    docs.extend(_template_docs(sql_template_items, kind="sql"))

    store = MongoStore()
    store.upsert_documents(docs)

    return {
        "schema_docs": len(_schema_docs(schema_catalog)),
        "glossary_docs": len(glossary_items),
        "diagnosis_map_docs": len(_diagnosis_map_docs(diagnosis_map_items)),
        "procedure_map_docs": len(_procedure_map_docs(procedure_map_items)),
        "label_intent_docs": len(_label_intent_docs(label_intent_items)),
        "column_value_docs": len(_column_value_docs(column_value_items)),
        "sql_examples_docs": len(example_items),
        "join_templates_docs": len(join_template_items) + len(sql_template_items),
    }
