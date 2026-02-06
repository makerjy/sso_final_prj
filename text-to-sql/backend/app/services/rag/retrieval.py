from __future__ import annotations

from dataclasses import dataclass
import math
from typing import Any
from pathlib import Path
import json

from app.core.config import get_settings
from app.services.rag.mongo_store import MongoStore
from app.services.runtime.context_budget import trim_context_to_budget
from app.services.runtime.settings_store import load_table_scope


@dataclass
class CandidateContext:
    schemas: list[dict[str, Any]]
    examples: list[dict[str, Any]]
    templates: list[dict[str, Any]]
    glossary: list[dict[str, Any]]


def _merge_hits(hit_lists: list[list[dict[str, Any]]], k: int) -> list[dict[str, Any]]:
    combined: dict[str, dict[str, Any]] = {}
    order = 0
    for hits in hit_lists:
        for item in hits:
            hit_id = str(item.get("id") or item.get("_id") or "")
            score = item.get("score")
            score = float(score) if score is not None else 0.0
            if not hit_id:
                hit_id = f"__idx__{order}"
            existing = combined.get(hit_id)
            if existing is None:
                combined[hit_id] = {**item, "_rank_score": score, "_rank_order": order}
            else:
                prev_score = float(existing.get("_rank_score", 0.0))
                if score > prev_score:
                    combined[hit_id] = {
                        **item,
                        "_rank_score": score,
                        "_rank_order": existing.get("_rank_order", order),
                    }
            order += 1
    ranked = sorted(
        combined.values(),
        key=lambda item: (-float(item.get("_rank_score", 0.0)), int(item.get("_rank_order", 0))),
    )
    results = []
    for item in ranked[:k]:
        item.pop("_rank_score", None)
        item.pop("_rank_order", None)
        results.append(item)
    return results


def build_candidate_context(question: str) -> CandidateContext:
    settings = get_settings()
    store = MongoStore()

    schema_hits = store.search(question, k=settings.rag_top_k, where={"type": "schema"})
    schema_hits = _apply_table_scope(schema_hits)
    example_hits = store.search(question, k=settings.examples_per_query, where={"type": "example"})
    template_hits = store.search(question, k=settings.templates_per_query, where={"type": "template"})
    glossary_hits = store.search(question, k=settings.rag_top_k, where={"type": "glossary"})

    context = CandidateContext(
        schemas=schema_hits,
        examples=example_hits,
        templates=template_hits,
        glossary=glossary_hits,
    )
    return trim_context_to_budget(context, settings.context_token_budget)


def _schema_docs_for_tables(selected: set[str]) -> list[dict[str, Any]]:
    base = Path("var/metadata/schema_catalog.json")
    if not base.exists():
        return []
    try:
        schema_catalog = json.loads(base.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        return []
    tables = schema_catalog.get("tables", {}) if isinstance(schema_catalog, dict) else {}
    docs: list[dict[str, Any]] = []
    for table_name, entry in tables.items():
        if str(table_name).lower() not in selected:
            continue
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


def _apply_table_scope(schema_hits: list[dict[str, Any]]) -> list[dict[str, Any]]:
    selected = {name.lower() for name in load_table_scope() if name}
    if not selected:
        return schema_hits
    filtered = [
        hit for hit in schema_hits
        if str(hit.get("metadata", {}).get("table", "")).lower() in selected
    ]
    existing = {
        str(hit.get("metadata", {}).get("table", "")).lower()
        for hit in filtered
    }
    extras = [doc for doc in _schema_docs_for_tables(selected) if doc["metadata"]["table"].lower() not in existing]
    return filtered + extras if filtered or extras else schema_hits


def build_candidate_context_multi(questions: list[str]) -> CandidateContext:
    settings = get_settings()
    store = MongoStore()

    deduped: list[str] = []
    for q in questions:
        text = (q or "").strip()
        if text and text not in deduped:
            deduped.append(text)
    if not deduped:
        deduped = [""]
    if len(deduped) == 1:
        return build_candidate_context(deduped[0])

    def _per_query_k(total: int) -> int:
        return max(1, int(math.ceil(total / len(deduped))))

    schema_hits = _merge_hits(
        [store.search(q, k=_per_query_k(settings.rag_top_k), where={"type": "schema"}) for q in deduped],
        k=settings.rag_top_k,
    )
    schema_hits = _apply_table_scope(schema_hits)
    example_hits = _merge_hits(
        [store.search(q, k=_per_query_k(settings.examples_per_query), where={"type": "example"}) for q in deduped],
        k=settings.examples_per_query,
    )
    template_hits = _merge_hits(
        [store.search(q, k=_per_query_k(settings.templates_per_query), where={"type": "template"}) for q in deduped],
        k=settings.templates_per_query,
    )
    glossary_hits = _merge_hits(
        [store.search(q, k=_per_query_k(settings.rag_top_k), where={"type": "glossary"}) for q in deduped],
        k=settings.rag_top_k,
    )

    context = CandidateContext(
        schemas=schema_hits,
        examples=example_hits,
        templates=template_hits,
        glossary=glossary_hits,
    )
    return trim_context_to_budget(context, settings.context_token_budget)
