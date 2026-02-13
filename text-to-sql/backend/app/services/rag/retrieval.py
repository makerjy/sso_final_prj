from __future__ import annotations

from dataclasses import dataclass
import math
from typing import Any
from pathlib import Path
import json
import re
from collections import Counter

from app.core.config import get_settings
from app.services.rag.mongo_store import MongoStore
from app.services.runtime.context_budget import trim_context_to_budget
from app.services.runtime.settings_store import load_table_scope
from app.services.runtime.column_value_store import load_column_value_rows, match_column_value_rows
from app.services.runtime.diagnosis_map_store import load_diagnosis_icd_map, match_diagnosis_mappings
from app.services.runtime.label_intent_store import load_label_intent_profiles, match_label_intent_profiles
from app.services.runtime.procedure_map_store import load_procedure_icd_map, match_procedure_mappings


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


def _hit_score(hit: dict[str, Any]) -> float:
    value = hit.get("score")
    try:
        return float(value) if value is not None else 0.0
    except Exception:
        return 0.0


_TOKEN_RE = re.compile(r"[A-Za-z0-9_]+|[가-힣]+")


def _tokenize_list(text: str) -> list[str]:
    return [token for token in _TOKEN_RE.findall((text or "").lower()) if token]


def _tokenize(text: str) -> set[str]:
    return set(_tokenize_list(text))


def _lexical_overlap(query: str, text: str) -> float:
    q_tokens = _tokenize(query)
    d_tokens = _tokenize(text)
    if not q_tokens or not d_tokens:
        return 0.0
    return len(q_tokens & d_tokens) / float(len(q_tokens))


def _normalize_scores(raw: dict[str, float]) -> dict[str, float]:
    if not raw:
        return {}
    max_score = max(raw.values())
    if max_score <= 0:
        return {key: 0.0 for key in raw}
    return {key: (value / max_score) for key, value in raw.items()}


def _bm25_rank(
    query: str,
    docs: list[dict[str, Any]],
    *,
    k: int,
) -> list[dict[str, Any]]:
    if not docs or k <= 0:
        return []
    query_terms = _tokenize_list(query)
    if not query_terms:
        return []

    tokenized_docs: list[tuple[str, dict[str, Any], Counter[str], int]] = []
    df: Counter[str] = Counter()
    total_len = 0
    for doc in docs:
        doc_id = str(doc.get("id") or doc.get("_id") or "")
        text = str(doc.get("text") or "")
        if not doc_id or not text:
            continue
        tokens = _tokenize_list(text)
        if not tokens:
            continue
        tf = Counter(tokens)
        tokenized_docs.append((doc_id, doc, tf, len(tokens)))
        total_len += len(tokens)
        for term in set(tokens):
            df[term] += 1
    if not tokenized_docs:
        return []

    n_docs = len(tokenized_docs)
    avg_len = (total_len / n_docs) if n_docs else 1.0
    k1 = 1.2
    b = 0.75

    ranked: list[tuple[float, dict[str, Any]]] = []
    query_set = set(query_terms)
    for doc_id, doc, tf, doc_len in tokenized_docs:
        score = 0.0
        for term in query_set:
            f = float(tf.get(term, 0))
            if f <= 0:
                continue
            n_q = float(df.get(term, 0))
            idf = math.log(1.0 + ((n_docs - n_q + 0.5) / (n_q + 0.5)))
            denom = f + k1 * (1.0 - b + b * (doc_len / max(avg_len, 1e-9)))
            score += idf * ((f * (k1 + 1.0)) / max(denom, 1e-9))
        if score > 0:
            ranked.append((score, {**doc, "id": doc_id, "score": score}))

    ranked.sort(key=lambda item: item[0], reverse=True)
    return [item for _, item in ranked[:k]]


def _hybrid_search(
    store: MongoStore,
    query: str,
    *,
    k: int,
    where: dict[str, Any] | None = None,
) -> list[dict[str, Any]]:
    settings = get_settings()
    if k <= 0:
        return []

    if not settings.rag_hybrid_enabled:
        return store.search(query, k=k, where=where)

    candidate_k = max(k, int(settings.rag_hybrid_candidates or k))
    vector_hits = store.search(query, k=candidate_k, where=where)
    lexical_docs = store.list_documents(
        where=where,
        limit=max(candidate_k * 5, int(settings.rag_bm25_max_docs or 0)),
    )
    bm25_hits = _bm25_rank(query, lexical_docs, k=candidate_k)

    vec_by_id: dict[str, dict[str, Any]] = {}
    bm25_by_id: dict[str, dict[str, Any]] = {}
    for hit in vector_hits:
        doc_id = str(hit.get("id") or hit.get("_id") or "")
        if doc_id:
            vec_by_id[doc_id] = hit
    for hit in bm25_hits:
        doc_id = str(hit.get("id") or hit.get("_id") or "")
        if doc_id:
            bm25_by_id[doc_id] = hit

    if not vec_by_id and not bm25_by_id:
        return []

    vec_scores = _normalize_scores({doc_id: _hit_score(hit) for doc_id, hit in vec_by_id.items()})
    bm25_scores = _normalize_scores({doc_id: _hit_score(hit) for doc_id, hit in bm25_by_id.items()})

    source_type = str((where or {}).get("type") or "").lower()
    if source_type in {"diagnosis_map", "procedure_map", "column_value", "label_intent"}:
        w_vec, w_bm25, w_overlap = 0.45, 0.45, 0.10
    else:
        w_vec, w_bm25, w_overlap = 0.60, 0.30, 0.10

    merged_ids = list({*vec_by_id.keys(), *bm25_by_id.keys()})
    reranked: list[tuple[float, dict[str, Any]]] = []
    for doc_id in merged_ids:
        base_hit = vec_by_id.get(doc_id) or bm25_by_id.get(doc_id) or {}
        text = str(base_hit.get("text") or "")
        overlap = _lexical_overlap(query, text)
        score = (
            w_vec * float(vec_scores.get(doc_id, 0.0))
            + w_bm25 * float(bm25_scores.get(doc_id, 0.0))
            + w_overlap * overlap
        )
        reranked.append(
            (
                score,
                {
                    "id": doc_id,
                    "text": text,
                    "metadata": base_hit.get("metadata", {}),
                    "score": score,
                },
            )
        )

    reranked.sort(key=lambda item: item[0], reverse=True)
    return [item for _, item in reranked[:k]]


def _filter_hits(
    hits: list[dict[str, Any]],
    *,
    max_items: int,
    min_abs_score: float = 0.0,
    relative_ratio: float | None = None,
    query: str = "",
    min_lexical_overlap: float = 0.0,
    allow_fallback: bool = True,
) -> list[dict[str, Any]]:
    if not hits or max_items <= 0:
        return []
    ranked = sorted(hits, key=_hit_score, reverse=True)
    top = _hit_score(ranked[0])
    threshold = min_abs_score
    if relative_ratio is not None and top > 0:
        threshold = max(threshold, top * relative_ratio)
    filtered = [hit for hit in ranked if _hit_score(hit) >= threshold]
    if query and min_lexical_overlap > 0:
        filtered = [
            hit
            for hit in filtered
            if _lexical_overlap(query, str(hit.get("text") or "")) >= min_lexical_overlap
        ]
    if not filtered:
        if allow_fallback:
            filtered = ranked[:1]
        else:
            return []
    return filtered[:max_items]


def _has_token(question: str, tokens: tuple[str, ...]) -> bool:
    lowered = question.lower()
    compact = re.sub(r"\s+", "", lowered)
    return any(token in lowered or token in compact for token in tokens)


def _detect_search_intent(question: str) -> dict[str, bool]:
    diagnosis_tokens = (
        "diagnosis", "diagnos", "disease", "icd", "질환", "진단", "병명", "코드",
    )
    procedure_tokens = (
        "procedure", "surgery", "surgical", "operation", "post-op", "postop", "cabg", "pci",
        "수술", "시술",
    )
    column_value_tokens = (
        "admission type", "admission_type", "status", "category", "type", "value", "gender",
        "유형", "종류", "구분", "값", "성별", "입원유형", "입원 유형",
    )
    label_intent_tokens = (
        "catheter", "dialysis", "hemodialysis", "device", "insert", "insertion", "placement",
        "카테터", "투석", "혈액투석", "장치", "삽입", "거치",
    )
    return {
        "diagnosis": _has_token(question, diagnosis_tokens),
        "procedure": _has_token(question, procedure_tokens),
        "column_value": _has_token(question, column_value_tokens),
        "label_intent": _has_token(question, label_intent_tokens),
    }


def _compose_glossary_hits(
    *,
    question: str,
    rag_top_k: int,
    general_glossary_hits: list[dict[str, Any]],
    diagnosis_map_hits: list[dict[str, Any]],
    procedure_map_hits: list[dict[str, Any]],
    column_value_hits: list[dict[str, Any]],
    label_intent_hits: list[dict[str, Any]],
    local_map_hits: list[dict[str, Any]],
    local_proc_hits: list[dict[str, Any]],
    local_column_hits: list[dict[str, Any]],
    local_label_hits: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    intent = _detect_search_intent(question)

    diag_hits = _merge_hits([local_map_hits, diagnosis_map_hits], k=max(rag_top_k, 3))
    proc_hits = _merge_hits([local_proc_hits, procedure_map_hits], k=max(rag_top_k, 3))
    col_hits = _merge_hits([local_column_hits, column_value_hits], k=max(rag_top_k, 3))
    label_hits = _merge_hits([local_label_hits, label_intent_hits], k=max(rag_top_k, 3))

    if not local_map_hits and not intent["diagnosis"]:
        diag_hits = []
    else:
        diag_hits = _filter_hits(
            diag_hits,
            max_items=2,
            min_abs_score=0.08,
            relative_ratio=0.70,
            query=question,
            min_lexical_overlap=0.06,
            allow_fallback=bool(local_map_hits or intent["diagnosis"]),
        )

    if not local_proc_hits and not intent["procedure"]:
        proc_hits = []
    else:
        proc_hits = _filter_hits(
            proc_hits,
            max_items=2,
            min_abs_score=0.08,
            relative_ratio=0.70,
            query=question,
            min_lexical_overlap=0.06,
            allow_fallback=bool(local_proc_hits or intent["procedure"]),
        )

    if not local_column_hits and not intent["column_value"]:
        col_hits = []
    else:
        col_hits = _filter_hits(
            col_hits,
            max_items=2,
            min_abs_score=0.08,
            relative_ratio=0.70,
            query=question,
            min_lexical_overlap=0.05,
            allow_fallback=bool(local_column_hits or intent["column_value"]),
        )

    if not local_label_hits and not intent["label_intent"]:
        label_hits = []
    else:
        label_hits = _filter_hits(
            label_hits,
            max_items=2,
            min_abs_score=0.08,
            relative_ratio=0.65,
            query=question,
            min_lexical_overlap=0.05,
            allow_fallback=bool(local_label_hits or intent["label_intent"]),
        )

    specialized_count = len(diag_hits) + len(proc_hits) + len(label_hits) + len(col_hits)
    general_max_items = 1 if specialized_count > 0 else max(2, min(rag_top_k, 3))
    general_hits = _filter_hits(
        general_glossary_hits,
        max_items=general_max_items,
        min_abs_score=0.06 if specialized_count > 0 else 0.03,
        relative_ratio=0.75 if specialized_count > 0 else 0.60,
        query=question,
        min_lexical_overlap=0.10 if specialized_count > 0 else 0.05,
        allow_fallback=specialized_count == 0,
    )

    total_hits = len(diag_hits) + len(proc_hits) + len(label_hits) + len(col_hits) + len(general_hits)
    if total_hits <= 0:
        return []
    target_k = min(rag_top_k, total_hits)
    return _merge_hits([diag_hits, proc_hits, label_hits, col_hits, general_hits], k=target_k)


def _build_diagnosis_map_hits(question: str, *, k: int) -> list[dict[str, Any]]:
    matches: list[dict[str, Any]] = []
    for item in match_diagnosis_mappings(question, diagnosis_map=load_diagnosis_icd_map()):
        term = str(item.get("term") or "").strip()
        aliases = [str(alias).strip() for alias in item.get("aliases", []) if str(alias).strip()]
        prefixes = [str(prefix).strip().upper() for prefix in item.get("icd_prefixes", []) if str(prefix).strip()]
        if not term or not prefixes:
            continue

        hit_score = int(item.get("_score") or 0)
        prefix_text = ", ".join(f"{prefix}%" for prefix in prefixes)
        text = (
            f"Diagnosis mapping: {term} -> ICD_CODE prefixes {prefix_text}. "
            "Prefer DIAGNOSES_ICD.ICD_CODE LIKE '<prefix>%', not LONG_TITLE keyword matching. "
            "Use ICD_VERSION=10 for alphabetic prefixes and ICD_VERSION=9 for numeric prefixes."
        )
        matches.append({
            "id": f"diagnosis_map::{term}",
            "text": text,
            "metadata": {"type": "diagnosis_map", "term": term},
            "score": float(hit_score),
        })
    if not matches:
        return []
    matches.sort(key=lambda entry: float(entry.get("score") or 0.0), reverse=True)
    return matches[:k]


def _build_procedure_map_hits(question: str, *, k: int) -> list[dict[str, Any]]:
    matches: list[dict[str, Any]] = []
    for item in match_procedure_mappings(question, procedure_map=load_procedure_icd_map()):
        term = str(item.get("term") or "").strip()
        aliases = [str(alias).strip() for alias in item.get("aliases", []) if str(alias).strip()]
        prefixes = [str(prefix).strip().upper() for prefix in item.get("icd_prefixes", []) if str(prefix).strip()]
        if not term or not prefixes:
            continue

        hit_score = int(item.get("_score") or 0)
        prefix_text = ", ".join(f"{prefix}%" for prefix in prefixes)
        text = (
            f"Procedure mapping: {term} -> ICD_CODE prefixes {prefix_text}. "
            "Prefer PROCEDURES_ICD.ICD_CODE LIKE '<prefix>%', not LONG_TITLE keyword matching. "
            "Use ICD_VERSION=10 for alphabetic prefixes and ICD_VERSION=9 for numeric prefixes."
        )
        matches.append({
            "id": f"procedure_map::{term}",
            "text": text,
            "metadata": {"type": "procedure_map", "term": term},
            "score": float(hit_score),
        })
    if not matches:
        return []
    matches.sort(key=lambda entry: float(entry.get("score") or 0.0), reverse=True)
    return matches[:k]


def _build_column_value_hits(question: str, *, k: int) -> list[dict[str, Any]]:
    matches: list[dict[str, Any]] = []
    for idx, item in enumerate(match_column_value_rows(question, rows=load_column_value_rows(), k=max(k, 8))):
        table = str(item.get("table") or "").strip().upper()
        column = str(item.get("column") or "").strip().upper()
        value = str(item.get("value") or "").strip()
        description = str(item.get("description") or "").strip()
        if not table or not column or not value:
            continue
        score = float(item.get("_score") or 0.0)
        if description:
            text = f"Column value hint: {table}.{column} can be '{value}' ({description})."
        else:
            text = f"Column value hint: {table}.{column} can be '{value}'."
        matches.append({
            "id": f"column_value::{table}.{column}::{idx}",
            "text": text,
            "metadata": {"type": "column_value", "table": table, "column": column, "value": value},
            "score": score,
        })
    matches.sort(key=lambda entry: float(entry.get("score") or 0.0), reverse=True)
    return matches[:k]


def _build_label_intent_hits(question: str, *, k: int) -> list[dict[str, Any]]:
    matches: list[dict[str, Any]] = []
    for item in match_label_intent_profiles(question, profiles=load_label_intent_profiles(), k=max(k, 8)):
        name = str(item.get("name") or item.get("id") or "").strip()
        if not name:
            continue
        table = str(item.get("table") or "D_ITEMS").strip().upper() or "D_ITEMS"
        event_table = str(item.get("event_table") or "PROCEDUREEVENTS").strip().upper() or "PROCEDUREEVENTS"
        anchor_terms = [str(token).strip().upper() for token in item.get("anchor_terms", []) if str(token).strip()]
        required_terms = [
            str(token).strip().upper()
            for token in item.get("required_terms_with_anchor", [])
            if str(token).strip()
        ]
        if not anchor_terms:
            continue
        score = float(item.get("_score") or 0.0)
        text = (
            f"Label intent profile: {name}. Use {event_table} JOIN {table} for label concept filtering. "
            f"Anchor labels: {', '.join(anchor_terms)}."
        )
        if required_terms:
            text += f" Require with anchor: {', '.join(required_terms)}."
        matches.append({
            "id": f"label_intent::{name}",
            "text": text,
            "metadata": {"type": "label_intent", "name": name, "table": table, "event_table": event_table},
            "score": score,
        })
    matches.sort(key=lambda entry: float(entry.get("score") or 0.0), reverse=True)
    return matches[:k]


def build_candidate_context(question: str) -> CandidateContext:
    settings = get_settings()
    store = MongoStore()

    schema_hits = _hybrid_search(store, question, k=settings.rag_top_k, where={"type": "schema"})
    schema_hits = _apply_table_scope(schema_hits)
    example_hits = _hybrid_search(store, question, k=settings.examples_per_query, where={"type": "example"})
    template_hits = _hybrid_search(store, question, k=settings.templates_per_query, where={"type": "template"})
    general_glossary_hits = _hybrid_search(store, question, k=settings.rag_top_k, where={"type": "glossary"})
    diagnosis_map_hits = _hybrid_search(store, question, k=settings.rag_top_k, where={"type": "diagnosis_map"})
    procedure_map_hits = _hybrid_search(store, question, k=settings.rag_top_k, where={"type": "procedure_map"})
    column_value_hits = _hybrid_search(store, question, k=settings.rag_top_k, where={"type": "column_value"})
    label_intent_hits = _hybrid_search(store, question, k=settings.rag_top_k, where={"type": "label_intent"})
    local_map_hits = _build_diagnosis_map_hits(question, k=settings.rag_top_k)
    local_proc_hits = _build_procedure_map_hits(question, k=settings.rag_top_k)
    local_column_hits = _build_column_value_hits(question, k=settings.rag_top_k)
    local_label_hits = _build_label_intent_hits(question, k=settings.rag_top_k)
    glossary_hits = _compose_glossary_hits(
        question=question,
        rag_top_k=settings.rag_top_k,
        general_glossary_hits=general_glossary_hits,
        diagnosis_map_hits=diagnosis_map_hits,
        procedure_map_hits=procedure_map_hits,
        column_value_hits=column_value_hits,
        label_intent_hits=label_intent_hits,
        local_map_hits=local_map_hits,
        local_proc_hits=local_proc_hits,
        local_column_hits=local_column_hits,
        local_label_hits=local_label_hits,
    )

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
        [_hybrid_search(store, q, k=_per_query_k(settings.rag_top_k), where={"type": "schema"}) for q in deduped],
        k=settings.rag_top_k,
    )
    schema_hits = _apply_table_scope(schema_hits)
    example_hits = _merge_hits(
        [_hybrid_search(store, q, k=_per_query_k(settings.examples_per_query), where={"type": "example"}) for q in deduped],
        k=settings.examples_per_query,
    )
    template_hits = _merge_hits(
        [_hybrid_search(store, q, k=_per_query_k(settings.templates_per_query), where={"type": "template"}) for q in deduped],
        k=settings.templates_per_query,
    )
    general_glossary_hits = _merge_hits(
        [_hybrid_search(store, q, k=_per_query_k(settings.rag_top_k), where={"type": "glossary"}) for q in deduped],
        k=settings.rag_top_k,
    )
    diagnosis_map_hits = _merge_hits(
        [_hybrid_search(store, q, k=_per_query_k(settings.rag_top_k), where={"type": "diagnosis_map"}) for q in deduped],
        k=settings.rag_top_k,
    )
    procedure_map_hits = _merge_hits(
        [_hybrid_search(store, q, k=_per_query_k(settings.rag_top_k), where={"type": "procedure_map"}) for q in deduped],
        k=settings.rag_top_k,
    )
    column_value_hits = _merge_hits(
        [_hybrid_search(store, q, k=_per_query_k(settings.rag_top_k), where={"type": "column_value"}) for q in deduped],
        k=settings.rag_top_k,
    )
    label_intent_hits = _merge_hits(
        [_hybrid_search(store, q, k=_per_query_k(settings.rag_top_k), where={"type": "label_intent"}) for q in deduped],
        k=settings.rag_top_k,
    )
    local_map_hits = _merge_hits(
        [_build_diagnosis_map_hits(q, k=_per_query_k(settings.rag_top_k)) for q in deduped],
        k=settings.rag_top_k,
    )
    local_proc_hits = _merge_hits(
        [_build_procedure_map_hits(q, k=_per_query_k(settings.rag_top_k)) for q in deduped],
        k=settings.rag_top_k,
    )
    local_column_hits = _merge_hits(
        [_build_column_value_hits(q, k=_per_query_k(settings.rag_top_k)) for q in deduped],
        k=settings.rag_top_k,
    )
    local_label_hits = _merge_hits(
        [_build_label_intent_hits(q, k=_per_query_k(settings.rag_top_k)) for q in deduped],
        k=settings.rag_top_k,
    )
    glossary_hits = _compose_glossary_hits(
        question=" ".join(deduped),
        rag_top_k=settings.rag_top_k,
        general_glossary_hits=general_glossary_hits,
        diagnosis_map_hits=diagnosis_map_hits,
        procedure_map_hits=procedure_map_hits,
        column_value_hits=column_value_hits,
        label_intent_hits=label_intent_hits,
        local_map_hits=local_map_hits,
        local_proc_hits=local_proc_hits,
        local_column_hits=local_column_hits,
        local_label_hits=local_label_hits,
    )

    context = CandidateContext(
        schemas=schema_hits,
        examples=example_hits,
        templates=template_hits,
        glossary=glossary_hits,
    )
    return trim_context_to_budget(context, settings.context_token_budget)
