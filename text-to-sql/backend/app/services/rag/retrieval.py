from __future__ import annotations

from dataclasses import dataclass
import math
from typing import Any
from pathlib import Path
import json
import re
from collections import Counter

from app.core.config import get_settings
from app.core.paths import project_path
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


_RAG_STORE_HAS_DOCS: bool | None = None
_LOCAL_DOC_CACHE: dict[str, list[dict[str, Any]]] | None = None


def _store_has_docs(store: MongoStore) -> bool:
    global _RAG_STORE_HAS_DOCS
    if _RAG_STORE_HAS_DOCS is True:
        return _RAG_STORE_HAS_DOCS
    try:
        has_docs = bool(store.list_documents(limit=1))
    except Exception:
        return bool(_RAG_STORE_HAS_DOCS)
    if has_docs:
        _RAG_STORE_HAS_DOCS = True
    return has_docs


def _load_json(path: Path) -> Any:
    if not path.exists():
        return None
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        return None


def _load_jsonl(path: Path) -> list[dict[str, Any]]:
    if not path.exists():
        return []
    rows: list[dict[str, Any]] = []
    for line in path.read_text(encoding="utf-8").splitlines():
        text = line.strip()
        if not text:
            continue
        try:
            item = json.loads(text)
        except json.JSONDecodeError:
            continue
        if isinstance(item, dict):
            rows.append(item)
    return rows


def _build_local_doc_cache() -> dict[str, list[dict[str, Any]]]:
    base = project_path("var/metadata")
    cache: dict[str, list[dict[str, Any]]] = {
        "schema": [],
        "example": [],
        "template": [],
        "glossary": [],
        "table_profile": [],
    }

    schema_catalog = _load_json(base / "schema_catalog.json") or {"tables": {}}
    join_graph = _load_json(base / "join_graph.json") or {"edges": []}
    tables = schema_catalog.get("tables", {}) if isinstance(schema_catalog, dict) else {}
    edges = join_graph.get("edges", []) if isinstance(join_graph, dict) else []
    fk_index: dict[str, list[str]] = {}
    for edge in edges:
        if not isinstance(edge, dict):
            continue
        src_table = str(edge.get("from_table") or "").strip().upper()
        src_col = str(edge.get("from_column") or "").strip().upper()
        dst_table = str(edge.get("to_table") or "").strip().upper()
        dst_col = str(edge.get("to_column") or "").strip().upper()
        if not (src_table and src_col and dst_table and dst_col):
            continue
        fk_index.setdefault(src_table, []).append(f"{src_col}->{dst_table}.{dst_col}")
    for table_name, entry in tables.items():
        columns = entry.get("columns", []) if isinstance(entry, dict) else []
        pk = entry.get("primary_keys", []) if isinstance(entry, dict) else []
        col_text = ", ".join(
            [
                f"{c.get('name')}:{c.get('type')}:{'NULL' if c.get('nullable') else 'NOT NULL'}"
                for c in columns
                if isinstance(c, dict)
            ]
        )
        pk_text = ", ".join(str(name) for name in pk)
        fk_text = ", ".join(fk_index.get(str(table_name).upper(), []))
        text = (
            f"Table {table_name}. "
            f"Columns(name:type:nullability): {col_text or '-'}; "
            f"Primary keys: {pk_text or '-'}; "
            f"Foreign keys: {fk_text or '-'}."
        )
        cache["schema"].append(
            {
                "id": f"schema::{table_name}",
                "text": text,
                "metadata": {"type": "schema", "table": table_name},
            }
        )

    glossary_items = _load_jsonl(base / "glossary_docs.jsonl")
    glossary_items.extend(_load_jsonl(base / "external_rag_docs.jsonl"))
    for idx, item in enumerate(glossary_items):
        if "text" in item:
            text = str(item.get("text") or "").strip()
            if not text:
                continue
            metadata = item.get("metadata") if isinstance(item.get("metadata"), dict) else {}
            term = str(metadata.get("term") or item.get("term") or item.get("key") or item.get("name") or "").strip()
            cache["glossary"].append(
                {
                    "id": f"glossary::{idx}",
                    "text": text,
                    "metadata": {"type": "glossary", "term": term, **metadata},
                }
            )
            continue
        term = str(item.get("term") or item.get("key") or item.get("name") or "").strip()
        desc = str(item.get("desc") or item.get("definition") or item.get("value") or "").strip()
        if not term and not desc:
            continue
        cache["glossary"].append(
            {
                "id": f"glossary::{idx}",
                "text": f"Glossary: {term} = {desc}".strip(),
                "metadata": {"type": "glossary", "term": term},
            }
        )

    table_profile_items = _load_jsonl(base / "table_value_profiles.jsonl")
    for idx, item in enumerate(table_profile_items):
        table = str(item.get("table") or "").strip().upper()
        column = str(item.get("column") or "").strip().upper()
        data_type = str(item.get("data_type") or "").strip().upper()
        if not table or not column:
            continue
        raw_values = item.get("top_values") or []
        values: list[str] = []
        if isinstance(raw_values, list):
            for value_row in raw_values[:10]:
                if not isinstance(value_row, dict):
                    continue
                value = str(value_row.get("value") or "").strip()
                count = value_row.get("count")
                if not value:
                    continue
                if count is None:
                    values.append(value)
                else:
                    values.append(f"{value}({count})")
        value_text = ", ".join(values) if values else "-"
        text = (
            f"Table value profile: {table}.{column} ({data_type or 'UNKNOWN'}). "
            f"Distinct={item.get('num_distinct')}, Nulls={item.get('num_nulls')}, Rows={item.get('row_count')}. "
            f"Top values: {value_text}."
        )
        cache["table_profile"].append(
            {
                "id": f"table_profile::{idx}",
                "text": text,
                "metadata": {"type": "table_profile", "table": table, "column": column},
            }
        )

    settings = get_settings()
    example_items = _load_jsonl(base / "sql_examples.jsonl")
    if bool(getattr(settings, "sql_examples_include_augmented", True)):
        example_items.extend(_load_jsonl(base / "sql_examples_augmented.jsonl"))
    for idx, item in enumerate(example_items):
        question = str(item.get("question") or "").strip()
        sql = str(item.get("sql") or "").strip()
        if not question or not sql:
            continue
        cache["example"].append(
            {
                "id": f"example::{idx}",
                "text": f"Question: {question}\nSQL: {sql}",
                "metadata": {"type": "example"},
            }
        )

    template_items = _load_jsonl(base / "join_templates.jsonl") + _load_jsonl(base / "sql_templates.jsonl")
    for idx, item in enumerate(template_items):
        name = str(item.get("name") or f"template_{idx}").strip()
        sql = str(item.get("sql") or "").strip()
        if not sql:
            continue
        cache["template"].append(
            {
                "id": f"template::{idx}",
                "text": f"Template: {name}\nSQL: {sql}",
                "metadata": {"type": "template", "name": name},
            }
        )

    return cache


def _get_local_docs(doc_type: str) -> list[dict[str, Any]]:
    global _LOCAL_DOC_CACHE
    if _LOCAL_DOC_CACHE is None:
        _LOCAL_DOC_CACHE = _build_local_doc_cache()
    docs = _LOCAL_DOC_CACHE.get(doc_type, [])
    return [dict(item) for item in docs]


def _local_fallback_search(
    query: str,
    *,
    k: int,
    where: dict[str, Any] | None = None,
) -> list[dict[str, Any]]:
    doc_type = str((where or {}).get("type") or "").strip().lower()
    if not doc_type:
        return []
    docs = _get_local_docs(doc_type)
    if not docs:
        return []
    ranked = _bm25_rank(query, docs, k=k)
    if ranked:
        return ranked
    return []


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
_TOKEN_STOPWORDS = {
    "the",
    "a",
    "an",
    "and",
    "or",
    "for",
    "to",
    "of",
    "in",
    "on",
    "with",
    "by",
    "show",
    "list",
    "please",
    "환자",
    "전체",
    "결과",
    "보여줘",
    "해줘",
}
_STRUCTURED_QUERY_RE = re.compile(
    r"(연도별|월별|주별|일별|분기별|추이|시계열|비교|대비|vs|versus|by\s+|according\s+to|quartile|q1|q2|q3|q4|사분위|ratio|rate|percentage|퍼센트|비율)",
    re.IGNORECASE,
)


def _tokenize_list(text: str) -> list[str]:
    tokens: list[str] = []
    for raw in _TOKEN_RE.findall((text or "").lower()):
        token = raw.strip()
        if len(token) < 2 or token in _TOKEN_STOPWORDS:
            continue
        tokens.append(token)
    return tokens


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


def _normalize_dedupe_text(text: str) -> str:
    return re.sub(r"\s+", " ", str(text or "").strip().lower())


def _hit_signature(hit: dict[str, Any]) -> str:
    metadata = hit.get("metadata", {}) if isinstance(hit.get("metadata"), dict) else {}
    source_type = str(metadata.get("type") or "").strip().lower()
    table = str(metadata.get("table") or "").strip().lower()
    column = str(metadata.get("column") or "").strip().lower()
    term = str(metadata.get("term") or metadata.get("name") or "").strip().lower()
    text = _normalize_dedupe_text(str(hit.get("text") or ""))[:240]
    if source_type in {"schema", "column_value", "table_profile"}:
        key = f"{source_type}|{table}|{column}|{text}"
    elif source_type in {"diagnosis_map", "procedure_map", "label_intent"}:
        key = f"{source_type}|{term}|{text}"
    else:
        key = f"{source_type}|{text}"
    return key


def _dedupe_hits(hits: list[dict[str, Any]], *, max_items: int | None = None) -> list[dict[str, Any]]:
    if not hits:
        return []
    combined: dict[str, dict[str, Any]] = {}
    order = 0
    for hit in hits:
        sig = _hit_signature(hit)
        score = _hit_score(hit)
        existing = combined.get(sig)
        if existing is None:
            combined[sig] = {**hit, "_rank_score": score, "_rank_order": order}
        else:
            prev_score = float(existing.get("_rank_score", 0.0))
            if score > prev_score:
                combined[sig] = {
                    **hit,
                    "_rank_score": score,
                    "_rank_order": existing.get("_rank_order", order),
                }
        order += 1
    ranked = sorted(
        combined.values(),
        key=lambda item: (-float(item.get("_rank_score", 0.0)), int(item.get("_rank_order", 0))),
    )
    results: list[dict[str, Any]] = []
    for item in ranked:
        item.pop("_rank_score", None)
        item.pop("_rank_order", None)
        results.append(item)
        if max_items is not None and len(results) >= max_items:
            break
    return results


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
    if not _store_has_docs(store):
        return _local_fallback_search(query, k=k, where=where)

    if not settings.rag_hybrid_enabled:
        return store.search(query, k=k, where=where)

    mode = str(getattr(settings, "rag_retrieval_mode", "bm25_then_rerank") or "bm25_then_rerank").strip().lower()
    source_type = str((where or {}).get("type") or "").lower()

    if mode in {"legacy", "hybrid_legacy"}:
        candidate_k = max(k, int(settings.rag_hybrid_candidates or k))
        lexical_docs = store.list_documents(
            where=where,
            limit=max(candidate_k * 5, int(settings.rag_bm25_max_docs or 0)),
        )
        bm25_hits = _bm25_rank(query, lexical_docs, k=candidate_k)
        vector_hits = store.search(query, k=candidate_k, where=where)
    else:
        # Step 1: lexical recall first (BM25 candidates).
        bm25_candidate_k = max(k, int(getattr(settings, "rag_bm25_candidates", 50) or 50))
        dense_candidate_k = max(k, int(getattr(settings, "rag_dense_candidates", bm25_candidate_k) or bm25_candidate_k))
        lexical_docs = store.list_documents(
            where=where,
            limit=max(bm25_candidate_k * 6, int(settings.rag_bm25_max_docs or 0)),
        )
        bm25_hits = _bm25_rank(query, lexical_docs, k=bm25_candidate_k)

        # Step 2: semantic signal + rerank (dense retrieval is used as semantic scorer).
        vector_hits = store.search(query, k=dense_candidate_k, where=where)

        # Keep BM25 candidates as the primary pool; allow a small dense expansion for recall.
        if bm25_hits:
            dense_boost_k = max(k * 2, min(24, dense_candidate_k))
            seed_ids: set[str] = {
                str(hit.get("id") or hit.get("_id") or "")
                for hit in bm25_hits
                if str(hit.get("id") or hit.get("_id") or "")
            }
            for hit in vector_hits[:dense_boost_k]:
                doc_id = str(hit.get("id") or hit.get("_id") or "")
                if doc_id:
                    seed_ids.add(doc_id)
            bm25_hits = [
                hit
                for hit in bm25_hits
                if str(hit.get("id") or hit.get("_id") or "") in seed_ids
            ]
            vector_hits = [
                hit
                for hit in vector_hits
                if str(hit.get("id") or hit.get("_id") or "") in seed_ids
            ]

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
    if mode in {"legacy", "hybrid_legacy"}:
        if source_type in {"diagnosis_map", "procedure_map", "column_value", "label_intent", "table_profile"}:
            w_vec, w_bm25, w_overlap = 0.45, 0.45, 0.10
        else:
            w_vec, w_bm25, w_overlap = 0.60, 0.30, 0.10
    else:
        if source_type in {"diagnosis_map", "procedure_map", "column_value", "label_intent", "table_profile"}:
            w_vec, w_bm25, w_overlap = 0.55, 0.35, 0.10
        else:
            w_vec, w_bm25, w_overlap = 0.50, 0.40, 0.10

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
        "admission type", "admission_type", "admission location", "discharge location",
        "insurance", "language", "race", "ethnicity", "marital status", "status code", "category code",
        "gender", "sex", "성별", "입원유형", "입원 유형", "퇴원 위치", "보험", "인종", "민족", "결혼 상태", "카테고리",
        "service", "department", "curr_service", "prev_service", "진료과", "서비스", "내과", "외과", "신경과", "이비인후과",
        "정형외과", "산부인과", "심장외과", "흉부외과",
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


def _resolve_context_limits(question: str, settings: Any) -> tuple[int, int]:
    intent = _detect_search_intent(question)
    examples_limit = max(1, int(getattr(settings, "examples_per_query", 3) or 3))
    templates_limit = max(0, int(getattr(settings, "templates_per_query", 1) or 0))

    has_structured_intent = bool(_STRUCTURED_QUERY_RE.search(question))
    if has_structured_intent or intent["diagnosis"] or intent["procedure"] or intent["label_intent"]:
        examples_limit = min(max(examples_limit, 3), 4)
    if not has_structured_intent and not intent["label_intent"] and not intent["procedure"]:
        templates_limit = 0

    # Pure categorical/value lookup questions benefit from fewer examples and no template noise.
    service_value_intent = _has_token(
        question,
        ("service", "department", "curr_service", "prev_service", "진료과", "서비스", "내과", "외과"),
    )
    if intent["column_value"] and not (intent["diagnosis"] or intent["procedure"] or intent["label_intent"]) and not service_value_intent:
        examples_limit = min(examples_limit, 1)
        templates_limit = 0

    return examples_limit, templates_limit


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
    service_value_intent = _has_token(
        question,
        ("service", "department", "curr_service", "prev_service", "진료과", "서비스", "내과", "외과"),
    )

    diag_hits = _dedupe_hits(
        _merge_hits([local_map_hits, diagnosis_map_hits], k=max(rag_top_k, 3)),
        max_items=max(rag_top_k, 3),
    )
    proc_hits = _dedupe_hits(
        _merge_hits([local_proc_hits, procedure_map_hits], k=max(rag_top_k, 3)),
        max_items=max(rag_top_k, 3),
    )
    col_hits = _dedupe_hits(
        _merge_hits([local_column_hits, column_value_hits], k=max(rag_top_k, 3)),
        max_items=max(rag_top_k, 3),
    )
    label_hits = _dedupe_hits(
        _merge_hits([local_label_hits, label_intent_hits], k=max(rag_top_k, 3)),
        max_items=max(rag_top_k, 3),
    )

    if not local_map_hits and not intent["diagnosis"]:
        diag_hits = []
    else:
        diag_hits = _filter_hits(
            diag_hits,
            max_items=2,
            min_abs_score=0.10,
            relative_ratio=0.75,
            query=question,
            min_lexical_overlap=0.08,
            allow_fallback=False,
        )

    if not local_proc_hits and not intent["procedure"]:
        proc_hits = []
    else:
        proc_hits = _filter_hits(
            proc_hits,
            max_items=2,
            min_abs_score=0.10,
            relative_ratio=0.75,
            query=question,
            min_lexical_overlap=0.08,
            allow_fallback=False,
        )

    has_structured_local_column = any(
        bool((hit.get("metadata") or {}).get("struct_match"))
        for hit in local_column_hits
    )
    has_value_local_column = any(
        bool((hit.get("metadata") or {}).get("value_match"))
        for hit in local_column_hits
    )
    if not has_structured_local_column and not intent["column_value"]:
        col_hits = []
    else:
        col_max_items = 3 if service_value_intent else 2
        col_min_overlap = 0.04 if service_value_intent else 0.10
        col_hits = _filter_hits(
            col_hits,
            max_items=col_max_items,
            min_abs_score=0.12,
            relative_ratio=0.80,
            query=question,
            min_lexical_overlap=col_min_overlap,
            allow_fallback=(
                has_structured_local_column
                or (intent["column_value"] and has_value_local_column)
                or service_value_intent
            ),
        )

    if not local_label_hits and not intent["label_intent"]:
        label_hits = []
    else:
        label_hits = _filter_hits(
            label_hits,
            max_items=2,
            min_abs_score=0.10,
            relative_ratio=0.70,
            query=question,
            min_lexical_overlap=0.08,
            allow_fallback=False,
        )

    specialized_count = len(diag_hits) + len(proc_hits) + len(label_hits) + len(col_hits)
    if specialized_count >= 1:
        general_hits: list[dict[str, Any]] = []
    else:
        general_max_items = 1
        general_hits = _filter_hits(
            _dedupe_hits(general_glossary_hits, max_items=max(rag_top_k, 4)),
            max_items=general_max_items,
            min_abs_score=0.06,
            relative_ratio=0.75,
            query=question,
            min_lexical_overlap=0.10,
            allow_fallback=False,
        )

    total_hits = len(diag_hits) + len(proc_hits) + len(label_hits) + len(col_hits) + len(general_hits)
    if total_hits <= 0:
        return []
    target_k = min(rag_top_k, total_hits)
    return _dedupe_hits(_merge_hits([diag_hits, proc_hits, label_hits, col_hits, general_hits], k=target_k * 2), max_items=target_k)


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
    candidate_rows = match_column_value_rows(question, rows=load_column_value_rows(), k=max(k, 8))
    value_counter: Counter[str] = Counter(
        _normalize_dedupe_text(str(item.get("value") or ""))
        for item in candidate_rows
        if str(item.get("value") or "").strip()
    )
    column_intent = _detect_search_intent(question).get("column_value", False)

    for idx, item in enumerate(candidate_rows):
        table = str(item.get("table") or "").strip().upper()
        column = str(item.get("column") or "").strip().upper()
        value = str(item.get("value") or "").strip()
        description = str(item.get("description") or "").strip()
        if not table or not column or not value:
            continue
        raw_score = float(item.get("_score") or 0.0)
        struct_match = bool(item.get("_struct_match"))
        value_match = bool(item.get("_value_match"))
        value_key = _normalize_dedupe_text(value)
        if not struct_match and not column_intent:
            continue
        if not struct_match and value_counter.get(value_key, 0) > 1:
            continue
        if raw_score < 12 and not (struct_match or value_match):
            continue
        score = min(1.0, raw_score / 40.0)
        display_column = column
        if table == "SERVICES" and column == "PREV_SERVICE":
            # Value catalogs may only list PREV_SERVICE, but service restriction
            # questions should usually filter CURR_SERVICE at admission grain.
            display_column = "CURR_SERVICE"
        if description:
            if display_column != column:
                text = (
                    f"Column value hint: {table}.{display_column} "
                    f"(and {table}.{column}) can be '{value}' ({description})."
                )
            else:
                text = f"Column value hint: {table}.{column} can be '{value}' ({description})."
        else:
            if display_column != column:
                text = (
                    f"Column value hint: {table}.{display_column} "
                    f"(and {table}.{column}) can be '{value}'."
                )
            else:
                text = f"Column value hint: {table}.{column} can be '{value}'."
        matches.append({
            "id": f"column_value::{table}.{column}::{idx}",
            "text": text,
            "metadata": {
                "type": "column_value",
                "table": table,
                "column": column,
                "display_column": display_column,
                "value": value,
                "struct_match": struct_match,
                "value_match": value_match,
            },
            "score": score,
        })
    matches.sort(key=lambda entry: float(entry.get("score") or 0.0), reverse=True)
    return _dedupe_hits(matches, max_items=k)


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
        raw_score = float(item.get("_score") or 0.0)
        if raw_score <= 0:
            continue
        score = min(1.0, raw_score / 12.0)
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
    return _dedupe_hits(matches, max_items=k)


def _filter_schema_hits(question: str, hits: list[dict[str, Any]], *, max_items: int) -> list[dict[str, Any]]:
    scoped_tables = {name.lower() for name in load_table_scope() if name}
    scoped_limit = max_items
    if scoped_tables:
        # When table scope is explicit, avoid truncating schema context to rag_top_k.
        # Keep at least one schema entry per scoped table (bounded for safety).
        scoped_limit = max(max_items, min(len(scoped_tables), 128))
    deduped = _dedupe_hits(hits, max_items=max(scoped_limit, 6))
    if scoped_tables:
        return deduped[:scoped_limit]
    return _filter_hits(
        deduped,
        max_items=max_items,
        min_abs_score=0.04,
        relative_ratio=0.45,
        query=question,
        min_lexical_overlap=0.03,
        allow_fallback=True,
    )


def _schema_retrieval_k(settings: Any) -> int:
    base = max(1, int(getattr(settings, "rag_top_k", 5) or 5))
    # Preserve broader schema coverage even when global rag_top_k is tuned low.
    return max(base, 12)


def build_candidate_context(question: str) -> CandidateContext:
    settings = get_settings()
    store = MongoStore()
    intent = _detect_search_intent(question)
    examples_limit, templates_limit = _resolve_context_limits(question, settings)
    schema_k = _schema_retrieval_k(settings)

    schema_hits = _hybrid_search(store, question, k=schema_k, where={"type": "schema"})
    schema_hits = _apply_table_scope(schema_hits)
    schema_hits = _filter_schema_hits(question, schema_hits, max_items=schema_k)
    example_hits = _hybrid_search(store, question, k=examples_limit, where={"type": "example"})
    example_hits = _dedupe_hits(example_hits, max_items=max(examples_limit * 2, examples_limit))
    example_hits = _filter_hits(
        example_hits,
        max_items=examples_limit,
        min_abs_score=0.06,
        relative_ratio=0.62,
        query=question,
        min_lexical_overlap=0.10,
        allow_fallback=False,
    )
    if templates_limit > 0:
        template_hits = _hybrid_search(store, question, k=templates_limit, where={"type": "template"})
        template_hits = _dedupe_hits(template_hits, max_items=max(templates_limit * 2, templates_limit))
        template_hits = _filter_hits(
            template_hits,
            max_items=templates_limit,
            min_abs_score=0.05,
            relative_ratio=0.60,
            query=question,
            min_lexical_overlap=0.08,
            allow_fallback=False,
        )
    else:
        template_hits = []
    raw_glossary_hits = _dedupe_hits(
        _hybrid_search(store, question, k=settings.rag_top_k, where={"type": "glossary"}),
        max_items=settings.rag_top_k,
    )
    table_profile_hits = _dedupe_hits(
        _hybrid_search(store, question, k=settings.rag_top_k, where={"type": "table_profile"}),
        max_items=settings.rag_top_k,
    )
    general_glossary_hits = _dedupe_hits(
        _merge_hits([raw_glossary_hits, table_profile_hits], k=max(settings.rag_top_k * 2, settings.rag_top_k)),
        max_items=max(settings.rag_top_k * 2, settings.rag_top_k),
    )
    diagnosis_map_hits = (
        _dedupe_hits(
            _hybrid_search(store, question, k=settings.rag_top_k, where={"type": "diagnosis_map"}),
            max_items=settings.rag_top_k,
        )
        if intent["diagnosis"]
        else []
    )
    procedure_map_hits = (
        _dedupe_hits(
            _hybrid_search(store, question, k=settings.rag_top_k, where={"type": "procedure_map"}),
            max_items=settings.rag_top_k,
        )
        if intent["procedure"]
        else []
    )
    column_value_hits = (
        _dedupe_hits(
            _hybrid_search(store, question, k=settings.rag_top_k, where={"type": "column_value"}),
            max_items=settings.rag_top_k,
        )
        if intent["column_value"]
        else []
    )
    label_intent_hits = (
        _dedupe_hits(
            _hybrid_search(store, question, k=settings.rag_top_k, where={"type": "label_intent"}),
            max_items=settings.rag_top_k,
        )
        if intent["label_intent"]
        else []
    )
    local_map_hits = _build_diagnosis_map_hits(question, k=settings.rag_top_k)
    local_proc_hits = _build_procedure_map_hits(question, k=settings.rag_top_k)
    local_column_hits = _build_column_value_hits(question, k=settings.rag_top_k)
    local_label_hits = _build_label_intent_hits(question, k=settings.rag_top_k)
    local_map_hits = _dedupe_hits(local_map_hits, max_items=settings.rag_top_k)
    local_proc_hits = _dedupe_hits(local_proc_hits, max_items=settings.rag_top_k)
    local_column_hits = _dedupe_hits(local_column_hits, max_items=settings.rag_top_k)
    local_label_hits = _dedupe_hits(local_label_hits, max_items=settings.rag_top_k)
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
    base = project_path("var/metadata/schema_catalog.json")
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
    merged_query = " ".join(deduped)
    merged_intent = _detect_search_intent(merged_query)
    examples_limit, templates_limit = _resolve_context_limits(merged_query, settings)
    schema_k = _schema_retrieval_k(settings)

    def _per_query_k(total: int) -> int:
        return max(1, int(math.ceil(total / len(deduped))))

    schema_hits = _merge_hits(
        [_hybrid_search(store, q, k=_per_query_k(schema_k), where={"type": "schema"}) for q in deduped],
        k=schema_k,
    )
    schema_hits = _apply_table_scope(schema_hits)
    schema_hits = _filter_schema_hits(merged_query, schema_hits, max_items=schema_k)
    example_hits = _merge_hits(
        [_hybrid_search(store, q, k=_per_query_k(examples_limit), where={"type": "example"}) for q in deduped],
        k=examples_limit,
    )
    example_hits = _dedupe_hits(example_hits, max_items=max(examples_limit * 2, examples_limit))
    example_hits = _filter_hits(
        example_hits,
        max_items=examples_limit,
        min_abs_score=0.06,
        relative_ratio=0.62,
        query=merged_query,
        min_lexical_overlap=0.10,
        allow_fallback=False,
    )
    if templates_limit > 0:
        template_hits = _merge_hits(
            [_hybrid_search(store, q, k=_per_query_k(templates_limit), where={"type": "template"}) for q in deduped],
            k=templates_limit,
        )
        template_hits = _dedupe_hits(template_hits, max_items=max(templates_limit * 2, templates_limit))
        template_hits = _filter_hits(
            template_hits,
            max_items=templates_limit,
            min_abs_score=0.05,
            relative_ratio=0.60,
            query=merged_query,
            min_lexical_overlap=0.08,
            allow_fallback=False,
        )
    else:
        template_hits = []
    raw_glossary_hits = _merge_hits(
        [_hybrid_search(store, q, k=_per_query_k(settings.rag_top_k), where={"type": "glossary"}) for q in deduped],
        k=settings.rag_top_k,
    )
    raw_glossary_hits = _dedupe_hits(raw_glossary_hits, max_items=settings.rag_top_k)
    table_profile_hits = _merge_hits(
        [_hybrid_search(store, q, k=_per_query_k(settings.rag_top_k), where={"type": "table_profile"}) for q in deduped],
        k=settings.rag_top_k,
    )
    table_profile_hits = _dedupe_hits(table_profile_hits, max_items=settings.rag_top_k)
    general_glossary_hits = _dedupe_hits(
        _merge_hits([raw_glossary_hits, table_profile_hits], k=max(settings.rag_top_k * 2, settings.rag_top_k)),
        max_items=max(settings.rag_top_k * 2, settings.rag_top_k),
    )
    if merged_intent["diagnosis"]:
        diagnosis_map_hits = _merge_hits(
            [_hybrid_search(store, q, k=_per_query_k(settings.rag_top_k), where={"type": "diagnosis_map"}) for q in deduped],
            k=settings.rag_top_k,
        )
        diagnosis_map_hits = _dedupe_hits(diagnosis_map_hits, max_items=settings.rag_top_k)
    else:
        diagnosis_map_hits = []
    if merged_intent["procedure"]:
        procedure_map_hits = _merge_hits(
            [_hybrid_search(store, q, k=_per_query_k(settings.rag_top_k), where={"type": "procedure_map"}) for q in deduped],
            k=settings.rag_top_k,
        )
        procedure_map_hits = _dedupe_hits(procedure_map_hits, max_items=settings.rag_top_k)
    else:
        procedure_map_hits = []
    if merged_intent["column_value"]:
        column_value_hits = _merge_hits(
            [_hybrid_search(store, q, k=_per_query_k(settings.rag_top_k), where={"type": "column_value"}) for q in deduped],
            k=settings.rag_top_k,
        )
        column_value_hits = _dedupe_hits(column_value_hits, max_items=settings.rag_top_k)
    else:
        column_value_hits = []
    if merged_intent["label_intent"]:
        label_intent_hits = _merge_hits(
            [_hybrid_search(store, q, k=_per_query_k(settings.rag_top_k), where={"type": "label_intent"}) for q in deduped],
            k=settings.rag_top_k,
        )
        label_intent_hits = _dedupe_hits(label_intent_hits, max_items=settings.rag_top_k)
    else:
        label_intent_hits = []
    local_map_hits = _merge_hits(
        [_build_diagnosis_map_hits(q, k=_per_query_k(settings.rag_top_k)) for q in deduped],
        k=settings.rag_top_k,
    )
    local_map_hits = _dedupe_hits(local_map_hits, max_items=settings.rag_top_k)
    local_proc_hits = _merge_hits(
        [_build_procedure_map_hits(q, k=_per_query_k(settings.rag_top_k)) for q in deduped],
        k=settings.rag_top_k,
    )
    local_proc_hits = _dedupe_hits(local_proc_hits, max_items=settings.rag_top_k)
    local_column_hits = _merge_hits(
        [_build_column_value_hits(q, k=_per_query_k(settings.rag_top_k)) for q in deduped],
        k=settings.rag_top_k,
    )
    local_column_hits = _dedupe_hits(local_column_hits, max_items=settings.rag_top_k)
    local_label_hits = _merge_hits(
        [_build_label_intent_hits(q, k=_per_query_k(settings.rag_top_k)) for q in deduped],
        k=settings.rag_top_k,
    )
    local_label_hits = _dedupe_hits(local_label_hits, max_items=settings.rag_top_k)
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
