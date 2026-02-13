from __future__ import annotations

from typing import Any, Dict, List

from openai import OpenAI

from src.config.rag_config import (
    EMBEDDING_MODEL,
    RAG_CONTEXT_MAX_CHARS,
    RAG_DOC_VERSION,
    RAG_MIN_SCORE,
    RAG_TOP_K,
)
from src.db.vector_store import get_mongo_collection, search_embeddings
from src.utils.logging import log_event


def _build_query_text(user_query: str, df_schema: Dict[str, Any]) -> str:
    columns = df_schema.get("columns", [])
    dtypes = df_schema.get("dtypes", {})
    return (
        "User query:\n"
        f"{user_query}\n\n"
        "DataFrame schema summary:\n"
        f"- columns: {columns}\n"
        f"- dtypes: {dtypes}\n"
    )


def _embed_texts(texts: List[str]) -> List[List[float]]:
    client = OpenAI()
    response = client.embeddings.create(model=EMBEDDING_MODEL, input=texts)
    return [item.embedding for item in response.data]


def retrieve_context(user_query: str, df_schema: Dict[str, Any]) -> Dict[str, Any]:
    """Retrieve related context snippets from vector store."""
    try:
        query_text = _build_query_text(user_query, df_schema)
        query_embedding = _embed_texts([query_text])[0]

        collection = get_mongo_collection()
        hits = search_embeddings(collection, query_embedding, limit=RAG_TOP_K)

        snippets = []
        kept_scores: List[float] = []
        for hit in hits:
            score = float(hit.get("score", 0.0))
            metadata = hit.get("metadata") or {}
            if score < RAG_MIN_SCORE:
                continue
            if metadata.get("doc_version") and metadata.get("doc_version") != RAG_DOC_VERSION:
                continue
            payload = (hit.get("metadata") or {}) | {"text": hit.get("text")}
            text = payload.get("text")
            if text:
                snippets.append(text)
                kept_scores.append(score)

        context_text = "\n\n".join(snippets)
        if len(context_text) > RAG_CONTEXT_MAX_CHARS:
            context_text = context_text[:RAG_CONTEXT_MAX_CHARS]
        log_event(
            "rag.search",
            {
                "count": len(snippets),
                "top_k": RAG_TOP_K,
                "min_score": RAG_MIN_SCORE,
                "score_max": max(kept_scores) if kept_scores else None,
                "score_min": min(kept_scores) if kept_scores else None,
            },
        )
        return {
            "snippets": snippets,
            "context_text": context_text,
            "scores": kept_scores,
        }
    except Exception as exc:  # pragma: no cover - environment dependent
        log_event("rag.search.error", {"error": str(exc)})
        return {"snippets": [], "context_text": ""}
