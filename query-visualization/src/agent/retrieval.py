from __future__ import annotations

from typing import Any, Dict, List

from openai import OpenAI

from src.config.rag_config import EMBEDDING_MODEL, RAG_TOP_K
from src.db.vector_store import get_mongo_collection, search_embeddings
from src.utils.logging import log_event


def _build_query_text(user_query: str, df_schema: Dict[str, Any]) -> str:
    columns = df_schema.get("columns", [])
    dtypes = df_schema.get("dtypes", {})
    return (
        "사용자 질문:\n"
        f"{user_query}\n\n"
        "데이터프레임 스키마 요약:\n"
        f"- columns: {columns}\n"
        f"- dtypes: {dtypes}\n"
    )


def _embed_texts(texts: List[str]) -> List[List[float]]:
    client = OpenAI()
    response = client.embeddings.create(model=EMBEDDING_MODEL, input=texts)
    return [item.embedding for item in response.data]


def retrieve_context(user_query: str, df_schema: Dict[str, Any]) -> Dict[str, Any]:
    """Qdrant에서 관련 컨텍스트를 검색해 반환."""
    try:
        query_text = _build_query_text(user_query, df_schema)
        query_embedding = _embed_texts([query_text])[0]

        collection = get_mongo_collection()
        hits = search_embeddings(collection, query_embedding, limit=RAG_TOP_K)

        snippets = []
        for hit in hits:
            payload = (hit.get('metadata') or {}) | {'text': hit.get('text')}
            text = payload.get("text")
            if text:
                snippets.append(text)

        context_text = "\n\n".join(snippets)
        log_event("rag.search", {"count": len(snippets)})
        return {
            "snippets": snippets,
            "context_text": context_text,
        }
    except Exception as exc:  # pragma: no cover - 환경 의존
        log_event("rag.search.error", {"error": str(exc)})
        return {"snippets": [], "context_text": ""}
