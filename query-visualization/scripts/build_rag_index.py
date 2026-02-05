from __future__ import annotations

import json
import sys
from pathlib import Path
from typing import Iterable, List, Tuple
from uuid import uuid4

from openai import OpenAI

BASE_DIR = Path(__file__).resolve().parents[1]
sys.path.append(str(BASE_DIR))

from src.config.rag_config import EMBEDDING_MODEL, RAG_BATCH_SIZE
from src.db.vector_store import ensure_collection, get_qdrant_client, upsert_embeddings


def _load_jsonl(path: Path) -> List[dict]:
    rows: List[dict] = []
    if not path.exists():
        return rows
    with path.open("r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            rows.append(json.loads(line))
    return rows


def _iter_seed_docs(data_dir: Path) -> Iterable[dict]:
    for path in sorted(data_dir.glob("*.jsonl")):
        yield from _load_jsonl(path)


def _normalize_doc(doc: dict) -> Tuple[str, dict]:
    """Normalize heterogeneous docs into (text, metadata)."""
    if "text" in doc:
        return doc["text"], doc.get("metadata", {})

    # SQL templates (visualization-first)
    if "template_id" in doc and "sql" in doc:
        template_id = doc.get("template_id")
        x_alias = doc.get("x_alias")
        y_alias = doc.get("y_alias")
        chart_candidates = doc.get("chart_candidates", [])
        chart_type = chart_candidates[0] if chart_candidates else "bar"
        text = (
            f"Template: {template_id}\n"
            f"x_alias: {x_alias}\n"
            f"y_alias: {y_alias}\n"
            f"chart_candidates: {chart_candidates}\n"
            f"SQL: {doc.get('sql')}\n"
            f"chart_spec: {{\"chart_type\": \"{chart_type}\", \"x\": \"{x_alias}\", \"y\": \"{y_alias}\"}}"
        )
        metadata = doc.get("metadata", {}) | {
            "type": "template",
            "template_id": template_id,
        }
        return text, metadata

    # SQL examples (question -> visualization)
    if "question" in doc and "sql" in doc:
        text = (
            f"Question: {doc.get('question')}\n"
            f"Intent: {doc.get('intent')}\n"
            f"X meaning: {doc.get('x_meaning')}\n"
            f"Y meaning: {doc.get('y_meaning')}\n"
            f"Chart: {doc.get('chart_type')}\n"
            f"SQL: {doc.get('sql')}"
        )
        metadata = doc.get("metadata", {}) | {"type": "example"}
        return text, metadata

    # Fallback
    return json.dumps(doc, ensure_ascii=False), doc.get("metadata", {})


def _embed_texts(texts: List[str]) -> List[List[float]]:
    client = OpenAI()
    response = client.embeddings.create(model=EMBEDDING_MODEL, input=texts)
    return [item.embedding for item in response.data]


def _batch(iterable: List[str], size: int) -> Iterable[List[str]]:
    for i in range(0, len(iterable), size):
        yield iterable[i : i + size]


def build_index() -> None:
    data_dir = BASE_DIR / "data"
    docs = list(_iter_seed_docs(data_dir))

    if not docs:
        raise RuntimeError("RAG 시드 데이터가 없습니다. data/*.jsonl 을 확인하세요.")

    normalized = [_normalize_doc(d) for d in docs]
    texts = [text for text, _ in normalized]
    metadatas = [meta | {"text": text} for text, meta in normalized]
    ids = [str(uuid4()) for _ in docs]
    for doc, doc_id in zip(docs, ids):
        doc["metadata"] = doc.get("metadata", {}) | {"doc_id": doc.get("id")}

    client = get_qdrant_client()

    # 첫 배치로 벡터 크기 확보 후 컬렉션 생성
    first_embeddings = _embed_texts(texts[:1])
    ensure_collection(client, vector_size=len(first_embeddings[0]))
    upsert_embeddings(client, first_embeddings, metadatas[:1], ids[:1])

    # 나머지 배치 업서트
    start_idx = 1
    for batch_texts in _batch(texts[start_idx:], RAG_BATCH_SIZE):
        batch_embeddings = _embed_texts(batch_texts)
        batch_size = len(batch_texts)
        batch_metadatas = metadatas[start_idx : start_idx + batch_size]
        batch_ids = ids[start_idx : start_idx + batch_size]
        upsert_embeddings(client, batch_embeddings, batch_metadatas, batch_ids)
        start_idx += batch_size


if __name__ == "__main__":
    build_index()
