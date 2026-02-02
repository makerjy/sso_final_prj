from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any
import hashlib
import json
import math

from app.core.config import get_settings

try:
    import chromadb  # type: ignore
except Exception:  # pragma: no cover - optional dependency
    chromadb = None


def _hash_token(token: str, dim: int) -> int:
    digest = hashlib.md5(token.encode("utf-8")).hexdigest()
    return int(digest, 16) % dim


def _embed_text(text: str, dim: int = 128) -> list[float]:
    vec = [0.0] * dim
    for tok in text.lower().split():
        idx = _hash_token(tok, dim)
        vec[idx] += 1.0
    norm = math.sqrt(sum(v * v for v in vec))
    if norm > 0:
        vec = [v / norm for v in vec]
    return vec


def _embed_texts(texts: list[str], dim: int = 128) -> list[list[float]]:
    return [_embed_text(t, dim=dim) for t in texts]


def _cosine(a: list[float], b: list[float]) -> float:
    if len(a) != len(b):
        return 0.0
    return sum(x * y for x, y in zip(a, b))


@dataclass
class SimpleStore:
    path: Path
    dim: int = 128
    docs: dict[str, dict[str, Any]] = None  # type: ignore

    def __post_init__(self) -> None:
        self.docs = {}
        if self.path.exists():
            try:
                data = json.loads(self.path.read_text(encoding="utf-8"))
                self.docs = data.get("docs", {})
            except json.JSONDecodeError:
                self.docs = {}

    def persist(self) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        payload = {"docs": self.docs}
        self.path.write_text(json.dumps(payload, ensure_ascii=True), encoding="utf-8")

    def upsert(self, ids: list[str], texts: list[str], metadatas: list[dict[str, Any]]) -> None:
        vectors = _embed_texts(texts, dim=self.dim)
        for doc_id, text, meta, vec in zip(ids, texts, metadatas, vectors):
            self.docs[doc_id] = {"text": text, "meta": meta, "vec": vec}
        self.persist()

    def query(self, query_text: str, k: int = 5, where: dict[str, Any] | None = None) -> list[dict[str, Any]]:
        qvec = _embed_text(query_text, dim=self.dim)
        scored = []
        for doc_id, doc in self.docs.items():
            if where:
                match = True
                for key, value in where.items():
                    if doc.get("meta", {}).get(key) != value:
                        match = False
                        break
                if not match:
                    continue
            score = _cosine(qvec, doc["vec"])
            scored.append((score, doc_id, doc))
        scored.sort(reverse=True)
        results = []
        for score, doc_id, doc in scored[:k]:
            results.append({
                "id": doc_id,
                "text": doc["text"],
                "metadata": doc["meta"],
                "score": score,
            })
        return results


class ChromaStore:
    def __init__(self, collection_name: str = "rag_docs") -> None:
        settings = get_settings()
        self.persist_dir = Path(settings.rag_persist_dir)
        self.collection_name = collection_name

        if chromadb is None:
            self._client = None
            self._collection = None
            self._simple = SimpleStore(self.persist_dir / "simple_store.json")
        else:
            self._simple = None
            self._client = chromadb.PersistentClient(path=str(self.persist_dir))
            self._collection = self._client.get_or_create_collection(collection_name)

    def upsert_documents(self, docs: list[dict[str, Any]]) -> None:
        ids = [d["id"] for d in docs]
        texts = [d["text"] for d in docs]
        metas = [d.get("metadata", {}) for d in docs]
        if self._simple is not None:
            self._simple.upsert(ids, texts, metas)
        else:
            self._collection.upsert(ids=ids, documents=texts, metadatas=metas)

    def search(self, query_text: str, k: int = 5, where: dict[str, Any] | None = None) -> list[dict[str, Any]]:
        if self._simple is not None:
            return self._simple.query(query_text, k=k, where=where)
        results = self._collection.query(query_texts=[query_text], n_results=k, where=where or {})
        hits = []
        for idx in range(len(results.get("ids", [[]])[0])):
            hits.append({
                "id": results["ids"][0][idx],
                "text": results["documents"][0][idx],
                "metadata": results["metadatas"][0][idx],
                "score": results.get("distances", [[None]])[0][idx],
            })
        return hits
