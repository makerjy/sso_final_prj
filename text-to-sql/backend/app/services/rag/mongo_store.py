from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any
import hashlib
import json
import math

from pymongo import MongoClient, ReplaceOne
from pymongo.errors import PyMongoError

from app.core.config import get_settings


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


def _build_metadata_filter(where: dict[str, Any] | None) -> dict[str, Any]:
    if not where:
        return {}
    return {f"metadata.{key}": value for key, value in where.items()}


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


class MongoStore:
    def __init__(self, collection_name: str = "rag_docs") -> None:
        settings = get_settings()
        self.persist_dir = Path(settings.rag_persist_dir)
        self.collection_name = settings.mongo_collection or collection_name
        self.dim = settings.rag_embedding_dim
        self.vector_index = settings.mongo_vector_index

        self._simple: SimpleStore | None = None
        self._client: MongoClient | None = None
        self._collection = None

        if settings.mongo_uri:
            self._client = MongoClient(settings.mongo_uri, serverSelectionTimeoutMS=2000)
            try:
                self._client.admin.command("ping")
            except Exception as exc:  # pragma: no cover - depends on runtime Mongo
                raise RuntimeError("MongoDB connection failed. Check MONGO_URI.") from exc
            database = self._client[settings.mongo_db]
            self._collection = database[self.collection_name]
            self._collection.create_index("metadata.type")
        else:
            self._simple = SimpleStore(self.persist_dir / "simple_store.json", dim=self.dim)

    def upsert_documents(self, docs: list[dict[str, Any]]) -> None:
        ids = [d["id"] for d in docs]
        texts = [d["text"] for d in docs]
        metas = [d.get("metadata", {}) for d in docs]

        if self._simple is not None:
            self._simple.upsert(ids, texts, metas)
            return

        vectors = _embed_texts(texts, dim=self.dim)
        ops = []
        for doc_id, text, meta, vec in zip(ids, texts, metas, vectors):
            ops.append(
                ReplaceOne(
                    {"_id": doc_id},
                    {"_id": doc_id, "text": text, "metadata": meta, "embedding": vec},
                    upsert=True,
                )
            )
        if ops:
            self._collection.bulk_write(ops, ordered=False)

    def _python_search(
        self,
        query_vec: list[float],
        filter_query: dict[str, Any],
        k: int,
    ) -> list[dict[str, Any]]:
        cursor = self._collection.find(filter_query, {"text": 1, "metadata": 1, "embedding": 1})
        scored = []
        for doc in cursor:
            text = doc.get("text", "")
            embedding = doc.get("embedding")
            if not embedding:
                embedding = _embed_text(text, dim=self.dim)
            score = _cosine(query_vec, embedding)
            scored.append((score, doc))
        scored.sort(key=lambda item: item[0], reverse=True)
        results = []
        for score, doc in scored[:k]:
            results.append({
                "id": str(doc.get("_id")),
                "text": doc.get("text", ""),
                "metadata": doc.get("metadata", {}),
                "score": score,
            })
        return results

    def search(self, query_text: str, k: int = 5, where: dict[str, Any] | None = None) -> list[dict[str, Any]]:
        if self._simple is not None:
            return self._simple.query(query_text, k=k, where=where)

        query_vec = _embed_text(query_text, dim=self.dim)
        filter_query = _build_metadata_filter(where)

        if self.vector_index:
            stage: dict[str, Any] = {
                "index": self.vector_index,
                "queryVector": query_vec,
                "path": "embedding",
                "numCandidates": max(k * 10, 50),
                "limit": k,
            }
            if filter_query:
                stage["filter"] = filter_query
            pipeline = [
                {"$vectorSearch": stage},
                {
                    "$project": {
                        "text": 1,
                        "metadata": 1,
                        "score": {"$meta": "vectorSearchScore"},
                    }
                },
            ]
            try:
                docs = list(self._collection.aggregate(pipeline))
            except PyMongoError:
                return self._python_search(query_vec, filter_query, k)
            results = []
            for doc in docs:
                results.append({
                    "id": str(doc.get("_id")),
                    "text": doc.get("text", ""),
                    "metadata": doc.get("metadata", {}),
                    "score": doc.get("score"),
                })
            return results

        return self._python_search(query_vec, filter_query, k)
