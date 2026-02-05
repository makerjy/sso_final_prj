from __future__ import annotations

from typing import Iterable, List, Optional
from uuid import uuid4

from qdrant_client import QdrantClient
from qdrant_client.http import models as qdrant_models

from src.config.rag_config import (
    QDRANT_API_KEY,
    QDRANT_PATH,
    QDRANT_URL,
    RAG_COLLECTION,
    RAG_DISTANCE,
)


def get_qdrant_client() -> QdrantClient:
    if QDRANT_URL:
        return QdrantClient(url=QDRANT_URL, api_key=QDRANT_API_KEY)
    return QdrantClient(path=QDRANT_PATH)


def ensure_collection(client: QdrantClient, vector_size: int) -> None:
    distance = getattr(qdrant_models.Distance, RAG_DISTANCE, qdrant_models.Distance.COSINE)
    collections = client.get_collections().collections
    if any(c.name == RAG_COLLECTION for c in collections):
        return
    client.create_collection(
        collection_name=RAG_COLLECTION,
        vectors_config=qdrant_models.VectorParams(size=vector_size, distance=distance),
    )


def upsert_embeddings(
    client: QdrantClient,
    embeddings: List[List[float]],
    payloads: List[dict],
    ids: Optional[Iterable[str]] = None,
) -> None:
    point_ids = list(ids) if ids else [str(uuid4()) for _ in embeddings]
    points = [
        qdrant_models.PointStruct(id=pid, vector=vec, payload=payload)
        for pid, vec, payload in zip(point_ids, embeddings, payloads)
    ]
    client.upsert(collection_name=RAG_COLLECTION, points=points)


def search_embeddings(
    client: QdrantClient,
    query_embedding: List[float],
    limit: int,
) -> list:
    response = client.query_points(
        collection_name=RAG_COLLECTION,
        query=query_embedding,
        limit=limit,
        with_payload=True,
    )
    return response.points
