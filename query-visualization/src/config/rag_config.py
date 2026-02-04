from __future__ import annotations

import os
from pathlib import Path

from dotenv import load_dotenv

# 실행 위치와 무관하게 프로젝트 .env를 로드
_DOTENV_PATH = Path(__file__).resolve().parents[2] / ".env"
load_dotenv(_DOTENV_PATH)

# Embedding
EMBEDDING_MODEL = os.getenv("OPENAI_EMBEDDING_MODEL", "text-embedding-3-small")

# Qdrant 연결 (둘 중 하나 사용)
QDRANT_URL = os.getenv("QDRANT_URL")
QDRANT_API_KEY = os.getenv("QDRANT_API_KEY")
QDRANT_PATH = os.getenv("QDRANT_PATH", "qdrant_data")

# 컬렉션/검색 설정
RAG_COLLECTION = os.getenv("RAG_COLLECTION", "query_visualization_rag")
RAG_TOP_K = int(os.getenv("RAG_TOP_K", "6"))
RAG_BATCH_SIZE = int(os.getenv("RAG_BATCH_SIZE", "64"))
RAG_DISTANCE = os.getenv("RAG_DISTANCE", "Cosine")
