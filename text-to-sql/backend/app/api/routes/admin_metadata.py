from __future__ import annotations

from fastapi import APIRouter
from pydantic import BaseModel
from pathlib import Path
import json

from app.services.oracle.metadata_extractor import extract_metadata
from app.services.rag.indexer import reindex

router = APIRouter()
rag_router = APIRouter()


class MetadataSyncRequest(BaseModel):
    owner: str


@router.post("/sync")
def sync_metadata(req: MetadataSyncRequest):
    return extract_metadata(req.owner)


@rag_router.post("/reindex")
def rag_reindex():
    return reindex()


def _load_json(path: Path):
    if not path.exists():
        return None
    return json.loads(path.read_text(encoding="utf-8"))


def _count_jsonl(path: Path) -> int:
    if not path.exists():
        return 0
    return sum(1 for line in path.read_text(encoding="utf-8").splitlines() if line.strip())


@rag_router.get("/status")
def rag_status():
    base = Path("var/metadata")
    schema = _load_json(base / "schema_catalog.json") or {"tables": {}}
    return {
        "schema_docs": len(schema.get("tables", {})),
        "sql_examples_docs": _count_jsonl(base / "sql_examples.jsonl"),
        "join_templates_docs": _count_jsonl(base / "join_templates.jsonl"),
        "glossary_docs": _count_jsonl(base / "glossary_docs.jsonl"),
    }
