from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from app.core.config import get_settings
from app.services.rag.chroma_store import ChromaStore
from app.services.runtime.context_budget import trim_context_to_budget


@dataclass
class CandidateContext:
    schemas: list[dict[str, Any]]
    examples: list[dict[str, Any]]
    templates: list[dict[str, Any]]
    glossary: list[dict[str, Any]]


def build_candidate_context(question: str) -> CandidateContext:
    settings = get_settings()
    store = ChromaStore()

    schema_hits = store.search(question, k=settings.rag_top_k, where={"type": "schema"})
    example_hits = store.search(question, k=settings.examples_per_query, where={"type": "example"})
    template_hits = store.search(question, k=settings.templates_per_query, where={"type": "template"})
    glossary_hits = store.search(question, k=settings.rag_top_k, where={"type": "glossary"})

    context = CandidateContext(
        schemas=schema_hits,
        examples=example_hits,
        templates=template_hits,
        glossary=glossary_hits,
    )
    return trim_context_to_budget(context, settings.context_token_budget)
