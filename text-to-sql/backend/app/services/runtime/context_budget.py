from __future__ import annotations

from typing import Any

try:
    import tiktoken  # type: ignore
except Exception:  # pragma: no cover
    tiktoken = None


def _count_tokens(text: str) -> int:
    if tiktoken is None:
        return max(1, len(text.split()))
    enc = tiktoken.get_encoding("cl100k_base")
    return len(enc.encode(text))


def _trim_items(items: list[dict[str, Any]], budget: int) -> tuple[list[dict[str, Any]], int]:
    kept: list[dict[str, Any]] = []
    used = 0
    for item in items:
        text = item.get("text", "")
        cost = _count_tokens(text)
        if used + cost > budget:
            break
        kept.append(item)
        used += cost
    return kept, used


def trim_context_to_budget(context: Any, budget: int) -> Any:
    remaining = budget

    examples, used = _trim_items(context.examples, remaining)
    remaining -= used

    templates, used = _trim_items(context.templates, remaining)
    remaining -= used

    schemas, used = _trim_items(context.schemas, remaining)
    remaining -= used

    glossary, _ = _trim_items(context.glossary, remaining)

    return context.__class__(
        schemas=schemas,
        examples=examples,
        templates=templates,
        glossary=glossary,
    )
