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


def _item_score(item: dict[str, Any]) -> float:
    value = item.get("score")
    try:
        return float(value) if value is not None else 0.0
    except Exception:
        return 0.0


def _rank_items(items: list[dict[str, Any]]) -> list[dict[str, Any]]:
    if not items:
        return []
    if all(item.get("score") is None for item in items):
        return list(items)
    return sorted(items, key=_item_score, reverse=True)


def _trim_items(
    items: list[dict[str, Any]],
    budget: int,
) -> tuple[list[dict[str, Any]], int, list[dict[str, Any]]]:
    kept: list[dict[str, Any]] = []
    remaining_items: list[dict[str, Any]] = []
    used = 0
    if budget <= 0:
        return kept, used, list(items)
    for item in items:
        text = item.get("text", "")
        cost = _count_tokens(text)
        if used + cost > budget:
            remaining_items.append(item)
            continue
        kept.append(item)
        used += cost
    return kept, used, remaining_items


def trim_context_to_budget(context: Any, budget: int) -> Any:
    if budget <= 0:
        return context.__class__(schemas=[], examples=[], templates=[], glossary=[])

    remaining = budget
    schemas = _rank_items(list(getattr(context, "schemas", [])))
    examples = _rank_items(list(getattr(context, "examples", [])))
    templates = _rank_items(list(getattr(context, "templates", [])))
    glossary = _rank_items(list(getattr(context, "glossary", [])))

    quotas = {
        "schemas": int(budget * 0.40),
        "examples": int(budget * 0.30),
        "glossary": int(budget * 0.20),
    }
    quotas["templates"] = max(0, budget - quotas["schemas"] - quotas["examples"] - quotas["glossary"])

    items = {
        "schemas": schemas,
        "examples": examples,
        "glossary": glossary,
        "templates": templates,
    }
    kept: dict[str, list[dict[str, Any]]] = {key: [] for key in items}

    # Pass 1: Reserve capacity for critical context first.
    for key in ("schemas", "examples", "glossary", "templates"):
        if remaining <= 0:
            break
        part_budget = min(remaining, quotas.get(key, 0))
        if part_budget <= 0:
            continue
        chunk, used, leftover = _trim_items(items[key], part_budget)
        kept[key].extend(chunk)
        items[key] = leftover
        remaining -= used

    # Pass 2: Fill leftovers by priority.
    for key in ("schemas", "examples", "glossary", "templates"):
        if remaining <= 0:
            break
        chunk, used, leftover = _trim_items(items[key], remaining)
        kept[key].extend(chunk)
        items[key] = leftover
        remaining -= used

    return context.__class__(
        schemas=kept["schemas"],
        examples=kept["examples"],
        templates=kept["templates"],
        glossary=kept["glossary"],
    )
