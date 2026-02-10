from __future__ import annotations

import json
import re
from typing import Any

from app.core.config import get_settings
from app.services.agents.llm_client import LLMClient
from app.services.agents.prompts import CLARIFIER_SYSTEM_PROMPT


def _extract_json(text: str) -> dict[str, Any]:
    try:
        return json.loads(text)
    except json.JSONDecodeError:
        match = re.search(r"\{.*\}", text, re.DOTALL)
        if match:
            return json.loads(match.group(0))
    raise ValueError("LLM response is not valid JSON")


def _normalize_conversation(conversation: list[dict[str, Any]] | None) -> list[dict[str, str]]:
    if not conversation:
        return []
    normalized: list[dict[str, str]] = []
    for turn in conversation[-10:]:
        role = str(turn.get("role") or "").strip().lower()
        content = str(turn.get("content") or "").strip()
        if role not in {"user", "assistant"} or not content:
            continue
        normalized.append({"role": role, "content": content[:2000]})
    return normalized


def evaluate_question_clarity(
    question: str,
    *,
    conversation: list[dict[str, Any]] | None = None,
) -> dict[str, Any]:
    settings = get_settings()
    client = LLMClient()
    messages = [
        {"role": "system", "content": CLARIFIER_SYSTEM_PROMPT},
        {
            "role": "user",
            "content": json.dumps(
                {
                    "latest_question": question,
                    "conversation": _normalize_conversation(conversation),
                },
                ensure_ascii=False,
            ),
        },
    ]
    response = client.chat(
        messages=messages,
        model=settings.expert_model,
        max_tokens=settings.llm_max_output_tokens,
    )
    payload = _extract_json(response["content"])
    payload["usage"] = response.get("usage", {})
    return payload
