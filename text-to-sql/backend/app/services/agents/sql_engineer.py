from __future__ import annotations

import json
import re
from typing import Any

from app.core.config import get_settings
from app.services.agents.llm_client import LLMClient
from app.services.agents.prompts import ENGINEER_SYSTEM_PROMPT


def _extract_json(text: str) -> dict[str, Any]:
    try:
        return json.loads(text)
    except json.JSONDecodeError:
        match = re.search(r"\{.*\}", text, re.DOTALL)
        if match:
            return json.loads(match.group(0))
    raise ValueError("LLM response is not valid JSON")


def generate_sql(
    question: str,
    context: dict[str, Any],
    *,
    question_en: str | None = None,
    planner_intent: dict[str, Any] | None = None,
) -> dict[str, Any]:
    settings = get_settings()
    client = LLMClient()
    payload: dict[str, Any] = {"question": question, "context": context}
    if question_en and question_en.strip() and question_en.strip() != question.strip():
        payload["question_en"] = question_en.strip()
    if isinstance(planner_intent, dict) and planner_intent:
        payload["planner_intent"] = planner_intent
    messages = [
        {"role": "system", "content": ENGINEER_SYSTEM_PROMPT},
        {
            "role": "user",
            "content": json.dumps(payload, ensure_ascii=True),
        },
    ]
    response = client.chat(
        messages=messages,
        model=settings.engineer_model,
        max_tokens=settings.llm_max_output_tokens,
    )
    payload = _extract_json(response["content"])
    payload["usage"] = response.get("usage", {})
    return payload
