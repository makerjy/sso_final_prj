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


def generate_sql(question: str, context: dict[str, Any]) -> dict[str, Any]:
    settings = get_settings()
    client = LLMClient()
    messages = [
        {"role": "system", "content": ENGINEER_SYSTEM_PROMPT},
        {
            "role": "user",
            "content": json.dumps({"question": question, "context": context}, ensure_ascii=True),
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
