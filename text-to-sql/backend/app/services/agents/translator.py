from __future__ import annotations

import re
from typing import Any

from app.core.config import get_settings
from app.services.agents.llm_client import LLMClient


_HANGUL_RE = re.compile(r"[\uac00-\ud7a3]")

TRANSLATE_SYSTEM_PROMPT = (
    "Translate Korean to concise English. Preserve medical terms, acronyms, "
    "table/column names, and code values as-is. Return only the translation."
)


def contains_korean(text: str) -> bool:
    return bool(_HANGUL_RE.search(text))


def translate_to_english(text: str) -> tuple[str, dict[str, Any]]:
    settings = get_settings()
    client = LLMClient()
    messages = [
        {"role": "system", "content": TRANSLATE_SYSTEM_PROMPT},
        {"role": "user", "content": text},
    ]
    response = client.chat(
        messages=messages,
        model=settings.expert_model,
        max_tokens=min(settings.llm_max_output_tokens, 256),
    )
    translated = (response.get("content") or "").strip()
    if translated.startswith('"') and translated.endswith('"'):
        translated = translated[1:-1].strip()
    return translated, response.get("usage", {})
