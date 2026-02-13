from __future__ import annotations

from typing import Any

from app.core.config import get_settings

try:
    from openai import OpenAI  # type: ignore
except Exception:  # pragma: no cover
    OpenAI = None


class LLMClient:
    def __init__(self) -> None:
        settings = get_settings()
        self._settings = settings
        if OpenAI is None:
            raise RuntimeError("openai library is not installed")
        self.client = OpenAI(
            api_key=settings.openai_api_key or None,
            base_url=settings.openai_base_url or None,
            organization=settings.openai_org or None,
            timeout=settings.llm_timeout_sec,
        )

    def chat(self, messages: list[dict[str, str]], model: str, max_tokens: int) -> dict[str, Any]:
        response = self.client.chat.completions.create(
            model=model,
            messages=messages,
            max_tokens=max_tokens,
            temperature=self._settings.llm_temperature,
        )
        content = response.choices[0].message.content or ""
        usage = {
            "prompt_tokens": getattr(response.usage, "prompt_tokens", 0),
            "completion_tokens": getattr(response.usage, "completion_tokens", 0),
            "total_tokens": getattr(response.usage, "total_tokens", 0),
        }
        return {"content": content, "usage": usage}
