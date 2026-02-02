from __future__ import annotations

from typing import Any
import json
import re
from pathlib import Path

from app.core.config import get_settings
from app.services.agents.sql_engineer import generate_sql
from app.services.agents.sql_expert import review_sql
from app.services.cost_tracker import get_cost_tracker
from app.services.policy.gate import precheck_sql
from app.services.runtime.context_builder import build_context_payload
from app.services.runtime.risk_classifier import classify


def _load_demo_cache(path: str) -> dict[str, Any]:
    file_path = Path(path)
    if not file_path.exists():
        return {}
    try:
        return json.loads(file_path.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        return {}


def _normalize_question(text: str) -> str:
    cleaned = re.sub(r"[^a-z0-9\\s]", " ", text.lower())
    cleaned = re.sub(r"\\s+", " ", cleaned).strip()
    return cleaned


def run_oneshot(question: str, *, skip_policy: bool = False) -> dict[str, Any]:
    settings = get_settings()

    if settings.demo_mode:
        cache = _load_demo_cache(settings.demo_cache_path)
        if question in cache:
            return {
                "mode": "demo",
                "question": question,
                "result": cache[question],
            }
        normalized = _normalize_question(question)
        if normalized:
            index = {_normalize_question(k): k for k in cache.keys()}
            matched_key = index.get(normalized)
            if matched_key:
                return {
                    "mode": "demo",
                    "question": question,
                    "result": cache[matched_key],
                    "matched": matched_key,
                }

    risk_info = classify(question)
    context = build_context_payload(question)

    attempt = 0
    last_error: Exception | None = None
    while attempt <= settings.max_retry_attempts:
        attempt += 1
        try:
            engineer = generate_sql(question, context)
            final_payload = engineer

            if settings.expert_trigger_mode == "score" and risk_info["risk"] >= settings.expert_score_threshold:
                expert = review_sql(question, context, engineer)
                final_payload = expert

            usage = final_payload.get("usage", {})
            get_cost_tracker().add_cost(0, {"usage": usage, "stage": "oneshot"})

            if not skip_policy:
                final_sql = final_payload.get("final_sql") or ""
                if final_sql:
                    precheck_sql(final_sql)
            return {
                "mode": "advanced",
                "question": question,
                "risk": risk_info,
                "context": context,
                "draft": engineer,
                "final": final_payload,
            }
        except Exception as exc:  # pragma: no cover - depends on LLM
            last_error = exc
            if attempt > settings.max_retry_attempts:
                raise
    raise last_error if last_error else RuntimeError("Unknown error")
