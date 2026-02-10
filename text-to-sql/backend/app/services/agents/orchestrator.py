from __future__ import annotations

from typing import Any
import math
import json
import re
from pathlib import Path

from app.core.config import get_settings
from app.services.agents.clarifier import evaluate_question_clarity
from app.services.agents.sql_engineer import generate_sql
from app.services.agents.sql_expert import review_sql
from app.services.agents.sql_postprocess import postprocess_sql
from app.services.agents.translator import contains_korean, translate_to_english
from app.services.cost_tracker import get_cost_tracker
from app.services.policy.gate import precheck_sql
from app.services.runtime.context_builder import build_context_payload, build_context_payload_multi
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
    cleaned = text.lower()
    cleaned = re.sub(r"[^a-z0-9\\s]", " ", cleaned)
    cleaned = re.sub(r"[\\uac00-\\ud7a3]", " ", cleaned)
    cleaned = re.sub(r"\\s+", " ", cleaned).strip()
    return cleaned


def _lookup_demo_cache(cache: dict[str, Any], question: str) -> dict[str, Any] | None:
    if question in cache:
        return {
            "mode": "demo",
            "question": question,
            "result": cache[question],
        }
    alias_map = cache.get("_aliases", {}) if isinstance(cache, dict) else {}
    if isinstance(alias_map, dict):
        aliased_key = alias_map.get(question)
        if aliased_key and aliased_key in cache:
            return {
                "mode": "demo",
                "question": question,
                "result": cache[aliased_key],
                "matched": aliased_key,
            }
    normalized = _normalize_question(question)
    if normalized:
        index = {_normalize_question(k): k for k in cache.keys() if k != "_aliases"}
        matched_key = index.get(normalized)
        if matched_key:
            return {
                "mode": "demo",
                "question": question,
                "result": cache[matched_key],
                "matched": matched_key,
            }
    return None


def _add_llm_cost(usage: dict[str, Any], stage: str) -> None:
    settings = get_settings()
    total_tokens = int(usage.get("total_tokens") or 0)
    if settings.llm_cost_per_1k_tokens_krw > 0 and total_tokens > 0:
        cost = int(math.ceil((total_tokens / 1000) * settings.llm_cost_per_1k_tokens_krw))
        get_cost_tracker().add_cost(cost, {"usage": usage, "stage": stage, "source": "llm"})


def _normalize_string_list(value: Any, *, limit: int) -> list[str]:
    if not isinstance(value, list):
        return []
    items: list[str] = []
    for item in value:
        text = str(item).strip()
        if not text:
            continue
        if text in items:
            continue
        items.append(text)
        if len(items) >= limit:
            break
    return items


def _normalize_clarifier_payload(payload: dict[str, Any]) -> dict[str, Any]:
    need_clarification = bool(payload.get("need_clarification"))
    reason = str(payload.get("reason") or "").strip()
    clarification_question = str(payload.get("clarification_question") or "").strip()
    if need_clarification and not clarification_question:
        clarification_question = "질문 범위를 조금 더 좁혀주세요."

    return {
        "need_clarification": need_clarification,
        "reason": reason,
        "clarification_question": clarification_question,
        "options": _normalize_string_list(payload.get("options"), limit=5),
        "example_inputs": _normalize_string_list(payload.get("example_inputs"), limit=3),
        "refined_question": str(payload.get("refined_question") or "").strip(),
        "usage": payload.get("usage", {}),
    }


def run_oneshot(
    question: str,
    *,
    skip_policy: bool = False,
    translate: bool | None = None,
    rag_multi: bool | None = None,
    conversation: list[dict[str, Any]] | None = None,
    enable_clarification: bool = False,
) -> dict[str, Any]:
    settings = get_settings()
    original_question = question
    translated_question = None
    use_translate = settings.translate_ko_to_en if translate is None else translate
    use_rag_multi = settings.rag_multi_query if rag_multi is None else rag_multi

    if settings.demo_mode or settings.demo_cache_always:
        cache = _load_demo_cache(settings.demo_cache_path)
        cached = _lookup_demo_cache(cache, question)
        if cached:
            return cached

    if enable_clarification:
        clarity: dict[str, Any] = {
            "need_clarification": False,
            "reason": "",
            "clarification_question": "",
            "options": [],
            "example_inputs": [],
            "refined_question": "",
            "usage": {},
        }
        try:
            clarity_raw = evaluate_question_clarity(original_question, conversation=conversation)
            clarity = _normalize_clarifier_payload(clarity_raw)
            _add_llm_cost(clarity.get("usage", {}), "clarify")
        except Exception:
            clarity = {**clarity, "need_clarification": False}
        if clarity["need_clarification"]:
            return {
                "mode": "clarify",
                "question": original_question,
                "clarification": {
                    "reason": clarity.get("reason"),
                    "question": clarity.get("clarification_question"),
                    "options": clarity.get("options", []),
                    "example_inputs": clarity.get("example_inputs", []),
                },
            }
        if clarity.get("refined_question"):
            original_question = clarity["refined_question"]
            question = clarity["refined_question"]
            if settings.demo_mode or settings.demo_cache_always:
                cache = _load_demo_cache(settings.demo_cache_path)
                cached = _lookup_demo_cache(cache, question)
                if cached:
                    return cached

    if use_translate and contains_korean(question):
        try:
            translated_question, usage = translate_to_english(question)
            _add_llm_cost(usage, "translate")
            if translated_question:
                question = translated_question
            else:
                translated_question = None
        except Exception:
            translated_question = None

    if (settings.demo_mode or settings.demo_cache_always) and translated_question:
        cache = _load_demo_cache(settings.demo_cache_path)
        cached = _lookup_demo_cache(cache, question)
        if cached:
            cached["question"] = original_question
            cached["question_en"] = question
            return cached

    risk_info = classify(question)
    if translated_question and use_rag_multi:
        context = build_context_payload_multi([question, original_question])
    else:
        context = build_context_payload(question)

    attempt = 0
    last_error: Exception | None = None
    while attempt <= settings.max_retry_attempts:
        attempt += 1
        try:
            engineer = generate_sql(question, context)
            # LLM 경고 문구는 사용하지 않도록 제거
            engineer.pop("warnings", None)
            final_payload = engineer

            if settings.expert_trigger_mode == "score" and risk_info["risk"] >= settings.expert_score_threshold:
                expert = review_sql(question, context, engineer)
                final_payload = expert
                # LLM 경고 문구는 사용하지 않도록 제거
                final_payload.pop("warnings", None)

            usage = final_payload.get("usage", {})
            _add_llm_cost(usage, "oneshot")

            final_sql = final_payload.get("final_sql") or ""
            if final_sql:
                final_sql, rules = postprocess_sql(question, final_sql)
                if rules:
                    final_payload["final_sql"] = final_sql
                    final_payload["postprocess"] = rules

            policy_result = None
            if not skip_policy and final_sql:
                policy_result = precheck_sql(final_sql, original_question)
            return {
                "mode": "advanced",
                "question": original_question,
                "question_en": question if translated_question else None,
                "risk": risk_info,
                "policy": policy_result,
                "context": context,
                "draft": engineer,
                "final": final_payload,
            }
        except Exception as exc:  # pragma: no cover - depends on LLM
            last_error = exc
            if attempt > settings.max_retry_attempts:
                raise
    raise last_error if last_error else RuntimeError("Unknown error")
