from __future__ import annotations

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel
from typing import Any
import uuid
from pathlib import Path
import json
import time

from app.services.agents.orchestrator import run_oneshot
from app.services.agents.sql_expert import repair_sql_after_error
from app.services.agents.sql_error_templates import apply_sql_error_templates
from app.services.agents.sql_postprocess import postprocess_sql, recommend_postprocess_profile
from app.services.budget_gate import ensure_budget_ok
from app.services.cost_tracker import get_cost_tracker
from app.services.logging_store.store import append_event
from app.services.oracle.executor import execute_sql
from app.services.policy.gate import precheck_sql
from app.services.runtime.sql_error_repair_store import (
    find_learned_sql_fix,
    mark_learned_sql_fix_used,
    upsert_learned_sql_fix,
)
from app.core.config import get_settings

router = APIRouter()

_QUERY_STORE: dict[str, dict] = {}


class OneShotRequest(BaseModel):
    question: str
    translate: bool | None = None
    rag_multi: bool | None = None
    conversation: list[dict] | None = None
    user_name: str | None = None
    user_role: str | None = None


class RunRequest(BaseModel):
    qid: str | None = None
    sql: str | None = None
    user_ack: bool = False
    user_name: str | None = None
    user_role: str | None = None


def _add_llm_cost(usage: dict[str, Any], stage: str) -> None:
    settings = get_settings()
    total_tokens = int(usage.get("total_tokens") or 0)
    if settings.llm_cost_per_1k_tokens_krw > 0 and total_tokens > 0:
        cost = int((total_tokens / 1000) * settings.llm_cost_per_1k_tokens_krw)
        if cost <= 0:
            cost = 1
        get_cost_tracker().add_cost(cost, {"usage": usage, "stage": stage, "source": "llm"})


def _repair_sql_once(
    *,
    question: str,
    question_en: str | None,
    context: dict[str, Any],
    planner_intent: dict[str, Any] | None,
    failed_sql: str,
    error_message: str,
) -> tuple[str, dict[str, Any]]:
    repaired = repair_sql_after_error(
        question,
        context,
        failed_sql,
        error_message,
        question_en=question_en,
        planner_intent=planner_intent,
    )
    _add_llm_cost(repaired.get("usage", {}), "run_repair")
    repaired_sql = str(repaired.get("final_sql") or "").strip()
    if not repaired_sql:
        raise HTTPException(status_code=400, detail="Auto-repair returned empty SQL")
    repaired_sql, rules = postprocess_sql(question, repaired_sql, profile="aggressive")
    if rules:
        repaired["postprocess"] = rules
    return repaired_sql, repaired


@router.post("/oneshot")
def oneshot(req: OneShotRequest):
    ensure_budget_ok()
    settings = get_settings()
    start = time.perf_counter()
    payload: dict | None = None
    qid: str | None = None
    status = "success"
    error_detail = None
    try:
        payload = run_oneshot(
            req.question,
            translate=req.translate,
            rag_multi=req.rag_multi,
            conversation=req.conversation,
            enable_clarification=settings.clarifier_enabled,
        )
        qid = str(uuid.uuid4())
        _QUERY_STORE[qid] = payload
        return {"qid": qid, "payload": payload}
    except HTTPException as exc:
        status = "error"
        error_detail = str(exc.detail) if exc.detail else str(exc)
        raise
    except Exception as exc:  # pragma: no cover - depends on LLM
        status = "error"
        error_detail = str(exc)
        raise
    finally:
        duration_ms = int((time.perf_counter() - start) * 1000)
        try:
            sql = ""
            rows_returned = 0
            mode = None
            if isinstance(payload, dict):
                mode = payload.get("mode")
                if mode == "demo":
                    result = payload.get("result") or {}
                    sql = str(result.get("sql") or "")
                    preview = result.get("preview") or {}
                    rows_returned = int(preview.get("row_count") or 0)
                else:
                    final = payload.get("final") or {}
                    draft = payload.get("draft") or {}
                    sql = str(final.get("final_sql") or draft.get("final_sql") or "")
            append_event(get_settings().events_log_path, {
                "type": "audit",
                "event": "query_oneshot",
                "qid": qid,
                "question": req.question,
                "sql": sql,
                "status": status,
                "rows_returned": rows_returned,
                "row_cap": None,
                "duration_ms": duration_ms,
                "mode": mode,
                "user": {
                    "name": req.user_name or "사용자",
                    "role": req.user_role or "연구원",
                },
                "error": error_detail,
                "applied_terms": [],
                "applied_metrics": [],
            })
        except Exception:
            pass


@router.get("/get")
def get_query(qid: str):
    payload = _QUERY_STORE.get(qid)
    if not payload:
        raise HTTPException(status_code=404, detail="Query not found")
    return {"qid": qid, "payload": payload}


@router.post("/run")
def run_query(req: RunRequest):
    ensure_budget_ok()
    if not req.user_ack:
        raise HTTPException(status_code=400, detail="user_ack is required")

    settings = get_settings()

    sql = req.sql
    stored = None
    if req.qid:
        stored = _QUERY_STORE.get(req.qid)
    if not sql and stored and "final" in stored:
        sql = stored["final"].get("final_sql")

    if not sql:
        raise HTTPException(status_code=400, detail="SQL not provided")

    question = None
    question_en = None
    context: dict[str, Any] = {}
    planner_intent: dict[str, Any] | None = None
    if isinstance(stored, dict):
        question = stored.get("question") or stored.get("question_en")
        question_en_value = stored.get("question_en")
        if isinstance(question_en_value, str):
            question_en = question_en_value
        context_value = stored.get("context")
        if isinstance(context_value, dict):
            context = context_value
        planner_value = stored.get("planner")
        if isinstance(planner_value, dict):
            intent_value = planner_value.get("intent")
            if isinstance(intent_value, dict):
                planner_intent = intent_value
    if not question:
        question = "Fix failed SQL while preserving original intent."

    user_name = req.user_name or "사용자"
    user_role = req.user_role or "연구원"

    status = "success"
    rows_returned = 0
    row_cap = None
    error_detail = None
    auto_repair_history: list[dict[str, Any]] = []
    llm_repair_pairs: list[dict[str, Any]] = []
    persisted_rule_ids: list[str] = []
    start = time.perf_counter()

    try:
        current_sql = sql
        seed_fix = find_learned_sql_fix(current_sql)
        if isinstance(seed_fix, dict):
            seeded_sql = str(seed_fix.get("fixed_sql") or "").strip()
            if seeded_sql and seeded_sql.strip() != current_sql.strip():
                current_sql = seeded_sql
                seed_rule_id = str(seed_fix.get("id") or "").strip()
                if seed_rule_id:
                    mark_learned_sql_fix_used(seed_rule_id)
                auto_repair_history.append(
                    {
                        "attempt": 0,
                        "source": "learned_rule",
                        "rule_id": seed_rule_id,
                        "error": "",
                        "risk_score": None,
                        "postprocess": [],
                    }
                )

        max_repair_attempts = (
            settings.sql_auto_repair_max_attempts if settings.sql_auto_repair_enabled else 0
        )
        for attempt in range(max_repair_attempts + 1):
            postprocess_profile = "relaxed" if attempt == 0 else "aggressive"
            postprocess_reasons: list[str] = []
            if attempt == 0:
                postprocess_profile, postprocess_reasons = recommend_postprocess_profile(
                    question,
                    current_sql,
                    default_profile=postprocess_profile,
                )
            preprocessed_sql, pre_rules = postprocess_sql(
                question,
                current_sql,
                profile=postprocess_profile,
            )
            if preprocessed_sql.strip() != current_sql.strip():
                current_sql = preprocessed_sql
            if pre_rules:
                auto_repair_history.append(
                    {
                        "attempt": attempt + 1,
                        "source": "postprocess",
                        "profile": postprocess_profile,
                        "profile_reasons": postprocess_reasons,
                        "error": "",
                        "risk_score": None,
                        "postprocess": pre_rules,
                    }
                )
            sql = current_sql
            try:
                policy_result = precheck_sql(current_sql, question)
                result = execute_sql(current_sql)
                rows_returned = int(result.get("row_count") or 0)
                row_cap = int(result.get("row_cap") or 0)
                if row_cap and rows_returned >= row_cap:
                    status = "warning"
                if settings.sql_run_cost_krw > 0:
                    get_cost_tracker().add_cost(settings.sql_run_cost_krw, {"stage": "run"})
                for pair in llm_repair_pairs:
                    saved_rule = upsert_learned_sql_fix(
                        failed_sql=pair.get("failed_sql") or "",
                        fixed_sql=pair.get("fixed_sql") or "",
                        error_message=pair.get("error_message"),
                        resolution_notes=(
                            pair.get("resolution_notes")
                            if isinstance(pair.get("resolution_notes"), list)
                            else None
                        ),
                    )
                    if isinstance(saved_rule, dict):
                        rule_id = str(saved_rule.get("id") or "").strip()
                        if rule_id and rule_id not in persisted_rule_ids:
                            persisted_rule_ids.append(rule_id)
                response: dict[str, Any] = {
                    "sql": current_sql,
                    "result": result,
                    "policy": policy_result,
                }
                if auto_repair_history:
                    response["repair"] = {
                        "applied": True,
                        "attempts": len(auto_repair_history),
                        "history": auto_repair_history,
                    }
                    if persisted_rule_ids:
                        response["repair"]["persisted_rule_ids"] = persisted_rule_ids
                return response
            except HTTPException as exc:
                error_message = str(exc.detail) if exc.detail else str(exc)
                if attempt >= max_repair_attempts:
                    raise
                known_fix = find_learned_sql_fix(current_sql, error_message=error_message)
                if isinstance(known_fix, dict):
                    known_fixed_sql = str(known_fix.get("fixed_sql") or "").strip()
                    if known_fixed_sql and known_fixed_sql.strip() != current_sql.strip():
                        known_rule_id = str(known_fix.get("id") or "").strip()
                        if known_rule_id:
                            mark_learned_sql_fix_used(known_rule_id)
                        current_sql = known_fixed_sql
                        auto_repair_history.append(
                            {
                                "attempt": attempt + 1,
                                "source": "learned_rule",
                                "rule_id": known_rule_id,
                                "error": error_message,
                                "risk_score": None,
                                "postprocess": [],
                            }
                        )
                        continue
                templated_sql, template_rules = apply_sql_error_templates(
                    question=question,
                    sql=current_sql,
                    error_message=error_message,
                )
                if templated_sql.strip() and templated_sql.strip() != current_sql.strip():
                    current_sql = templated_sql
                    auto_repair_history.append(
                        {
                            "attempt": attempt + 1,
                            "source": "template_repair",
                            "error": error_message,
                            "risk_score": None,
                            "postprocess": template_rules,
                        }
                    )
                    continue
                failed_sql = current_sql
                repaired_sql, repaired_payload = _repair_sql_once(
                    question=question,
                    question_en=question_en,
                    context=context,
                    planner_intent=planner_intent,
                    failed_sql=failed_sql,
                    error_message=error_message,
                )
                if repaired_sql.strip() == current_sql.strip():
                    raise
                current_sql = repaired_sql
                llm_repair_pairs.append(
                    {
                        "failed_sql": failed_sql,
                        "fixed_sql": repaired_sql,
                        "error_message": error_message,
                        "resolution_notes": repaired_payload.get("postprocess", []),
                    }
                )
                auto_repair_history.append(
                    {
                        "attempt": attempt + 1,
                        "source": "llm_repair",
                        "error": error_message,
                        "risk_score": repaired_payload.get("risk_score"),
                        "postprocess": repaired_payload.get("postprocess", []),
                    }
                )
            except Exception as exc:  # pragma: no cover - depends on driver/SDK
                error_message = str(exc)
                if attempt >= max_repair_attempts:
                    raise
                known_fix = find_learned_sql_fix(current_sql, error_message=error_message)
                if isinstance(known_fix, dict):
                    known_fixed_sql = str(known_fix.get("fixed_sql") or "").strip()
                    if known_fixed_sql and known_fixed_sql.strip() != current_sql.strip():
                        known_rule_id = str(known_fix.get("id") or "").strip()
                        if known_rule_id:
                            mark_learned_sql_fix_used(known_rule_id)
                        current_sql = known_fixed_sql
                        auto_repair_history.append(
                            {
                                "attempt": attempt + 1,
                                "source": "learned_rule",
                                "rule_id": known_rule_id,
                                "error": error_message,
                                "risk_score": None,
                                "postprocess": [],
                            }
                        )
                        continue
                templated_sql, template_rules = apply_sql_error_templates(
                    question=question,
                    sql=current_sql,
                    error_message=error_message,
                )
                if templated_sql.strip() and templated_sql.strip() != current_sql.strip():
                    current_sql = templated_sql
                    auto_repair_history.append(
                        {
                            "attempt": attempt + 1,
                            "source": "template_repair",
                            "error": error_message,
                            "risk_score": None,
                            "postprocess": template_rules,
                        }
                    )
                    continue
                failed_sql = current_sql
                repaired_sql, repaired_payload = _repair_sql_once(
                    question=question,
                    question_en=question_en,
                    context=context,
                    planner_intent=planner_intent,
                    failed_sql=failed_sql,
                    error_message=error_message,
                )
                if repaired_sql.strip() == current_sql.strip():
                    raise
                current_sql = repaired_sql
                llm_repair_pairs.append(
                    {
                        "failed_sql": failed_sql,
                        "fixed_sql": repaired_sql,
                        "error_message": error_message,
                        "resolution_notes": repaired_payload.get("postprocess", []),
                    }
                )
                auto_repair_history.append(
                    {
                        "attempt": attempt + 1,
                        "source": "llm_repair",
                        "error": error_message,
                        "risk_score": repaired_payload.get("risk_score"),
                        "postprocess": repaired_payload.get("postprocess", []),
                    }
                )
    except HTTPException as exc:
        status = "error"
        error_detail = str(exc.detail) if exc.detail else str(exc)
        raise
    except Exception as exc:  # pragma: no cover - depends on driver
        status = "error"
        error_detail = str(exc)
        raise
    finally:
        duration_ms = int((time.perf_counter() - start) * 1000)
        try:
            append_event(settings.events_log_path, {
                "type": "audit",
                "event": "query_run",
                "qid": req.qid,
                "question": question,
                "sql": sql,
                "status": status,
                "rows_returned": rows_returned,
                "row_cap": row_cap,
                "duration_ms": duration_ms,
                "auto_repair_attempts": len(auto_repair_history),
                "learned_rule_ids": persisted_rule_ids,
                "user": {"name": user_name, "role": user_role},
                "error": error_detail,
                "applied_terms": [],
                "applied_metrics": [],
            })
        except Exception:
            pass


def _load_json(path: Path):
    if not path.exists():
        return None
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        return None


def _load_questions_jsonl(path: Path) -> list[str]:
    if not path.exists():
        return []
    questions: list[str] = []
    for line in path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line:
            continue
        try:
            item = json.loads(line)
            if isinstance(item, dict) and "question" in item:
                questions.append(str(item["question"]))
            elif isinstance(item, str):
                questions.append(item)
        except json.JSONDecodeError:
            questions.append(line)
    return questions


@router.get("/demo/questions")
def demo_questions():
    settings = get_settings()
    cache = _load_json(Path(settings.demo_cache_path))
    if isinstance(cache, dict) and cache:
        aliases = cache.get("_aliases")
        if isinstance(aliases, dict) and aliases:
            return {"questions": list(aliases.keys())}
        return {"questions": [key for key in cache.keys() if key != "_aliases"]}

    questions = _load_questions_jsonl(Path("var/metadata/demo_questions.jsonl"))
    return {"questions": questions}
