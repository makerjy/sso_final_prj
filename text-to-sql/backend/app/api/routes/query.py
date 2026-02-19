from __future__ import annotations

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel
from typing import Any
import uuid
from pathlib import Path
import json
import time
import re

from app.services.agents.orchestrator import run_oneshot
from app.services.agents.intent_guard import enforce_intent_alignment
from app.services.agents.sql_error_parser import parse_sql_error
from app.services.agents.sql_expert import repair_sql_after_error
from app.services.agents.sql_error_templates import apply_sql_error_templates
from app.services.agents.sql_postprocess import postprocess_sql, recommend_postprocess_profile
from app.services.budget_gate import ensure_budget_ok
from app.services.cost_tracker import get_cost_tracker
from app.services.logging_store.store import append_event, read_events
from app.services.oracle.executor import execute_sql
from app.services.policy.gate import precheck_sql
from app.services.runtime.sql_error_repair_store import (
    find_learned_sql_fix,
    mark_learned_sql_fix_used,
    upsert_learned_sql_fix,
)
from app.services.runtime.request_context import reset_request_user, set_request_user
from app.services.runtime.user_scope import normalize_user_id
from app.core.config import get_settings
from app.core.paths import project_path

router = APIRouter()

_QUERY_STORE: dict[str, dict] = {}
_ZERO_RESULT_HINTS = (
    "ratio",
    "rate",
    "trend",
    "distribution",
    "top",
    "비율",
    "비중",
    "분포",
    "추이",
    "상위",
    "비교",
    "대비",
    "차이",
)
_ENABLE_RUNTIME_REWRITE_ON_RETRY = False
_TEMPLATE_REPAIR_ERROR_CODES = {
    "ORA-00904",
    "ORA-00905",
    "ORA-00933",
    "ORA-00942",
    "ORA-00979",
    "ORA-01722",
}
_TEMPLATE_REPAIR_ERROR_MARKERS = (
    "ORA-00904",
    "ORA-00905",
    "ORA-00933",
    "ORA-00942",
    "ORA-00979",
    "ORA-01722",
)


class OneShotRequest(BaseModel):
    question: str
    translate: bool | None = None
    rag_multi: bool | None = None
    conversation: list[dict] | None = None
    user_id: str | None = None
    user_name: str | None = None
    user_role: str | None = None


class RunRequest(BaseModel):
    qid: str | None = None
    sql: str | None = None
    question: str | None = None
    user_ack: bool = False
    user_id: str | None = None
    user_name: str | None = None
    user_role: str | None = None


def _resolve_user_id(user_id: str | None, user_name: str | None) -> str:
    normalized = normalize_user_id(user_id)
    if normalized:
        return normalized
    return normalize_user_id(user_name)


def _add_llm_cost(usage: dict[str, Any], stage: str) -> None:
    settings = get_settings()
    total_tokens = int(usage.get("total_tokens") or 0)
    if settings.llm_cost_per_1k_tokens_krw > 0 and total_tokens > 0:
        cost = int((total_tokens / 1000) * settings.llm_cost_per_1k_tokens_krw)
        if cost <= 0:
            cost = 1
        get_cost_tracker().add_cost(cost, {"usage": usage, "stage": stage, "source": "llm"})


def _is_template_repair_candidate(
    *,
    structured_error: dict[str, Any] | None,
    error_message: str,
) -> bool:
    if isinstance(structured_error, dict):
        code = str(structured_error.get("error_code") or "").strip().upper()
        if code in _TEMPLATE_REPAIR_ERROR_CODES:
            return True
    upper = str(error_message or "").upper()
    return any(marker in upper for marker in _TEMPLATE_REPAIR_ERROR_MARKERS)


def _repair_sql_once(
    *,
    question: str,
    question_en: str | None,
    context: dict[str, Any],
    planner_intent: dict[str, Any] | None,
    failed_sql: str,
    error_message: str,
    structured_error: dict[str, Any] | None = None,
) -> tuple[str, dict[str, Any]]:
    repaired = repair_sql_after_error(
        question,
        context,
        failed_sql,
        error_message,
        question_en=question_en,
        planner_intent=planner_intent,
        structured_error=structured_error,
    )
    _add_llm_cost(repaired.get("usage", {}), "run_repair")
    repaired_sql = str(repaired.get("final_sql") or "").strip()
    if not repaired_sql:
        raise HTTPException(status_code=400, detail="Auto-repair returned empty SQL")
    profile, profile_reasons = recommend_postprocess_profile(
        question,
        repaired_sql,
        default_profile="auto",
    )
    repaired_sql, rules = postprocess_sql(question, repaired_sql, profile=profile)
    if rules:
        repaired["postprocess"] = rules
    if profile_reasons:
        repaired["postprocess_profile"] = profile
        repaired["postprocess_profile_reasons"] = profile_reasons
    return repaired_sql, repaired


def _should_attempt_zero_result_repair(question: str, sql: str) -> bool:
    text = str(sql or "")
    if not text:
        return False
    q = str(question or "").lower().strip()
    if not q:
        return False
    has_where = bool(re.search(r"\bwhere\b", text, re.IGNORECASE))
    has_agg_shape = bool(
        re.search(r"\bgroup\s+by\b", text, re.IGNORECASE)
        or re.search(r"\b(count|avg|sum|min|max|stddev|median|ntile)\s*\(", text, re.IGNORECASE)
    )
    has_intent = any(token in q for token in _ZERO_RESULT_HINTS)
    if not has_intent:
        return False
    if not (has_where and has_agg_shape):
        return False
    if re.search(r"\b(subject_id|hadm_id|stay_id|icd_code|itemid)\b", text, re.IGNORECASE):
        # Identifier-level filters often legitimately return zero rows; avoid broadening them.
        return False
    analytic_intent = bool(
        re.search(
            r"(비교|대비|차이|분포|추이|상위|비율|비중|vs|versus|trend|distribution|top|rate|ratio)",
            q,
            re.IGNORECASE,
        )
    )
    return analytic_intent


@router.post("/oneshot")
def oneshot(req: OneShotRequest):
    ensure_budget_ok()
    settings = get_settings()
    user_id = _resolve_user_id(req.user_id, req.user_name)
    user_token = set_request_user(user_id)
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
                    "id": user_id or None,
                    "name": req.user_name or "사용자",
                    "role": req.user_role or "연구원",
                },
                "error": error_detail,
                "applied_terms": [],
                "applied_metrics": [],
            })
        except Exception:
            pass
        finally:
            reset_request_user(user_token)


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
        req_question = str(req.question or "").strip()
        if req_question:
            question = req_question
    if not question:
        question = ""
    has_original_question = bool(str(question).strip())
    allow_template_repair = has_original_question
    allow_llm_repair = has_original_question

    user_name = req.user_name or "사용자"
    user_role = req.user_role or "연구원"
    user_id = _resolve_user_id(req.user_id, req.user_name)
    user_token = set_request_user(user_id)

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
        max_zero_result_attempts = (
            settings.sql_zero_result_repair_max_attempts
            if settings.sql_zero_result_repair_enabled and has_original_question
            else 0
        )
        repair_round = 0
        zero_result_round = 0
        seen_sql_signatures: set[str] = {current_sql.strip().rstrip(";")}
        while True:
            should_apply_runtime_rewrite = (
                _ENABLE_RUNTIME_REWRITE_ON_RETRY
                and has_original_question
                and (repair_round > 0 or zero_result_round > 0)
            )
            if should_apply_runtime_rewrite:
                postprocess_profile, postprocess_reasons = recommend_postprocess_profile(
                    question,
                    current_sql,
                    default_profile="relaxed",
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
                            "attempt": repair_round + 1,
                            "source": "postprocess",
                            "profile": postprocess_profile,
                            "profile_reasons": postprocess_reasons,
                            "error": "",
                            "risk_score": None,
                            "postprocess": pre_rules,
                        }
                    )
            should_apply_guard = should_apply_runtime_rewrite
            if should_apply_guard:
                guarded_sql, guard_rules, guard_issues = enforce_intent_alignment(
                    question,
                    current_sql,
                    planner_intent=planner_intent,
                )
                guard_applied = False
                if guarded_sql.strip() != current_sql.strip():
                    current_sql = guarded_sql
                    guard_applied = True
                if guard_rules or guard_issues or guard_applied:
                    auto_repair_history.append(
                        {
                            "attempt": repair_round + 1,
                            "source": "intent_guard",
                            "error": "",
                            "risk_score": None,
                            "postprocess": guard_rules,
                            "issues": guard_issues,
                        }
                    )
            sql = current_sql
            try:
                policy_result = precheck_sql(current_sql, question)
                result = execute_sql(current_sql)
                rows_returned = int(result.get("row_count") or 0)
                row_cap = int(result.get("row_cap") or 0)
                if (
                    rows_returned == 0
                    and zero_result_round < max_zero_result_attempts
                    and _should_attempt_zero_result_repair(question, current_sql)
                ):
                    zero_profile, zero_profile_reasons = recommend_postprocess_profile(
                        question,
                        current_sql,
                        default_profile="relaxed",
                    )
                    zero_sql, zero_rules = postprocess_sql(
                        question,
                        current_sql,
                        profile=zero_profile,
                    )
                    zero_sql = str(zero_sql or "").strip()
                    if zero_sql and zero_sql.rstrip(";") != current_sql.rstrip(";"):
                        signature = zero_sql.rstrip(";")
                        if signature not in seen_sql_signatures:
                            current_sql = zero_sql
                            seen_sql_signatures.add(signature)
                            zero_result_round += 1
                            auto_repair_history.append(
                                {
                                    "attempt": repair_round + 1,
                                    "source": "zero_result_postprocess",
                                    "profile": zero_profile,
                                    "profile_reasons": zero_profile_reasons,
                                    "error": "NO_ROWS_RETURNED",
                                    "risk_score": None,
                                    "postprocess": zero_rules,
                                }
                            )
                            continue
                    if allow_llm_repair and repair_round < max_repair_attempts:
                        try:
                            failed_sql = current_sql
                            repaired_sql, repaired_payload = _repair_sql_once(
                                question=question,
                                question_en=question_en,
                                context=context,
                                planner_intent=planner_intent,
                                failed_sql=failed_sql,
                                error_message="NO_ROWS_RETURNED: query executed successfully but returned 0 rows.",
                                structured_error={
                                    "error_code": "NO_ROWS_RETURNED",
                                    "error_message": "Query executed successfully but returned 0 rows.",
                                    "hint": "Broaden restrictive predicates while preserving original intent.",
                                },
                            )
                            repaired_sql = str(repaired_sql or "").strip()
                            if repaired_sql and repaired_sql.rstrip(";") != current_sql.rstrip(";"):
                                signature = repaired_sql.rstrip(";")
                                if signature not in seen_sql_signatures:
                                    current_sql = repaired_sql
                                    seen_sql_signatures.add(signature)
                                    zero_result_round += 1
                                    llm_repair_pairs.append(
                                        {
                                            "failed_sql": failed_sql,
                                            "fixed_sql": repaired_sql,
                                            "error_message": "NO_ROWS_RETURNED",
                                            "resolution_notes": repaired_payload.get("postprocess", []),
                                        }
                                    )
                                    auto_repair_history.append(
                                        {
                                            "attempt": repair_round + 1,
                                            "source": "zero_result_llm_repair",
                                            "error": "NO_ROWS_RETURNED",
                                            "risk_score": repaired_payload.get("risk_score"),
                                            "postprocess": repaired_payload.get("postprocess", []),
                                        }
                                    )
                                    continue
                        except Exception:
                            pass
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
            except Exception as exc:  # pragma: no cover - depends on driver/SDK
                error_message = (
                    str(exc.detail) if isinstance(exc, HTTPException) and exc.detail else str(exc)
                )
                structured_error = parse_sql_error(error_message, sql=current_sql)
                if repair_round >= max_repair_attempts:
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
                                "attempt": repair_round + 1,
                                "source": "learned_rule",
                                "rule_id": known_rule_id,
                                "error": error_message,
                                "risk_score": None,
                                "postprocess": [],
                            }
                        )
                        repair_round += 1
                        continue
                if allow_template_repair and _is_template_repair_candidate(
                    structured_error=structured_error,
                    error_message=error_message,
                ):
                    templated_sql, template_rules = apply_sql_error_templates(
                        question=question,
                        sql=current_sql,
                        error_message=error_message,
                    )
                    if templated_sql.strip() and templated_sql.strip() != current_sql.strip():
                        current_sql = templated_sql
                        auto_repair_history.append(
                            {
                                "attempt": repair_round + 1,
                                "source": "template_repair",
                                "error": error_message,
                                "risk_score": None,
                                "postprocess": template_rules,
                            }
                        )
                        repair_round += 1
                        continue
                if not allow_llm_repair:
                    raise
                failed_sql = current_sql
                repaired_sql, repaired_payload = _repair_sql_once(
                    question=question,
                    question_en=question_en,
                    context=context,
                    planner_intent=planner_intent,
                    failed_sql=failed_sql,
                    error_message=error_message,
                    structured_error=structured_error,
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
                        "attempt": repair_round + 1,
                        "source": "llm_repair",
                        "error": error_message,
                        "error_detail": structured_error,
                        "risk_score": repaired_payload.get("risk_score"),
                        "postprocess": repaired_payload.get("postprocess", []),
                    }
                )
                repair_round += 1
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
                "user": {"id": user_id or None, "name": user_name, "role": user_role},
                "error": error_detail,
                "applied_terms": [],
                "applied_metrics": [],
            })
        except Exception:
            pass
        finally:
            reset_request_user(user_token)


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


def _normalize_demo_question(text: str) -> str:
    normalized = re.sub(r"\s+", " ", str(text or "")).strip()
    if normalized.startswith(('"', "'")) and normalized.endswith(('"', "'")):
        normalized = normalized[1:-1].strip()
    return normalized


def _is_valid_demo_question(text: str) -> bool:
    q = _normalize_demo_question(text)
    if not q:
        return False
    if len(q) < 6 or len(q) > 180:
        return False
    upper = q.upper()
    if upper in {"직접 SQL 실행", "DIRECT SQL"}:
        return False
    lower = q.casefold()
    if re.search(r"\b(debug|retry|fix|tmp|temp|smoke)\b", lower):
        return False
    if re.search(r"(디버그|재시도|임시|스모크)", q):
        return False
    if re.search(r"\b(SELECT|INSERT|UPDATE|DELETE|MERGE|CREATE|DROP|ALTER)\b", upper):
        return False
    return True


def _questions_from_audit_logs(limit: int = 3000, user_id: str | None = None) -> list[str]:
    settings = get_settings()
    events = read_events(settings.events_log_path, limit=limit)
    if not events:
        return []

    requested_user = normalize_user_id(user_id)
    stats_by_question: dict[str, dict[str, Any]] = {}
    seen_per_qid: set[str] = set()
    now_ts = int(time.time())

    for event in events:
        if not isinstance(event, dict) or event.get("type") != "audit":
            continue
        if requested_user:
            user = event.get("user") if isinstance(event.get("user"), dict) else {}
            event_user_id = normalize_user_id(str(user.get("id") or ""))
            if event_user_id != requested_user:
                continue
        raw_question = str(event.get("question") or "").strip()
        question = _normalize_demo_question(raw_question)
        if not _is_valid_demo_question(question):
            continue

        key = question.casefold()
        qid = str(event.get("qid") or "").strip()
        if qid:
            pair_key = f"{qid}:{key}"
            if pair_key in seen_per_qid:
                continue
            seen_per_qid.add(pair_key)

        status = str(event.get("status") or "").strip().lower()
        ts = 0
        try:
            ts = int(event.get("ts") or 0)
        except Exception:
            ts = 0

        item = stats_by_question.get(key)
        if item is None:
            item = {
                "question": question,
                "count": 0,
                "success": 0,
                "latest_ts": 0,
            }
            stats_by_question[key] = item

        item["count"] += 1
        if status == "success":
            item["success"] += 1
        if ts >= int(item["latest_ts"]):
            item["latest_ts"] = ts
            item["question"] = question

    if not stats_by_question:
        return []

    ranked: list[tuple[float, dict[str, Any]]] = []
    for item in stats_by_question.values():
        count = int(item.get("count") or 0)
        success = int(item.get("success") or 0)
        latest_ts = int(item.get("latest_ts") or 0)
        success_ratio = (success / count) if count > 0 else 0.0
        age_days = ((now_ts - latest_ts) / 86400) if latest_ts else 9999
        recency_bonus = 3 if age_days <= 7 else 2 if age_days <= 30 else 1 if age_days <= 90 else 0
        score = (success * 3.0) + (count * 1.0) + recency_bonus + success_ratio
        ranked.append((score, item))

    ranked.sort(
        key=lambda pair: (
            pair[0],
            int(pair[1].get("success") or 0),
            int(pair[1].get("count") or 0),
            int(pair[1].get("latest_ts") or 0),
        ),
        reverse=True,
    )

    questions: list[str] = []
    korean_questions: list[str] = []
    for _, item in ranked:
        question = str(item.get("question") or "").strip()
        if not question:
            continue
        if question in questions:
            continue
        questions.append(question)
        if re.search(r"[가-힣]", question):
            korean_questions.append(question)
        if len(questions) >= 12:
            break
    if len(korean_questions) >= 3:
        return korean_questions[:12]
    return questions


@router.get("/demo/questions")
def demo_questions(user: str | None = None):
    audit_questions = _questions_from_audit_logs(limit=3000, user_id=user)
    if audit_questions:
        return {"questions": audit_questions, "source": "audit"}

    settings = get_settings()
    cache = _load_json(Path(settings.demo_cache_path))
    if isinstance(cache, dict) and cache:
        aliases = cache.get("_aliases")
        if isinstance(aliases, dict) and aliases:
            return {"questions": list(aliases.keys()), "source": "cache_aliases"}
        return {"questions": [key for key in cache.keys() if key != "_aliases"], "source": "cache_keys"}

    questions = _load_questions_jsonl(project_path("var/metadata/demo_questions.jsonl"))
    return {"questions": questions, "source": "file"}
