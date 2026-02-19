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
from app.services.agents.llm_client import LLMClient
from app.services.agents.json_utils import extract_json_object
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
from app.services.runtime.settings_store import load_table_scope
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
_IN_SCOPE_CLINICAL_TOKEN_RE = re.compile(
    r"(환자|입원|퇴원|중환자|icu|진단|질환|약물|처방|검사|재입원|사망률|코호트|admission|patient|diagnos|disease|medication|prescription|lab|readmission|mortality)",
    re.IGNORECASE,
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


class QueryAnswerRequest(BaseModel):
    question: str
    sql: str = ""
    columns: list[str] = []
    rows: list[list[Any]] = []
    total_rows: int | None = None
    fetched_rows: int | None = None


def _normalize_table_name(raw: str) -> str:
    text = str(raw or "").strip()
    if not text:
        return ""
    if "." in text:
        text = text.rsplit(".", 1)[-1]
    return text.lower()


def _load_available_table_names(user_id: str | None = None) -> list[str]:
    names: list[str] = []
    try:
        selected = load_table_scope(user_id, include_global_fallback=True)
    except Exception:
        selected = []
    for item in selected:
        name = _normalize_table_name(str(item))
        if name:
            names.append(name)
    if names:
        return sorted(list(dict.fromkeys(names)))

    path = project_path("var/metadata/schema_catalog.json")
    if not path.exists():
        return []
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return []
    tables = data.get("tables", {}) if isinstance(data, dict) else {}
    if not isinstance(tables, dict):
        return []
    for table_name in tables.keys():
        name = _normalize_table_name(str(table_name))
        if name:
            names.append(name)
    return sorted(list(dict.fromkeys(names)))


def _looks_like_in_scope_clinical_question(question: str, tables: list[str]) -> bool:
    q = str(question or "").strip().lower()
    if not q:
        return False
    if _IN_SCOPE_CLINICAL_TOKEN_RE.search(q):
        return True
    # Avoid false out-of-scope blocks when user explicitly references connected tables.
    for table_name in tables[:64]:
        token = str(table_name or "").strip().lower()
        if token and token in q:
            return True
    return False


def _normalize_text_list(value: Any, *, limit: int) -> list[str]:
    if not isinstance(value, list):
        return []
    items: list[str] = []
    for item in value:
        text = str(item or "").strip()
        if not text:
            continue
        if text in items:
            continue
        items.append(text)
        if len(items) >= limit:
            break
    return items


def _with_answer_opening(text: str, *, kind: str = "default") -> str:
    body = str(text or "").strip()
    opening = "요청하신 내용을 반영해 먼저 핵심부터 정리해드릴게요."
    if kind == "scope":
        opening = "요청해주신 질문 기준으로 현재 조회 가능 범위를 먼저 안내드릴게요."
    elif kind == "result":
        opening = "요청하신 질문과 조회 결과를 바탕으로 핵심부터 정리해드릴게요."
    if not body:
        return opening
    if body.startswith(("요청하신", "요청해주신", "말씀해주신", "아래는")):
        return body
    return f"{opening}\n{body}"


def _llm_scope_guidance(question: str, user_id: str | None) -> dict[str, Any] | None:
    q = str(question or "").strip()
    if not q:
        return None
    settings = get_settings()
    tables = _load_available_table_names(user_id)
    messages = [
        {
            "role": "system",
            "content": (
                "You are a scope-gate for a clinical text-to-sql/sql-to-plot assistant. "
                "Decide whether the user's question is in scope for currently connected clinical tables. "
                "Return JSON only with keys: in_scope, reason, guidance, options, example_inputs. "
                "If out of scope (non-clinical topic or asks data not present in connected tables), "
                "set in_scope=false and provide concise Korean guidance. "
                "options must be 2-4 short alternative questions. "
                "example_inputs must be 1-3 concrete Korean examples. "
                "If in_scope=true, keep reason short and guidance/options/example_inputs empty."
            ),
        },
        {
            "role": "user",
            "content": json.dumps(
                {
                    "service_scope": "임상연구용 text-to-sql/sql-to-plot",
                    "question": q,
                    "available_tables": tables[:24],
                },
                ensure_ascii=False,
            ),
        },
    ]
    try:
        client = LLMClient()
        response = client.chat(
            messages=messages,
            model=settings.planner_model or settings.expert_model,
            max_tokens=max(180, min(420, int(getattr(settings, "llm_max_output_tokens_clarifier", settings.llm_max_output_tokens)))),
            expect_json=True,
        )
        _add_llm_cost(response.get("usage", {}), "scope_guard")
        parsed = extract_json_object(str(response.get("content") or ""))
    except Exception:
        return None

    if bool(parsed.get("in_scope")):
        return None
    # LLM scope gating can occasionally over-block in-scope clinical questions.
    # If question clearly looks clinical or references connected tables, keep pipeline running.
    if _looks_like_in_scope_clinical_question(q, tables):
        return None
    reason = str(parsed.get("reason") or "").strip()
    guidance = str(parsed.get("guidance") or "").strip()
    options = _normalize_text_list(parsed.get("options"), limit=4)
    example_inputs = _normalize_text_list(parsed.get("example_inputs"), limit=3)
    table_preview = ", ".join(tables[:8])
    if table_preview and table_preview not in reason:
        reason = (
            f"{reason} 현재 연결된 테이블 예시: {table_preview}."
            if reason
            else f"현재 연결된 테이블 예시: {table_preview}."
        )
    if not guidance:
        guidance = "현재 연결된 임상 데이터 범위에서 다시 질문해 주세요."
    if not options:
        if tables:
            options = [f"{name} 기준으로 건수 추이를 보여줘" for name in tables[:3]]
        else:
            options = ["현재 연결된 임상 데이터 기준으로 다시 질문할게요."]
    if not example_inputs:
        if tables:
            example_inputs = [f"{tables[0]} 테이블에서 최근 1년 추이를 보여줘"]
        else:
            example_inputs = ["현재 연결된 임상 데이터에서 조회 가능한 질문 예시를 알려줘"]
    return {
        "mode": "clarify",
        "question": question,
        "clarification": {
            "reason": reason,
            "question": guidance,
            "options": options,
            "example_inputs": example_inputs,
        },
    }


def _fallback_oneshot_assistant_message(question: str, payload: dict[str, Any]) -> str:
    mode = str(payload.get("mode") or "").strip().lower()
    if mode == "clarify":
        clarification = payload.get("clarification") if isinstance(payload.get("clarification"), dict) else {}
        prompt = str(clarification.get("question") or "").strip() or "질문 범위를 조금 더 좁혀주세요."
        reason = str(clarification.get("reason") or "").strip()
        lines = [prompt]
        if reason:
            lines.append(f"이유: {reason}")
        options = _normalize_text_list(clarification.get("options"), limit=4)
        if options:
            lines.append(f"선택 예시: {', '.join(options)}")
        examples = _normalize_text_list(clarification.get("example_inputs"), limit=2)
        if examples:
            lines.append(f"입력 예: {' / '.join(examples)}")
        return "\n".join(lines)
    if mode == "demo":
        result = payload.get("result") if isinstance(payload.get("result"), dict) else {}
        parts: list[str] = []
        summary_text = str(result.get("summary") or "").strip()
        if summary_text:
            parts.append(summary_text if summary_text.endswith(".") else f"{summary_text}.")
        else:
            parts.append("데모 캐시 결과를 가져왔어요.")
        preview = result.get("preview") if isinstance(result.get("preview"), dict) else {}
        row_count = preview.get("row_count")
        if isinstance(row_count, int):
            parts.append(f"미리보기로 {row_count}행을 보여드렸어요.")
        source = str(result.get("source") or "").strip()
        if source:
            parts.append(f"데모 캐시(source: {source}) 기반입니다.")
        return " ".join(parts).strip()
    base = "요청하신 내용을 바탕으로 SQL을 준비했어요. 실행하면 결과를 가져올게요."
    final = payload.get("final") if isinstance(payload.get("final"), dict) else {}
    risk = payload.get("risk") if isinstance(payload.get("risk"), dict) else {}
    local_risk_score = final.get("risk_score")
    if local_risk_score is None:
        local_risk_score = risk.get("risk")
    local_risk_intent = str(risk.get("intent") or "read").strip()
    risk_label = (
        f"위험도 {local_risk_score}({local_risk_intent})로 평가되었어요."
        if local_risk_score is not None
        else ""
    )
    return " ".join([base, risk_label]).strip()


def _llm_oneshot_assistant_message(question: str, payload: dict[str, Any]) -> str | None:
    mode = str(payload.get("mode") or "").strip().lower()
    context: dict[str, Any] = {
        "mode": mode,
        "question": str(question or "").strip(),
    }
    if mode == "clarify":
        clarification = payload.get("clarification") if isinstance(payload.get("clarification"), dict) else {}
        context["clarification"] = {
            "reason": str(clarification.get("reason") or "").strip(),
            "question": str(clarification.get("question") or "").strip(),
            "options": _normalize_text_list(clarification.get("options"), limit=4),
            "example_inputs": _normalize_text_list(clarification.get("example_inputs"), limit=3),
        }
    elif mode == "demo":
        result = payload.get("result") if isinstance(payload.get("result"), dict) else {}
        preview = result.get("preview") if isinstance(result.get("preview"), dict) else {}
        context["result"] = {
            "summary": str(result.get("summary") or "").strip(),
            "source": str(result.get("source") or "").strip(),
            "row_count": preview.get("row_count"),
            "columns": (preview.get("columns") or [])[:10] if isinstance(preview.get("columns"), list) else [],
        }
    else:
        final = payload.get("final") if isinstance(payload.get("final"), dict) else {}
        risk = payload.get("risk") if isinstance(payload.get("risk"), dict) else {}
        context["sql"] = str(final.get("final_sql") or payload.get("sql") or "").strip()[:1200]
        context["risk_score"] = final.get("risk_score") if final.get("risk_score") is not None else risk.get("risk")
        context["risk_intent"] = str(risk.get("intent") or "read").strip()
        used_tables = final.get("used_tables")
        context["used_tables"] = used_tables[:8] if isinstance(used_tables, list) else []

    messages = [
        {
            "role": "system",
            "content": (
                "너는 임상연구용 text-to-sql 서비스의 채팅 응답 작성기다. "
                "JSON으로 들어온 mode/question/context를 읽고 사용자에게 보여줄 최종 문장만 작성하라. "
                "질문 언어를 따라라. "
                "mode=clarify면 현재 서비스 범위를 짧게 설명하고, 왜 조회가 어려운지, 어떻게 다시 질문하면 좋은지 안내하라. "
                "mode=advanced/demo면 현재 단계(예: SQL 생성 완료, 실행 필요)를 자연스럽게 알려라. "
                "과장/추측 금지. "
                "출력은 평문 2~4문장만 작성하고 Markdown/번호/불릿/JSON은 금지한다."
            ),
        },
        {"role": "user", "content": json.dumps(context, ensure_ascii=False)},
    ]
    try:
        settings = get_settings()
        client = LLMClient()
        response = client.chat(
            messages=messages,
            model=settings.planner_model or settings.expert_model,
            max_tokens=max(
                140,
                min(320, int(getattr(settings, "llm_max_output_tokens_clarifier", settings.llm_max_output_tokens))),
            ),
            expect_json=False,
        )
        _add_llm_cost(response.get("usage", {}), "oneshot_message")
        text = re.sub(r"\n{3,}", "\n\n", str(response.get("content") or "").strip())
        return text or None
    except Exception:
        return None


def _attach_oneshot_assistant_message(question: str, payload: dict[str, Any]) -> dict[str, Any]:
    if not isinstance(payload, dict):
        return payload
    text = _llm_oneshot_assistant_message(question, payload)
    if not text:
        text = _fallback_oneshot_assistant_message(question, payload)
    mode = str(payload.get("mode") or "").strip().lower()
    payload["assistant_message"] = _with_answer_opening(
        text,
        kind="scope" if mode == "clarify" else "default",
    )
    return payload


def _resolve_user_id(user_id: str | None, user_name: str | None) -> str:
    normalized = normalize_user_id(user_id)
    if normalized:
        return normalized
    return normalize_user_id(user_name)


def _fallback_query_answer(*, total_rows: int | None, fetched_rows: int) -> str:
    if total_rows is not None:
        return f"쿼리를 실행했고 전체 결과는 {total_rows}행입니다. 필요한 조건을 추가해 범위를 좁혀 보세요."
    return f"쿼리를 실행했고 미리보기 {fetched_rows}행을 확인했습니다. 필요한 경우 COUNT(*)로 전체 건수를 확인하세요."


_RESULT_OPENING_CLAUSE = "요청하신 질문 기준으로 핵심만 먼저 말씀드리면,"
_RESULT_CLOSING_MENT = "더 궁금한 점이 있다면 추가 질문을 남겨 주세요."
_OPENING_NOISE_PREFIXES = (
    "요청하신 질문과 조회 결과를 바탕으로 핵심부터 정리해드릴게요.",
    "요청하신 내용을 반영해 먼저 핵심부터 정리해드릴게요.",
    "요청하신 내용을 바탕으로 핵심부터 정리해드릴게요.",
    "요청해주신 질문 기준으로 현재 조회 가능 범위를 먼저 안내드릴게요.",
    "요청하신 질문 기준으로 핵심만 먼저 말씀드릴게요.",
    "요청하신 질문 기준으로 핵심만 먼저 말씀드리면,",
)


def _extract_core_summary_line(text: str) -> str:
    body = re.sub(r"\s+", " ", str(text or "").strip())
    if not body:
        return ""

    for prefix in _OPENING_NOISE_PREFIXES:
        if body.startswith(prefix):
            body = body[len(prefix):].strip()
            break

    body = re.sub(r"^\s*(요약|핵심 결과)\s*:\s*", "", body, flags=re.IGNORECASE)
    body = body.lstrip("-• ").strip()
    if not body:
        return ""

    first = re.split(r"(?<=[.!?])\s+", body, maxsplit=1)[0].strip()
    if not first:
        return ""
    if first[-1] not in ".!?":
        first = f"{first}."
    return first


def _compose_result_answer(text: str) -> str:
    summary = _extract_core_summary_line(text)
    if not summary:
        summary = "결과 요약을 생성하지 못했습니다."
    if summary.startswith(_RESULT_OPENING_CLAUSE):
        summary = summary[len(_RESULT_OPENING_CLAUSE):].strip()
    if summary.endswith(_RESULT_CLOSING_MENT):
        summary = summary[: -len(_RESULT_CLOSING_MENT)].strip()
    return f"{_RESULT_OPENING_CLAUSE} {summary} {_RESULT_CLOSING_MENT}"


def _fallback_followup_suggestions(question: str, columns: list[str]) -> list[str]:
    suggestions: list[str] = []
    q = str(question or "").lower()
    cols = [str(col or "").lower() for col in (columns or [])]

    def push(text: str) -> None:
        value = str(text or "").strip()
        if not value or value in suggestions:
            return
        suggestions.append(value)

    if "진단" in q or "diagnos" in q or any("icd" in col for col in cols):
        push("상위 10개 진단을 보여줘")
        push("진단별 환자 수를 성별로 나눠 보여줘")
        push("진단 추이를 월별로 보여줘")
    elif "icu" in q or "재원" in q or any("stay" in col for col in cols):
        push("ICU 재원일수 분포를 구간별로 보여줘")
        push("ICU 재원 상위 10명을 보여줘")
        push("연령대별 ICU 평균 재원일수를 보여줘")
    elif "입원" in q or "admission" in q:
        push("입원 건수를 월별 추이로 보여줘")
        push("입원 환자의 평균 재원기간을 보여줘")
        push("입원 유형별 환자 수를 보여줘")

    if any("date" in col or "time" in col for col in cols):
        push("기간별 추이를 보여줘")
    if any("gender" in col for col in cols):
        push("성별 분포를 보여줘")
    if any("age" in col for col in cols):
        push("연령대별 분포를 보여줘")

    if not suggestions:
        push("같은 조건에서 상위 10개를 보여줘")
        push("같은 조건을 월별 추이로 보여줘")
        push("성별로 나눠서 다시 보여줘")
    return suggestions[:3]


def _normalize_answer_preview(
    *,
    columns: list[str],
    rows: list[list[Any]],
    max_rows: int = 120,
    max_cols: int = 20,
) -> list[dict[str, Any]]:
    safe_cols = [str(col).strip() for col in columns if str(col).strip()][:max_cols]
    out: list[dict[str, Any]] = []
    for raw in rows[:max_rows]:
        if not isinstance(raw, list):
            continue
        item: dict[str, Any] = {}
        for idx, col in enumerate(safe_cols):
            value = raw[idx] if idx < len(raw) else None
            if isinstance(value, (dict, list, tuple, set)):
                item[col] = str(value)
            else:
                item[col] = value
        out.append(item)
    return out


@router.post("/answer")
def answer_query(req: QueryAnswerRequest):
    ensure_budget_ok()
    fetched_rows = int(req.fetched_rows or 0)
    total_rows = int(req.total_rows) if req.total_rows is not None else None
    fallback = _fallback_query_answer(total_rows=total_rows, fetched_rows=fetched_rows)
    fallback_suggestions = _fallback_followup_suggestions(req.question, req.columns)
    question = str(req.question or "").strip()
    if not question:
        return {
            "answer": _compose_result_answer(fallback),
            "source": "fallback",
            "suggested_questions": fallback_suggestions,
            "suggestions_source": "fallback",
        }

    preview_rows = _normalize_answer_preview(columns=req.columns, rows=req.rows)
    payload = {
        "question": question,
        "sql": str(req.sql or "").strip()[:4000],
        "total_rows": total_rows,
        "fetched_rows": fetched_rows,
        "columns": [str(col).strip() for col in req.columns[:20]],
        "preview_rows": preview_rows,
    }
    messages = [
        {
            "role": "system",
            "content": (
                "너는 임상 SQL 결과 해석 및 후속 질문 추천 도우미다. "
                "출력은 JSON만 허용되며 키는 answer, suggested_questions만 사용하라. "
                "answer는 질문 언어를 따라 핵심 한 줄 요약 1문장으로만 작성한다. "
                "한국어 answer는 반드시 '-습니다/입니다' 종결형 존댓말로 작성한다. "
                "시작 멘트/마무리 멘트/불릿/번호/줄바꿈은 answer에 넣지 마라. "
                "데이터에 없는 사실은 추측하지 말고 과장하지 마라. "
                "suggested_questions는 자연어 후속 질문 정확히 3개 배열이다. "
                "후속 질문은 현재 결과를 바탕으로 바로 분석 가능한 형태로 짧고 구체적으로 작성하라."
            ),
        },
        {"role": "user", "content": json.dumps(payload, ensure_ascii=False)},
    ]
    try:
        settings = get_settings()
        client = LLMClient()
        response = client.chat(
            messages=messages,
            model=settings.expert_model,
            max_tokens=max(180, min(420, int(getattr(settings, "llm_max_output_tokens_expert", settings.llm_max_output_tokens)))),
            expect_json=True,
        )
        _add_llm_cost(response.get("usage", {}), "answer")
        parsed = extract_json_object(str(response.get("content") or ""))
        answer = re.sub(r"\n{3,}", "\n\n", str(parsed.get("answer") or "").strip())
        suggested_questions = _normalize_text_list(parsed.get("suggested_questions"), limit=3)
        if not suggested_questions:
            suggested_questions = fallback_suggestions
        if not answer:
            return {
                "answer": _compose_result_answer(fallback),
                "source": "fallback",
                "suggested_questions": suggested_questions,
                "suggestions_source": "fallback",
            }
        return {
            "answer": _compose_result_answer(answer),
            "source": "llm",
            "suggested_questions": suggested_questions,
            "suggestions_source": "llm" if suggested_questions else "fallback",
        }
    except Exception:
        return {
            "answer": _compose_result_answer(fallback),
            "source": "fallback",
            "suggested_questions": fallback_suggestions,
            "suggestions_source": "fallback",
        }


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
        scope_guidance = _llm_scope_guidance(req.question, user_id)
        if scope_guidance:
            payload = _attach_oneshot_assistant_message(req.question, scope_guidance)
            qid = str(uuid.uuid4())
            _QUERY_STORE[qid] = payload
            return {"qid": qid, "payload": payload}
        payload = run_oneshot(
            req.question,
            translate=req.translate,
            rag_multi=req.rag_multi,
            conversation=req.conversation,
            enable_clarification=settings.clarifier_enabled,
        )
        payload = _attach_oneshot_assistant_message(req.question, payload)
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
