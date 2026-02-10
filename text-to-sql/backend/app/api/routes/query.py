from __future__ import annotations

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel
import uuid
from pathlib import Path
import json
import time

from app.services.agents.orchestrator import run_oneshot
from app.services.budget_gate import ensure_budget_ok
from app.services.cost_tracker import get_cost_tracker
from app.services.logging_store.store import append_event
from app.services.oracle.executor import execute_sql
from app.services.policy.gate import precheck_sql
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


@router.post("/oneshot")
def oneshot(req: OneShotRequest):
    ensure_budget_ok()
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
            enable_clarification=True,
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
    if isinstance(stored, dict):
        question = stored.get("question") or stored.get("question_en")

    user_name = req.user_name or "사용자"
    user_role = req.user_role or "연구원"

    status = "success"
    rows_returned = 0
    row_cap = None
    error_detail = None
    start = time.perf_counter()

    try:
        policy_result = precheck_sql(sql, question)
        result = execute_sql(sql)
        rows_returned = int(result.get("row_count") or 0)
        row_cap = int(result.get("row_cap") or 0)
        if row_cap and rows_returned >= row_cap:
            status = "warning"
        if settings.sql_run_cost_krw > 0:
            get_cost_tracker().add_cost(settings.sql_run_cost_krw, {"stage": "run"})
        return {"sql": sql, "result": result, "policy": policy_result}
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
