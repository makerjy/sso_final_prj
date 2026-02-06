from __future__ import annotations

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel
import uuid
from pathlib import Path
import json

from app.services.agents.orchestrator import run_oneshot
from app.services.budget_gate import ensure_budget_ok
from app.services.cost_tracker import get_cost_tracker
from app.services.oracle.executor import execute_sql
from app.services.policy.gate import precheck_sql
from app.core.config import get_settings

router = APIRouter()

_QUERY_STORE: dict[str, dict] = {}


class OneShotRequest(BaseModel):
    question: str
    translate: bool | None = None
    rag_multi: bool | None = None


class RunRequest(BaseModel):
    qid: str | None = None
    sql: str | None = None
    user_ack: bool = False


@router.post("/oneshot")
def oneshot(req: OneShotRequest):
    ensure_budget_ok()
    payload = run_oneshot(req.question, translate=req.translate, rag_multi=req.rag_multi)
    qid = str(uuid.uuid4())
    _QUERY_STORE[qid] = payload
    return {"qid": qid, "payload": payload}


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
    if not sql and req.qid:
        stored = _QUERY_STORE.get(req.qid)
        if stored and "final" in stored:
            sql = stored["final"].get("final_sql")

    if not sql:
        raise HTTPException(status_code=400, detail="SQL not provided")

    precheck_sql(sql)
    result = execute_sql(sql)
    if settings.sql_run_cost_krw > 0:
        get_cost_tracker().add_cost(settings.sql_run_cost_krw, {"stage": "run"})
    return {"sql": sql, "result": result}


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
