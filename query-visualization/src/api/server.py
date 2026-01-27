from __future__ import annotations

from typing import Any, Dict, List

import pandas as pd
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from dotenv import load_dotenv

from src.db.oracle_client import fetch_all
from src.models.chart_spec import VisualizationResponse
from src.utils.logging import log_event

# .env 자동 로딩
load_dotenv()

app = FastAPI(title="Query Visualization API")


class VisualizeRequest(BaseModel):
    # 자연어 질문
    user_query: str
    # text-to-sql 모듈이 만든 SQL
    sql: str
    # SQL 실행 결과
    rows: List[Dict[str, Any]]


@app.get("/health")
def health_check() -> dict:
    return {"status": "ok"}


@app.get("/db-test")
def db_test() -> dict:
    """DB 연결 테스트: DUAL 테이블로 간단 조회."""
    try:
        rows = fetch_all("SELECT 1 AS ok FROM dual")
        return {"ok": True, "rows": rows}
    except Exception as exc:  # pragma: no cover - 환경 의존
        log_event("db.test.error", {"error": str(exc)})
        raise HTTPException(status_code=500, detail=str(exc)) from exc


@app.post("/visualize", response_model=VisualizationResponse)
def visualize(req: VisualizeRequest) -> VisualizationResponse:
    """쿼리 결과를 받아 시각화 추천을 반환."""
    df = pd.DataFrame(req.rows)

    log_event("request.visualize", {"user_query": req.user_query, "sql": req.sql})

    try:
        from src.agent.analysis_agent import analyze_and_visualize
    except Exception as exc:  # pragma: no cover - 아직 미구현일 수 있음
        log_event("analysis.import.error", {"error": str(exc)})
        raise HTTPException(
            status_code=501,
            detail="analysis_agent가 아직 구현되지 않았습니다.",
        ) from exc

    return analyze_and_visualize(req.user_query, req.sql, df)
