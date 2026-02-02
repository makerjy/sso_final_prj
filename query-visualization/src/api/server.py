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

# 입력: user_query, sql, rows
# 출력: VisualizeRequest 모델
# 역할: 클라이언트가 보낸 질문 + SQL + SQL 실행 결과(ROW 리스트)를 담는 모델
class VisualizeRequest(BaseModel):
    # 자연어 질문
    user_query: str
    # text-to-sql 모듈이 만든 SQL
    sql: str
    # SQL 실행 결과
    rows: List[Dict[str, Any]]


# 입력 없음
# 출력: 상태 딕셔너리
# 간단한 체크 엔드포인트
@app.get("/health")
def health_check() -> dict:
    return {"status": "ok"}


# 입력 없음
# 출력: DB connection 테스트 결과
# 간단한 DB 연결 테스트 엔드포인트
@app.get("/db-test")
def db_test() -> dict:
    """DB 연결 테스트: DUAL 테이블로 간단 조회."""
    try:
        rows = fetch_all("SELECT * from sso.patients where rownum = 1")
        return {"ok": True, "rows": rows}
    except Exception as exc:  # pragma: no cover - 환경 의존
        log_event("db.test.error", {"error": str(exc)})
        raise HTTPException(status_code=500, detail=str(exc)) from exc


# 입력: VisualizeRequert
# 출력: VisualizationResponse
# 역할 : 클라이언트가 보낸 질문 + SQL + SQL 실행 결과(ROW 리스트)를 받아 시각화 추천을 반환
@app.post("/visualize", response_model=VisualizationResponse)
def visualize(req: VisualizeRequest) -> VisualizationResponse:
    """쿼리 결과를 받아 시각화 추천을 반환."""
    df = pd.DataFrame(req.rows)

    log_event("request.visualize", {
              "user_query": req.user_query, "sql": req.sql})

    try:
        from src.agent.analysis_agent import analyze_and_visualize
    except Exception as exc:  # pragma: no cover - 아직 미구현일 수 있음
        log_event("analysis.import.error", {"error": str(exc)})
        raise HTTPException(
            status_code=501,
            detail="analysis_agent가 아직 구현되지 않았습니다.",
        ) from exc

    return analyze_and_visualize(req.user_query, req.sql, df)
