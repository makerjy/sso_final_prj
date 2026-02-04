"""쿼리 시각화 에이전트 

역할:
- 질문 + df -> 시각화 결과 생성 흐름을 한 곳에서 관리
"""
from __future__ import annotations

from typing import Any, Dict, List

import pandas as pd

from src.agent import chart_rule_engine, code_generator, intent_extractor, retrieval
from src.db.schema_introspect import summarize_dataframe_schema
from src.models.chart_spec import AnalysisCard, ChartSpec, VisualizationResponse
from src.utils.logging import log_event

# 입력: df
# 출력: 데이터프레임 스키마 요약 딕셔너리
# 데이터프레임 스키마를 간단히 요약
def summarize_schema(df: pd.DataFrame) -> Dict[str, Any]:
    """데이터프레임 스키마를 간단히 요약."""
    return summarize_dataframe_schema(df)

# 입력: df
# 출력: df
# 경과시간 파생 컬럼을 조건부로 생성
def _add_elapsed_columns(df: pd.DataFrame) -> pd.DataFrame:
    # 대소문자/스네이크케이스/카멜케이스 대응
    cols = {c.lower(): c for c in df.columns}

    def _col(*names: str) -> str | None:
        for n in names:
            if n in cols:
                return cols[n]
        return None

    chart_col = _col("charttime", "chart_time", "charttimestamp")
    intime_col = _col("intime", "in_time", "icu_intime")
    admittime_col = _col("admittime", "admit_time")

    # ICU 경과시간: charttime - intime
    if chart_col and intime_col:
        try:
            ct = pd.to_datetime(df[chart_col], errors="coerce")
            it = pd.to_datetime(df[intime_col], errors="coerce")
            df["elapsed_icu_days"] = (ct - it).dt.total_seconds() / 86400.0
        except Exception:
            pass

    # 입원 경과시간: charttime - admittime
    if chart_col and admittime_col:
        try:
            ct = pd.to_datetime(df[chart_col], errors="coerce")
            at = pd.to_datetime(df[admittime_col], errors="coerce")
            df["elapsed_admit_days"] = (ct - at).dt.total_seconds() / 86400.0
        except Exception:
            pass

    return df

# 입력: user_query, sql, df
# 출력: VisualizationResponse
# 질문과 데이터프레임을 받아 시각화 추천 결과를 생성
# 흐름 - 스키마 요약 -> 의도 추출 -> 분석 플랜 생성 -> 차트 코드 생성/실행 -> 결과 묶기
def analyze_and_visualize(user_query: str, sql: str, df: pd.DataFrame) -> VisualizationResponse:
    """질문과 데이터프레임을 받아 시각화 추천 결과를 생성."""
    log_event("analysis.start", {"user_query": user_query, "sql": sql})

    # 0) 경과시간 파생 컬럼 추가 (가능한 경우에만)
    df = _add_elapsed_columns(df)

    # 1) 스키마 요약
    df_schema = summarize_schema(df)
    log_event("analysis.schema", {"columns": df_schema.get("columns", [])})

    # 2) RAG 컨텍스트 검색
    rag = retrieval.retrieve_context(user_query, df_schema)
    rag_context = rag.get("context_text", "")
    log_event("analysis.rag", {"context_size": len(rag_context)})

    # 3) 의도 추출
    intent_info = intent_extractor.extract_intent(user_query, df_schema, rag_context)
    log_event("analysis.intent", intent_info)

    # 4) 분석 플랜 생성 (여러 개)
    plans = chart_rule_engine.plan_analyses(intent_info, df, rag_context)
    log_event("analysis.plans", {"count": len(plans)})

    analyses: List[AnalysisCard] = []
    for plan in plans:
        chart_spec_dict = plan.get("chart_spec")
        reason = plan.get("reason")

        # chart_spec을 모델로 변환
        chart_spec = ChartSpec(**chart_spec_dict) if chart_spec_dict else None

        # 4) 차트 코드 생성/실행
        try:
            chart_result = code_generator.generate_chart(
                chart_spec_dict or {},
                df,
            )
            log_event("analysis.chart.success", {"chart_type": chart_spec_dict.get("chart_type")})
        except Exception as exc:
            log_event("analysis.chart.error", {"error": str(exc)})
            chart_result = {"figure_json": None, "code": None}

        analyses.append(
            AnalysisCard(
                chart_spec=chart_spec or ChartSpec(chart_type="unknown"),
                reason=reason,
                figure_json=chart_result.get("figure_json"),
                code=chart_result.get("code"),
            )
        )

    # 5) 결과 묶기
    return VisualizationResponse(
        sql=sql,
        table_preview=df.head(20).to_dict(orient="records"),
        analyses=analyses,
    )
