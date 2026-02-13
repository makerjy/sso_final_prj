"""쿼리 시각화 에이전트 

역할:
- 질문 + df -> 시각화 결과 생성 흐름을 한 곳에서 관리
"""
from __future__ import annotations

import os
from pathlib import Path
from typing import Any, Dict, List, Optional

import pandas as pd
from dotenv import load_dotenv
from openai import OpenAI

from src.agent import chart_rule_engine, code_generator, intent_extractor, retrieval
from src.config.llm_config import OPENAI_MODEL
from src.db.schema_introspect import summarize_dataframe_schema
from src.models.chart_spec import AnalysisCard, ChartSpec, VisualizationResponse
from src.utils.logging import log_event

_DOTENV_PATH = Path(__file__).resolve().parents[2] / ".env"
load_dotenv(_DOTENV_PATH)

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

    chart_col = _col(
        "charttime",
        "chart_time",
        "charttimestamp",
        "starttime",
        "endtime",
        "storetime",
        "outtime",
        "dischtime",
        "transfertime",
        "ordertime",
    )
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


def _stats_snapshot(df: pd.DataFrame) -> Dict[str, Dict[str, float]]:
    stats: Dict[str, Dict[str, float]] = {}
    if df.empty:
        return stats
    numeric_cols = list(df.select_dtypes(include=["number"]).columns)[:8]
    for col in numeric_cols:
        series = pd.to_numeric(df[col], errors="coerce").dropna()
        if series.empty:
            continue
        stats[col] = {
            "min": float(series.min()),
            "q1": float(series.quantile(0.25)),
            "median": float(series.quantile(0.5)),
            "q3": float(series.quantile(0.75)),
            "max": float(series.max()),
            "mean": float(series.mean()),
        }
    return stats


def _fallback_insight(user_query: str, df: pd.DataFrame, analyses: List[AnalysisCard]) -> str:
    row_count = len(df)
    col_count = len(df.columns)
    chart_hint = "차트 추천이 생성되지 않았습니다."
    if analyses:
        first = analyses[0]
        if first.chart_spec and first.chart_spec.chart_type:
            chart_hint = f"주요 추천 차트는 {first.chart_spec.chart_type} 입니다."
        if first.reason:
            chart_hint = f"{chart_hint} {first.reason}"
    stats = _stats_snapshot(df)
    stats_hint = "수치형 통계 요약 대상이 부족합니다."
    if stats:
        top = sorted(
            stats.items(),
            key=lambda kv: (kv[1].get("max", 0.0) - kv[1].get("min", 0.0)),
            reverse=True,
        )[0]
        stats_hint = (
            f"통계표 기준 '{top[0]}'의 범위가 가장 큽니다 "
            f"(min {top[1].get('min'):.3f}, max {top[1].get('max'):.3f})."
        )
    return (
        f"질문 '{user_query}' 기준으로 결과 {row_count}행, {col_count}개 컬럼을 분석했습니다. "
        f"{chart_hint} {stats_hint} 쿼리 결과, 차트, 통계표를 함께 보고 해석하세요."
    )


def _record_failure(
    failure_reasons: List[str],
    reason: str,
) -> None:
    normalized = (reason or "").strip()
    if normalized and normalized not in failure_reasons:
        failure_reasons.append(normalized)


def _has_renderable_chart(analyses: List[AnalysisCard]) -> bool:
    return any(card.figure_json is not None for card in analyses)


def _build_analyses_from_plans(
    plans: List[Dict[str, Any]],
    df: pd.DataFrame,
    failure_reasons: List[str],
    pass_label: str,
) -> List[AnalysisCard]:
    analyses: List[AnalysisCard] = []
    if not plans:
        _record_failure(failure_reasons, f"{pass_label}: no_plans")
        return analyses

    for plan in plans:
        chart_spec_dict = plan.get("chart_spec") or {}
        reason = plan.get("reason")
        chart_type = chart_spec_dict.get("chart_type", "unknown")

        try:
            chart_spec = ChartSpec(**chart_spec_dict)
        except Exception as exc:
            _record_failure(
                failure_reasons,
                f"{pass_label}: invalid_chart_spec({chart_type}) - {str(exc)}",
            )
            chart_spec = ChartSpec(chart_type="unknown")

        try:
            chart_result = code_generator.generate_chart(chart_spec_dict, df)
            if chart_result.get("figure_json") is None:
                _record_failure(
                    failure_reasons,
                    f"{pass_label}: empty_figure({chart_type})",
                )
            else:
                log_event("analysis.chart.success", {"pass": pass_label, "chart_type": chart_type})
        except Exception as exc:
            _record_failure(
                failure_reasons,
                f"{pass_label}: chart_error({chart_type}) - {str(exc)}",
            )
            log_event("analysis.chart.error", {"pass": pass_label, "error": str(exc)})
            chart_result = {"figure_json": None, "code": None}

        analyses.append(
            AnalysisCard(
                chart_spec=chart_spec,
                reason=reason,
                figure_json=chart_result.get("figure_json"),
                code=chart_result.get("code"),
            )
        )

    if analyses and not _has_renderable_chart(analyses):
        _record_failure(failure_reasons, f"{pass_label}: all_figures_empty")

    return analyses


def _llm_generate_insight(
    user_query: str,
    sql: str,
    df: pd.DataFrame,
    analyses: List[AnalysisCard],
    df_schema: Dict[str, Any],
) -> str:
    api_key = os.getenv("OPENAI_API_KEY")
    if not api_key:
        raise RuntimeError("OPENAI_API_KEY가 설정되어 있지 않습니다.")

    client = OpenAI(api_key=api_key)
    analysis_briefs = [
        {
            "chart_type": a.chart_spec.chart_type if a.chart_spec else None,
            "x": a.chart_spec.x if a.chart_spec else None,
            "y": a.chart_spec.y if a.chart_spec else None,
            "reason": a.reason,
            "summary": a.summary,
        }
        for a in analyses[:3]
    ]
    stats_snapshot = _stats_snapshot(df)
    model_name = OPENAI_MODEL or "gpt-4o-mini"

    def _build_prompt(max_rows: int) -> str:
        sample_rows = df.head(max_rows).to_dict(orient="records")
        return (
            "다음 정보를 바탕으로 한국어 데이터 분석 인사이트를 작성하라.\n"
            "- 사용자 질문, SQL, 쿼리 결과 샘플, 통계요약, 차트추천 정보를 종합할 것\n"
            "- 출력은 4~6문장, 실행 가능한 인사이트 중심으로 작성\n"
            "- 단순 나열 금지, 핵심 패턴/이상치/해석/주의사항 포함\n"
            "- SQL 핵심, 결과 요약, 차트 해석, 주의사항을 한 번에 포함할 것\n\n"
            f"질문: {user_query}\n"
            f"SQL: {sql}\n"
            f"스키마 요약: {df_schema}\n"
            f"통계 요약: {stats_snapshot}\n"
            f"차트 추천: {analysis_briefs}\n"
            f"결과 샘플(최대 {max_rows}행): {sample_rows}\n"
        )

    last_error: Optional[Exception] = None
    for max_rows in (20, 8):
        try:
            response = client.responses.create(
                model=model_name,
                input=[
                    {"role": "system", "content": "너는 임상 데이터 분석 인사이트 작성 도우미다."},
                    {"role": "user", "content": _build_prompt(max_rows)},
                ],
            )
            text = (getattr(response, "output_text", None) or "").strip()
            if text:
                return text
            raise RuntimeError("LLM insight 응답이 비어 있습니다.")
        except Exception as exc:
            last_error = exc

    raise RuntimeError(f"LLM insight 생성 실패: {str(last_error) if last_error else 'unknown'}")

# 입력: user_query, sql, df
# 출력: VisualizationResponse
# 질문과 데이터프레임을 받아 시각화 추천 결과를 생성
# 흐름 - 스키마 요약 -> 의도 추출 -> 분석 플랜 생성 -> 차트 코드 생성/실행 -> 결과 묶기
def analyze_and_visualize(user_query: str, sql: str, df: pd.DataFrame) -> VisualizationResponse:
    """질문과 데이터프레임을 받아 시각화 추천 결과를 생성."""
    log_event("analysis.start", {"user_query": user_query, "sql": sql})
    failure_reasons: List[str] = []
    fallback_used = False
    fallback_stage: Optional[str] = None
    attempt_count = 1

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

    # 4) 1차: normal 플랜 생성 및 차트 생성
    plans = chart_rule_engine.plan_analyses(
        intent_info,
        df,
        rag_context,
        retry_mode="normal",
        failure_reasons=failure_reasons,
    )
    log_event("analysis.plans", {"mode": "normal", "count": len(plans)})
    analyses = _build_analyses_from_plans(plans, df, failure_reasons, "normal")

    # 5) 2차: normal 실패 시 relaxed 모드 재시도
    if not _has_renderable_chart(analyses):
        fallback_used = True
        fallback_stage = "retry_relaxed"
        attempt_count = 2
        _record_failure(failure_reasons, "normal: no_renderable_chart")

        relaxed_intent_info = dict(intent_info)
        relaxed_intent_info["group_var"] = None

        log_event("analysis.retry.start", {"mode": "relaxed"})
        relaxed_plans = chart_rule_engine.plan_analyses(
            relaxed_intent_info,
            df,
            rag_context,
            retry_mode="relaxed",
            failure_reasons=failure_reasons,
        )
        log_event("analysis.plans", {"mode": "relaxed", "count": len(relaxed_plans)})
        relaxed_analyses = _build_analyses_from_plans(
            relaxed_plans,
            df,
            failure_reasons,
            "relaxed",
        )
        if relaxed_analyses:
            analyses = relaxed_analyses

        if not _has_renderable_chart(analyses):
            _record_failure(failure_reasons, "relaxed: no_renderable_chart")
        log_event("analysis.retry.done", {"mode": "relaxed", "renderable": _has_renderable_chart(analyses)})

    try:
        insight = _llm_generate_insight(user_query, sql, df, analyses, df_schema)
    except Exception as exc:
        log_event("analysis.insight.error", {"error": str(exc)})
        _record_failure(failure_reasons, f"insight_error: {str(exc)}")
        insight = _fallback_insight(user_query, df, analyses)

    # 6) 결과 묶기
    return VisualizationResponse(
        sql=sql,
        table_preview=df.head(20).to_dict(orient="records"),
        analyses=analyses,
        insight=insight,
        fallback_used=fallback_used,
        fallback_stage=fallback_stage,
        failure_reasons=failure_reasons,
        attempt_count=attempt_count,
    )
