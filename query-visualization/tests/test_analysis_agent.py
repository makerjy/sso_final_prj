from __future__ import annotations

import pandas as pd

from src.agent.analysis_agent import analyze_and_visualize


def _load_df() -> pd.DataFrame:
    return pd.read_csv("tests/fixtures/sample.csv")


def test_analysis_agent_trend() -> None:
    df = _load_df()
    user_query = "icu_admit_month 별 mortality_rate 추세 보여줘"
    sql = "SELECT icu_admit_month, mortality_rate, age, gender FROM sample"

    result = analyze_and_visualize(user_query, sql, df)

    assert result.analyses
    chart_types = {a.chart_spec.chart_type for a in result.analyses}

    assert chart_types.intersection({"line", "bar", "box", "hist"})


def test_analysis_agent_empty_rows_uses_fallback_insight() -> None:
    df = pd.DataFrame(columns=["age", "gender"])
    result = analyze_and_visualize("빈 결과도 처리해줘", "SELECT age, gender FROM sample WHERE 1=0", df)

    assert result.insight
    assert result.total_latency_ms is not None
    assert isinstance(result.stage_latency_ms, dict)
