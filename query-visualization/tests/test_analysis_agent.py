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

    assert "line" in chart_types or "bar" in chart_types
