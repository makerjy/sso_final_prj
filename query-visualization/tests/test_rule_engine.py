from __future__ import annotations

import pandas as pd

from src.agent.chart_rule_engine import plan_analyses


def _load_df() -> pd.DataFrame:
    return pd.read_csv("tests/fixtures/sample.csv")


def test_plan_analyses_trend() -> None:
    df = _load_df()
    intent_info = {
        "analysis_intent": "trend",
        "primary_outcome": "mortality_rate",
        "time_var": "icu_admit_month",
        "group_var": "gender",
    }

    plans = plan_analyses(intent_info, df)
    chart_types = {p["chart_spec"]["chart_type"] for p in plans}

    assert "line" in chart_types
    assert "bar" in chart_types


def test_plan_analyses_distribution() -> None:
    df = _load_df()
    intent_info = {
        "analysis_intent": "distribution",
        "primary_outcome": "age",
        "time_var": None,
        "group_var": "gender",
    }

    plans = plan_analyses(intent_info, df)
    chart_types = {p["chart_spec"]["chart_type"] for p in plans}

    assert "hist" in chart_types
    assert "box" in chart_types
