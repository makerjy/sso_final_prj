"""차트 코드 생성 + 실행 유틸.

- Plotly로 인터랙티브 차트를 생성한다.
- 결과는 figure JSON 형태로 반환한다.
"""
from __future__ import annotations

from typing import Any, Dict
import json

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import plotly.io as pio

from src.utils.logging import log_event


def _build_code(chart_spec: Dict[str, Any]) -> str:
    # 시각화 코드를 문자열로 반환(로그/디버깅용)
    chart_type = chart_spec.get("chart_type")
    x = chart_spec.get("x")
    y = chart_spec.get("y")
    group = chart_spec.get("group")
    agg = chart_spec.get("agg")

    return (
        "# plotly 코드(요약)\n"
        f"# chart_type={chart_type}, x={x}, y={y}, group={group}, agg={agg}\n"
    )


def _aggregate_frame(
    df: pd.DataFrame,
    x: str,
    y: str,
    group: str | None,
    agg: str | None,
) -> pd.DataFrame:
    if not agg:
        return df

    agg_map = {
        "avg": "mean",
        "mean": "mean",
        "sum": "sum",
        "min": "min",
        "max": "max",
        "count": "count",
        "median": "median",
    }
    agg_func = agg_map.get(str(agg).lower())
    if not agg_func:
        return df

    by_cols = [x]
    if group:
        by_cols.append(group)
    return df.groupby(by_cols, dropna=False, as_index=False)[y].agg(agg_func)


def generate_chart(
    chart_spec: Dict[str, Any],
    df: pd.DataFrame,
) -> Dict[str, Any]:
    """차트를 생성하고 figure JSON과 코드를 반환."""
    chart_type = chart_spec.get("chart_type")
    x = chart_spec.get("x")
    y = chart_spec.get("y")
    group = chart_spec.get("group")
    agg = chart_spec.get("agg")

    log_event(
        "codegen.start",
        {"chart_type": chart_type, "x": x, "y": y, "group": group, "agg": agg},
    )

    fig = None

    if chart_type == "line" and x and y:
        chart_df = _aggregate_frame(df, x, y, group, agg)
        fig = px.line(chart_df, x=x, y=y, color=group)
    elif chart_type == "bar" and x and y:
        chart_df = _aggregate_frame(df, x, y, group, agg)
        if agg:
            fig = px.bar(chart_df, x=x, y=y, color=group, barmode="group")
        else:
            fig = px.bar(chart_df, x=x, y=y, color=group)
    elif chart_type == "hist" and x:
        fig = px.histogram(df, x=x, color=group)
    elif chart_type == "scatter" and x and y:
        fig = px.scatter(df, x=x, y=y, color=group)
    elif chart_type == "box" and x and y:
        fig = px.box(df, x=x, y=y, color=group)
    elif chart_type == "pyramid" and x and y and group:
        chart_df = _aggregate_frame(df, x, y, None, agg)
        right_df = _aggregate_frame(df, x, group, None, agg)
        merged = chart_df.merge(right_df, on=x, how="inner", suffixes=("_left", "_right"))
        if not merged.empty:
            fig = go.Figure()
            fig.add_trace(
                go.Bar(
                    y=merged[x],
                    x=-merged[f"{y}_left"],
                    name=str(y),
                    orientation="h",
                )
            )
            fig.add_trace(
                go.Bar(
                    y=merged[x],
                    x=merged[f"{group}_right"],
                    name=str(group),
                    orientation="h",
                )
            )
            fig.update_layout(barmode="relative")

    if fig is None:
        log_event("codegen.noop", {"chart_type": chart_type})
        return {"figure_json": None, "code": _build_code(chart_spec)}

    # Numpy types in figure JSON can break Pydantic serialization
    fig_json = json.loads(pio.to_json(fig))
    log_event("codegen.success", {"chart_type": chart_type})

    return {
        "figure_json": fig_json,
        "code": _build_code(chart_spec),
    }
