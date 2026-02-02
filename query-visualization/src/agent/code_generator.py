"""차트 코드 생성 + 실행 유틸.

- Plotly로 인터랙티브 차트를 생성한다.
- 결과는 figure JSON 형태로 반환한다.
"""
from __future__ import annotations

from typing import Any, Dict
import json

import pandas as pd
import plotly.express as px
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
        fig = px.line(df, x=x, y=y, color=group)
    elif chart_type == "bar" and x and y:
        if agg:
            fig = px.bar(df, x=x, y=y, color=group, barmode="group")
        else:
            fig = px.bar(df, x=x, y=y, color=group)
    elif chart_type == "hist" and x:
        fig = px.histogram(df, x=x, color=group)
    elif chart_type == "scatter" and x and y:
        fig = px.scatter(df, x=x, y=y, color=group)
    elif chart_type == "box" and x and y:
        fig = px.box(df, x=x, y=y, color=group)

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
