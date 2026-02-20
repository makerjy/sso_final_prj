"""차트 코드 생성 + 실행 유틸.

- Plotly로 인터랙티브 차트를 생성한다.
- 결과는 figure JSON 형태로 반환한다.
"""
from __future__ import annotations

import os
from typing import Any, Dict, Optional, Tuple
import json

import pandas as pd
from pandas.api import types as pdt
import plotly.express as px
import plotly.graph_objects as go
import plotly.io as pio

from src.utils.logging import log_event


def _read_bar_max_categories(default: int = 30) -> int:
    raw = str(os.getenv("QV_BAR_MAX_CATEGORIES", str(default))).strip()
    try:
        return int(raw)
    except Exception:
        return default


_BAR_MAX_CATEGORIES = _read_bar_max_categories(30)
_BAR_OTHER_LABEL = "기타"
_BAR_LONG_LABEL_LEN = 14
_BAR_DENSE_COUNT = 12


def _hex_to_rgb(color: str) -> Tuple[int, int, int]:
    text = str(color or "").strip().lstrip("#")
    if len(text) == 3:
        text = "".join(ch * 2 for ch in text)
    if len(text) != 6:
        return (59, 130, 246)
    try:
        return (
            int(text[0:2], 16),
            int(text[2:4], 16),
            int(text[4:6], 16),
        )
    except Exception:
        return (59, 130, 246)


def _rgba(color: str, alpha: float) -> str:
    r, g, b = _hex_to_rgb(color)
    a = min(1.0, max(0.0, float(alpha)))
    return f"rgba({r},{g},{b},{a})"


def _build_code(chart_spec: Dict[str, Any]) -> str:
    # 시각화 코드를 문자열로 반환(로그/디버깅용)
    chart_type = chart_spec.get("chart_type")
    x = chart_spec.get("x")
    y = chart_spec.get("y")
    group = chart_spec.get("group")
    agg = chart_spec.get("agg")
    size = chart_spec.get("size")
    animation_frame = chart_spec.get("animation_frame")
    mode = chart_spec.get("mode")
    bar_mode = chart_spec.get("bar_mode")
    orientation = chart_spec.get("orientation")
    series_cols = chart_spec.get("series_cols")
    max_categories = chart_spec.get("max_categories")

    return (
        "# plotly 코드(요약)\n"
        f"# chart_type={chart_type}, x={x}, y={y}, group={group}, agg={agg}, size={size}, animation_frame={animation_frame}, mode={mode}, "
        f"bar_mode={bar_mode}, orientation={orientation}, series_cols={series_cols}, max_categories={max_categories}\n"
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


def _aggregate_pyramid_frame(
    df: pd.DataFrame,
    x: str,
    y: str,
    group: str,
    agg: str | None,
) -> pd.DataFrame:
    chart_df = df[[x, y, group]].copy()
    chart_df[y] = pd.to_numeric(chart_df[y], errors="coerce")
    chart_df = chart_df.dropna(subset=[x, y, group])
    if chart_df.empty:
        return chart_df
    agg_map = {
        "avg": "mean",
        "mean": "mean",
        "sum": "sum",
        "min": "min",
        "max": "max",
        "count": "count",
        "median": "median",
    }
    agg_func = agg_map.get(str(agg).lower(), "sum") if agg else "sum"
    return (
        chart_df.groupby([x, group], dropna=False, as_index=False)[y]
        .agg(agg_func)
    )


def _limit_bar_categories(
    chart_df: pd.DataFrame,
    category_col: str,
    value_col: str,
    group_col: Optional[str] = None,
    *,
    top_n: int = _BAR_MAX_CATEGORIES,
    agg: Optional[str] = None,
) -> pd.DataFrame:
    if category_col not in chart_df.columns or value_col not in chart_df.columns:
        return chart_df

    df2 = chart_df.copy()
    if top_n <= 0:
        return df2
    df2[category_col] = df2[category_col].astype(str)
    unique_count = int(df2[category_col].nunique(dropna=False))
    if unique_count <= top_n:
        return df2

    numeric_value = pd.to_numeric(df2[value_col], errors="coerce").fillna(0.0)
    score_by_category = (
        pd.DataFrame({category_col: df2[category_col], "__score__": numeric_value.abs()})
        .groupby(category_col, dropna=False)["__score__"]
        .sum()
        .sort_values(ascending=False)
    )
    top_categories = score_by_category.head(top_n).index.astype(str).tolist()
    top_set = set(top_categories)

    keep_df = df2[df2[category_col].isin(top_set)].copy()
    rest_df = df2[~df2[category_col].isin(top_set)].copy()

    agg_norm = str(agg or "").strip().lower()
    can_rollup_other = agg_norm in {"", "sum", "count"}
    if can_rollup_other and not rest_df.empty:
        if group_col and group_col in rest_df.columns:
            other_rows = (
                rest_df.groupby(group_col, dropna=False, as_index=False)[value_col]
                .sum()
            )
            other_rows[category_col] = _BAR_OTHER_LABEL
            keep_df = pd.concat(
                [keep_df, other_rows[[category_col, group_col, value_col]]],
                ignore_index=True,
            )
        else:
            other_value = pd.to_numeric(rest_df[value_col], errors="coerce").fillna(0.0).sum()
            keep_df = pd.concat(
                [
                    keep_df,
                    pd.DataFrame([{category_col: _BAR_OTHER_LABEL, value_col: float(other_value)}]),
                ],
                ignore_index=True,
            )
        top_categories.append(_BAR_OTHER_LABEL)

    order_map = {cat: idx for idx, cat in enumerate(top_categories)}
    keep_df["__order__"] = keep_df[category_col].map(order_map).fillna(len(order_map))
    sort_cols = ["__order__"]
    if group_col and group_col in keep_df.columns:
        sort_cols.append(group_col)
    keep_df = keep_df.sort_values(sort_cols, kind="stable").drop(columns=["__order__"])

    log_event(
        "codegen.bar.capped_categories",
        {
            "category_col": category_col,
            "group_col": group_col,
            "before": unique_count,
            "after": int(keep_df[category_col].nunique(dropna=False)),
            "top_n": top_n,
        },
    )
    return keep_df


def _should_use_horizontal_bar(category_values: pd.Series) -> bool:
    if category_values.empty:
        return False
    labels = category_values.astype(str).tolist()
    max_len = max((len(text) for text in labels), default=0)
    return len(labels) >= _BAR_DENSE_COUNT or max_len >= _BAR_LONG_LABEL_LEN


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
    size = chart_spec.get("size")
    animation_frame = chart_spec.get("animation_frame")
    mode = str(chart_spec.get("mode") or "").strip().lower()
    bar_mode = str(chart_spec.get("bar_mode") or "").strip().lower()
    orientation = str(chart_spec.get("orientation") or "").strip().lower()
    raw_series_cols = chart_spec.get("series_cols")
    title = chart_spec.get("title")
    x_title = chart_spec.get("x_title")
    y_title = chart_spec.get("y_title")
    max_categories_raw = chart_spec.get("max_categories")
    max_categories = _BAR_MAX_CATEGORIES
    if max_categories_raw is not None:
        try:
            max_categories = int(max_categories_raw)
        except Exception:
            max_categories = _BAR_MAX_CATEGORIES

    log_event(
        "codegen.start",
        {
            "chart_type": chart_type,
            "x": x,
            "y": y,
            "group": group,
            "agg": agg,
            "size": size,
            "animation_frame": animation_frame,
            "mode": mode,
            "bar_mode": bar_mode,
            "orientation": orientation,
            "max_categories": max_categories,
        },
    )

    fig = None

    if chart_type == "line" and x and y:
        chart_df = _aggregate_frame(df, x, y, group, agg)
        fig = px.line(chart_df, x=x, y=y, color=group)
    elif chart_type == "line_scatter" and x and y:
        chart_df = _aggregate_frame(df, x, y, group, agg)
        fig = px.line(chart_df, x=x, y=y, color=group, markers=True)
        fig.update_traces(mode="lines+markers")
    elif chart_type in (
        "bar",
        "bar_basic",
        "bar_grouped",
        "bar_stacked",
        "bar_hgroup",
        "bar_hstack",
        "bar_percent",
        "bar_hpercent",
    ) and x:
        # Defaults by style (simple -> detailed bar variants)
        if chart_type == "bar_basic":
            default_mode = "group"
            default_orientation = "v"
        elif chart_type == "bar_grouped":
            default_mode = "group"
            default_orientation = "v"
        elif chart_type == "bar_stacked":
            default_mode = "stack"
            default_orientation = "v"
        elif chart_type == "bar_hgroup":
            default_mode = "group"
            default_orientation = "h"
        elif chart_type == "bar_hstack":
            default_mode = "stack"
            default_orientation = "h"
        elif chart_type == "bar_percent":
            default_mode = "stack"
            default_orientation = "v"
        elif chart_type == "bar_hpercent":
            default_mode = "stack"
            default_orientation = "h"
        else:
            default_mode = "group" if group else "group"
            default_orientation = "v"

        resolved_mode = bar_mode or default_mode
        resolved_orientation = orientation or default_orientation
        if resolved_mode not in {"group", "stack", "relative", "overlay"}:
            resolved_mode = default_mode
        if resolved_orientation not in {"h", "v"}:
            resolved_orientation = default_orientation
        auto_orientation = not bool(orientation)

        series_cols: list[str] = []
        if isinstance(raw_series_cols, list):
            for col in raw_series_cols:
                if isinstance(col, str) and col in df.columns and col != x:
                    series_cols.append(col)

        # Wide -> long transform for grouped/stacked multi-series bars.
        if series_cols:
            keep_cols = [x] + series_cols
            work_df = df[keep_cols].copy()
            for col in series_cols:
                work_df[col] = pd.to_numeric(work_df[col], errors="coerce")
            melt_df = (
                work_df.melt(
                    id_vars=[x],
                    value_vars=series_cols,
                    var_name="__series__",
                    value_name="__value__",
                )
                .dropna(subset=[x, "__value__"])
            )
            if not melt_df.empty:
                chart_df = (
                    melt_df.groupby([x, "__series__"], dropna=False, as_index=False)["__value__"]
                    .sum()
                )
                chart_df = _limit_bar_categories(
                    chart_df,
                    category_col=x,
                    value_col="__value__",
                    group_col="__series__",
                    top_n=max_categories,
                    agg="sum",
                )
                if auto_orientation and _should_use_horizontal_bar(chart_df[x]):
                    resolved_orientation = "h"
                if resolved_orientation == "h":
                    fig = px.bar(
                        chart_df,
                        x="__value__",
                        y=x,
                        color="__series__",
                        orientation="h",
                        barmode=resolved_mode,
                    )
                else:
                    fig = px.bar(
                        chart_df,
                        x=x,
                        y="__value__",
                        color="__series__",
                        orientation="v",
                        barmode=resolved_mode,
                    )
        elif y and y in df.columns:
            chart_df = _aggregate_frame(df, x, y, group, agg)
            if not chart_df.empty:
                if y in chart_df.columns:
                    chart_df[y] = pd.to_numeric(chart_df[y], errors="coerce")
                chart_df = _limit_bar_categories(
                    chart_df,
                    category_col=x,
                    value_col=y,
                    group_col=group if group and group in chart_df.columns else None,
                    top_n=max_categories,
                    agg=agg,
                )
                if auto_orientation and _should_use_horizontal_bar(chart_df[x]):
                    resolved_orientation = "h"
                if resolved_orientation == "h":
                    fig = px.bar(
                        chart_df,
                        x=y,
                        y=x,
                        color=group,
                        orientation="h",
                        barmode=resolved_mode,
                    )
                else:
                    fig = px.bar(
                        chart_df,
                        x=x,
                        y=y,
                        color=group,
                        orientation="v",
                        barmode=resolved_mode,
                    )

        if fig is not None:
            category_count = int(chart_df[x].nunique(dropna=False)) if "chart_df" in locals() and x in chart_df.columns else 0
            if chart_type in {"bar_percent", "bar_hpercent"}:
                fig.update_layout(barnorm="percent")
            fig.update_traces(
                marker=dict(line=dict(color="white", width=0.6)),
                textposition="auto",
            )
            fig.update_layout(
                margin=dict(l=56, r=24, t=36, b=56),
                legend_title_text=None,
            )
            if resolved_orientation == "v" and category_count >= _BAR_DENSE_COUNT:
                fig.update_xaxes(tickangle=-35, automargin=True)
            if resolved_orientation == "h" and category_count >= _BAR_DENSE_COUNT:
                fig.update_layout(height=min(1400, max(480, 220 + category_count * 18)))
            if title:
                fig.update_layout(title=str(title))
            if x_title:
                fig.update_xaxes(title_text=str(x_title))
            if y_title:
                fig.update_yaxes(title_text=str(y_title))
    elif chart_type == "pie" and x:
        value_col = "__value__"
        chart_df = pd.DataFrame()
        if y and y in df.columns and pdt.is_numeric_dtype(df[y]):
            chart_df = df[[x, y]].copy()
            chart_df[y] = pd.to_numeric(chart_df[y], errors="coerce")
            chart_df = chart_df.dropna(subset=[x, y])
            if not chart_df.empty:
                agg_map = {
                    "avg": "mean",
                    "mean": "mean",
                    "sum": "sum",
                    "min": "min",
                    "max": "max",
                    "count": "count",
                    "median": "median",
                }
                agg_func = agg_map.get(str(agg).lower(), "sum") if agg else "sum"
                chart_df = (
                    chart_df.groupby([x], dropna=False, as_index=False)[y]
                    .agg(agg_func)
                    .rename(columns={y: value_col})
                )
        else:
            chart_df = df[[x]].dropna()
            if not chart_df.empty:
                chart_df = (
                    chart_df.groupby([x], dropna=False, as_index=False)
                    .size()
                    .rename(columns={"size": value_col})
                )
        if not chart_df.empty and value_col in chart_df.columns:
            fig = px.pie(chart_df, names=x, values=value_col)
            fig.update_traces(
                textinfo="label+percent",
                textposition="outside",
                automargin=True,
                marker=dict(line=dict(color="white", width=1.2)),
            )
            fig.update_layout(
                margin=dict(l=36, r=36, t=24, b=24),
            )
    elif chart_type == "hist" and x:
        fig = px.histogram(df, x=x, color=group)
    elif chart_type == "scatter" and x and y:
        fig = px.scatter(df, x=x, y=y, color=group)
    elif chart_type == "dynamic_scatter" and x and y:
        use_cols = [x, y]
        if group and group in df.columns:
            use_cols.append(group)
        if size and size in df.columns:
            use_cols.append(size)
        if animation_frame and animation_frame in df.columns:
            use_cols.append(animation_frame)
        chart_df = df[use_cols].copy()
        chart_df[x] = pd.to_numeric(chart_df[x], errors="coerce")
        chart_df[y] = pd.to_numeric(chart_df[y], errors="coerce")
        if size and size in chart_df.columns:
            chart_df[size] = pd.to_numeric(chart_df[size], errors="coerce")
        drop_subset = [x, y]
        if animation_frame and animation_frame in chart_df.columns:
            drop_subset.append(animation_frame)
        chart_df = chart_df.dropna(subset=drop_subset)
        if not chart_df.empty:
            scatter_kwargs: Dict[str, Any] = {
                "x": x,
                "y": y,
                "color": group if group and group in chart_df.columns else None,
                "size": size if size and size in chart_df.columns else None,
                "animation_frame": animation_frame if animation_frame and animation_frame in chart_df.columns else None,
                "size_max": 48,
            }
            scatter_kwargs = {k: v for k, v in scatter_kwargs.items() if v is not None}
            fig = px.scatter(chart_df, **scatter_kwargs)
            fig.update_traces(marker=dict(opacity=0.8, line=dict(width=0.6, color="white")))
            fig.update_layout(margin=dict(l=56, r=24, t=36, b=56))
    elif chart_type == "box" and x and y:
        fig = px.box(df, x=x, y=y, color=group)
    elif chart_type == "pyramid" and x and y and group:
        # Preferred: x(category) + y(numeric) + group(category) -> mirrored population pyramid.
        if not pdt.is_numeric_dtype(df[group]):
            pyr_df = _aggregate_pyramid_frame(df, x, y, group, agg)
            if not pyr_df.empty:
                g_order = (
                    pyr_df.groupby(group, dropna=False)[y]
                    .sum()
                    .sort_values(ascending=False)
                    .index.tolist()
                )
                if len(g_order) >= 2:
                    left_name = g_order[0]
                    right_name = g_order[1]
                    pivot = (
                        pyr_df[pyr_df[group].isin([left_name, right_name])]
                        .pivot_table(index=x, columns=group, values=y, fill_value=0)
                        .reset_index()
                    )
                    if left_name in pivot.columns and right_name in pivot.columns:
                        fig = go.Figure()
                        fig.add_trace(
                            go.Bar(
                                y=pivot[x],
                                x=-pivot[left_name],
                                name=str(left_name),
                                orientation="h",
                            )
                        )
                        fig.add_trace(
                            go.Bar(
                                y=pivot[x],
                                x=pivot[right_name],
                                name=str(right_name),
                                orientation="h",
                            )
                        )
                        fig.update_layout(
                            barmode="relative",
                            xaxis=dict(tickformat="~s"),
                        )

        # Legacy fallback: x(category) + y(numeric_left) + group(numeric_right)
        if fig is None and pdt.is_numeric_dtype(df[group]):
            chart_df = _aggregate_frame(df, x, y, None, agg)
            right_df = _aggregate_frame(df, x, group, None, agg)
            merged = chart_df.merge(
                right_df, on=x, how="inner", suffixes=("_left", "_right")
            )
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
                fig.update_layout(barmode="relative", xaxis=dict(tickformat="~s"))
    elif chart_type in ("nested_pie", "sunburst") and x and group:
        chart_df = df[[x, group]].copy()
        value_col = "__value__"
        if y and y in df.columns and pdt.is_numeric_dtype(df[y]):
            chart_df[y] = pd.to_numeric(df[y], errors="coerce")
            chart_df = chart_df.dropna(subset=[x, group, y])
            if not chart_df.empty:
                agg_map = {
                    "avg": "mean",
                    "mean": "mean",
                    "sum": "sum",
                    "min": "min",
                    "max": "max",
                    "count": "count",
                    "median": "median",
                }
                agg_func = agg_map.get(str(agg).lower(), "sum") if agg else "sum"
                chart_df = (
                    chart_df.groupby([x, group], dropna=False, as_index=False)[y]
                    .agg(agg_func)
                    .rename(columns={y: value_col})
                )
        else:
            chart_df = chart_df.dropna(subset=[x, group])
            if not chart_df.empty:
                chart_df = (
                    chart_df.groupby([x, group], dropna=False, as_index=False)
                    .size()
                    .rename(columns={"size": value_col})
                )
        if not chart_df.empty and value_col in chart_df.columns:
            # Donut-of-donut style:
            # inner ring: parent(x), outer ring: child(group) split by parent
            inner_df = (
                chart_df.groupby([x], dropna=False, as_index=False)[value_col]
                .sum()
                .sort_values(value_col, ascending=False)
                .reset_index(drop=True)
            )
            if not inner_df.empty:
                parent_order = inner_df[x].astype(str).tolist()
                parent_rank = {k: i for i, k in enumerate(parent_order)}
                outer_df = chart_df.copy()
                outer_df[x] = outer_df[x].astype(str)
                outer_df[group] = outer_df[group].astype(str)
                outer_df["__parent_rank__"] = outer_df[x].map(parent_rank).fillna(999999)
                outer_df = outer_df.sort_values(
                    by=["__parent_rank__", value_col],
                    ascending=[True, False],
                ).reset_index(drop=True)

                palette = px.colors.qualitative.Plotly
                parent_colors: Dict[str, str] = {}
                for idx, parent_name in enumerate(parent_order):
                    parent_colors[parent_name] = palette[idx % len(palette)]

                # Keep parent hue on outer slices and vary opacity by within-parent rank.
                outer_colors: list[str] = []
                for parent_name, grp in outer_df.groupby(x, sort=False):
                    base = parent_colors.get(str(parent_name), "#3b82f6")
                    n = max(len(grp), 1)
                    for i in range(len(grp)):
                        alpha = 0.95 - (0.55 * (i / max(n - 1, 1)))
                        outer_colors.append(_rgba(base, alpha))

                fig = go.Figure()
                fig.add_trace(
                    go.Pie(
                        labels=inner_df[x].astype(str),
                        values=inner_df[value_col],
                        # Shrink inner pie domain to avoid overlap with the outer ring.
                        domain=dict(x=[0.18, 0.82], y=[0.18, 0.82]),
                        hole=0.42,
                        sort=False,
                        textinfo="label+percent",
                        textposition="inside",
                        insidetextorientation="horizontal",
                        marker=dict(
                            colors=[parent_colors[name] for name in inner_df[x].astype(str)],
                            line=dict(color="white", width=2),
                        ),
                        showlegend=True,
                        name=str(x),
                        hovertemplate=f"{x}: %{{label}}<br>value=%{{value}}<br>share=%{{percent}}<extra></extra>",
                    )
                )
                fig.add_trace(
                    go.Pie(
                        labels=outer_df[group].astype(str),
                        values=outer_df[value_col],
                        hole=0.72,
                        sort=False,
                        textinfo="label+percent",
                        textposition="outside",
                        texttemplate="%{label}<br>%{percent}",
                        automargin=True,
                        marker=dict(
                            colors=outer_colors,
                            line=dict(color="white", width=1.5),
                        ),
                        outsidetextfont=dict(size=11),
                        showlegend=False,
                        name=str(group),
                        customdata=outer_df[[x, group]].values,
                        hovertemplate=(
                            f"{x}: %{{customdata[0]}}<br>"
                            f"{group}: %{{customdata[1]}}<br>"
                            "value=%{value}<br>"
                            "share=%{percent}<extra></extra>"
                        ),
                    )
                )
                total_value = float(inner_df[value_col].sum())
                center_text = f"{x}<br>{total_value:,.0f}"
                fig.update_layout(
                    annotations=[
                        dict(
                            text=center_text,
                            x=0.5,
                            y=0.5,
                            showarrow=False,
                            font=dict(size=13),
                            align="center",
                        )
                    ],
                    uniformtext=dict(minsize=9, mode="hide"),
                    margin=dict(l=56, r=56, t=28, b=28),
                )

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
