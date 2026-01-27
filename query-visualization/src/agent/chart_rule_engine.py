"""분석·차트 플랜 생성 룰 엔진.

- intent_info와 df를 기반으로 여러 개의 분석 플랜을 만든다.
- 각 플랜은 chart_spec 초안과 간단한 설명을 포함한다.
"""
from __future__ import annotations

from typing import Any, Dict, List

import pandas as pd

from src.utils.logging import log_event


def _first_categorical(df: pd.DataFrame) -> str | None:
    # 문자열/카테고리 컬럼을 하나 고른다
    for col in df.columns:
        if df[col].dtype == "object":
            return col
    return None


def plan_analyses(intent_info: Dict[str, Any], df: pd.DataFrame) -> List[Dict[str, Any]]:
    """intent_info를 기반으로 분석 플랜 여러 개를 반환."""
    intent = intent_info.get("analysis_intent")
    primary = intent_info.get("primary_outcome")
    time_var = intent_info.get("time_var")
    group_var = intent_info.get("group_var") or _first_categorical(df)

    log_event(
        "rule_engine.start",
        {
            "intent": intent,
            "primary": primary,
            "time_var": time_var,
            "group_var": group_var,
        },
    )

    plans: List[Dict[str, Any]] = []

    if intent == "trend" and time_var and primary:
        plans.append(
            {
                "chart_spec": {
                    "chart_type": "line",
                    "x": time_var,
                    "y": primary,
                    "agg": "mean",
                    "group": group_var,
                },
                "reason": "시간에 따른 추세를 확인하기 적합합니다.",
            }
        )
        plans.append(
            {
                "chart_spec": {
                    "chart_type": "bar",
                    "x": time_var,
                    "y": primary,
                    "agg": "mean",
                },
                "reason": "기간별 평균을 막대그래프로 비교할 수 있습니다.",
            }
        )
    elif intent == "distribution" and primary:
        plans.append(
            {
                "chart_spec": {
                    "chart_type": "hist",
                    "x": primary,
                },
                "reason": "전체 분포를 확인하기에 적합합니다.",
            }
        )
        if group_var:
            plans.append(
                {
                    "chart_spec": {
                        "chart_type": "box",
                        "x": group_var,
                        "y": primary,
                    },
                    "reason": "그룹별 분포 차이를 비교할 수 있습니다.",
                }
            )
    elif intent == "comparison" and primary:
        if group_var:
            plans.append(
                {
                    "chart_spec": {
                        "chart_type": "bar",
                        "x": group_var,
                        "y": primary,
                        "agg": "mean",
                    },
                    "reason": "그룹별 평균을 비교하기 좋습니다.",
                }
            )
    elif intent == "correlation" and primary:
        other = None
        for col in df.columns:
            if col != primary and df[col].dtype != "object":
                other = col
                break
        if other:
            plans.append(
                {
                    "chart_spec": {
                        "chart_type": "scatter",
                        "x": other,
                        "y": primary,
                    },
                    "reason": "두 변수의 상관관계를 시각화합니다.",
                }
            )
    else:
        # 기본: 컬럼 개요 수준의 간단한 차트
        if primary:
            plans.append(
                {
                    "chart_spec": {"chart_type": "hist", "x": primary},
                    "reason": "기본 분포를 확인하기 위한 플랜입니다.",
                }
            )

    log_event("rule_engine.plans", {"count": len(plans)})

    return plans
