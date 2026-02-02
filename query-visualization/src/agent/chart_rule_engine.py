"""분석·차트 플랜 생성 룰 엔진.

- intent_info와 df를 기반으로 여러 개의 분석 플랜을 만든다.
- 각 플랜은 chart_spec 초안과 간단한 설명을 포함한다.
"""
from __future__ import annotations

from typing import Any, Dict, List

import pandas as pd

from src.utils.logging import log_event

# 입력: df, col, max_groups
# 출력: bool
# 그룹 후보의 카디널리티를 제한
def _is_low_cardinality(df: pd.DataFrame, col: str, max_groups: int) -> bool:
    try:
        return int(df[col].nunique(dropna=True)) <= max_groups
    except Exception:
        return False

# 입력: df
# 출력: str | None
# 임상 데이터에서 안전한 그룹 컬럼을 선택(화이트리스트 + 카디널리티 제한)
def _pick_safe_group(df: pd.DataFrame) -> str | None:
    allow_tokens = (
        "careunit",
        "icu",
        "ward",
        "admission_type",
        "service",
        "gender",
        "sex",
        "ethnicity",
        "race",
        "insurance",
        "payer",
        "language",
    )
    deny_tokens = (
        "name",
        "patient",
        "subject_id",
        "hadm_id",
        "stay_id",
        "icd",
        "drg",
        "diagnosis",
        "mrn",
        "ssn",
    )
    max_groups = 30

    for col in df.columns:
        lower = col.lower()
        if any(d in lower for d in deny_tokens):
            continue
        if not any(a in lower for a in allow_tokens):
            continue
        if df[col].dtype not in ("object", "category"):
            continue
        if _is_low_cardinality(df, col, max_groups):
            return col
    return None

# 입력: df
# 출력: str | None
# 임상 추세에서 환자 단위 trajectory를 위한 그룹 컬럼 선택
def _pick_patient_group(df: pd.DataFrame) -> str | None:
    preferred = ("stay_id", "subject_id", "hadm_id", "patient_id")
    for col in df.columns:
        lower = col.lower()
        if any(p == lower for p in preferred) or any(p in lower for p in preferred):
            return col
    return None

# 입력: df, hints
# 출력: bool
# 컬럼명에 의료/임상 관련 힌트가 있는지 확인
def _has_column_hint(df: pd.DataFrame, hints: List[str]) -> bool:
    cols = [c.lower() for c in df.columns]
    return any(any(h in c for h in hints) for c in cols)

# 입력: df
# 출력: str | None
# 임상 데이터에서 의미 있는 그룹 컬럼 후보를 고른다
def _pick_clinical_group(df: pd.DataFrame) -> str | None:
    preferred = (
        "careunit",
        "icu",
        "ward",
        "admission_type",
        "admission",
        "discharge",
        "service",
        "diagnosis",
        "icd",
        "drg",
    )
    for col in df.columns:
        lower = col.lower()
        if any(p in lower for p in preferred):
            if _is_low_cardinality(df, col, 30):
                return col
    return None

# 입력: intent_info, df
# 출력: List[Dict[str, Any]]
# intent_info 기반으로 분석 플랜 여러 개 생성
def plan_analyses(intent_info: Dict[str, Any], df: pd.DataFrame) -> List[Dict[str, Any]]:
    """intent_info를 기반으로 분석 플랜 여러 개를 반환."""
    intent = intent_info.get("analysis_intent")
    primary = intent_info.get("primary_outcome")
    time_var = intent_info.get("time_var")
    group_var = intent_info.get("group_var") or _pick_safe_group(df)

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
        patient_group = _pick_patient_group(df)
        # 임상 해석을 위한 다중 후보: 개인 변화 + 분포 변화
        plans.append(
            {
                "chart_spec": {
                    "chart_type": "line",
                    "x": time_var,
                    "y": primary,
                    "group": patient_group or group_var,
                },
                "reason": "환자별 변화(trajectory)를 직접 확인할 수 있습니다.",
            }
        )
        plans.append(
            {
                "chart_spec": {
                    "chart_type": "box",
                    "x": time_var,
                    "y": primary,
                },
                "reason": "시간 구간별 분포와 이상치를 함께 확인할 수 있습니다.",
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
                    "reason": "그룹별 분포 차이와 이상치를 추가로 비교할 수 있습니다.",
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
                        "chart_type": "box",
                        "x": group_var,
                        "y": primary,
                    },
                    "reason": "그룹별 분포 차이와 이상치를 비교하기 좋습니다.",
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

    # 임상 의료진 대상 플랜: MIMIC-IV 계열 컬럼 힌트가 있으면 추가
    clinical_hints = [
        "subject_id",
        "hadm_id",
        "stay_id",
        "icd",
        "drg",
        "diagnosis",
        "admission",
        "discharge",
        "mortality",
        "los",
        "length_of_stay",
        "careunit",
        "icu",
        "ward",
        "charttime",
        "lab",
        "vital",
    ]
    if _has_column_hint(df, clinical_hints):
        clinical_group = group_var or _pick_clinical_group(df) or _pick_safe_group(df)
        if primary and clinical_group:
            plans.append(
                {
                    "chart_spec": {
                        "chart_type": "box",
                        "x": clinical_group,
                        "y": primary,
                    },
                    "reason": "임상 의사결정에 유용한 그룹별 분포를 확인합니다.",
                }
            )
        if time_var and primary:
            plans.append(
                {
                    "chart_spec": {
                        "chart_type": "line",
                        "x": time_var,
                        "y": primary,
                    },
                    "reason": "임상 경과를 개인 단위로 확인하기 적합합니다.",
                }
            )

    log_event("rule_engine.plans", {"count": len(plans)})

    return plans
