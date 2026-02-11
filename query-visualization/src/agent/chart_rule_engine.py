"""분석·차트 플랜 생성 룰 엔진.

- intent_info와 df를 기반으로 여러 개의 분석 플랜을 만든다.
- 각 플랜은 chart_spec 초안과 간단한 설명을 포함한다.
- 전제: ADMISSIONS/ICUSTAYS가 중심 테이블이며, 이벤트는 HADM_ID/STAY_ID에 종속된다.
- PATIENTS는 속성 테이블이며 분석 기준 테이블이 아니다.
"""
from __future__ import annotations

import json
import re
from typing import Any, Dict, List, Optional

import pandas as pd
from pandas.api import types as pdt

from src.utils.logging import log_event

# 도메인 금지/허용 토큰 상수 (임상 의미 보존)
# 전제: 중심 테이블은 ADMISSIONS / ICUSTAYS, 이벤트는 HADM_ID/STAY_ID 종속
_FORBIDDEN_TRAJECTORY = ("subject_id", "patient_id")
_ALLOWED_TRAJECTORY = ("stay_id", "hadm_id")
_IDENTIFIER_COLS = ("subject_id", "hadm_id", "stay_id", "patient_id")
_FORBIDDEN_GROUP_COLS = (
    "subject_id",
    "hadm_id",
    "stay_id",
    "seq_num",
    "transfer_id",
    "orderid",
    "linkorderid",
    "order_provider_id",
    "caregiver_id",
    "pharmacy_id",
    "icd_code",
    "itemid",
    "emar_id",
    "poe_id",
)
# low-cardinality 허용 그룹(컬럼 실명 기준)
_ALLOWED_GROUP_COLS = (
    "gender",
    "anchor_year_group",
    "admission_type",
    "insurance",
    "language",
    "race",
    "marital_status",
    "first_careunit",
    "last_careunit",
    "curr_service",
    "careunit",
)
_CLINICAL_HINTS = [
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
_TIME_CANDIDATES = (
    "charttime",
    "admittime",
    "dischtime",
    "intime",
    "outtime",
    "starttime",
    "endtime",
    "storetime",
    "storedate",
    "edregtime",
    "edouttime",
    "ordertime",
    "transfertime",
    "chartdate",
)
_PREFERRED_NUMERIC_Y = (
    "valuenum",
    "value",
    "amount",
    "rate",
    "los",
    "diagnosis_count",
    "count",
    "anchor_age",
    "doses_per_24_hrs",
)


def _extract_chart_spec_from_context(
    retrieved_context: Optional[str],
    df: pd.DataFrame,
) -> Optional[Dict[str, Any]]:
    if not retrieved_context:
        return None
    # 간단한 패턴: "chart_spec: { ... }"
    match = re.search(r"chart_spec:\s*(\{.*?\})", retrieved_context)
    if not match:
        return None
    try:
        spec = json.loads(match.group(1))
    except Exception:
        return None

    chart_type = spec.get("chart_type")
    x = spec.get("x")
    y = spec.get("y")
    group = spec.get("group")

    # 최소 유효성: 컬럼이 실제로 존재하는지 확인
    cols = set(df.columns)
    for col in (x, y, group):
        if col and col not in cols:
            return None

    return {
        "chart_spec": {k: v for k, v in spec.items() if v is not None},
        "reason": "RAG 예시 기반 추천 플랜입니다.",
    }


def _infer_chart_from_columns(df: pd.DataFrame) -> Optional[Dict[str, Any]]:
    """Infer a chart spec using only result columns."""
    cols = list(df.columns)
    lower = {c.lower(): c for c in cols}

    # Alias-based hints (from SQL templates)
    if "x_time" in lower and "y_value" in lower:
        return {
            "chart_spec": {"chart_type": "line", "x": lower["x_time"], "y": lower["y_value"]},
            "reason": "Result aliases indicate a time-series aggregate.",
        }
    if "x_group" in lower and "y_value" in lower:
        return {
            "chart_spec": {"chart_type": "bar", "x": lower["x_group"], "y": lower["y_value"]},
            "reason": "Result aliases indicate a grouped aggregate.",
        }

    # Time-series heuristics
    time_cols = [c for c in cols if any(t in c.lower() for t in ("time", "date", "day", "month", "year"))]
    # numeric은 식별자/코드 컬럼을 제외한 실제 측정치 우선
    numeric_cols = [
        c
        for c in cols
        if pdt.is_numeric_dtype(df[c]) and not _is_identifier_col(c) and "code" not in c.lower()
    ]
    numeric_cols.sort(
        key=lambda c: next(
            (idx for idx, token in enumerate(_PREFERRED_NUMERIC_Y) if token in c.lower()),
            999,
        )
    )
    time_cols.sort(
        key=lambda c: next(
            (idx for idx, token in enumerate(_TIME_CANDIDATES) if token == c.lower()),
            999,
        )
    )
    categorical_cols = [c for c in cols if df[c].dtype == "object"]

    if time_cols and numeric_cols:
        return {
            "chart_spec": {"chart_type": "line", "x": time_cols[0], "y": numeric_cols[0]},
            "reason": "Detected time-like and numeric columns for a trend chart.",
        }

    # Two numeric columns -> scatter
    if len(numeric_cols) >= 2:
        return {
            "chart_spec": {"chart_type": "scatter", "x": numeric_cols[0], "y": numeric_cols[1]},
            "reason": "Detected multiple numeric columns for correlation.",
        }

    # Categorical + numeric -> bar
    if categorical_cols and numeric_cols:
        return {
            "chart_spec": {"chart_type": "bar", "x": categorical_cols[0], "y": numeric_cols[0]},
            "reason": "Detected category + numeric for comparison.",
        }

    # Single numeric -> histogram
    if len(numeric_cols) == 1:
        return {
            "chart_spec": {"chart_type": "hist", "x": numeric_cols[0]},
            "reason": "Detected a single numeric column for distribution.",
        }

    return None

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
    allow_tokens = _ALLOWED_GROUP_COLS
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
        "itemid",
        "emar_id",
        "poe_id",
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
    preferred = _ALLOWED_TRAJECTORY
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

# 입력: user_query
# 출력: context_flags
# ICU/입실 후 문맥을 보수적으로 판단 (의미 보존 우선)


def _infer_context_flags(user_query: Optional[str], available_columns: List[str]) -> Dict[str, bool]:
    q = (user_query or "").lower()
    cols = {c.lower() for c in available_columns}
    icu_kw = any(k in q for k in ("icu", "중환자실", "입실", "입실 후", "입실후"))
    admit_kw = any(k in q for k in ("입원", "입원 후", "admission", "admit"))
    # "후" 단독 키워드는 오탐이 많아 숫자+시간 단위 패턴만 허용한다.
    post_days = bool(
        re.search(r"\bafter\s+\d+\s*(day|days|hour|hours|d|h)\b", q)
        or re.search(r"\b\d+\s*(day|days|hour|hours|d|h)\s+after\b", q)
        or re.search(r"\b후\s*\d+\s*(일|시간)\b", q)
        or re.search(r"\b\d+\s*(일|시간)\s*후\b", q)
        or re.search(r"\bn일\s*후\b", q)
    )

    # ICU 맥락: 키워드 또는 stay_id+intime이 있는 경우 보수적으로 판단
    icu_context = icu_kw or ("stay_id" in cols and "intime" in cols)
    admit_context = admit_kw or ("hadm_id" in cols and "admittime" in cols)

    return {
        "icu_context": icu_context,
        "admit_context": admit_context and not icu_context,
        "post_days": post_days,
    }

# 입력: cols, candidates
# 출력: str | None
# 컬럼 실명 기준으로 가장 먼저 매칭되는 컬럼 선택


def _first_matching_col(cols: List[str], candidates: List[str]) -> Optional[str]:
    lower_map = {c.lower(): c for c in cols}
    for cand in candidates:
        if cand in lower_map:
            return lower_map[cand]
    return None


def _first_time_col(cols: List[str]) -> Optional[str]:
    return _first_matching_col(cols, list(_TIME_CANDIDATES))

# 입력: cols
# 출력: str | None
# 경과시간 파생 컬럼 후보를 찾는다


def _find_elapsed_column(cols: List[str], context: str) -> Optional[str]:
    lower = [c.lower() for c in cols]
    if context == "icu":
        candidates = [
            "elapsed_icu_days",
            "icu_elapsed_days",
            "days_since_intime",
            "hours_since_intime",
            "icu_day",
        ]
    else:
        candidates = [
            "elapsed_admit_days",
            "admit_elapsed_days",
            "days_since_admittime",
            "hours_since_admittime",
            "admit_day",
        ]
    for cand in candidates:
        if cand in lower:
            return cols[lower.index(cand)]
    return None

# 입력: col
# 출력: bool
# 식별자 계열 컬럼인지 판단


def _is_identifier_col(col: str) -> bool:
    lower = col.lower()
    return any(tok == lower or tok in lower for tok in _IDENTIFIER_COLS)


def _dedupe_plans(plans: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    unique: List[Dict[str, Any]] = []
    seen = set()
    for plan in plans:
        spec = plan.get("chart_spec") or {}
        key = (
            spec.get("chart_type"),
            spec.get("x"),
            spec.get("y"),
            spec.get("group"),
            spec.get("agg"),
        )
        if key in seen:
            continue
        seen.add(key)
        unique.append(plan)
    return unique

# 입력: intent, group_var, time_var, available_columns, context_flags
# 출력: None | Exception
# 임상 규칙 위반 시 예외 발생 (잘못된 차트 생성 방지)


def validate_plan(
    intent: str | None,
    group_var: Optional[str],
    time_info: Optional[Dict[str, str]],
    available_columns: List[str],
    context_flags: Dict[str, bool],
) -> None:
    cols_lower = {c.lower() for c in available_columns}
    group_lower = (group_var or "").lower()

    # 1) ICU/입실 후 trend는 stay_id 없이 생성 금지, subject_id trajectory 금지
    # 이유: ICU 경과는 ICU 체류(STAY_ID) 단위가 아니면 해석이 무의미하다.
    if intent == "trend" and context_flags.get("icu_context"):
        if "stay_id" not in cols_lower:
            raise ValueError("ICU/입실 후 trend는 stay_id 없이 생성할 수 없습니다.")
        if "intime" not in cols_lower:
            raise ValueError("ICU/입실 후 trend는 ICUSTAYS.INTIME 조인이 필요합니다.")
        if not any(t in cols_lower for t in _TIME_CANDIDATES):
            raise ValueError("ICU/입실 후 trend는 시간 컬럼(chart/start/end/out/store time)이 필요합니다.")
        if group_lower in _FORBIDDEN_TRAJECTORY:
            raise ValueError(
                "ICU/입실 후 trend에서 subject_id/patient_id trajectory는 금지입니다.")
        # 경과 시간 축 강제
        # 이유: ICU 맥락에서 calendar time은 입실 기준을 흐려 의미 오류를 만든다.
        if not time_info or time_info.get("type") != "elapsed":
            raise ValueError("ICU/입실 후 trend는 경과시간 축만 허용됩니다.")
        if time_info.get("expr") is None:
            raise ValueError("ICU/입실 후 trend는 경과시간 파생 컬럼이 필요합니다.")

    # 2) trend line group은 trajectory 단위만 허용
    # 이유: 개인/입원 단위 trajectory가 아닌 집계 라인은 임상 해석을 왜곡한다.
    if intent == "trend" and group_lower:
        if group_lower in _FORBIDDEN_TRAJECTORY:
            raise ValueError("trend의 group_var로 subject_id/patient_id는 금지입니다.")
        if group_lower not in _ALLOWED_TRAJECTORY:
            raise ValueError("trend의 group_var는 stay_id/hadm_id만 허용됩니다.")

    # 3) comparison/distribution에서 식별자 그룹 금지
    # 이유: 식별자 기준 그룹은 과도한 분할로 해석 불가능.
    if intent in ("comparison", "distribution") and group_lower:
        if any(tok in group_lower for tok in _IDENTIFIER_COLS):
            raise ValueError("comparison/distribution에서 식별자 그룹은 금지입니다.")

    # 4) correlation은 식별자 변수 제외
    # 이유: 식별자와의 상관은 통계적으로 의미가 없다.
    if intent == "correlation":
        if any(tok in group_lower for tok in _IDENTIFIER_COLS):
            raise ValueError("correlation에서 식별자 group은 금지입니다.")

    # 5) '후 N일' 맥락은 경과시간 축 필수
    # 이유: 기준 시점 이후 경과를 묻는 질문은 경과시간 파생 없이는 답할 수 없다.
    if intent == "trend" and context_flags.get("post_days"):
        if not time_info or time_info.get("type") != "elapsed" or time_info.get("expr") is None:
            raise ValueError("'후 N일' 맥락은 경과시간 파생 컬럼이 필요합니다.")

    # 6) 입원 기준 trend는 ADMITTIME 필요
    # 이유: 입원 기준 분석은 ADMISSIONS 기준 시간이 필수다.
    if intent == "trend" and context_flags.get("admit_context"):
        if "admittime" not in cols_lower:
            raise ValueError("입원 기준 trend는 ADMISSIONS.ADMITTIME 조인이 필요합니다.")
        if not any(t in cols_lower for t in _TIME_CANDIDATES):
            raise ValueError("입원 기준 trend는 시간 컬럼(chart/start/end/out/store time)이 필요합니다.")

    # 7) INPUT/OUTPUT 계열 rate/amount는 시간 binning 없이 의미 없음
    # 이유: rate/amount는 시간 집계 없으면 임상적으로 해석 불가.
    if intent == "trend":
        has_rate_amount = any("rate" in c or "amount" in c for c in cols_lower)
        if has_rate_amount and (not time_info or time_info.get("expr") in (None, "charttime")):
            raise ValueError("rate/amount trend는 시간 binning(경과시간 포함)이 필요합니다.")

# 입력: intent, context_flags, available_columns
# 출력: str | None
# ICU/입실 문맥이면 stay_id 최우선, 금지 그룹은 반환하지 않음


def choose_group_var(
    intent: Optional[str],
    context_flags: Dict[str, bool],
    available_columns: List[str],
) -> Optional[str]:
    cols = [c.lower() for c in available_columns]

    if context_flags.get("icu_context"):
        if "stay_id" in cols:
            return "stay_id"
        if "hadm_id" in cols:
            return "hadm_id"
        return None

    # trend는 trajectory 단위만 허용
    if intent == "trend":
        if "stay_id" in cols:
            return "stay_id"
        if "hadm_id" in cols:
            return "hadm_id"
        return None

    # distribution/comparison은 low-cardinality만 허용
    group = _first_matching_col(available_columns, list(_ALLOWED_GROUP_COLS))
    if group and group.lower() in _FORBIDDEN_GROUP_COLS:
        return None
    return group

# 입력: intent, context_flags, available_columns
# 출력: dict | None
# 입실 후 문맥에서는 경과시간 축만 허용


def derive_time_var(
    intent: Optional[str],
    context_flags: Dict[str, bool],
    available_columns: List[str],
) -> Optional[Dict[str, str]]:
    cols_lower = {c.lower() for c in available_columns}
    if intent != "trend":
        return None

    # ICU 맥락: 경과시간 강제 (Oracle: DATE - DATE = days)
    if context_flags.get("icu_context"):
        elapsed_col = _find_elapsed_column(available_columns, "icu")
        if not elapsed_col:
            return {"type": "elapsed", "expr": None, "source": "charttime - intime", "unit": "day"}
        return {"type": "elapsed", "expr": elapsed_col, "source": "charttime - intime", "unit": "day"}

    # 입원 맥락: 경과시간 강제
    if context_flags.get("admit_context"):
        elapsed_col = _find_elapsed_column(available_columns, "admit")
        if not elapsed_col:
            return {"type": "elapsed", "expr": None, "source": "charttime - admittime", "unit": "day"}
        return {"type": "elapsed", "expr": elapsed_col, "source": "charttime - admittime", "unit": "day"}

    # 단순 시간 추세: 이용 가능한 시간 컬럼 우선 선택
    chart_col = _first_matching_col(available_columns, ["charttime", "chart_time", "charttimestamp"])
    if not chart_col:
        chart_col = _first_time_col(available_columns)
    if chart_col:
        return {"type": "calendar", "expr": chart_col, "unit": "day"}
    return None

# 입력: intent_info, df
# 출력: List[Dict[str, Any]]
# intent_info 기반으로 분석 플랜 여러 개 생성


def plan_analyses(
    intent_info: Dict[str, Any],
    df: pd.DataFrame,
    retrieved_context: Optional[str] = None,
) -> List[Dict[str, Any]]:
    """intent_info를 기반으로 분석 플랜 여러 개를 반환."""
    intent = intent_info.get("analysis_intent")
    primary = intent_info.get("primary_outcome")
    user_query = intent_info.get("user_query")
    # RAG 컨텍스트에서 예시 chart_spec 추출 (가능하면 우선 제안)
    suggested_plan = _extract_chart_spec_from_context(retrieved_context, df)
    column_only_plan = _infer_chart_from_columns(df)
    context_flags = intent_info.get("context_flags") or _infer_context_flags(
        user_query, list(df.columns))

    # time/group은 규칙 기반으로 강제 선택
    time_info = derive_time_var(intent, context_flags, list(df.columns))
    group_var = intent_info.get("group_var") or choose_group_var(
        intent, context_flags, list(df.columns))
    time_var = time_info.get(
        "expr") if time_info else intent_info.get("time_var")

    # low-cardinality 검사 (group_var가 허용 리스트여도 값이 과다하면 제거)
    if group_var and group_var in df.columns and not _is_low_cardinality(df, group_var, 30):
        group_var = None
    # intent별 금지 group_var 정리
    if group_var:
        group_lower = group_var.lower()
        if intent != "trend" and group_lower in _FORBIDDEN_GROUP_COLS:
            group_var = None
        elif intent == "trend" and group_lower not in _ALLOWED_TRAJECTORY:
            group_var = None
        elif intent in ("distribution", "comparison") and _is_identifier_col(group_var):
            group_var = None

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
    if suggested_plan:
        spec = suggested_plan.get("chart_spec", {})
        if intent == "trend":
            # trend는 시간축 정합성 확인
            if time_info and spec.get("x") == time_info.get("expr"):
                try:
                    validate_plan(
                        intent,
                        spec.get("group"),
                        time_info,
                        list(df.columns),
                        context_flags,
                    )
                except Exception as exc:
                    log_event(
                        "rule_engine.suggested_plan.blocked",
                        {"reason": str(exc), "chart_spec": spec},
                    )
                else:
                    plans.append(suggested_plan)
        else:
            plans.append(suggested_plan)
    elif column_only_plan:
        plans.append(column_only_plan)

    if intent == "trend" and time_var and primary:
        try:
            validate_plan(intent, group_var, time_info,
                          list(df.columns), context_flags)
        except Exception as exc:
            # 규칙 위반 시 trend 플랜을 만들지 않는다(의미 보존 우선)
            log_event("rule_engine.trend.blocked",
                      {"reason": str(exc)})
        else:
            patient_group = _pick_patient_group(df)
            line_group = patient_group or group_var
            # trajectory 그룹이 없으면 line 생성 금지
            if line_group:
                plans.append(
                    {
                        "chart_spec": {
                            "chart_type": "line",
                            "x": time_var,
                            "y": primary,
                            "group": line_group,
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
    elif intent == "proportion" and primary:
        # 비율 질문은 추세/그룹 비교 둘 다 가능하므로 time 우선, 없으면 group 기반 bar.
        if time_var:
            plans.append(
                {
                    "chart_spec": {
                        "chart_type": "line",
                        "x": time_var,
                        "y": primary,
                        "group": group_var,
                    },
                    "reason": "시간에 따른 비율 변화를 확인할 수 있습니다.",
                }
            )
        elif group_var:
            plans.append(
                {
                    "chart_spec": {
                        "chart_type": "bar",
                        "x": group_var,
                        "y": primary,
                    },
                    "reason": "그룹별 비율 차이를 확인할 수 있습니다.",
                }
            )
    elif intent == "correlation" and primary:
        if not _is_identifier_col(primary):
            other = None
            for col in df.columns:
                if col == primary:
                    continue
                if df[col].dtype == "object":
                    continue
                if _is_identifier_col(col):
                    continue
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

    # 임상 의료진 대상 힌트는 후보로만 사용 (distribution intent에선 group 사용 금지)
    if _has_column_hint(df, _CLINICAL_HINTS) and intent not in ("distribution", "comparison"):
        clinical_group = _pick_clinical_group(df) or _pick_safe_group(df)
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

    plans = _dedupe_plans(plans)
    log_event("rule_engine.plans", {"count": len(plans)})

    return plans
