from __future__ import annotations

from datetime import datetime
import hashlib
import math
import os
from pathlib import Path
import random
from typing import Any
import uuid
import json

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel, Field

from app.core.paths import project_path
from app.services.oracle.executor import execute_sql
from app.services.runtime.diagnosis_map_store import load_diagnosis_icd_map, map_prefixes_for_terms
from app.services.runtime.state_store import get_state_store


router = APIRouter()

DEFAULT_PARAMS = {
    "readmit_days": 30,
    "age_threshold": 65,
    "los_threshold": 7,
    "gender": "all",
    "icu_only": False,
    "entry_filter": "all",
    "outcome_filter": "all",
}
_SAVED_COHORTS_KEY = "cohort::saved"
_FALLBACK_SAVED_COHORTS: list[dict[str, Any]] = []
_SURVIVAL_TIME_POINTS = [0, 7, 14, 21, 30, 45, 60, 75, 90, 120, 150, 180]
_COHORT_COMORBIDITY_SPECS_PATH = project_path("var/metadata/cohort_comorbidity_specs.json")


class CohortParams(BaseModel):
    readmit_days: int = Field(30, ge=7, le=90)
    age_threshold: int = Field(65, ge=18, le=95)
    los_threshold: int = Field(7, ge=1, le=30)
    gender: str = Field("all", pattern="^(all|M|F)$")
    icu_only: bool = False
    entry_filter: str = Field("all", pattern="^(all|er|non_er)$")
    outcome_filter: str = Field("all", pattern="^(all|survived|expired)$")


class SimulationRequest(BaseModel):
    params: CohortParams = Field(default_factory=CohortParams)
    include_baseline: bool = True


class CohortSqlRequest(BaseModel):
    params: CohortParams = Field(default_factory=CohortParams)


class SaveCohortRequest(BaseModel):
    name: str = Field(min_length=1, max_length=120)
    params: CohortParams = Field(default_factory=CohortParams)
    status: str = Field(default="active", pattern="^(active|archived)$")


def _to_float(value: Any, default: float = 0.0) -> float:
    if value is None:
        return default
    try:
        return float(value)
    except Exception:
        return default


def _to_int(value: Any, default: int = 0) -> int:
    if value is None:
        return default
    try:
        return int(value)
    except Exception:
        return default


def _cohort_sample_rows() -> int:
    raw = (os.getenv("COHORT_SAMPLE_ROWS") or "50000").strip()
    try:
        value = int(raw)
    except Exception:
        return 50000
    return max(0, value)


def _load_comorbidity_specs() -> list[dict[str, Any]]:
    if not _COHORT_COMORBIDITY_SPECS_PATH.exists():
        return []
    try:
        payload = json.loads(_COHORT_COMORBIDITY_SPECS_PATH.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        return []
    if not isinstance(payload, list):
        return []

    specs: list[dict[str, Any]] = []
    for item in payload:
        if not isinstance(item, dict):
            continue
        group_key = str(item.get("group_key") or "").strip()
        group_label = str(item.get("group_label") or "").strip()
        flag_col = str(item.get("flag_col") or "").strip()
        if not group_key or not group_label or not flag_col:
            continue
        map_terms_raw = item.get("map_terms") or []
        map_terms = [str(term).strip() for term in map_terms_raw if str(term).strip()] if isinstance(map_terms_raw, list) else []
        fallback_raw = item.get("fallback_prefixes") or []
        fallback_prefixes = [str(prefix).strip().upper() for prefix in fallback_raw if str(prefix).strip()] if isinstance(fallback_raw, list) else []
        try:
            sort_order = int(item.get("sort_order") or len(specs) + 1)
        except Exception:
            sort_order = len(specs) + 1
        specs.append({
            "group_key": group_key,
            "group_label": group_label,
            "flag_col": flag_col,
            "sort_order": sort_order,
            "map_terms": map_terms,
            "fallback_prefixes": fallback_prefixes,
        })

    return specs


def _icd_prefix_condition(dx_code_expr: str, prefixes: list[str]) -> str:
    parts = [f"{dx_code_expr} LIKE '{prefix}%'" for prefix in prefixes if prefix]
    if not parts:
        return "1 = 0"
    return "(" + " OR ".join(parts) + ")"


def _comorbidity_specs_from_mapping(dx_code_expr: str) -> list[dict[str, Any]]:
    base_specs = _load_comorbidity_specs()
    if not base_specs:
        return []
    diagnosis_map = load_diagnosis_icd_map()
    specs: list[dict[str, Any]] = []
    for base in base_specs:
        mapped_prefixes = map_prefixes_for_terms(diagnosis_map, list(base.get("map_terms", [])))
        prefixes = mapped_prefixes or [str(item).strip().upper() for item in base.get("fallback_prefixes", []) if str(item).strip()]
        if not prefixes:
            continue
        specs.append({
            "group_key": base["group_key"],
            "group_label": base["group_label"],
            "flag_col": base["flag_col"],
            "sort_order": int(base["sort_order"]),
            "condition_sql": _icd_prefix_condition(dx_code_expr, prefixes),
        })
    return specs


def _cohort_cte(params: CohortParams) -> str:
    age = int(params.age_threshold)
    los = int(params.los_threshold)
    gender = (params.gender or "all").upper()
    entry_filter = (params.entry_filter or "all").lower()
    outcome_filter = (params.outcome_filter or "all").lower()
    sample_rows = _cohort_sample_rows()
    gender_clause = f"AND UPPER(TRIM(p.GENDER)) = '{gender}' " if gender in {"M", "F"} else ""
    icu_clause = "AND EXISTS (SELECT 1 FROM ICUSTAYS i WHERE i.HADM_ID = a.HADM_ID) " if params.icu_only else ""
    if entry_filter == "er":
        entry_clause = (
            "AND (UPPER(NVL(a.ADMISSION_LOCATION, '')) LIKE '%EMERGENCY%' "
            "OR UPPER(NVL(a.ADMISSION_LOCATION, '')) LIKE '%ER%' "
            "OR UPPER(NVL(a.ADMISSION_LOCATION, '')) LIKE '%ED%') "
        )
    elif entry_filter == "non_er":
        entry_clause = (
            "AND (UPPER(NVL(a.ADMISSION_LOCATION, '')) NOT LIKE '%EMERGENCY%' "
            "AND UPPER(NVL(a.ADMISSION_LOCATION, '')) NOT LIKE '%ER%' "
            "AND UPPER(NVL(a.ADMISSION_LOCATION, '')) NOT LIKE '%ED%') "
        )
    else:
        entry_clause = ""
    if outcome_filter == "expired":
        outcome_clause = "AND a.HOSPITAL_EXPIRE_FLAG = 1 "
    elif outcome_filter == "survived":
        outcome_clause = "AND NVL(a.HOSPITAL_EXPIRE_FLAG, 0) = 0 "
    else:
        outcome_clause = ""
    sample_clause = f"AND ROWNUM <= {sample_rows} " if sample_rows > 0 else ""
    return (
        "WITH admissions_sample AS ( "
        "SELECT b.HADM_ID, b.SUBJECT_ID, b.ADMITTIME, b.DISCHTIME, b.HOSPITAL_EXPIRE_FLAG, b.ADMISSION_LOCATION, "
        "LEAD(b.ADMITTIME) OVER (PARTITION BY b.SUBJECT_ID ORDER BY b.ADMITTIME) AS NEXT_ADMITTIME "
        "FROM ( "
        "SELECT a.HADM_ID, a.SUBJECT_ID, a.ADMITTIME, a.DISCHTIME, a.HOSPITAL_EXPIRE_FLAG, a.ADMISSION_LOCATION "
        "FROM ADMISSIONS a "
        "WHERE a.ADMITTIME IS NOT NULL "
        "AND a.DISCHTIME IS NOT NULL "
        f"{sample_clause}"
        ") b "
        "), cohort AS ( "
        "SELECT a.HADM_ID, a.SUBJECT_ID, a.ADMITTIME, a.DISCHTIME, a.HOSPITAL_EXPIRE_FLAG, a.ADMISSION_LOCATION, "
        "a.NEXT_ADMITTIME, UPPER(TRIM(p.GENDER)) AS GENDER, p.ANCHOR_AGE "
        "FROM admissions_sample a "
        "JOIN PATIENTS p ON p.SUBJECT_ID = a.SUBJECT_ID "
        "WHERE p.ANCHOR_AGE IS NOT NULL "
        f"AND p.ANCHOR_AGE >= {age} "
        f"AND (CAST(a.DISCHTIME AS DATE) - CAST(a.ADMITTIME AS DATE)) >= {los} "
        f"{gender_clause}"
        f"{icu_clause}"
        f"{entry_clause}"
        f"{outcome_clause}"
        ") "
    )


def _cohort_sql_bundle(params: CohortParams) -> dict[str, str]:
    cte = _cohort_cte(params)
    readmit_days = int(params.readmit_days)
    los_expr = "(CAST(c.DISCHTIME AS DATE) - CAST(c.ADMITTIME AS DATE))"
    readmit_30_case = (
        "CASE WHEN c.NEXT_ADMITTIME IS NOT NULL "
        "AND c.NEXT_ADMITTIME > c.DISCHTIME "
        f"AND c.NEXT_ADMITTIME <= c.DISCHTIME + {readmit_days} "
        "THEN 1 ELSE 0 END"
    )
    readmit_7_case = (
        "CASE WHEN c.NEXT_ADMITTIME IS NOT NULL "
        "AND c.NEXT_ADMITTIME > c.DISCHTIME "
        "AND c.NEXT_ADMITTIME <= c.DISCHTIME + 7 "
        "THEN 1 ELSE 0 END"
    )
    death_case = "CASE WHEN c.HOSPITAL_EXPIRE_FLAG = 1 THEN 1 ELSE 0 END"
    long_stay_case = f"CASE WHEN {los_expr} >= 14 THEN 1 ELSE 0 END"
    icu_case = "CASE WHEN icu.HADM_ID IS NOT NULL THEN 1 ELSE 0 END"
    er_case = (
        "CASE WHEN UPPER(NVL(c.ADMISSION_LOCATION, '')) LIKE '%EMERGENCY%' "
        "OR UPPER(NVL(c.ADMISSION_LOCATION, '')) LIKE '%ER%' "
        "OR UPPER(NVL(c.ADMISSION_LOCATION, '')) LIKE '%ED%' "
        "THEN 1 ELSE 0 END"
    )
    age_band_key = (
        "CASE "
        "WHEN c.ANCHOR_AGE < 40 THEN '18_39' "
        "WHEN c.ANCHOR_AGE < 50 THEN '40_49' "
        "WHEN c.ANCHOR_AGE < 60 THEN '50_59' "
        "WHEN c.ANCHOR_AGE < 70 THEN '60_69' "
        "WHEN c.ANCHOR_AGE < 80 THEN '70_79' "
        "ELSE '80_PLUS' END"
    )
    age_band_label = (
        "CASE "
        "WHEN c.ANCHOR_AGE < 40 THEN '18-39세' "
        "WHEN c.ANCHOR_AGE < 50 THEN '40-49세' "
        "WHEN c.ANCHOR_AGE < 60 THEN '50-59세' "
        "WHEN c.ANCHOR_AGE < 70 THEN '60-69세' "
        "WHEN c.ANCHOR_AGE < 80 THEN '70-79세' "
        "ELSE '80세 이상' END"
    )
    gender_key = (
        "CASE "
        "WHEN c.GENDER = 'M' THEN 'M' "
        "WHEN c.GENDER = 'F' THEN 'F' "
        "ELSE 'UNKNOWN' END"
    )
    gender_label = (
        "CASE "
        "WHEN c.GENDER = 'M' THEN '남성' "
        "WHEN c.GENDER = 'F' THEN '여성' "
        "ELSE '미상' END"
    )
    subgroup_metric_cols = (
        "COUNT(*) AS ADMISSION_CNT, "
        "COUNT(DISTINCT c.SUBJECT_ID) AS PATIENT_CNT, "
        f"ROUND(100 * AVG({readmit_30_case}), 2) AS READMIT_RATE_PCT, "
        f"ROUND(100 * AVG({death_case}), 2) AS MORTALITY_RATE_PCT, "
        f"ROUND(AVG({los_expr}), 2) AS AVG_LOS_DAYS "
    )
    age_subgroup_sql = (
        cte
        + "SELECT "
        f"{age_band_key} AS GROUP_KEY, "
        f"{age_band_label} AS GROUP_LABEL, "
        f"{subgroup_metric_cols}"
        "FROM cohort c "
        f"GROUP BY {age_band_key}, {age_band_label} "
        "ORDER BY GROUP_KEY"
    )
    gender_subgroup_sql = (
        cte
        + "SELECT "
        f"{gender_key} AS GROUP_KEY, "
        f"{gender_label} AS GROUP_LABEL, "
        f"{subgroup_metric_cols}"
        "FROM cohort c "
        f"GROUP BY {gender_key}, {gender_label} "
        "ORDER BY CASE "
        "WHEN GROUP_KEY = 'M' THEN 1 "
        "WHEN GROUP_KEY = 'F' THEN 2 "
        "ELSE 3 END"
    )
    dx_code_expr = "UPPER(REPLACE(NVL(d.ICD_CODE, ''), '.', ''))"
    comorbidity_specs = _comorbidity_specs_from_mapping(dx_code_expr)
    if comorbidity_specs:
        flag_columns = ", ".join(
            f"MAX(CASE WHEN {spec['condition_sql']} THEN 1 ELSE 0 END) AS {spec['flag_col']}"
            for spec in comorbidity_specs
        )
        dx_flags_cte = (
            ", dx_flags AS ( "
            "SELECT d.HADM_ID, "
            f"{flag_columns} "
            "FROM DIAGNOSES_ICD d "
            "JOIN (SELECT DISTINCT HADM_ID FROM cohort) ch ON ch.HADM_ID = d.HADM_ID "
            "GROUP BY d.HADM_ID "
            ") "
        )

        def comorb_select(group_key: str, group_label: str, flag_col: str, sort_order: int) -> str:
            return (
                "SELECT "
                f"'{group_key}' AS GROUP_KEY, "
                f"'{group_label}' AS GROUP_LABEL, "
                "COUNT(*) AS ADMISSION_CNT, "
                "COUNT(DISTINCT c.SUBJECT_ID) AS PATIENT_CNT, "
                f"ROUND(100 * AVG({readmit_30_case}), 2) AS READMIT_RATE_PCT, "
                f"ROUND(100 * AVG({death_case}), 2) AS MORTALITY_RATE_PCT, "
                f"ROUND(AVG({los_expr}), 2) AS AVG_LOS_DAYS, "
                f"{sort_order} AS SORT_ORD "
                "FROM cohort c "
                "JOIN dx_flags f ON f.HADM_ID = c.HADM_ID "
                f"WHERE f.{flag_col} = 1"
            )

        comorbidity_union_sql = " UNION ALL ".join(
            comorb_select(
                str(spec["group_key"]),
                str(spec["group_label"]),
                str(spec["flag_col"]),
                int(spec["sort_order"]),
            )
            for spec in comorbidity_specs
        )
        comorbidity_subgroup_sql = (
            cte
            + dx_flags_cte
            + "SELECT GROUP_KEY, GROUP_LABEL, ADMISSION_CNT, PATIENT_CNT, READMIT_RATE_PCT, MORTALITY_RATE_PCT, AVG_LOS_DAYS "
            "FROM ("
            + comorbidity_union_sql
            + ") "
            "ORDER BY SORT_ORD"
        )
    else:
        comorbidity_subgroup_sql = (
            cte
            + "SELECT "
            "CAST(NULL AS VARCHAR2(64)) AS GROUP_KEY, "
            "CAST(NULL AS VARCHAR2(128)) AS GROUP_LABEL, "
            "CAST(NULL AS NUMBER) AS ADMISSION_CNT, "
            "CAST(NULL AS NUMBER) AS PATIENT_CNT, "
            "CAST(NULL AS NUMBER) AS READMIT_RATE_PCT, "
            "CAST(NULL AS NUMBER) AS MORTALITY_RATE_PCT, "
            "CAST(NULL AS NUMBER) AS AVG_LOS_DAYS "
            "FROM cohort c WHERE 1 = 0"
        )
    metrics_sql = (
        cte
        + "SELECT "
        "COUNT(DISTINCT c.SUBJECT_ID) AS PATIENT_CNT, "
        f"ROUND(100 * AVG({readmit_30_case}), 2) AS READMIT_RATE_PCT, "
        f"ROUND(100 * AVG({death_case}), 2) AS MORTALITY_RATE_PCT, "
        f"ROUND(AVG({los_expr}), 2) AS AVG_LOS_DAYS, "
        f"ROUND(PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY {los_expr}), 2) AS MEDIAN_LOS_DAYS, "
        f"ROUND(100 * AVG({readmit_7_case}), 2) AS READMIT_7D_RATE_PCT, "
        f"ROUND(100 * AVG({long_stay_case}), 2) AS LONG_STAY_RATE_PCT, "
        f"ROUND(100 * AVG({icu_case}), 2) AS ICU_ADMISSION_RATE_PCT, "
        f"ROUND(100 * AVG({er_case}), 2) AS ER_ADMISSION_RATE_PCT, "
        "COUNT(*) AS ADMISSION_CNT, "
        f"SUM({readmit_30_case}) AS READMIT_30_CNT, "
        f"SUM({death_case}) AS DEATH_CNT, "
        f"ROUND(NVL(STDDEV({los_expr}), 0), 6) AS LOS_STDDEV_DAYS, "
        f"SUM({icu_case}) AS ICU_ADMISSION_CNT, "
        f"SUM({er_case}) AS ER_ADMISSION_CNT, "
        f"SUM({readmit_7_case}) AS READMIT_7_CNT, "
        f"SUM({long_stay_case}) AS LONG_STAY_CNT "
        "FROM cohort c "
        "LEFT JOIN (SELECT DISTINCT HADM_ID FROM ICUSTAYS) icu ON icu.HADM_ID = c.HADM_ID"
    )
    return {
        "cohort_cte": cte,
        "metrics_sql": metrics_sql,
        "age_subgroup_sql": age_subgroup_sql,
        "gender_subgroup_sql": gender_subgroup_sql,
        "comorbidity_subgroup_sql": comorbidity_subgroup_sql,
        "patient_count_sql": cte + "SELECT COUNT(DISTINCT c.SUBJECT_ID) AS PATIENT_CNT FROM cohort c",
        "readmission_rate_sql": (
            cte
            + "SELECT ROUND(100 * AVG(CASE "
            "WHEN c.NEXT_ADMITTIME IS NOT NULL "
            "AND c.NEXT_ADMITTIME > c.DISCHTIME "
            f"AND c.NEXT_ADMITTIME <= c.DISCHTIME + {readmit_days} "
            "THEN 1 ELSE 0 END), 2) AS READMIT_RATE_PCT "
            "FROM cohort c"
        ),
        "mortality_rate_sql": (
            cte
            + "SELECT ROUND(100 * AVG(CASE WHEN c.HOSPITAL_EXPIRE_FLAG = 1 THEN 1 ELSE 0 END), 2) "
            "AS MORTALITY_RATE_PCT FROM cohort c"
        ),
        "avg_los_sql": (
            cte
            + "SELECT ROUND(AVG(CAST(c.DISCHTIME AS DATE) - CAST(c.ADMITTIME AS DATE)), 2) AS AVG_LOS_DAYS "
            "FROM cohort c"
        ),
        "median_los_sql": (
            cte
            + "SELECT ROUND(PERCENTILE_CONT(0.5) WITHIN GROUP "
            "(ORDER BY (CAST(c.DISCHTIME AS DATE) - CAST(c.ADMITTIME AS DATE))), 2) AS MEDIAN_LOS_DAYS "
            "FROM cohort c"
        ),
        "readmission_7d_rate_sql": (
            cte
            + "SELECT ROUND(100 * AVG(CASE "
            "WHEN c.NEXT_ADMITTIME IS NOT NULL "
            "AND c.NEXT_ADMITTIME > c.DISCHTIME "
            "AND c.NEXT_ADMITTIME <= c.DISCHTIME + 7 "
            "THEN 1 ELSE 0 END), 2) AS READMIT_7D_RATE_PCT "
            "FROM cohort c"
        ),
        "long_stay_rate_sql": (
            cte
            + "SELECT ROUND(100 * AVG(CASE "
            "WHEN (CAST(c.DISCHTIME AS DATE) - CAST(c.ADMITTIME AS DATE)) >= 14 THEN 1 "
            "ELSE 0 END), 2) AS LONG_STAY_RATE_PCT "
            "FROM cohort c"
        ),
        "icu_admission_rate_sql": (
            cte
            + "SELECT ROUND(100 * AVG(CASE "
            "WHEN EXISTS (SELECT 1 FROM ICUSTAYS i WHERE i.HADM_ID = c.HADM_ID) THEN 1 "
            "ELSE 0 END), 2) AS ICU_ADMISSION_RATE_PCT "
            "FROM cohort c"
        ),
        "er_admission_rate_sql": (
            cte
            + "SELECT ROUND(100 * AVG(CASE "
            "WHEN UPPER(NVL(c.ADMISSION_LOCATION, '')) LIKE '%EMERGENCY%' "
            "OR UPPER(NVL(c.ADMISSION_LOCATION, '')) LIKE '%ER%' "
            "OR UPPER(NVL(c.ADMISSION_LOCATION, '')) LIKE '%ED%' "
            "THEN 1 ELSE 0 END), 2) AS ER_ADMISSION_RATE_PCT "
            "FROM cohort c"
        ),
        "life_table_sql": (
            cte
            + "SELECT "
            "FLOOR(CAST(c.DISCHTIME AS DATE) - CAST(c.ADMITTIME AS DATE)) AS LOS_DAY, "
            "SUM(CASE WHEN c.HOSPITAL_EXPIRE_FLAG = 1 THEN 1 ELSE 0 END) AS EVENT_CNT, "
            "SUM(CASE WHEN c.HOSPITAL_EXPIRE_FLAG = 1 THEN 0 ELSE 1 END) AS CENSOR_CNT "
            "FROM cohort c "
            "WHERE c.ADMITTIME IS NOT NULL AND c.DISCHTIME IS NOT NULL "
            "AND (CAST(c.DISCHTIME AS DATE) - CAST(c.ADMITTIME AS DATE)) >= 0 "
            "GROUP BY FLOOR(CAST(c.DISCHTIME AS DATE) - CAST(c.ADMITTIME AS DATE)) "
            "ORDER BY LOS_DAY"
        ),
}


def _simulate_metrics_and_stats(params: CohortParams) -> tuple[dict[str, float], dict[str, float]]:
    sqls = _cohort_sql_bundle(params)
    result = execute_sql(sqls["metrics_sql"])
    rows = result.get("rows") or []
    first_row = rows[0] if rows and isinstance(rows[0], (list, tuple)) else []
    if not first_row:
        metrics = {
            "patient_count": 0.0,
            "readmission_rate": 0.0,
            "mortality_rate": 0.0,
            "avg_los_days": 0.0,
            "median_los_days": 0.0,
            "readmission_7d_rate": 0.0,
            "long_stay_rate": 0.0,
            "icu_admission_rate": 0.0,
            "er_admission_rate": 0.0,
        }
        stats = {
            "n_admissions": 0.0,
            "readmit_30_cnt": 0.0,
            "death_cnt": 0.0,
            "los_stddev_days": 0.0,
            "icu_admission_cnt": 0.0,
            "er_admission_cnt": 0.0,
            "readmit_7_cnt": 0.0,
            "long_stay_cnt": 0.0,
        }
        return metrics, stats

    patient_count = max(0.0, _to_float(first_row[0], default=0.0))
    readmission_rate = max(0.0, min(100.0, _to_float(first_row[1], default=0.0)))
    mortality_rate = max(0.0, min(100.0, _to_float(first_row[2], default=0.0)))
    avg_los_days = max(0.0, _to_float(first_row[3], default=0.0))
    median_los_days = max(0.0, _to_float(first_row[4], default=0.0))
    readmission_7d_rate = max(0.0, min(100.0, _to_float(first_row[5], default=0.0)))
    long_stay_rate = max(0.0, min(100.0, _to_float(first_row[6], default=0.0)))
    icu_admission_rate = max(0.0, min(100.0, _to_float(first_row[7], default=0.0)))
    er_admission_rate = max(0.0, min(100.0, _to_float(first_row[8], default=0.0)))
    n_admissions = max(0, _to_int(first_row[9], default=0))
    readmit_30_cnt = max(0, _to_int(first_row[10], default=0))
    death_cnt = max(0, _to_int(first_row[11], default=0))
    los_stddev_days = max(0.0, _to_float(first_row[12], default=0.0))
    icu_admission_cnt = max(0, _to_int(first_row[13], default=0))
    er_admission_cnt = max(0, _to_int(first_row[14], default=0))
    readmit_7_cnt = max(0, _to_int(first_row[15], default=0))
    long_stay_cnt = max(0, _to_int(first_row[16], default=0))

    metrics = {
        "patient_count": float(round(patient_count)),
        "readmission_rate": float(round(readmission_rate, 2)),
        "mortality_rate": float(round(mortality_rate, 2)),
        "avg_los_days": float(round(avg_los_days, 2)),
        "median_los_days": float(round(median_los_days, 2)),
        "readmission_7d_rate": float(round(readmission_7d_rate, 2)),
        "long_stay_rate": float(round(long_stay_rate, 2)),
        "icu_admission_rate": float(round(icu_admission_rate, 2)),
        "er_admission_rate": float(round(er_admission_rate, 2)),
    }
    stats = {
        "n_admissions": float(n_admissions),
        "readmit_30_cnt": float(readmit_30_cnt),
        "death_cnt": float(death_cnt),
        "los_stddev_days": float(los_stddev_days),
        "icu_admission_cnt": float(icu_admission_cnt),
        "er_admission_cnt": float(er_admission_cnt),
        "readmit_7_cnt": float(readmit_7_cnt),
        "long_stay_cnt": float(long_stay_cnt),
    }
    return metrics, stats


def _simulate_metrics(params: CohortParams) -> dict[str, float]:
    metrics, _ = _simulate_metrics_and_stats(params)
    return metrics


def _parse_subgroup_rows(rows: list[Any]) -> list[dict[str, Any]]:
    parsed: list[dict[str, Any]] = []
    for row in rows:
        if not isinstance(row, (list, tuple)) or len(row) < 7:
            continue
        parsed.append(
            {
                "key": str(row[0] or ""),
                "label": str(row[1] or ""),
                "admission_count": max(0, _to_int(row[2], default=0)),
                "patient_count": max(0, _to_int(row[3], default=0)),
                "readmission_rate": max(0.0, min(100.0, _to_float(row[4], default=0.0))),
                "mortality_rate": max(0.0, min(100.0, _to_float(row[5], default=0.0))),
                "avg_los_days": max(0.0, _to_float(row[6], default=0.0)),
            }
        )
    return parsed


def _simulate_subgroups(params: CohortParams) -> dict[str, list[dict[str, Any]]]:
    sqls = _cohort_sql_bundle(params)

    def run(sql_key: str) -> list[dict[str, Any]]:
        result = execute_sql(sqls[sql_key])
        rows = result.get("rows") or []
        return _parse_subgroup_rows(rows if isinstance(rows, list) else [])

    return {
        "age": run("age_subgroup_sql"),
        "gender": run("gender_subgroup_sql"),
        "comorbidity": run("comorbidity_subgroup_sql"),
    }


def _build_subgroup_comparison(
    current_subgroups: dict[str, list[dict[str, Any]]],
    simulated_subgroups: dict[str, list[dict[str, Any]]],
) -> dict[str, list[dict[str, Any]]]:
    def empty_metrics() -> dict[str, float]:
        return {
            "admission_count": 0.0,
            "patient_count": 0.0,
            "readmission_rate": 0.0,
            "mortality_rate": 0.0,
            "avg_los_days": 0.0,
        }

    def normalize_metrics(item: dict[str, Any] | None) -> dict[str, float]:
        if not item:
            return empty_metrics()
        return {
            "admission_count": float(_to_int(item.get("admission_count"), default=0)),
            "patient_count": float(_to_int(item.get("patient_count"), default=0)),
            "readmission_rate": float(_to_float(item.get("readmission_rate"), default=0.0)),
            "mortality_rate": float(_to_float(item.get("mortality_rate"), default=0.0)),
            "avg_los_days": float(_to_float(item.get("avg_los_days"), default=0.0)),
        }

    def merge_section(
        current_rows: list[dict[str, Any]],
        simulated_rows: list[dict[str, Any]],
    ) -> list[dict[str, Any]]:
        current_by_key = {str(item.get("key")): item for item in current_rows}
        simulated_by_key = {str(item.get("key")): item for item in simulated_rows}
        ordered_keys: list[str] = [str(item.get("key")) for item in current_rows]
        for item in simulated_rows:
            key = str(item.get("key"))
            if key not in ordered_keys:
                ordered_keys.append(key)

        merged: list[dict[str, Any]] = []
        for key in ordered_keys:
            current_item = current_by_key.get(key)
            simulated_item = simulated_by_key.get(key)
            current_metrics = normalize_metrics(current_item)
            simulated_metrics = normalize_metrics(simulated_item)
            label = str(
                (current_item or {}).get("label")
                or (simulated_item or {}).get("label")
                or key
            )
            delta_metrics = {
                "admission_count": int(round(simulated_metrics["admission_count"] - current_metrics["admission_count"])),
                "patient_count": int(round(simulated_metrics["patient_count"] - current_metrics["patient_count"])),
                "readmission_rate": float(round(simulated_metrics["readmission_rate"] - current_metrics["readmission_rate"], 2)),
                "mortality_rate": float(round(simulated_metrics["mortality_rate"] - current_metrics["mortality_rate"], 2)),
                "avg_los_days": float(round(simulated_metrics["avg_los_days"] - current_metrics["avg_los_days"], 2)),
            }
            merged.append(
                {
                    "key": key,
                    "label": label,
                    "current": {
                        "admission_count": int(round(current_metrics["admission_count"])),
                        "patient_count": int(round(current_metrics["patient_count"])),
                        "readmission_rate": float(round(current_metrics["readmission_rate"], 2)),
                        "mortality_rate": float(round(current_metrics["mortality_rate"], 2)),
                        "avg_los_days": float(round(current_metrics["avg_los_days"], 2)),
                    },
                    "simulated": {
                        "admission_count": int(round(simulated_metrics["admission_count"])),
                        "patient_count": int(round(simulated_metrics["patient_count"])),
                        "readmission_rate": float(round(simulated_metrics["readmission_rate"], 2)),
                        "mortality_rate": float(round(simulated_metrics["mortality_rate"], 2)),
                        "avg_los_days": float(round(simulated_metrics["avg_los_days"], 2)),
                    },
                    "delta": delta_metrics,
                }
            )
        return merged

    return {
        "age": merge_section(current_subgroups.get("age", []), simulated_subgroups.get("age", [])),
        "gender": merge_section(current_subgroups.get("gender", []), simulated_subgroups.get("gender", [])),
        "comorbidity": merge_section(current_subgroups.get("comorbidity", []), simulated_subgroups.get("comorbidity", [])),
    }


def _normal_cdf(value: float) -> float:
    return 0.5 * (1.0 + math.erf(value / math.sqrt(2.0)))


def _two_sided_p_from_z(value: float) -> float:
    return max(0.0, min(1.0, 2.0 * (1.0 - _normal_cdf(abs(value)))))


def _percentile(values: list[float], q: float) -> float:
    if not values:
        return 0.0
    if q <= 0:
        return float(values[0])
    if q >= 1:
        return float(values[-1])
    pos = (len(values) - 1) * q
    lo = int(math.floor(pos))
    hi = int(math.ceil(pos))
    if lo == hi:
        return float(values[lo])
    weight = pos - lo
    return float(values[lo] * (1.0 - weight) + values[hi] * weight)


def _cohen_h(p1: float, p2: float) -> float:
    p1 = min(1.0, max(0.0, p1))
    p2 = min(1.0, max(0.0, p2))
    return 2.0 * (math.asin(math.sqrt(p2)) - math.asin(math.sqrt(p1)))


def _cohen_d(mean1: float, mean2: float, sd1: float, sd2: float, n1: float, n2: float) -> float:
    if n1 <= 1 or n2 <= 1:
        return 0.0
    pooled_var = (((n1 - 1.0) * (sd1**2)) + ((n2 - 1.0) * (sd2**2))) / (n1 + n2 - 2.0)
    pooled_sd = math.sqrt(max(0.0, pooled_var))
    if pooled_sd <= 0:
        return 0.0
    return (mean2 - mean1) / pooled_sd


def _bootstrap_prop_diff(
    rng: random.Random,
    success_1: float,
    n_1: float,
    success_2: float,
    n_2: float,
    iterations: int,
) -> tuple[float, float]:
    if n_1 <= 0 or n_2 <= 0:
        return 0.0, 0.0
    diffs: list[float] = []
    a1 = max(1.0, success_1 + 1.0)
    b1 = max(1.0, (n_1 - success_1) + 1.0)
    a2 = max(1.0, success_2 + 1.0)
    b2 = max(1.0, (n_2 - success_2) + 1.0)
    for _ in range(iterations):
        p1 = rng.betavariate(a1, b1)
        p2 = rng.betavariate(a2, b2)
        diffs.append((p2 - p1) * 100.0)
    diffs.sort()
    return _percentile(diffs, 0.025), _percentile(diffs, 0.975)


def _bootstrap_mean_diff(
    rng: random.Random,
    mean_1: float,
    sd_1: float,
    n_1: float,
    mean_2: float,
    sd_2: float,
    n_2: float,
    iterations: int,
) -> tuple[float, float]:
    if n_1 <= 0 or n_2 <= 0:
        return 0.0, 0.0
    se1 = sd_1 / math.sqrt(max(1.0, n_1))
    se2 = sd_2 / math.sqrt(max(1.0, n_2))
    diffs: list[float] = []
    for _ in range(iterations):
        m1 = rng.gauss(mean_1, se1)
        m2 = rng.gauss(mean_2, se2)
        diffs.append(m2 - m1)
    diffs.sort()
    return _percentile(diffs, 0.025), _percentile(diffs, 0.975)


def _build_confidence_payload(
    current_metrics: dict[str, float],
    current_stats: dict[str, float],
    simulated_metrics: dict[str, float],
    simulated_stats: dict[str, float],
    baseline_params: CohortParams,
    simulated_params: CohortParams,
) -> dict[str, Any]:
    z_critical = 1.959963984540054
    alpha = 0.05
    iterations = 800
    seed_input = f"{baseline_params.model_dump_json()}::{simulated_params.model_dump_json()}"
    seed = int(hashlib.sha256(seed_input.encode("utf-8")).hexdigest()[:16], 16)
    rng = random.Random(seed)

    n1 = max(0.0, current_stats.get("n_admissions", 0.0))
    n2 = max(0.0, simulated_stats.get("n_admissions", 0.0))

    def build_prop_item(metric_key: str, label: str, count_key: str) -> dict[str, Any]:
        c1 = max(0.0, current_stats.get(count_key, 0.0))
        c2 = max(0.0, simulated_stats.get(count_key, 0.0))
        p1 = (c1 / n1) if n1 > 0 else 0.0
        p2 = (c2 / n2) if n2 > 0 else 0.0
        diff = (p2 - p1) * 100.0
        se = math.sqrt(max(0.0, (p1 * (1.0 - p1) / max(1.0, n1)) + (p2 * (1.0 - p2) / max(1.0, n2))))
        ci_low = diff - (z_critical * se * 100.0)
        ci_high = diff + (z_critical * se * 100.0)
        pooled = ((c1 + c2) / (n1 + n2)) if (n1 + n2) > 0 else 0.0
        se_pooled = math.sqrt(max(0.0, pooled * (1.0 - pooled) * ((1.0 / max(1.0, n1)) + (1.0 / max(1.0, n2)))))
        z = (p2 - p1) / se_pooled if se_pooled > 0 else 0.0
        p_value = _two_sided_p_from_z(z)
        effect_size = _cohen_h(p1, p2)
        boot_low, boot_high = _bootstrap_prop_diff(rng, c1, n1, c2, n2, iterations)
        return {
            "metric": metric_key,
            "label": label,
            "unit": "%",
            "current": float(round(current_metrics.get(metric_key, 0.0), 2)),
            "simulated": float(round(simulated_metrics.get(metric_key, 0.0), 2)),
            "difference": float(round(diff, 2)),
            "ci": [float(round(ci_low, 2)), float(round(ci_high, 2))],
            "p_value": float(round(p_value, 6)),
            "effect_size": float(round(effect_size, 4)),
            "effect_size_type": "cohen_h",
            "bootstrap_ci": [float(round(boot_low, 2)), float(round(boot_high, 2))],
            "significant": bool(p_value < alpha),
        }

    def build_mean_item(metric_key: str, label: str) -> dict[str, Any]:
        mean1 = float(current_metrics.get(metric_key, 0.0))
        mean2 = float(simulated_metrics.get(metric_key, 0.0))
        sd1 = max(0.0, float(current_stats.get("los_stddev_days", 0.0)))
        sd2 = max(0.0, float(simulated_stats.get("los_stddev_days", 0.0)))
        diff = mean2 - mean1
        se = math.sqrt(max(0.0, ((sd1**2) / max(1.0, n1)) + ((sd2**2) / max(1.0, n2))))
        ci_low = diff - (z_critical * se)
        ci_high = diff + (z_critical * se)
        z = diff / se if se > 0 else 0.0
        p_value = _two_sided_p_from_z(z)
        effect_size = _cohen_d(mean1, mean2, sd1, sd2, n1, n2)
        boot_low, boot_high = _bootstrap_mean_diff(rng, mean1, sd1, n1, mean2, sd2, n2, iterations)
        return {
            "metric": metric_key,
            "label": label,
            "unit": "days",
            "current": float(round(mean1, 2)),
            "simulated": float(round(mean2, 2)),
            "difference": float(round(diff, 2)),
            "ci": [float(round(ci_low, 2)), float(round(ci_high, 2))],
            "p_value": float(round(p_value, 6)),
            "effect_size": float(round(effect_size, 4)),
            "effect_size_type": "cohen_d",
            "bootstrap_ci": [float(round(boot_low, 2)), float(round(boot_high, 2))],
            "significant": bool(p_value < alpha),
        }

    metrics = [
        build_prop_item("readmission_rate", "재입원율(30일)", "readmit_30_cnt"),
        build_prop_item("readmission_7d_rate", "재입원율(7일)", "readmit_7_cnt"),
        build_prop_item("mortality_rate", "사망률", "death_cnt"),
        build_prop_item("long_stay_rate", "장기재원 비율(14일+)", "long_stay_cnt"),
        build_prop_item("icu_admission_rate", "ICU 입실 비율", "icu_admission_cnt"),
        build_prop_item("er_admission_rate", "응급실 입원 비율", "er_admission_cnt"),
        build_mean_item("avg_los_days", "평균 재원일수"),
    ]

    return {
        "method": "Wald CI + normal approximation p-value + effect size + parametric bootstrap",
        "alpha": alpha,
        "bootstrap_iterations": iterations,
        "n_current": int(round(n1)),
        "n_simulated": int(round(n2)),
        "metrics": metrics,
    }


def _life_table(params: CohortParams) -> list[tuple[float, float, float]]:
    sql = _cohort_sql_bundle(params)["life_table_sql"]
    result = execute_sql(sql)
    rows = result.get("rows") or []
    table: list[tuple[float, float, float]] = []
    for row in rows:
        if not isinstance(row, (list, tuple)) or len(row) < 3:
            continue
        day = _to_float(row[0], default=0.0)
        event_cnt = max(0.0, _to_float(row[1], default=0.0))
        censor_cnt = max(0.0, _to_float(row[2], default=0.0))
        table.append((day, event_cnt, censor_cnt))
    table.sort(key=lambda item: item[0])
    return table


def _km_curve_from_life_table(
    life_table: list[tuple[float, float, float]],
    time_points: list[int],
) -> list[float]:
    total = sum(event_cnt + censor_cnt for _, event_cnt, censor_cnt in life_table)
    if total <= 0:
        return [0.0 for _ in time_points]

    n_risk = total
    survival = 1.0
    idx = 0
    values: list[float] = []

    for t in time_points:
        while idx < len(life_table) and life_table[idx][0] <= t:
            _, event_cnt, censor_cnt = life_table[idx]
            if n_risk > 0 and event_cnt > 0:
                step = max(0.0, 1.0 - (event_cnt / n_risk))
                survival *= step
            n_risk = max(0.0, n_risk - event_cnt - censor_cnt)
            idx += 1
        values.append(round(max(0.0, min(100.0, survival * 100.0)), 1))
    return values


def _build_survival_payload(
    current_params: CohortParams,
    simulated_params: CohortParams,
) -> list[dict[str, float]]:
    points = _SURVIVAL_TIME_POINTS
    if current_params.model_dump() == simulated_params.model_dump():
        current_values = _km_curve_from_life_table(_life_table(current_params), points)
        simulated_values = list(current_values)
    else:
        current_values = _km_curve_from_life_table(_life_table(current_params), points)
        simulated_values = _km_curve_from_life_table(_life_table(simulated_params), points)
    return [
        {
            "time": float(day),
            "current": float(current_values[idx]),
            "simulated": float(simulated_values[idx]),
        }
        for idx, day in enumerate(points)
    ]


def _get_saved_cohorts() -> list[dict[str, Any]]:
    store = get_state_store()
    if not store.enabled:
        return list(_FALLBACK_SAVED_COHORTS)
    payload = store.get(_SAVED_COHORTS_KEY) or {}
    cohorts = payload.get("cohorts", []) if isinstance(payload, dict) else []
    return cohorts if isinstance(cohorts, list) else []


def _set_saved_cohorts(cohorts: list[dict[str, Any]]) -> None:
    store = get_state_store()
    if not store.enabled:
        _FALLBACK_SAVED_COHORTS.clear()
        _FALLBACK_SAVED_COHORTS.extend(cohorts)
        return
    ok = store.set(_SAVED_COHORTS_KEY, {"cohorts": cohorts})
    if not ok:
        raise HTTPException(status_code=500, detail="Failed to persist cohorts")


@router.post("/simulate")
def cohort_simulate(req: SimulationRequest):
    simulated_params = req.params
    simulated_metrics, simulated_stats = _simulate_metrics_and_stats(simulated_params)
    simulated_subgroups = _simulate_subgroups(simulated_params)

    if req.include_baseline:
        baseline_params = CohortParams(**DEFAULT_PARAMS)
        current_metrics, current_stats = _simulate_metrics_and_stats(baseline_params)
        current_subgroups = _simulate_subgroups(baseline_params)
    else:
        baseline_params = simulated_params
        current_metrics = simulated_metrics
        current_stats = simulated_stats
        current_subgroups = simulated_subgroups

    survival = _build_survival_payload(baseline_params, simulated_params)
    confidence = _build_confidence_payload(
        current_metrics=current_metrics,
        current_stats=current_stats,
        simulated_metrics=simulated_metrics,
        simulated_stats=simulated_stats,
        baseline_params=baseline_params,
        simulated_params=simulated_params,
    )
    subgroups = _build_subgroup_comparison(current_subgroups, simulated_subgroups)
    return {
        "params": simulated_params.model_dump(),
        "baseline_params": baseline_params.model_dump(),
        "current": current_metrics,
        "simulated": simulated_metrics,
        "survival": survival,
        "confidence": confidence,
        "subgroups": subgroups,
    }


@router.post("/sql")
def cohort_sql(req: CohortSqlRequest):
    params = req.params
    return {
        "params": params.model_dump(),
        "sample_rows": _cohort_sample_rows(),
        "sql": _cohort_sql_bundle(params),
    }


@router.get("/saved")
def list_saved_cohorts():
    cohorts = _get_saved_cohorts()
    cohorts.sort(key=lambda item: str(item.get("created_at") or ""), reverse=True)
    return {"cohorts": cohorts}


@router.post("/saved")
def save_cohort(req: SaveCohortRequest):
    params = req.params
    metrics = _simulate_metrics(params)
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    cohort = {
        "id": str(uuid.uuid4()),
        "name": req.name.strip(),
        "created_at": now,
        "status": req.status,
        "params": params.model_dump(),
        "metrics": metrics,
    }
    cohorts = _get_saved_cohorts()
    cohorts.append(cohort)
    _set_saved_cohorts(cohorts)
    return {"ok": True, "cohort": cohort}


@router.delete("/saved/{cohort_id}")
def delete_saved_cohort(cohort_id: str):
    cohorts = _get_saved_cohorts()
    next_cohorts = [item for item in cohorts if str(item.get("id")) != cohort_id]
    if len(next_cohorts) == len(cohorts):
        raise HTTPException(status_code=404, detail="Cohort not found")
    _set_saved_cohorts(next_cohorts)
    return {"ok": True, "count": len(next_cohorts)}
