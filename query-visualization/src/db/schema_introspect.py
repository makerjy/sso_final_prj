"""DataFrame/Oracle 메타데이터로 스키마 요약(df_schema)을 만드는 유틸."""
from __future__ import annotations

from typing import Any, Dict, List, Optional

import pandas as pd
from pandas.api import types as pdt

from src.db.oracle_client import get_connection


# 입력: value
# 출력: JSON 직렬화가 가능한 값
# JSON 직렬화가 어려운 값은 문자열로 변환
def _safe_value(value: Any) -> Any:
    # JSON 직렬화가 어려운 값은 문자열로 변환
    if isinstance(value, (int, float, str, bool)) or value is None:
        return value
    return str(value)

# 입력: name
# 출력: 시간 컬럼 여부 추정(bool)
# 컬럼이 시간축(x축)이 될 수 있는지를 자동으로 판단하기 위함
# 하나의 힌트로 이름 기반 추정을 수행


def _infer_time_by_name(name: str) -> bool:
    # 컬럼명 기반으로 시간 컬럼 여부를 추정
    hints = ("date", "time", "day", "month", "year", "dt", "timestamp")
    return any(h in name.lower() for h in hints)

# _infer_time_by_name 함수로 컬럼 이름 검사 후 2차 검사
# 입력: series, sample_size
# 출력: 시간 컬럼 여부 추정(bool)
# 샘플 중 하나라도 any() datetime으로 파싱되면 시간 컬럼으로 추정


def _infer_time_by_sample(series: pd.Series, sample_size: int = 20) -> bool:
    # dropna 후 20개만 샘플링
    sample = series.dropna().head(sample_size)
    if sample.empty:
        return False
    parsed = pd.to_datetime(sample, errors="coerce")
    return parsed.notna().any()

# 입력: row_count
# 출력: 범주형 판단 기준 유니크 수(int)
# 범주형 판단 기준: 행 수의 20% 이내이되 10~50 범위


def _categorical_threshold(row_count: int) -> int:
    if row_count <= 0:
        return 0
    return max(10, min(50, int(row_count * 0.2)))

# 입력: name, series, unique_count, row_count
# 출력: 컬럼 역할 분류 문자열(time/numeric/categorical/...)
# dtype/이름/유니크 수를 조합해 역할 분류(time/numeric/categorical/...)
# 흐름 - datetime dtype -> 이름/샘플 기반 시간 컬럼 추정 -> bool dtype -> numeric dtype
#     -> object/string dtype + 유니크 수 기반 범주형/텍스트/


def _infer_column_role(
    name: str,
    series: pd.Series,
    unique_count: int,
    row_count: int,
) -> str:
    # dtype/이름/유니크 수를 조합해 역할 분류(time/numeric/categorical/...)
    if pdt.is_datetime64_any_dtype(series):
        return "time"
    if _infer_time_by_name(name) and _infer_time_by_sample(series):
        return "time"
    if pdt.is_bool_dtype(series):
        return "boolean"
    if pdt.is_numeric_dtype(series):
        return "numeric"

    # categorical vs text 판단
    # categorical: 유니크 수가 행 수의 20% 이내이되 10~50 범위
    # text: 그 외 (시각화 대상 아님)
    if pdt.is_object_dtype(series) or pdt.is_string_dtype(series):
        threshold = _categorical_threshold(row_count)
        if unique_count <= threshold:
            return "categorical"
        return "text"

    return "other"

# 입력: 없음
# 출력: 역할별 컬럼 리스트 딕셔너리
# 역할별 컬럼 리스트 초기화


def _init_roles_dict() -> Dict[str, List[str]]:
    # 역할별 컬럼 리스트 초기화
    return {
        "time": [],
        "numeric": [],
        "categorical": [],
        "boolean": [],
        "text": [],
        "other": [],
    }

# 입력: df, sample_size
# 출력: df_schema 요약 딕셔너리
# DataFrame에서 df_schema 요약을 생성


def summarize_dataframe_schema(df: pd.DataFrame, sample_size: int = 3) -> Dict[str, Any]:
    """DataFrame에서 df_schema 요약을 생성한다."""
    rows = int(len(df))
    columns = list(df.columns)
    dtypes = {col: str(dtype) for col, dtype in df.dtypes.items()}

    unique_counts: Dict[str, int] = {}
    null_counts: Dict[str, int] = {}
    examples: Dict[str, List[Any]] = {}
    inferred_types: Dict[str, str] = {}
    roles = _init_roles_dict()

    for col in columns:
        series = df[col]
        unique_count = int(series.nunique(dropna=True))
        unique_counts[col] = unique_count
        null_counts[col] = int(series.isna().sum())
        sample = series.dropna().head(sample_size).tolist()
        examples[col] = [_safe_value(v) for v in sample]

        role = _infer_column_role(col, series, unique_count, rows)
        inferred_types[col] = role
        roles[role].append(col)

    return {
        "source": "dataframe",  # 요약 정보 출처
        "columns": columns,  # 컬럼 리스트
        "dtypes": dtypes,  # 컬럼별 데이터 타입
        "rows": rows,  # 총 행 수
        "unique_counts": unique_counts,  # 컬럼별 유니크 값 수
        "null_counts": null_counts,  # 컬럼별 널 값 수
        "examples": examples,  # 컬럼별 예시 값
        "inferred_types": inferred_types,  # 컬럼별 추정 타입
        "column_roles": roles,  # 역할별 컬럼 리스트
    }

# 입력 : data_type, length, precision, scale
# 출력 : 사람이 읽기 쉬운 Oracle 타입 문자열


def _format_oracle_type(
    data_type: str,
    length: Optional[int],
    precision: Optional[int],
    scale: Optional[int],
) -> str:
    # Oracle 타입을 사람이 읽기 쉬운 문자열로 정규화
    if data_type in ("VARCHAR2", "NVARCHAR2", "CHAR", "NCHAR") and length:
        return f"{data_type}({length})"
    if data_type == "NUMBER" and precision is not None:
        if scale is not None:
            return f"{data_type}({precision},{scale})"
        return f"{data_type}({precision})"
    return data_type

# 입력: name, data_type
# 출력: 컬럼 역할 분류 문자열(time/numeric/categorical/...)


def _infer_oracle_role(name: str, data_type: str) -> str:
    # Oracle 데이터 타입 기반 역할 분류
    upper = data_type.upper()
    if upper in ("DATE", "TIMESTAMP", "TIMESTAMP WITH TIME ZONE", "TIMESTAMP WITH LOCAL TIME ZONE"):
        return "time"
    if upper in ("NUMBER", "FLOAT", "BINARY_FLOAT", "BINARY_DOUBLE"):
        return "numeric"
    if upper in ("CHAR", "NCHAR", "VARCHAR2", "NVARCHAR2"):
        return "categorical"
    if _infer_time_by_name(name):
        return "time"
    return "other"


# 입력: table_name, owner
# 출력: df_schema 요약 딕셔너리
def summarize_oracle_schema(
    table_name: str,
    owner: Optional[str] = None,
) -> Dict[str, Any]:
    """Oracle 메타데이터에서 df_schema 요약을 생성한다."""
    table = table_name.upper()
    owner_upper = owner.upper() if owner else None

    sql = """
        SELECT
            c.column_name,
            c.data_type,
            c.data_length,
            c.data_precision,
            c.data_scale,
            c.nullable,
            s.num_distinct,
            s.num_nulls
        FROM all_tab_columns c
        LEFT JOIN all_tab_col_statistics s
            ON s.owner = c.owner
           AND s.table_name = c.table_name
           AND s.column_name = c.column_name
        WHERE c.table_name = :table_name
          AND (:owner IS NULL OR c.owner = :owner)
        ORDER BY c.column_id
    """

    rows = None
    with get_connection() as conn:
        cur = conn.cursor()
        # 컬럼 메타정보 + 통계(유니크/널) 조회
        cur.execute(sql, {"table_name": table, "owner": owner_upper})
        results = cur.fetchall()

        # 테이블 전체 row 수 통계 조회
        cur.execute(
            """
            SELECT num_rows
            FROM all_tables
            WHERE table_name = :table_name
              AND (:owner IS NULL OR owner = :owner)
            """,
            {"table_name": table, "owner": owner_upper},
        )
        table_stats = cur.fetchone()
        if table_stats and table_stats[0] is not None:
            rows = int(table_stats[0])

    columns: List[str] = []
    dtypes: Dict[str, str] = {}
    unique_counts: Dict[str, int] = {}
    null_counts: Dict[str, int] = {}
    inferred_types: Dict[str, str] = {}
    roles = _init_roles_dict()

    for (
        column_name,
        data_type,
        data_length,
        data_precision,
        data_scale,
        _nullable,
        num_distinct,
        num_nulls,
    ) in results:
        name = column_name.lower()
        columns.append(name)

        dtype = _format_oracle_type(
            data_type,
            int(data_length) if data_length is not None else None,
            int(data_precision) if data_precision is not None else None,
            int(data_scale) if data_scale is not None else None,
        )
        dtypes[name] = dtype

        if num_distinct is not None:
            unique_counts[name] = int(num_distinct)
        if num_nulls is not None:
            null_counts[name] = int(num_nulls)

        role = _infer_oracle_role(name, data_type)
        inferred_types[name] = role
        roles[role].append(name)

    return {
        "source": "oracle",
        "columns": columns,
        "dtypes": dtypes,
        "rows": rows,
        "unique_counts": unique_counts,
        "null_counts": null_counts,
        "examples": {},
        "inferred_types": inferred_types,
        "column_roles": roles,
    }
