from __future__ import annotations

from typing import Any, Iterable
import re

from app.core.config import get_settings
from app.services.runtime.diagnosis_map_store import match_diagnosis_mappings
from app.services.runtime.sql_postprocess_rules_store import load_sql_postprocess_rules
from app.services.runtime.sql_schema_hints_store import load_sql_schema_hints

_COUNT_RE = re.compile(r"^Count rows in ([A-Za-z0-9_]+) \(sampled\)$", re.IGNORECASE)
_SAMPLE_RE = re.compile(r"^Show sample ([A-Za-z0-9_]+) rows with (.+)$", re.IGNORECASE)
_DISTINCT_RE = re.compile(
    r"^List distinct values of ([A-Za-z0-9_]+) in ([A-Za-z0-9_]+) \(sample\)$",
    re.IGNORECASE,
)
_IDENT_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_$#]*$")
_LIMIT_RE = re.compile(r"\blimit\s+(\d+)\s*;?\s*$", re.IGNORECASE)
_FETCH_RE = re.compile(r"\bfetch\s+first\s+(\d+)\s+rows\s+only\s*;?\s*$", re.IGNORECASE)
_TOP_RE = re.compile(r"^\s*select\s+top\s+(\d+)\s+", re.IGNORECASE)
_WHERE_TRUE_RE = re.compile(r"\bwhere\s+true\b", re.IGNORECASE)
_AND_TRUE_RE = re.compile(r"\band\s+true\b", re.IGNORECASE)
_INTERVAL_YEAR_RE = re.compile(r"interval\s+'(\d+)\s*year[s]?'", re.IGNORECASE)
_INTERVAL_MONTH_RE = re.compile(r"interval\s+'(\d+)\s*month[s]?'", re.IGNORECASE)
_INTERVAL_DAY_RE = re.compile(r"interval\s+'(\d+)\s*day[s]?'", re.IGNORECASE)
_TO_DATE_RE = re.compile(r"TO_DATE\s*\(\s*([A-Za-z0-9_\\.]+)\s*,\s*'[^']+'\s*\)", re.IGNORECASE)
_HAVING_WHERE_RE = re.compile(r"\bHAVING\s+WHERE\b", re.IGNORECASE)
_HAVING_TRUE_RE = re.compile(r"\bHAVING\s+1\s*=\s*1\b", re.IGNORECASE)
_EXTRACT_DAY_RE = re.compile(r"EXTRACT\s*\(\s*DAY\s+FROM\s+([^)]+)\)", re.IGNORECASE)
_COUNT_ALIAS_RE = re.compile(r"(COUNT\s*\([^)]*\)\s*(?:AS\s+)?)([A-Za-z_][A-Za-z0-9_$#]*)", re.IGNORECASE)
_HOSPITAL_EXPIRE_RE = re.compile(r"\bHOSPITAL_EXPIRE_FLAG\s+IS\s+NOT\s+NULL\b", re.IGNORECASE)
_AGE_FROM_ANCHOR_RE = re.compile(
    r"EXTRACT\s*\(\s*YEAR\s+FROM\s+(?:CURRENT_DATE|SYSDATE)\s*\)\s*-\s*([A-Za-z0-9_\\.]*ANCHOR_YEAR)",
    re.IGNORECASE,
)
_AGE_FROM_BIRTHDATE_RE = re.compile(
    r"EXTRACT\s*\(\s*YEAR\s+FROM\s+(?:CURRENT_DATE|SYSDATE)\s*\)\s*-\s*EXTRACT\s*\(\s*YEAR\s+FROM\s+([A-Za-z0-9_\\.]*BIRTHDATE)\s*\)",
    re.IGNORECASE,
)
_BIRTH_YEAR_DIFF_RE = re.compile(r"([A-Za-z0-9_\\.]*ANCHOR_YEAR)\s*-\s*([A-Za-z0-9_\\.]*BIRTH_YEAR)", re.IGNORECASE)
_BIRTH_YEAR_RE = re.compile(r"\bBIRTH_YEAR\b", re.IGNORECASE)
_ORDER_BY_BAD_COUNT_RE = re.compile(
    r"\border\s+by\s+(?:count\(\*\)\s+)?"
    r"(?:label_count|test_count|organism_count|transition_count|event_count|admission_count|patient_count|transfer_count|count)\b",
    re.IGNORECASE,
)
_FOR_UPDATE_RE = re.compile(r"\bFOR\s+UPDATE\b(?:\s+SKIP\s+LOCKED)?", re.IGNORECASE)
_TIME_WINDOW_RE = re.compile(
    r"\b([A-Za-z0-9_\\.]*TIME)\b\s*(>=|>)\s*(SYSDATE|CURRENT_DATE)"
    r"(?:\s*-\s*INTERVAL\s*'[^']+'\s*(DAY|MONTH|YEAR))?"
    r"(?:\s+AND\s+\1\s*<=\s*(SYSDATE|CURRENT_DATE)"
    r"(?:\s*-\s*INTERVAL\s*'[^']+'\s*(DAY|MONTH|YEAR))?)?",
    re.IGNORECASE,
)
_DIAGNOSIS_TITLE_FILTER_RE = re.compile(
    r"(?:UPPER|LOWER)?\s*\(\s*(?:[A-Za-z0-9_]+\.)?LONG_TITLE\s*\)\s+(?:LIKE|=)\s+'[^']+'"
    r"|(?:[A-Za-z0-9_]+\.)?LONG_TITLE\s+(?:LIKE|=)\s+'[^']+'",
    re.IGNORECASE,
)
_ICD_CODE_LIKE_RE = re.compile(
    r"(?P<lhs>(?:[A-Za-z0-9_]+\.)?ICD_CODE)\s+LIKE\s+(?P<quote>'?)(?P<prefix>[A-Za-z0-9]+)%(?P=quote)",
    re.IGNORECASE,
)
_TO_CHAR_BARE_FMT_RE = re.compile(
    r"TO_CHAR\s*\(\s*(?P<expr>[^,]+?)\s*,\s*(?P<fmt>YYYY|YYY|YY|Y|MM|MON|MONTH|DD|HH24|MI|SS)\s*\)",
    re.IGNORECASE,
)

def _schema_hints() -> dict[str, Any]:
    return load_sql_schema_hints()


def _table_aliases() -> dict[str, str]:
    value = _schema_hints().get("table_aliases")
    return value if isinstance(value, dict) else {}


def _column_aliases() -> dict[str, str]:
    value = _schema_hints().get("column_aliases")
    return value if isinstance(value, dict) else {}


def _patients_only_cols() -> set[str]:
    value = _schema_hints().get("patients_only_cols")
    return value if isinstance(value, set) else set()


def _admissions_only_cols() -> set[str]:
    value = _schema_hints().get("admissions_only_cols")
    return value if isinstance(value, set) else set()


def _tables_with_subject_id() -> set[str]:
    value = _schema_hints().get("tables_with_subject_id")
    return value if isinstance(value, set) else set()


def _tables_with_hadm_id() -> set[str]:
    value = _schema_hints().get("tables_with_hadm_id")
    return value if isinstance(value, set) else set()


def _micro_only_cols() -> set[str]:
    value = _schema_hints().get("micro_only_cols")
    return value if isinstance(value, set) else set()


def _timestamp_cols() -> set[str]:
    value = _schema_hints().get("timestamp_cols")
    return value if isinstance(value, set) else set()

_HAS_ICU_RE = re.compile(r"\bHAS_ICU_STAY\b\s*=\s*(?:'Y'|1|TRUE)", re.IGNORECASE)
_ICU_STAY_RE = re.compile(r"\bICU_STAY\b\s*=\s*(?:'Y'|'YES'|1|TRUE)", re.IGNORECASE)
_ICUSTAYS_FLAG_RE = re.compile(r"\bICUSTAYS\b\s*=\s*(?:'Y'|'YES'|1|TRUE)", re.IGNORECASE)
_ICUSTAYS_NOT_NULL_RE = re.compile(r"\bICUSTAYS\b\s+IS\s+NOT\s+NULL", re.IGNORECASE)
_DIFF_RE = re.compile(r"([A-Za-z0-9_\\.]+)\s*-\s*([A-Za-z0-9_\\.]+)")
_TS_DIFF_RE = re.compile(r"TIMESTAMPDIFF\s*\(\s*DAY\s*,\s*([A-Za-z0-9_\\.]+)\s*,\s*([A-Za-z0-9_\\.]+)\s*\)", re.IGNORECASE)
_EXTRACT_YEAR_RE = re.compile(r"EXTRACT\s*\(\s*YEAR\s+FROM\s+([A-Za-z0-9_\\.]+)\s*\)", re.IGNORECASE)
_OUTER_ROWNUM_RE = re.compile(
    r"^\s*SELECT\s+\*\s+FROM\s*\((SELECT .*?)\)\s*WHERE\s+ROWNUM\s*<=\s*(\d+)\s*;?\s*$",
    re.IGNORECASE | re.DOTALL,
)
_ABS_YEAR_RE = re.compile(r"(?<!\d)(?:19|20|21)\d{2}(?!\d)")
_SYSDATE_YEAR_DIFF_RE = re.compile(
    r"\(\s*(?:SYSDATE|CURRENT_DATE)\s*-\s*(?:CAST\s*\(\s*)?([A-Za-z0-9_\\.]+)"
    r"(?:\s+AS\s+DATE\s*\))?\s*\)\s*/\s*365(?:\.25)?",
    re.IGNORECASE,
)
_ADD_MONTHS_PRED_RE = re.compile(
    r"([A-Za-z0-9_\\.]+)\s*(>=|>|<=|<)\s*ADD_MONTHS\s*\(\s*(?:SYSDATE|CURRENT_DATE)\s*,\s*[-+]?\d+\s*\*\s*12\s*\)",
    re.IGNORECASE,
)


def _parse_columns(text: str) -> list[str]:
    cleaned = re.sub(r"\s+and\s+", ",", text.strip(), flags=re.IGNORECASE)
    cols = [c.strip() for c in cleaned.split(",") if c.strip()]
    if not cols:
        return []
    if any(not _IDENT_RE.fullmatch(c) for c in cols):
        return []
    return cols


def _first(items: Iterable[str]) -> str | None:
    for item in items:
        return item
    return None


def _find_table_alias(text: str, table: str) -> str | None:
    pattern = re.compile(rf"\b(from|join)\s+{re.escape(table)}(?:\s+([A-Za-z0-9_]+))?", re.IGNORECASE)
    match = pattern.search(text)
    if not match:
        return None
    alias = match.group(2) or table
    if alias.upper() in {"WHERE", "JOIN", "GROUP", "ORDER"}:
        return table
    return alias


def _insert_join(text: str, join_clause: str) -> str:
    if re.search(r"\bwhere\b", text, re.IGNORECASE):
        return re.sub(r"\bwhere\b", join_clause + " WHERE", text, count=1, flags=re.IGNORECASE)
    if re.search(r"\bgroup\s+by\b", text, re.IGNORECASE):
        return re.sub(r"\bgroup\s+by\b", join_clause + " GROUP BY", text, count=1, flags=re.IGNORECASE)
    if re.search(r"\border\s+by\b", text, re.IGNORECASE):
        return re.sub(r"\border\s+by\b", join_clause + " ORDER BY", text, count=1, flags=re.IGNORECASE)
    return text.rstrip(";") + join_clause


def _inject_join_in_outer(
    sql: str,
    base_table: str,
    join_template: str,
    replace_from: str,
    replace_to: str,
) -> tuple[str | None, list[str]]:
    rules: list[str] = []
    match = _OUTER_ROWNUM_RE.match(sql)
    if not match:
        return None, rules
    inner = match.group(1)
    limit = match.group(2)

    pattern = re.compile(rf"\bfrom\s+{re.escape(base_table)}(?:\s+([A-Za-z0-9_]+))?", re.IGNORECASE)
    m = pattern.search(inner)
    if not m:
        return None, rules
    alias = m.group(1) or base_table
    if alias.upper() in {"WHERE", "JOIN", "GROUP", "ORDER"}:
        alias = base_table

    base_clause = f"FROM {base_table}"
    if m.group(1):
        base_clause = f"FROM {base_table} {alias}"
    join_clause = join_template.format(alias=alias)

    inner = pattern.sub(base_clause + join_clause, inner, count=1)
    inner = re.sub(replace_from, replace_to, inner, flags=re.IGNORECASE)
    rules.append("inject_join_in_outer")
    return f"SELECT * FROM ({inner}) WHERE ROWNUM <= {limit}", rules


def _wrap_with_rownum(sql: str, n: int) -> str:
    core = sql.strip().rstrip(";")
    return f"SELECT * FROM ({core}) WHERE ROWNUM <= {n}"


def _apply_rownum_cap(sql: str, cap: int = 100000) -> tuple[str, list[str]]:
    rules: list[str] = []
    text = sql

    def _inject_cap(inner_sql: str) -> str:
        if re.search(r"\bwhere\b", inner_sql, re.IGNORECASE):
            return re.sub(r"\bwhere\b", f"WHERE ROWNUM <= {cap} AND", inner_sql, count=1, flags=re.IGNORECASE)
        if re.search(r"\bgroup\s+by\b", inner_sql, re.IGNORECASE):
            return re.sub(
                r"\bgroup\s+by\b",
                f"WHERE ROWNUM <= {cap} GROUP BY",
                inner_sql,
                count=1,
                flags=re.IGNORECASE,
            )
        if re.search(r"\border\s+by\b", inner_sql, re.IGNORECASE):
            return re.sub(
                r"\border\s+by\b",
                f"WHERE ROWNUM <= {cap} ORDER BY",
                inner_sql,
                count=1,
                flags=re.IGNORECASE,
            )
        return inner_sql.rstrip(";") + f" WHERE ROWNUM <= {cap}"

    if "ROWNUM" in text.upper():
        match = _OUTER_ROWNUM_RE.match(text)
        if match:
            inner = match.group(1)
            limit = match.group(2)
            heavy_tables = {
                "LABEVENTS",
                "CHARTEVENTS",
                "MICROBIOLOGYEVENTS",
                "INPUTEVENTS",
                "OUTPUTEVENTS",
                "EMAR",
                "PRESCRIPTIONS",
            }
            if "ROWNUM" not in inner.upper() and any(
                re.search(rf"\b{t}\b", inner, re.IGNORECASE) for t in heavy_tables
            ):
                inner = _inject_cap(inner)
                rules.append(f"rownum_cap_inner_{cap}")
                return f"SELECT * FROM ({inner}) WHERE ROWNUM <= {limit}", rules
        return text, rules
    heavy_tables = {
        "LABEVENTS",
        "CHARTEVENTS",
        "MICROBIOLOGYEVENTS",
        "INPUTEVENTS",
        "OUTPUTEVENTS",
        "EMAR",
        "PRESCRIPTIONS",
    }
    if not any(re.search(rf"\b{t}\b", text, re.IGNORECASE) for t in heavy_tables):
        return text, rules
    if re.search(r"\bwhere\b", text, re.IGNORECASE):
        text = re.sub(r"\bwhere\b", f"WHERE ROWNUM <= {cap} AND", text, count=1, flags=re.IGNORECASE)
    else:
        # Insert WHERE before GROUP BY / ORDER BY if present
        if re.search(r"\bgroup\s+by\b", text, re.IGNORECASE):
            text = re.sub(r"\bgroup\s+by\b", f"WHERE ROWNUM <= {cap} GROUP BY", text, count=1, flags=re.IGNORECASE)
        elif re.search(r"\border\s+by\b", text, re.IGNORECASE):
            text = re.sub(r"\border\s+by\b", f"WHERE ROWNUM <= {cap} ORDER BY", text, count=1, flags=re.IGNORECASE)
        else:
            text = text.rstrip(";") + f" WHERE ROWNUM <= {cap}"
    rules.append(f"rownum_cap_{cap}")
    return text, rules


def _rewrite_oracle_syntax(sql: str) -> tuple[str, list[str]]:
    rules: list[str] = []
    text = sql

    # Replace WHERE TRUE / AND TRUE with Oracle-friendly boolean
    if _WHERE_TRUE_RE.search(text):
        text = _WHERE_TRUE_RE.sub("WHERE 1=1", text)
        rules.append("where_true_to_1eq1")
    if _AND_TRUE_RE.search(text):
        text = _AND_TRUE_RE.sub("AND 1=1", text)
        rules.append("and_true_to_1eq1")
    if "WHERE" not in text.upper() and re.search(r"\b1=1\b", text):
        text = re.sub(r"\b1=1\b", "WHERE 1=1", text, count=1, flags=re.IGNORECASE)
        rules.append("insert_where_for_1eq1")

    # Preserve JOIN location if WHERE is injected after an outer join rewrite
    text = re.sub(r"\bJOIN\b\s+(.*)\s+WHERE\s+1=1", r"JOIN \\1 WHERE 1=1", text, flags=re.IGNORECASE)

    # Normalize INTERVAL literals (Oracle expects INTERVAL 'n' YEAR|MONTH|DAY)
    if _INTERVAL_YEAR_RE.search(text):
        text = _INTERVAL_YEAR_RE.sub(r"INTERVAL '\1' YEAR", text)
        rules.append("interval_year_normalized")
    if _INTERVAL_MONTH_RE.search(text):
        text = _INTERVAL_MONTH_RE.sub(r"INTERVAL '\1' MONTH", text)
        rules.append("interval_month_normalized")
    if _INTERVAL_DAY_RE.search(text):
        text = _INTERVAL_DAY_RE.sub(r"INTERVAL '\1' DAY", text)
        rules.append("interval_day_normalized")

    # LIMIT / FETCH FIRST / TOP -> ROWNUM wrapper
    if _LIMIT_RE.search(text):
        n = int(_LIMIT_RE.search(text).group(1))
        text = _LIMIT_RE.sub("", text).rstrip()
        if "ROWNUM" not in text.upper():
            text = _wrap_with_rownum(text, n)
            rules.append("limit_to_rownum")
    if _FETCH_RE.search(text):
        n = int(_FETCH_RE.search(text).group(1))
        text = _FETCH_RE.sub("", text).rstrip()
        if "ROWNUM" not in text.upper():
            text = _wrap_with_rownum(text, n)
            rules.append("fetch_first_to_rownum")
    if _TOP_RE.search(text):
        n = int(_TOP_RE.search(text).group(1))
        text = _TOP_RE.sub("SELECT ", text, count=1)
        if "ROWNUM" not in text.upper():
            text = _wrap_with_rownum(text, n)
            rules.append("top_to_rownum")

    return text, rules


def _apply_schema_mappings(sql: str) -> tuple[str, list[str]]:
    rules: list[str] = []
    text = sql
    rules_cfg = load_sql_postprocess_rules().get("schema_aliases", {})
    table_aliases_cfg = rules_cfg.get("table_aliases", {})
    column_aliases_cfg = rules_cfg.get("column_aliases", {})
    table_aliases = dict(_table_aliases())
    column_aliases = dict(_column_aliases())
    if isinstance(table_aliases_cfg, dict):
        table_aliases.update({
            str(src): str(dest)
            for src, dest in table_aliases_cfg.items()
            if str(src).strip() and str(dest).strip()
        })
    if isinstance(column_aliases_cfg, dict):
        column_aliases.update({
            str(src): str(dest)
            for src, dest in column_aliases_cfg.items()
            if str(src).strip() and str(dest).strip()
        })

    # Table name replacements (case-insensitive, word boundaries)
    for src, dest in table_aliases.items():
        pattern = re.compile(rf"\b{re.escape(src)}\b", re.IGNORECASE)
        if pattern.search(text):
            text = pattern.sub(dest, text)
            rules.append(f"table:{src}->{dest}")

    # Column name replacements (case-insensitive, word boundaries)
    for src, dest in column_aliases.items():
        pattern = re.compile(rf"\b{re.escape(src)}\b", re.IGNORECASE)
        if pattern.search(text):
            text = pattern.sub(dest, text)
            rules.append(f"column:{src}->{dest}")

    return text, rules


def _ensure_patients_join(sql: str) -> tuple[str, list[str]]:
    rules: list[str] = []
    text = sql

    # Skip if PATIENTS already referenced
    if re.search(r"\bPATIENTS\b", text, re.IGNORECASE):
        return text, rules

    # Trigger only if patients-only columns appear unqualified
    needed = [c for c in _patients_only_cols() if re.search(rf"(?<!\.)\b{c}\b", text, re.IGNORECASE)]
    if not needed:
        return text, rules

    # Find base FROM table and optional alias (simple SQL only)
    m = re.search(r"\bfrom\s+([A-Za-z0-9_]+)(?:\s+([A-Za-z0-9_]+))?", text, re.IGNORECASE)
    if not m:
        return text, rules
    base_table = m.group(1)
    base_alias = m.group(2) or base_table
    # If alias accidentally captured a keyword, ignore
    if base_alias.upper() in {"WHERE", "JOIN", "GROUP", "ORDER"}:
        base_alias = base_table

    if base_table.upper() not in _tables_with_subject_id():
        return text, rules

    # Insert JOIN before WHERE (or end if WHERE missing)
    join_clause = f" JOIN PATIENTS p ON {base_alias}.SUBJECT_ID = p.SUBJECT_ID"
    text = _insert_join(text, join_clause)

    # Qualify unqualified patients-only columns
    for col in needed:
        text = re.sub(rf"(?<!\.)\b{col}\b", f"p.{col}", text, flags=re.IGNORECASE)

    rules.append("join_patients_for_demographics")
    return text, rules


def _rewrite_patients_id(sql: str) -> tuple[str, list[str]]:
    rules: list[str] = []
    text = sql
    alias = _find_table_alias(text, "PATIENTS")
    if not alias:
        return text, rules
    pattern = re.compile(rf"\b{re.escape(alias)}\s*\.\s*ID\b", re.IGNORECASE)
    if not pattern.search(text):
        return text, rules
    text = pattern.sub(f"{alias}.SUBJECT_ID", text)
    rules.append("patients_id_to_subject_id")
    return text, rules


def _ensure_admissions_join(sql: str) -> tuple[str, list[str]]:
    rules: list[str] = []
    text = sql

    if re.search(r"\bADMISSIONS\b", text, re.IGNORECASE):
        return text, rules

    needed = [c for c in _admissions_only_cols() if re.search(rf"(?<!\.)\b{c}\b", text, re.IGNORECASE)]
    if not needed:
        return text, rules

    m = re.search(r"\bfrom\s+([A-Za-z0-9_]+)(?:\s+([A-Za-z0-9_]+))?", text, re.IGNORECASE)
    if not m:
        return text, rules
    base_table = m.group(1)
    base_alias = m.group(2) or base_table
    if base_alias.upper() in {"WHERE", "JOIN", "GROUP", "ORDER"}:
        base_alias = base_table

    if base_table.upper() not in _tables_with_subject_id():
        return text, rules

    join_clause = f" JOIN ADMISSIONS a ON {base_alias}.SUBJECT_ID = a.SUBJECT_ID"
    if re.search(r"\bHADM_ID\b", text, re.IGNORECASE):
        join_clause = f" JOIN ADMISSIONS a ON {base_alias}.SUBJECT_ID = a.SUBJECT_ID AND {base_alias}.HADM_ID = a.HADM_ID"

    text = _insert_join(text, join_clause)
    for col in needed:
        text = re.sub(rf"(?<!\.)\b{col}\b", f"a.{col}", text, flags=re.IGNORECASE)

    rules.append("join_admissions_for_admission_fields")
    return text, rules


def _ensure_microbiology_table(sql: str) -> tuple[str, list[str]]:
    rules: list[str] = []
    text = sql

    if re.search(r"\bMICROBIOLOGYEVENTS\b", text, re.IGNORECASE):
        return text, rules

    needed = [c for c in _micro_only_cols() if re.search(rf"(?<!\.)\b{c}\b", text, re.IGNORECASE)]
    if not needed:
        return text, rules

    m = re.search(r"\bfrom\s+([A-Za-z0-9_]+)(?:\s+([A-Za-z0-9_]+))?", text, re.IGNORECASE)
    if not m:
        return text, rules
    base_table = m.group(1)
    base_alias = m.group(2) or base_table
    if base_alias.upper() in {"WHERE", "JOIN", "GROUP", "ORDER"}:
        base_alias = base_table

    replacement = "FROM MICROBIOLOGYEVENTS"
    if m.group(2):
        replacement = f"FROM MICROBIOLOGYEVENTS {base_alias}"
    text = re.sub(r"\bfrom\s+[A-Za-z0-9_]+(?:\s+[A-Za-z0-9_]+)?", replacement, text, count=1, flags=re.IGNORECASE)
    rules.append("force_microbiology_table")
    return text, rules


def _ensure_microbiology_by_question(question: str, sql: str) -> tuple[str, list[str]]:
    rules: list[str] = []
    text = sql
    if re.search(r"\bMICROBIOLOGYEVENTS\b", text, re.IGNORECASE):
        return text, rules
    q = question.lower()
    if not any(k in q for k in ("micro", "microbiology", "organism", "antibiotic", "culture", "specimen")):
        return text, rules

    m = re.search(r"\bfrom\s+([A-Za-z0-9_]+)(?:\s+([A-Za-z0-9_]+))?", text, re.IGNORECASE)
    if not m:
        return text, rules
    alias = m.group(2)
    replacement = "FROM MICROBIOLOGYEVENTS"
    if alias:
        replacement = f"FROM MICROBIOLOGYEVENTS {alias}"
    text = re.sub(r"\bfrom\s+[A-Za-z0-9_]+(?:\s+[A-Za-z0-9_]+)?", replacement, text, count=1, flags=re.IGNORECASE)
    rules.append("force_microbiology_by_question")
    return text, rules


def _ensure_icustays_table(question: str, sql: str) -> tuple[str, list[str]]:
    rules: list[str] = []
    text = sql
    if re.search(r"\bICUSTAYS\b", text, re.IGNORECASE):
        return text, rules

    q = question.lower()
    icu_only = "icu stay" in q or "icu stays" in q or ("icu" in q and "los" in q)
    if "admission" in q or "admissions" in q or "patient" in q or "patients" in q:
        icu_only = False

    icu_cols = {"FIRST_CAREUNIT", "LAST_CAREUNIT", "LOS", "STAY_ID", "INTIME", "OUTTIME"}
    has_icu_cols = any(re.search(rf"(?<!\.)\b{c}\b", text, re.IGNORECASE) for c in icu_cols)
    if not icu_only and not has_icu_cols:
        return text, rules

    m = re.search(r"\bfrom\s+([A-Za-z0-9_]+)(?:\s+([A-Za-z0-9_]+))?", text, re.IGNORECASE)
    if not m:
        return text, rules
    alias = m.group(2)
    replacement = "FROM ICUSTAYS"
    if alias:
        replacement = f"FROM ICUSTAYS {alias}"
    text = re.sub(r"\bfrom\s+[A-Za-z0-9_]+(?:\s+[A-Za-z0-9_]+)?", replacement, text, count=1, flags=re.IGNORECASE)
    rules.append("force_icustays_table")
    return text, rules


def _ensure_chartevents_table(question: str, sql: str) -> tuple[str, list[str]]:
    rules: list[str] = []
    text = sql
    if re.search(r"\bCHARTEVENTS\b", text, re.IGNORECASE):
        return text, rules

    q = question.lower()
    if "chart event" not in q and "chart events" not in q and "chart" not in q:
        return text, rules

    m = re.search(r"\bfrom\s+([A-Za-z0-9_]+)(?:\s+([A-Za-z0-9_]+))?", text, re.IGNORECASE)
    if not m:
        return text, rules
    alias = m.group(2)
    replacement = "FROM CHARTEVENTS"
    if alias:
        replacement = f"FROM CHARTEVENTS {alias}"
    text = re.sub(r"\bfrom\s+[A-Za-z0-9_]+(?:\s+[A-Za-z0-9_]+)?", replacement, text, count=1, flags=re.IGNORECASE)
    rules.append("force_chartevents_table")
    return text, rules


def _ensure_chart_label(question: str, sql: str) -> tuple[str, list[str]]:
    rules: list[str] = []
    text = sql
    q = question.lower()
    if "label" not in q or "chart" not in q:
        return text, rules
    if re.search(r"\bD_ITEMS\b", text, re.IGNORECASE):
        return text, rules
    if not re.search(r"\bCHARTEVENTS\b", text, re.IGNORECASE):
        return text, rules

    alias = _find_table_alias(text, "CHARTEVENTS") or "CHARTEVENTS"
    join_clause = f" JOIN D_ITEMS d ON {alias}.ITEMID = d.ITEMID"
    text = _insert_join(text, join_clause)
    text = re.sub(r"(?<!\.)\bLABEL\b", "d.LABEL", text, flags=re.IGNORECASE)
    rules.append("force_chart_label")
    return text, rules


def _ensure_labevents_table(question: str, sql: str) -> tuple[str, list[str]]:
    rules: list[str] = []
    text = sql
    if re.search(r"\bLABEVENTS\b", text, re.IGNORECASE):
        return text, rules

    q = question.lower()
    if "lab" not in q and "laboratory" not in q:
        return text, rules
    if "micro" in q or "microbiology" in q:
        return text, rules

    m = re.search(r"\bfrom\s+([A-Za-z0-9_]+)(?:\s+([A-Za-z0-9_]+))?", text, re.IGNORECASE)
    if not m:
        return text, rules
    alias = m.group(2)
    replacement = "FROM LABEVENTS"
    if alias:
        replacement = f"FROM LABEVENTS {alias}"
    text = re.sub(r"\bfrom\s+[A-Za-z0-9_]+(?:\s+[A-Za-z0-9_]+)?", replacement, text, count=1, flags=re.IGNORECASE)
    rules.append("force_labevents_table")
    return text, rules


def _ensure_lab_label(question: str, sql: str) -> tuple[str, list[str]]:
    rules: list[str] = []
    text = sql
    q = question.lower()
    if "label" not in q or "lab" not in q:
        return text, rules
    if re.search(r"\bD_LABITEMS\b", text, re.IGNORECASE):
        return text, rules
    if not re.search(r"\bLABEVENTS\b", text, re.IGNORECASE):
        return text, rules

    alias = _find_table_alias(text, "LABEVENTS") or "LABEVENTS"
    join_clause = f" JOIN D_LABITEMS d ON {alias}.ITEMID = d.ITEMID"
    text = _insert_join(text, join_clause)
    text = re.sub(r"(?<!\.)\bLABEL\b", "d.LABEL", text, flags=re.IGNORECASE)
    rules.append("force_lab_label")
    return text, rules


def _rewrite_label_field(question: str, sql: str) -> tuple[str, list[str]]:
    rules: list[str] = []
    text = sql
    q = question.lower()
    if "label" not in q:
        return text, rules

    if "chart" in q and "lab" not in q:
        alias = _find_table_alias(text, "D_ITEMS")
        if alias:
            text = re.sub(r"(?<!\.)\bITEMID\b", f"{alias}.LABEL", text, flags=re.IGNORECASE)
            text = re.sub(r"(?<!\.)\bLABEL\b", f"{alias}.LABEL", text, flags=re.IGNORECASE)
            rules.append("chart_label_itemid_to_label")
        return text, rules

    if "lab" in q or "laboratory" in q:
        alias = _find_table_alias(text, "D_LABITEMS")
        if alias:
            text = re.sub(r"(?<!\.)\bITEMID\b", f"{alias}.LABEL", text, flags=re.IGNORECASE)
            text = re.sub(r"(?<!\.)\bLABEL\b", f"{alias}.LABEL", text, flags=re.IGNORECASE)
            rules.append("lab_label_itemid_to_label")
    return text, rules


def _ensure_prescriptions_table(question: str, sql: str) -> tuple[str, list[str]]:
    rules: list[str] = []
    text = sql
    if re.search(r"\bPRESCRIPTIONS\b", text, re.IGNORECASE):
        return text, rules

    q = question.lower()
    if "emar" in q or "ingredient" in q:
        return text, rules
    triggers = ("prescription", "drug", "medication", "doses", "formulation")
    if not any(t in q for t in triggers):
        return text, rules

    m = re.search(r"\bfrom\s+([A-Za-z0-9_]+)(?:\s+([A-Za-z0-9_]+))?", text, re.IGNORECASE)
    if not m:
        return text, rules
    alias = m.group(2)
    replacement = "FROM PRESCRIPTIONS"
    if alias:
        replacement = f"FROM PRESCRIPTIONS {alias}"
    text = re.sub(r"\bfrom\s+[A-Za-z0-9_]+(?:\s+[A-Za-z0-9_]+)?", replacement, text, count=1, flags=re.IGNORECASE)
    rules.append("force_prescriptions_table")
    return text, rules


def _ensure_inputevents_table(question: str, sql: str) -> tuple[str, list[str]]:
    rules: list[str] = []
    text = sql
    if re.search(r"\bINPUTEVENTS\b", text, re.IGNORECASE):
        return text, rules
    q = question.lower()
    triggers = ("input event", "input events", "input amount", "intake", "fluid intake", "infusion", "infusions")
    if not any(t in q for t in triggers):
        return text, rules
    if "ingredient" in q:
        return text, rules

    m = re.search(r"\bfrom\s+([A-Za-z0-9_]+)(?:\s+([A-Za-z0-9_]+))?", text, re.IGNORECASE)
    if not m:
        return text, rules
    alias = m.group(2)
    replacement = "FROM INPUTEVENTS"
    if alias:
        replacement = f"FROM INPUTEVENTS {alias}"
    text = re.sub(r"\bfrom\s+[A-Za-z0-9_]+(?:\s+[A-Za-z0-9_]+)?", replacement, text, count=1, flags=re.IGNORECASE)
    rules.append("force_inputevents_table")
    return text, rules


def _ensure_outputevents_table(question: str, sql: str) -> tuple[str, list[str]]:
    rules: list[str] = []
    text = sql
    if re.search(r"\bOUTPUTEVENTS\b", text, re.IGNORECASE):
        return text, rules
    q = question.lower()
    triggers = ("output event", "output events", "output value", "output volume", "urine output", "drain output")
    if not any(t in q for t in triggers):
        return text, rules

    m = re.search(r"\bfrom\s+([A-Za-z0-9_]+)(?:\s+([A-Za-z0-9_]+))?", text, re.IGNORECASE)
    if not m:
        return text, rules
    alias = m.group(2)
    replacement = "FROM OUTPUTEVENTS"
    if alias:
        replacement = f"FROM OUTPUTEVENTS {alias}"
    text = re.sub(r"\bfrom\s+[A-Za-z0-9_]+(?:\s+[A-Za-z0-9_]+)?", replacement, text, count=1, flags=re.IGNORECASE)
    rules.append("force_outputevents_table")
    return text, rules


def _ensure_emar_table(question: str, sql: str) -> tuple[str, list[str]]:
    rules: list[str] = []
    text = sql
    q = question.lower()
    triggers = ("emar", "med admin", "medication administration", "administration record", "dose given", "dose due")
    if not any(t in q for t in triggers):
        return text, rules

    detail_triggers = ("detail", "administration type", "dose given", "dose due", "barcode")
    target = "EMAR_DETAIL" if any(t in q for t in detail_triggers) else "EMAR"
    if re.search(rf"\b{target}\b", text, re.IGNORECASE):
        return text, rules

    m = re.search(r"\bfrom\s+([A-Za-z0-9_]+)(?:\s+([A-Za-z0-9_]+))?", text, re.IGNORECASE)
    if not m:
        return text, rules
    alias = m.group(2)
    replacement = f"FROM {target}"
    if alias:
        replacement = f"FROM {target} {alias}"
    text = re.sub(r"\bfrom\s+[A-Za-z0-9_]+(?:\s+[A-Za-z0-9_]+)?", replacement, text, count=1, flags=re.IGNORECASE)
    rules.append(f"force_{target.lower()}_table")
    return text, rules


def _ensure_diagnoses_icd_table(question: str, sql: str) -> tuple[str, list[str]]:
    rules: list[str] = []
    text = sql
    q = question.lower()
    if "diagnos" not in q:
        return text, rules
    if "title" in q:
        return text, rules
    if re.search(r"\bDIAGNOSES_ICD\b", text, re.IGNORECASE):
        return text, rules

    m = re.search(r"\bfrom\s+([A-Za-z0-9_]+)(?:\s+([A-Za-z0-9_]+))?", text, re.IGNORECASE)
    if not m:
        return text, rules
    alias = m.group(2)
    replacement = "FROM DIAGNOSES_ICD"
    if alias:
        replacement = f"FROM DIAGNOSES_ICD {alias}"
    text = re.sub(r"\bfrom\s+[A-Za-z0-9_]+(?:\s+[A-Za-z0-9_]+)?", replacement, text, count=1, flags=re.IGNORECASE)
    rules.append("force_diagnoses_icd_table")
    return text, rules


def _ensure_procedures_icd_table(question: str, sql: str) -> tuple[str, list[str]]:
    rules: list[str] = []
    text = sql
    q = question.lower()
    if "procedur" not in q:
        return text, rules
    if "title" in q:
        return text, rules
    if "procedure event" in q or "procedureevents" in q:
        return text, rules
    if re.search(r"\bPROCEDURES_ICD\b", text, re.IGNORECASE):
        return text, rules

    m = re.search(r"\bfrom\s+([A-Za-z0-9_]+)(?:\s+([A-Za-z0-9_]+))?", text, re.IGNORECASE)
    if not m:
        return text, rules
    alias = m.group(2)
    replacement = "FROM PROCEDURES_ICD"
    if alias:
        replacement = f"FROM PROCEDURES_ICD {alias}"
    text = re.sub(r"\bfrom\s+[A-Za-z0-9_]+(?:\s+[A-Za-z0-9_]+)?", replacement, text, count=1, flags=re.IGNORECASE)
    rules.append("force_procedures_icd_table")
    return text, rules


def _rewrite_prescriptions_drug_field(question: str, sql: str) -> tuple[str, list[str]]:
    rules: list[str] = []
    text = sql
    if not re.search(r"\bPRESCRIPTIONS\b", text, re.IGNORECASE):
        return text, rules
    q = question.lower()
    if "drug" not in q and "medication" not in q:
        return text, rules

    if re.search(r"(?<!\.)\bITEMID\b", text, re.IGNORECASE):
        text = re.sub(r"(?<!\.)\bITEMID\b", "DRUG", text, flags=re.IGNORECASE)
        rules.append("prescriptions_itemid_to_drug")
    return text, rules


def _rewrite_prescriptions_columns(sql: str) -> tuple[str, list[str]]:
    rules: list[str] = []
    text = sql

    aliases: set[str] = {"PRESCRIPTIONS"}
    for m in re.finditer(
        r"\b(?:FROM|JOIN)\s+PRESCRIPTIONS(?:\s+(?:AS\s+)?([A-Za-z0-9_]+))?",
        text,
        re.IGNORECASE,
    ):
        alias = m.group(1)
        if alias and alias.upper() not in {"ON", "WHERE", "JOIN", "GROUP", "ORDER", "INNER", "LEFT", "RIGHT", "FULL"}:
            aliases.add(alias)

    aliases_upper = {a.upper() for a in aliases}

    def replace_qualified(col: str, repl: str) -> bool:
        nonlocal text
        changed = False

        def _repl(match: re.Match) -> str:
            nonlocal changed
            alias = match.group(1)
            if alias.upper() in aliases_upper:
                changed = True
                return f"{alias}.{repl}"
            return match.group(0)

        text = re.sub(
            rf"\b([A-Za-z0-9_]+)\.{col}\b",
            _repl,
            text,
            flags=re.IGNORECASE,
        )
        return changed

    if replace_qualified("MEDICATION", "DRUG"):
        rules.append("prescriptions_medication_to_drug")
    if replace_qualified("CHARTTIME", "STARTTIME"):
        rules.append("prescriptions_charttime_to_starttime")

    # If EMAR is absent, unqualified MEDICATION/CHARTTIME in PRESCRIPTIONS context should map to DRUG/STARTTIME.
    has_emar = re.search(r"\bEMAR(?:_DETAIL)?\b", text, re.IGNORECASE) is not None
    if not has_emar:
        if re.search(r"(?<!\.)\bMEDICATION\b", text, re.IGNORECASE):
            text = re.sub(r"(?<!\.)\bMEDICATION\b", "DRUG", text, flags=re.IGNORECASE)
            rules.append("prescriptions_unqualified_medication_to_drug")
        if re.search(r"(?<!\.)\bCHARTTIME\b", text, re.IGNORECASE):
            text = re.sub(r"(?<!\.)\bCHARTTIME\b", "STARTTIME", text, flags=re.IGNORECASE)
            rules.append("prescriptions_unqualified_charttime_to_starttime")

    return text, rules


def _rewrite_icd_code_field(question: str, sql: str) -> tuple[str, list[str]]:
    rules: list[str] = []
    text = sql
    q = question.lower()
    if "code" not in q:
        return text, rules

    if "diagnos" in q and re.search(r"\bDIAGNOSES_ICD\b", text, re.IGNORECASE):
        if re.search(r"(?<!\.)\bITEMID\b", text, re.IGNORECASE):
            text = re.sub(r"(?<!\.)\bITEMID\b", "ICD_CODE", text, flags=re.IGNORECASE)
            rules.append("diagnoses_itemid_to_icd_code")
        return text, rules

    if "procedur" in q and re.search(r"\bPROCEDURES_ICD\b", text, re.IGNORECASE):
        if re.search(r"(?<!\.)\bITEMID\b", text, re.IGNORECASE):
            text = re.sub(r"(?<!\.)\bITEMID\b", "ICD_CODE", text, flags=re.IGNORECASE)
            rules.append("procedures_itemid_to_icd_code")
    return text, rules


def _rewrite_itemid_in_icd_tables(sql: str) -> tuple[str, list[str]]:
    rules: list[str] = []
    text = sql
    if re.search(r"\bDIAGNOSES_ICD\b", text, re.IGNORECASE) or re.search(r"\bPROCEDURES_ICD\b", text, re.IGNORECASE):
        if re.search(r"\bITEMID\b", text, re.IGNORECASE):
            text = re.sub(r"(?<!\.)\bITEMID\b", "ICD_CODE", text, flags=re.IGNORECASE)
            text = re.sub(r"\b([A-Za-z0-9_]+)\.ITEMID\b", r"\1.ICD_CODE", text, flags=re.IGNORECASE)
            rules.append("icd_tables_itemid_to_icd_code")
    return text, rules


def _rewrite_emar_medication_field(question: str, sql: str) -> tuple[str, list[str]]:
    rules: list[str] = []
    text = sql
    if not re.search(r"\bEMAR\b", text, re.IGNORECASE):
        return text, rules
    q = question.lower()
    if "medication" not in q and "drug" not in q:
        return text, rules

    if re.search(r"(?<!\.)\bITEMID\b", text, re.IGNORECASE):
        text = re.sub(r"(?<!\.)\bITEMID\b", "MEDICATION", text, flags=re.IGNORECASE)
        rules.append("emar_itemid_to_medication")
    return text, rules


def _ensure_diagnosis_title_join(question: str, sql: str) -> tuple[str, list[str]]:
    rules: list[str] = []
    text = sql
    q = question.lower()
    if "diagnos" not in q or "title" not in q:
        return text, rules
    if re.search(r"\bDIAGNOSES_ICD\b", text, re.IGNORECASE):
        return text, rules
    if not re.search(r"\bD_ICD_DIAGNOSES\b", text, re.IGNORECASE):
        return text, rules

    replacement = (
        "FROM DIAGNOSES_ICD dx JOIN D_ICD_DIAGNOSES d "
        "ON dx.ICD_CODE = d.ICD_CODE AND dx.ICD_VERSION = d.ICD_VERSION"
    )
    text = re.sub(
        r"\bfrom\s+D_ICD_DIAGNOSES\b(?:\s+[A-Za-z0-9_]+)?",
        replacement,
        text,
        count=1,
        flags=re.IGNORECASE,
    )
    text = re.sub(r"(?<!\.)\bLONG_TITLE\b", "d.LONG_TITLE", text, flags=re.IGNORECASE)
    rules.append("diagnosis_title_join")
    return text, rules


def _ensure_procedure_title_join(question: str, sql: str) -> tuple[str, list[str]]:
    rules: list[str] = []
    text = sql
    q = question.lower()
    if "procedur" not in q or "title" not in q:
        return text, rules
    if re.search(r"\bPROCEDURES_ICD\b", text, re.IGNORECASE):
        return text, rules

    replacement = (
        "FROM PROCEDURES_ICD p JOIN D_ICD_PROCEDURES d "
        "ON p.ICD_CODE = d.ICD_CODE AND p.ICD_VERSION = d.ICD_VERSION"
    )
    text = re.sub(
        r"\bfrom\s+[A-Za-z0-9_]+(?:\s+[A-Za-z0-9_]+)?",
        replacement,
        text,
        count=1,
        flags=re.IGNORECASE,
    )
    text = re.sub(r"(?<!\.)\bLONG_TITLE\b", "d.LONG_TITLE", text, flags=re.IGNORECASE)
    rules.append("procedure_title_join")
    return text, rules


def _cleanup_procedure_title_joins(sql: str) -> tuple[str, list[str]]:
    rules: list[str] = []
    text = sql
    if not re.search(r"\bD_ICD_PROCEDURES\b", text, re.IGNORECASE):
        return text, rules
    if not re.search(r"\b(ITEMID|TO_NUMBER)\b", text, re.IGNORECASE):
        return text, rules

    pattern = re.compile(r"\bJOIN\s+D_ICD_PROCEDURES\b", re.IGNORECASE)
    pos = 0
    while True:
        m = pattern.search(text, pos)
        if not m:
            break
        start = m.start()
        tail = text[m.end():]
        end_match = re.search(r"\bJOIN\b|\bWHERE\b|\bGROUP\b|\bORDER\b", tail, re.IGNORECASE)
        end = m.end() + (end_match.start() if end_match else len(tail))
        segment = text[start:end]
        if re.search(r"\bITEMID\b|\bTO_NUMBER\b", segment, re.IGNORECASE):
            text = text[:start] + " " + text[end:]
            rules.append("drop_bad_d_icd_procedures_join")
            pos = start
        else:
            pos = end
    return text, rules


def _ensure_services_table(question: str, sql: str) -> tuple[str, list[str]]:
    rules: list[str] = []
    text = sql
    if re.search(r"\bSERVICES\b", text, re.IGNORECASE):
        return text, rules

    q = question.lower()
    if "service" not in q:
        return text, rules
    if "order" in q or "poe" in q:
        return text, rules

    if not re.search(r"\b(CURR_SERVICE|PREV_SERVICE)\b", text, re.IGNORECASE) and "current service" not in q:
        return text, rules

    m = re.search(r"\bfrom\s+([A-Za-z0-9_]+)(?:\s+([A-Za-z0-9_]+))?", text, re.IGNORECASE)
    if not m:
        return text, rules
    alias = m.group(2)
    replacement = "FROM SERVICES"
    if alias:
        replacement = f"FROM SERVICES {alias}"
    text = re.sub(r"\bfrom\s+[A-Za-z0-9_]+(?:\s+[A-Za-z0-9_]+)?", replacement, text, count=1, flags=re.IGNORECASE)
    rules.append("force_services_table")
    return text, rules


def _ensure_transfers_eventtype(question: str, sql: str) -> tuple[str, list[str]]:
    rules: list[str] = []
    text = sql
    q = question.lower()
    if "transfer" not in q:
        return text, rules
    if "event type" not in q and "eventtype" not in q:
        return text, rules

    if re.search(r"\bSERVICES\b", text, re.IGNORECASE) or re.search(
        r"\b(CURR_SERVICE|PREV_SERVICE|ORDER_TYPE)\b", text, re.IGNORECASE
    ):
        m = re.search(r"\bfrom\s+([A-Za-z0-9_]+)(?:\s+([A-Za-z0-9_]+))?", text, re.IGNORECASE)
        if m:
            alias = m.group(2)
            replacement = "FROM TRANSFERS"
            if alias:
                replacement = f"FROM TRANSFERS {alias}"
            text = re.sub(
                r"\bfrom\s+[A-Za-z0-9_]+(?:\s+[A-Za-z0-9_]+)?",
                replacement,
                text,
                count=1,
                flags=re.IGNORECASE,
            )
            rules.append("force_transfers_table")

    if re.search(r"(?<!\.)\b(CURR_SERVICE|PREV_SERVICE|ORDER_TYPE)\b", text, re.IGNORECASE):
        text = re.sub(
            r"(?<!\.)\b(CURR_SERVICE|PREV_SERVICE|ORDER_TYPE)\b",
            "EVENTTYPE",
            text,
            flags=re.IGNORECASE,
        )
        rules.append("eventtype_from_transfers")
    return text, rules


def _rewrite_services_order_type(question: str, sql: str) -> tuple[str, list[str]]:
    rules: list[str] = []
    text = sql
    if not re.search(r"\bSERVICES\b", text, re.IGNORECASE):
        return text, rules
    if not re.search(r"(?<!\.)\bORDER_TYPE\b", text, re.IGNORECASE):
        return text, rules

    q = question.lower()
    target = "CURR_SERVICE"
    if "previous service" in q or "prev service" in q or "prior service" in q:
        target = "PREV_SERVICE"
    elif "current service" in q:
        target = "CURR_SERVICE"
    text = re.sub(r"(?<!\.)\bORDER_TYPE\b", target, text, flags=re.IGNORECASE)
    rules.append("services_order_type_to_curr_prev")
    return text, rules


def _rewrite_icustays_careunit(question: str, sql: str) -> tuple[str, list[str]]:
    rules: list[str] = []
    text = sql
    if not re.search(r"\bICUSTAYS\b", text, re.IGNORECASE):
        return text, rules
    if not re.search(r"\bCAREUNIT\b", text, re.IGNORECASE):
        return text, rules

    q = question.lower()
    target = "FIRST_CAREUNIT"
    if "last careunit" in q or "last care unit" in q:
        target = "LAST_CAREUNIT"
    elif "first careunit" in q or "first care unit" in q:
        target = "FIRST_CAREUNIT"

    aliases: set[str] = {"ICUSTAYS"}
    for m in re.finditer(
        r"\b(?:FROM|JOIN)\s+ICUSTAYS(?:\s+(?:AS\s+)?([A-Za-z0-9_]+))?",
        text,
        re.IGNORECASE,
    ):
        alias = m.group(1)
        if alias and alias.upper() not in {"ON", "WHERE", "JOIN", "GROUP", "ORDER", "INNER", "LEFT", "RIGHT", "FULL"}:
            aliases.add(alias)

    aliases_upper = {a.upper() for a in aliases}
    updated = re.sub(
        r"\b([A-Za-z0-9_]+)\.CAREUNIT\b",
        lambda m: f"{m.group(1)}.{target}" if m.group(1).upper() in aliases_upper else m.group(0),
        text,
        flags=re.IGNORECASE,
    )

    # ICUSTAYS 단일 문맥일 때만 비한정 CAREUNIT을 FIRST/LAST로 보정
    if not re.search(r"\bTRANSFERS\b", updated, re.IGNORECASE):
        updated = re.sub(r"(?<!\.)\bCAREUNIT\b", target, updated, flags=re.IGNORECASE)

    if updated != text:
        text = updated
        rules.append("icustays_careunit_to_first_last")
    return text, rules


def _rewrite_transfers_careunit_fields(sql: str) -> tuple[str, list[str]]:
    rules: list[str] = []
    text = sql
    if not re.search(r"\bTRANSFERS\b", text, re.IGNORECASE):
        return text, rules
    if not re.search(r"\b(FIRST_CAREUNIT|LAST_CAREUNIT)\b", text, re.IGNORECASE):
        return text, rules

    aliases: set[str] = {"TRANSFERS"}
    for m in re.finditer(
        r"\b(?:FROM|JOIN)\s+TRANSFERS(?:\s+(?:AS\s+)?([A-Za-z0-9_]+))?",
        text,
        re.IGNORECASE,
    ):
        alias = m.group(1)
        if alias and alias.upper() not in {"ON", "WHERE", "JOIN", "GROUP", "ORDER", "INNER", "LEFT", "RIGHT", "FULL"}:
            aliases.add(alias)

    aliases_upper = {a.upper() for a in aliases}
    updated = re.sub(
        r"\b([A-Za-z0-9_]+)\.(FIRST_CAREUNIT|LAST_CAREUNIT)\b",
        lambda m: f"{m.group(1)}.CAREUNIT" if m.group(1).upper() in aliases_upper else m.group(0),
        text,
        flags=re.IGNORECASE,
    )

    # TRANSFERS만 사용하는 문맥의 비한정 FIRST/LAST_CAREUNIT 보정
    if not re.search(r"\bICUSTAYS\b", updated, re.IGNORECASE):
        updated = re.sub(r"(?<!\.)\b(FIRST_CAREUNIT|LAST_CAREUNIT)\b", "CAREUNIT", updated, flags=re.IGNORECASE)

    if updated != text:
        text = updated
        rules.append("transfers_careunit_to_careunit")
    return text, rules


def _strip_rownum_cap_for_micro_topk(sql: str) -> tuple[str, list[str]]:
    rules: list[str] = []
    text = sql
    match = _OUTER_ROWNUM_RE.match(text)
    if not match:
        return text, rules
    inner = match.group(1)
    limit = match.group(2)
    if not re.search(r"\bMICROBIOLOGYEVENTS\b", inner, re.IGNORECASE):
        return text, rules

    new_inner = re.sub(
        r"\bWHERE\s+ROWNUM\s*<=\s*\d+\s+AND\s+",
        "WHERE ",
        inner,
        flags=re.IGNORECASE,
    )
    new_inner = re.sub(
        r"\bWHERE\s+ROWNUM\s*<=\s*\d+\s+GROUP\s+BY\b",
        "GROUP BY",
        new_inner,
        flags=re.IGNORECASE,
    )
    new_inner = re.sub(
        r"\bWHERE\s+ROWNUM\s*<=\s*\d+\s+ORDER\s+BY\b",
        "ORDER BY",
        new_inner,
        flags=re.IGNORECASE,
    )
    new_inner = re.sub(
        r"\bWHERE\s+ROWNUM\s*<=\s*\d+\b",
        "",
        new_inner,
        flags=re.IGNORECASE,
    )

    if new_inner != inner:
        text = f"SELECT * FROM ({new_inner.strip()}) WHERE ROWNUM <= {limit}"
        rules.append("strip_rownum_cap_for_micro_topk")
    return text, rules


def _strip_rownum_cap_for_grouped_tables(sql: str) -> tuple[str, list[str]]:
    rules: list[str] = []
    text = sql
    upper = text.upper()
    if "GROUP BY" not in upper:
        return text, rules

    if not re.search(r"\b(PRESCRIPTIONS|INPUTEVENTS|OUTPUTEVENTS)\b", upper):
        return text, rules

    def _maybe_strip(match: re.Match, replacement: str) -> str:
        nonlocal changed
        try:
            limit = int(match.group(1))
        except (TypeError, ValueError):
            return match.group(0)
        if limit < 1000:
            return match.group(0)
        changed = True
        return replacement

    changed = False
    text = re.sub(
        r"\bWHERE\s+ROWNUM\s*<=\s*(\d+)\s+AND\s+",
        lambda m: _maybe_strip(m, "WHERE "),
        text,
        flags=re.IGNORECASE,
    )
    text = re.sub(
        r"\bWHERE\s+ROWNUM\s*<=\s*(\d+)\s+GROUP\s+BY\b",
        lambda m: _maybe_strip(m, "GROUP BY"),
        text,
        flags=re.IGNORECASE,
    )
    text = re.sub(
        r"\bWHERE\s+ROWNUM\s*<=\s*(\d+)\s+ORDER\s+BY\b",
        lambda m: _maybe_strip(m, "ORDER BY"),
        text,
        flags=re.IGNORECASE,
    )
    text = re.sub(
        r"\bWHERE\s+ROWNUM\s*<=\s*(\d+)\b",
        lambda m: _maybe_strip(m, ""),
        text,
        flags=re.IGNORECASE,
    )

    new_text, changed = text, changed
    if changed:
        rules.append("strip_rownum_cap_for_grouped_tables")
    return new_text, rules


def _pushdown_outer_predicates(sql: str) -> tuple[str, list[str]]:
    rules: list[str] = []
    text = sql
    pattern = re.compile(
        r"^\s*SELECT\s+\*\s+FROM\s*\((SELECT .*?)\)\s*WHERE\s+(.+?)\s*;?\s*$",
        re.IGNORECASE | re.DOTALL,
    )
    match = pattern.match(text)
    if not match:
        return text, rules
    inner = match.group(1)
    where_clause = match.group(2)
    m_limit = re.search(r"\bROWNUM\s*<=\s*(\d+)\b", where_clause, re.IGNORECASE)
    if not m_limit:
        return text, rules
    limit = m_limit.group(1)
    pred = re.sub(r"\bROWNUM\s*<=\s*\d+\b", "", where_clause, flags=re.IGNORECASE)
    pred = re.sub(r"\bAND\b\s*(\bAND\b)?", "AND", pred, flags=re.IGNORECASE)
    pred = pred.strip()
    pred = re.sub(r"^(AND|OR)\s+", "", pred, flags=re.IGNORECASE)
    pred = re.sub(r"\s+(AND|OR)$", "", pred, flags=re.IGNORECASE)
    pred = pred.strip()
    if not pred:
        return text, rules

    if re.search(r"\bwhere\b", inner, re.IGNORECASE):
        inner = re.sub(r"\bwhere\b", f"WHERE {pred} AND", inner, count=1, flags=re.IGNORECASE)
    elif re.search(r"\bgroup\s+by\b", inner, re.IGNORECASE):
        inner = re.sub(r"\bgroup\s+by\b", f"WHERE {pred} GROUP BY", inner, count=1, flags=re.IGNORECASE)
    elif re.search(r"\border\s+by\b", inner, re.IGNORECASE):
        inner = re.sub(r"\border\s+by\b", f"WHERE {pred} ORDER BY", inner, count=1, flags=re.IGNORECASE)
    else:
        inner = inner.rstrip(";") + f" WHERE {pred}"

    text = f"SELECT * FROM ({inner}) WHERE ROWNUM <= {limit}"
    rules.append("pushdown_outer_predicate")
    return text, rules


def _fix_missing_where_predicate(sql: str) -> tuple[str, list[str]]:
    rules: list[str] = []
    text = sql

    def fix_segment(segment: str) -> tuple[str, bool]:
        if re.search(r"\bwhere\b", segment, re.IGNORECASE):
            return segment, False
        match = re.search(
            r"\b([A-Za-z0-9_\.]+(?:\s+IS\s+NOT\s+NULL|\s+IS\s+NULL)"
            r"(?:\s+AND\s+[A-Za-z0-9_\.]+(?:\s+IS\s+NOT\s+NULL|\s+IS\s+NULL))*)\s+GROUP\s+BY\b",
            segment,
            re.IGNORECASE,
        )
        if not match:
            return segment, False
        predicate = match.group(1)
        fixed = segment.replace(f"{predicate} GROUP BY", f"WHERE {predicate} GROUP BY", 1)
        return fixed, True

    match = _OUTER_ROWNUM_RE.match(text)
    if match:
        inner = match.group(1)
        limit = match.group(2)
        fixed_inner, changed = fix_segment(inner)
        if changed:
            text = f"SELECT * FROM ({fixed_inner}) WHERE ROWNUM <= {limit}"
            rules.append("insert_missing_where_predicate")
        return text, rules

    fixed_text, changed = fix_segment(text)
    if changed:
        rules.append("insert_missing_where_predicate")
    return fixed_text, rules


def _rewrite_icustays_los(sql: str) -> tuple[str, list[str]]:
    rules: list[str] = []
    text = sql
    if not re.search(r"\bICUSTAYS\b", text, re.IGNORECASE):
        return text, rules

    pattern = re.compile(
        r"CAST\(([^)]+OUTTIME[^)]*)\s+AS\s+DATE\)\s*-\s*CAST\(([^)]+INTIME[^)]*)\s+AS\s+DATE\)",
        re.IGNORECASE,
    )
    new_text = pattern.sub("LOS", text)
    if new_text != text:
        rules.append("icustays_diff_to_los")
        return new_text, rules

    pattern_rev = re.compile(
        r"CAST\(([^)]+INTIME[^)]*)\s+AS\s+DATE\)\s*-\s*CAST\(([^)]+OUTTIME[^)]*)\s+AS\s+DATE\)",
        re.IGNORECASE,
    )
    new_text = pattern_rev.sub("LOS", text)
    if new_text != text:
        rules.append("icustays_diff_to_los")
        return new_text, rules
    return text, rules


def _rewrite_warning_flag(question: str, sql: str) -> tuple[str, list[str]]:
    rules: list[str] = []
    text = sql
    q = question.lower()
    if "warning" not in q:
        return text, rules
    if not re.search(r"\bCHARTEVENTS\b", text, re.IGNORECASE):
        return text, rules
    if not re.search(r"(?<!\.)\bSTATUSDESCRIPTION\b", text, re.IGNORECASE):
        return text, rules
    text = re.sub(r"(?<!\.)\bSTATUSDESCRIPTION\b", "WARNING", text, flags=re.IGNORECASE)
    rules.append("warning_flag_from_chartevents")
    return text, rules


def _rewrite_lab_priority(question: str, sql: str) -> tuple[str, list[str]]:
    rules: list[str] = []
    text = sql
    q = question.lower()
    if "priority" not in q:
        return text, rules
    if not re.search(r"\bLABEVENTS\b", text, re.IGNORECASE):
        return text, rules
    if re.search(r"(?<!\.)\bPRIORITY\b", text, re.IGNORECASE):
        return text, rules
    text = re.sub(r"(?<!\.)\bSPEC_TYPE_DESC\b", "PRIORITY", text, flags=re.IGNORECASE)
    rules.append("lab_priority_from_labevents")
    return text, rules


def _rewrite_micro_count_field(question: str, sql: str) -> tuple[str, list[str]]:
    rules: list[str] = []
    text = sql
    if not re.search(r"\bMICROBIOLOGYEVENTS\b", text, re.IGNORECASE):
        return text, rules
    q = question.lower()
    target = None
    if "antibiotic" in q:
        target = "AB_NAME"
    elif "organism" in q:
        target = "ORG_NAME"
    elif "test" in q:
        target = "TEST_NAME"
    if not target:
        return text, rules

    # Replace the selected/grouped field if it is a generic ID.
    text = re.sub(
        r"(?<!\.)\b(MICROEVENT_ID|MICRO_SPECIMEN_ID|ITEMID|TEST_ITEMID|ORG_ITEMID|AB_ITEMID)\b",
        target,
        text,
        flags=re.IGNORECASE,
    )
    text = re.sub(
        r"\bGROUP\s+BY\s+(.*)",
        lambda m: re.sub(
            r"(?<!\.)\b(MICROEVENT_ID|MICRO_SPECIMEN_ID|ITEMID|TEST_ITEMID|ORG_ITEMID|AB_ITEMID)\b",
            target,
            m.group(0),
            flags=re.IGNORECASE,
        ),
        text,
        flags=re.IGNORECASE,
    )
    rules.append("micro_count_field_to_name")
    return text, rules


def _ensure_icd_join(question: str, sql: str) -> tuple[str, list[str]]:
    rules: list[str] = []
    text = sql
    if not re.search(r"(?<!\.)\bICD_CODE\b", text, re.IGNORECASE):
        return text, rules

    q = question.lower()
    target = "DIAGNOSES_ICD"
    if "procedure" in q:
        target = "PROCEDURES_ICD"

    if re.search(rf"\b{target}\b", text, re.IGNORECASE):
        return text, rules

    m = re.search(r"\bfrom\s+([A-Za-z0-9_]+)(?:\s+([A-Za-z0-9_]+))?", text, re.IGNORECASE)
    if not m:
        return text, rules
    base_table = m.group(1)
    base_alias = m.group(2) or base_table
    if base_alias.upper() in {"WHERE", "JOIN", "GROUP", "ORDER"}:
        base_alias = base_table

    if base_table.upper() not in _tables_with_subject_id():
        return text, rules

    join_clause = f" JOIN {target} d ON {base_alias}.SUBJECT_ID = d.SUBJECT_ID"
    if re.search(r"\bHADM_ID\b", text, re.IGNORECASE):
        join_clause = f" JOIN {target} d ON {base_alias}.SUBJECT_ID = d.SUBJECT_ID AND {base_alias}.HADM_ID = d.HADM_ID"

    text = _insert_join(text, join_clause)
    text = re.sub(r"(?<!\.)\bICD_CODE\b", "d.ICD_CODE", text, flags=re.IGNORECASE)
    rules.append(f"join_{target.lower()}_for_icd_code")
    return text, rules


def _rewrite_admission_length(sql: str) -> tuple[str, list[str]]:
    rules: list[str] = []
    text = sql
    if not re.search(r"(?<!\.)\b(ADMISSION_LENGTH|ADMISSION_DAYS)\b", text, re.IGNORECASE):
        return text, rules

    alias = _find_table_alias(text, "ADMISSIONS") or "ADMISSIONS"
    replacement = f"CAST({alias}.DISCHTIME AS DATE) - CAST({alias}.ADMITTIME AS DATE)"
    text = re.sub(r"(?<!\.)\b(ADMISSION_LENGTH|ADMISSION_DAYS)\b", replacement, text, flags=re.IGNORECASE)
    rules.append("admission_length_to_date_diff")
    return text, rules


def _rewrite_to_date_cast(sql: str) -> tuple[str, list[str]]:
    rules: list[str] = []
    text = sql
    changed = False

    def repl(match: re.Match) -> str:
        nonlocal changed
        col = match.group(1)
        col_name = col.split(".")[-1].upper()
        if col_name in _timestamp_cols():
            changed = True
            return f"CAST({col} AS DATE)"
        return match.group(0)

    new_text = _TO_DATE_RE.sub(repl, text)
    if changed:
        rules.append("to_date_on_timestamp_to_cast")
    return new_text, rules


def _rewrite_duration(sql: str) -> tuple[str, list[str]]:
    rules: list[str] = []
    text = sql
    if not re.search(r"(?<!\.)\b(DURATION_DAYS|DURATION)\b", text, re.IGNORECASE):
        return text, rules

    alias = _find_table_alias(text, "TRANSFERS") or "TRANSFERS"
    replacement = f"CAST({alias}.OUTTIME AS DATE) - CAST({alias}.INTIME AS DATE)"
    text = re.sub(r"(?<!\.)\b(DURATION_DAYS|DURATION)\b", replacement, text, flags=re.IGNORECASE)
    rules.append("duration_to_date_diff")
    return text, rules


def _fix_orphan_by(sql: str) -> tuple[str, list[str]]:
    rules: list[str] = []
    text = sql
    if "GROUP BY" in text.upper():
        return text, rules
    if not re.search(r"\b(COUNT|AVG|SUM|MIN|MAX)\s*\(", text, re.IGNORECASE):
        return text, rules
    for match in re.finditer(r"\bBY\b", text, re.IGNORECASE):
        prefix = text[:match.start()].upper()
        if prefix.endswith("ORDER "):
            continue
        text = text[:match.start()] + "GROUP BY" + text[match.end():]
        rules.append("orphan_by_to_group_by")
        break
    return text, rules


def _fix_having_where(sql: str) -> tuple[str, list[str]]:
    rules: list[str] = []
    text = sql
    if _HAVING_WHERE_RE.search(text):
        text = _HAVING_WHERE_RE.sub("HAVING", text)
        rules.append("fix_having_where")

    new_text = _HAVING_TRUE_RE.sub("", text)
    if new_text != text:
        text = new_text
        rules.append("drop_having_true")
    return text, rules


def _rewrite_hospital_expire_flag(sql: str) -> tuple[str, list[str]]:
    rules: list[str] = []
    text = sql
    if not _HOSPITAL_EXPIRE_RE.search(text):
        return text, rules
    text = _HOSPITAL_EXPIRE_RE.sub("HOSPITAL_EXPIRE_FLAG = 1", text)
    rules.append("hospital_expire_flag_to_one")
    return text, rules


def _rewrite_age_from_sysdate_diff(sql: str) -> tuple[str, list[str]]:
    rules: list[str] = []
    text = sql

    new_text = _SYSDATE_YEAR_DIFF_RE.sub("ANCHOR_AGE", text)
    if new_text != text:
        rules.append("sysdate_diff_years_to_anchor_age")
    return new_text, rules


def _rewrite_absolute_year_range(question: str, sql: str) -> tuple[str, list[str]]:
    rules: list[str] = []
    text = sql

    years = sorted({int(m.group(0)) for m in _ABS_YEAR_RE.finditer(question)})
    if len(years) < 2:
        return text, rules
    start_year = years[0]
    end_year = years[-1]
    if end_year < start_year or (end_year - start_year) > 30:
        return text, rules
    if "ADD_MONTHS" not in text.upper():
        return text, rules

    changed = False

    def _repl(match: re.Match) -> str:
        nonlocal changed
        col = match.group(1)
        op = match.group(2)
        changed = True
        if op in (">=", ">"):
            return f"{col} >= TO_DATE('{start_year}-01-01', 'YYYY-MM-DD')"
        # Upper bound is normalized to strict less-than of next year.
        return f"{col} < TO_DATE('{end_year + 1}-01-01', 'YYYY-MM-DD')"

    new_text = _ADD_MONTHS_PRED_RE.sub(_repl, text)
    if changed:
        rules.append("absolute_year_range_from_question")
    return new_text, rules


def _rewrite_extract_day_diff(sql: str) -> tuple[str, list[str]]:
    rules: list[str] = []
    text = sql

    def repl(match: re.Match) -> str:
        expr = match.group(1).strip()
        # If the inner expression is already a date diff, EXTRACT(DAY FROM ...) is unnecessary.
        if "-" in expr:
            return expr
        return match.group(0)

    new_text = _EXTRACT_DAY_RE.sub(repl, text)
    if new_text != text:
        rules.append("extract_day_to_date_diff")
    return new_text, rules


def _rewrite_age_from_anchor(sql: str) -> tuple[str, list[str]]:
    rules: list[str] = []
    text = sql

    def repl(match: re.Match) -> str:
        expr = match.group(1)
        if "." in expr:
            alias = expr.split(".")[0]
            return f"{alias}.ANCHOR_AGE"
        return "ANCHOR_AGE"

    new_text = _AGE_FROM_ANCHOR_RE.sub(repl, text)
    if new_text != text:
        rules.append("anchor_year_to_anchor_age")
    return new_text, rules


def _rewrite_age_from_birthdate(sql: str) -> tuple[str, list[str]]:
    rules: list[str] = []
    text = sql

    def repl(match: re.Match) -> str:
        expr = match.group(1)
        if "." in expr:
            alias = expr.split(".")[0]
            return f"{alias}.ANCHOR_AGE"
        return "ANCHOR_AGE"

    new_text = _AGE_FROM_BIRTHDATE_RE.sub(repl, text)
    if new_text != text:
        rules.append("birthdate_to_anchor_age")
    return new_text, rules


def _rewrite_birthdate_to_anchor_age(sql: str) -> tuple[str, list[str]]:
    rules: list[str] = []
    text = sql
    if re.search(r"\bBIRTHDATE\b", text, re.IGNORECASE):
        text = re.sub(r"\bBIRTHDATE\b", "ANCHOR_AGE", text, flags=re.IGNORECASE)
        rules.append("birthdate_col_to_anchor_age")
    if re.search(r"\bDOB\b", text, re.IGNORECASE):
        text = re.sub(r"\bDOB\b", "ANCHOR_AGE", text, flags=re.IGNORECASE)
        rules.append("dob_col_to_anchor_age")
    return text, rules


def _rewrite_birth_year_age(sql: str) -> tuple[str, list[str]]:
    rules: list[str] = []
    text = sql

    def repl(match: re.Match) -> str:
        return "ANCHOR_AGE"

    new_text = _BIRTH_YEAR_DIFF_RE.sub(repl, text)
    if new_text != text:
        rules.append("birth_year_diff_to_anchor_age")
        text = new_text

    if _BIRTH_YEAR_RE.search(text):
        text = _BIRTH_YEAR_RE.sub("ANCHOR_YEAR", text)
        rules.append("birth_year_to_anchor_year")

    return text, rules


def _normalize_count_aliases(sql: str) -> tuple[str, list[str]]:
    rules: list[str] = []
    text = sql
    keywords = {"FROM", "WHERE", "GROUP", "ORDER", "HAVING", "JOIN", "UNION", "LIMIT", "FETCH"}
    aliases: list[str] = []

    def repl(match: re.Match) -> str:
        alias = match.group(2)
        if alias.upper() == "CNT" or alias.upper() in keywords:
            return match.group(0)
        aliases.append(alias)
        return match.group(1) + "CNT"

    new_text = _COUNT_ALIAS_RE.sub(repl, text)
    if aliases:
        def fix_order(match: re.Match) -> str:
            clause = match.group(1)
            for old in aliases:
                clause = re.sub(rf"\b{re.escape(old)}\b", "CNT", clause, flags=re.IGNORECASE)
            return clause

        new_text = re.sub(r"(\border\s+by\b[^;]*)", fix_order, new_text, count=1, flags=re.IGNORECASE)
        rules.append("count_alias_to_cnt")
    return new_text, rules


def _rewrite_avg_count_alias(sql: str) -> tuple[str, list[str]]:
    rules: list[str] = []
    text = sql
    if re.search(r"\bAVG\s*\(\s*(diagnosis_count|procedure_count|num_diagnoses|num_procedures|[A-Za-z0-9_]*_count)\s*\)", text, re.IGNORECASE):
        text = re.sub(
            r"\bAVG\s*\(\s*(diagnosis_count|procedure_count|num_diagnoses|num_procedures|[A-Za-z0-9_]*_count)\s*\)",
            "AVG(CNT)",
            text,
            flags=re.IGNORECASE,
        )
        rules.append("avg_count_alias_to_cnt")
    return text, rules


def _normalize_avg_aliases(sql: str) -> tuple[str, list[str]]:
    rules: list[str] = []
    text = sql
    alias_map = {
        "DOSES_PER_24_HRS": "avg_doses",
        "AMOUNT": "avg_amount",
        "VALUE": "avg_value",
    }
    for col, alias in alias_map.items():
        pattern = re.compile(
            rf"AVG\(\s*([A-Za-z0-9_\.]*{col})\s*\)\s+AS\s+[A-Za-z0-9_]+",
            re.IGNORECASE,
        )
        if pattern.search(text):
            text = pattern.sub(lambda m: f"AVG({m.group(1)}) AS {alias}", text)
            rules.append(f"avg_alias_{col.lower()}")
    return text, rules


def _fix_order_by_bad_alias(sql: str) -> tuple[str, list[str]]:
    rules: list[str] = []
    text = sql
    if _ORDER_BY_BAD_COUNT_RE.search(text):
        text = _ORDER_BY_BAD_COUNT_RE.sub("ORDER BY CNT", text)
        rules.append("order_by_bad_alias_to_cnt")
    if re.search(r"\bORDER\s+BY\s+CNT\s*\(\s*\*\s*\)\s+CNT\b", text, re.IGNORECASE):
        text = re.sub(r"\bORDER\s+BY\s+CNT\s*\(\s*\*\s*\)\s+CNT\b", "ORDER BY CNT", text, flags=re.IGNORECASE)
        rules.append("order_by_cnt_star")
    if re.search(r"\bORDER\s+BY\s+COUNT\(\*\)\s+CNT\b", text, re.IGNORECASE):
        text = re.sub(r"\bORDER\s+BY\s+COUNT\(\*\)\s+CNT\b", "ORDER BY CNT", text, flags=re.IGNORECASE)
        rules.append("order_by_count_cnt")
    return text, rules


def _fix_order_by_count_suffix(sql: str) -> tuple[str, list[str]]:
    rules: list[str] = []
    text = sql
    if not re.search(r"\bORDER\s+BY\b", text, re.IGNORECASE):
        return text, rules
    if not re.search(r"\bCNT\b", text, re.IGNORECASE):
        return text, rules
    match = re.search(r"\bORDER\s+BY\s+([A-Za-z0-9_]+)(\s+DESC|\s+ASC)?\b", text, re.IGNORECASE)
    if not match:
        return text, rules
    alias = match.group(1)
    direction = match.group(2) or ""
    if alias.upper() != "CNT" and alias.upper().endswith("_COUNT"):
        text = re.sub(
            r"\bORDER\s+BY\s+[A-Za-z0-9_]+(\s+DESC|\s+ASC)?\b",
            f"ORDER BY CNT{direction}",
            text,
            count=1,
            flags=re.IGNORECASE,
        )
        rules.append("order_by_count_suffix_to_cnt")
    return text, rules


def _strip_for_update(sql: str) -> tuple[str, list[str]]:
    rules: list[str] = []
    text = sql
    if _FOR_UPDATE_RE.search(text):
        text = _FOR_UPDATE_RE.sub("", text)
        rules.append("strip_for_update")
    return text, rules


def _wrap_top_n(question: str, sql: str) -> tuple[str, list[str]]:
    rules: list[str] = []
    text = sql
    if re.search(r"\bROWNUM\b", text, re.IGNORECASE) or _LIMIT_RE.search(text) or _FETCH_RE.search(text):
        return text, rules

    q = question.lower()
    match = re.search(r"\btop\s+(\d+)\b", q)
    if not match and not any(k in q for k in ("top", "most", "highest")):
        return text, rules
    n = int(match.group(1)) if match else 10
    if n <= 0:
        return text, rules

    text = _wrap_with_rownum(text, n)
    rules.append(f"wrap_top_{n}_rownum")
    return text, rules


def _reorder_count_select(sql: str) -> tuple[str, list[str]]:
    rules: list[str] = []
    text = sql
    pattern = re.compile(
        r"^\s*SELECT\s+COUNT\(\*\)\s+AS\s+CNT\s*,\s*([A-Za-z0-9_\.]+)\s+FROM",
        re.IGNORECASE,
    )
    match = pattern.search(text)
    if not match:
        return text, rules
    col = match.group(1)
    text = pattern.sub(f"SELECT {col}, COUNT(*) AS CNT FROM", text, count=1)
    rules.append("reorder_count_select")
    return text, rules


def _reorder_avg_select(sql: str) -> tuple[str, list[str]]:
    rules: list[str] = []
    text = sql
    pattern = re.compile(
        r"^\s*SELECT\s+AVG\(\s*([A-Za-z0-9_\.]+)\s*\)\s+AS\s+([A-Za-z0-9_]+)\s*,\s*([A-Za-z0-9_\.]+)\s+FROM",
        re.IGNORECASE,
    )
    match = pattern.search(text)
    if not match:
        return text, rules
    avg_expr = match.group(1)
    avg_alias = match.group(2)
    col = match.group(3)
    text = pattern.sub(f"SELECT {col}, AVG({avg_expr}) AS {avg_alias} FROM", text, count=1)
    rules.append("reorder_avg_select")
    return text, rules


def _ensure_avg_not_null(sql: str) -> tuple[str, list[str]]:
    rules: list[str] = []
    text = sql
    targets = {"DOSES_PER_24_HRS", "AMOUNT", "VALUE"}
    avg_exprs: list[str] = []

    for match in re.finditer(r"AVG\s*\(\s*([A-Za-z0-9_\.]+)\s*\)", text, re.IGNORECASE):
        expr = match.group(1)
        col = expr.split(".")[-1].upper()
        if col in targets:
            avg_exprs.append(expr)

    if not avg_exprs:
        return text, rules

    for expr in avg_exprs:
        col = expr.split(".")[-1]
        if re.search(rf"\b{re.escape(expr)}\b\s+IS\s+NOT\s+NULL", text, re.IGNORECASE):
            continue
        if re.search(rf"(?<!\.)\b{re.escape(col)}\b\s+IS\s+NOT\s+NULL", text, re.IGNORECASE):
            continue

        predicate = f"{expr} IS NOT NULL"
        if re.search(r"\bwhere\b", text, re.IGNORECASE):
            text = re.sub(r"\bwhere\b", f"WHERE {predicate} AND", text, count=1, flags=re.IGNORECASE)
        elif re.search(r"\bgroup\s+by\b", text, re.IGNORECASE):
            text = re.sub(r"\bgroup\s+by\b", f"WHERE {predicate} GROUP BY", text, count=1, flags=re.IGNORECASE)
        elif re.search(r"\border\s+by\b", text, re.IGNORECASE):
            text = re.sub(r"\border\s+by\b", f"WHERE {predicate} ORDER BY", text, count=1, flags=re.IGNORECASE)
        else:
            text = text.rstrip(";") + f" WHERE {predicate}"
        rules.append(f"avg_not_null_{col.lower()}")

    return text, rules


def _strip_inpatient_admission_type_filter(question: str, sql: str) -> tuple[str, list[str]]:
    rules: list[str] = []
    text = sql
    if not re.search(r"\bADMISSION_TYPE\b\s*=\s*'INPATIENT'", text, re.IGNORECASE):
        return text, rules

    q = question.lower()
    explicit_admission_type_intent = any(
        token in q
        for token in (
            "admission type",
            "admission_type",
            "encounter class",
            "admit type",
            "입원 유형",
            "입원 타입",
            "입원 형태",
            "입원 종류",
        )
    )
    if explicit_admission_type_intent:
        return text, rules

    column_pattern = r"(?:[A-Za-z0-9_]+\.)?ADMISSION_TYPE"
    value_pattern = r"'INPATIENT'"

    # WHERE admission_type='INPATIENT' AND ...
    text = re.sub(
        rf"\bWHERE\s+{column_pattern}\s*=\s*{value_pattern}\s+AND\s+",
        "WHERE ",
        text,
        flags=re.IGNORECASE,
    )
    # ... AND admission_type='INPATIENT'
    text = re.sub(
        rf"\s+AND\s+{column_pattern}\s*=\s*{value_pattern}",
        "",
        text,
        flags=re.IGNORECASE,
    )
    # WHERE admission_type='INPATIENT' GROUP/ORDER/HAVING ...
    text = re.sub(
        rf"\bWHERE\s+{column_pattern}\s*=\s*{value_pattern}\s+(GROUP\s+BY|ORDER\s+BY|HAVING)\b",
        r" \1",
        text,
        flags=re.IGNORECASE,
    )
    # WHERE admission_type='INPATIENT' (end)
    text = re.sub(
        rf"\bWHERE\s+{column_pattern}\s*=\s*{value_pattern}\s*(;)?\s*$",
        r"\1",
        text,
        flags=re.IGNORECASE,
    )
    text = re.sub(r"\bWHERE\s+AND\b", "WHERE", text, flags=re.IGNORECASE)
    text = re.sub(r"\s{2,}", " ", text).strip()
    if text != sql:
        rules.append("strip_inpatient_admission_type_filter")
    return text, rules


def _strip_time_window_if_absent(question: str, sql: str) -> tuple[str, list[str]]:
    rules: list[str] = []
    text = sql
    q = question.lower()
    time_keywords = {
        "last",
        "past",
        "recent",
        "today",
        "yesterday",
        "week",
        "month",
        "year",
        "since",
        "before",
        "after",
        "between",
    }
    if any(k in q for k in time_keywords):
        return text, rules

    if not _TIME_WINDOW_RE.search(text):
        return text, rules

    text = _TIME_WINDOW_RE.sub("", text)
    text = re.sub(r"\bWHERE\s+AND\b", "WHERE", text, flags=re.IGNORECASE)
    text = re.sub(r"\bAND\s+AND\b", "AND", text, flags=re.IGNORECASE)
    text = re.sub(r"\bWHERE\s*(GROUP|ORDER)\b", r"\1", text, flags=re.IGNORECASE)
    text = re.sub(r"\bWHERE\s*$", "", text, flags=re.IGNORECASE)
    rules.append("strip_time_window")
    return text, rules


def _ensure_group_by_not_null(question: str, sql: str) -> tuple[str, list[str]]:
    rules: list[str] = []
    text = sql
    if "GROUP BY" not in text.upper():
        return text, rules
    q = question.lower()
    if "by" not in q and "count" not in q:
        return text, rules

    match = re.search(r"\bgroup\s+by\b\s+(.+?)(?:\border\s+by\b|$)", text, re.IGNORECASE | re.DOTALL)
    if not match:
        return text, rules
    group_clause = match.group(1)
    cols = [c.strip() for c in group_clause.split(",") if c.strip()]
    simple_cols = []
    for col in cols:
        if _IDENT_RE.fullmatch(col) or re.fullmatch(r"[A-Za-z_][A-Za-z0-9_$#\\.]*", col):
            simple_cols.append(col)
    if not simple_cols:
        return text, rules

    filters = []
    for col in simple_cols:
        if re.search(rf"\b{re.escape(col)}\b\s+IS\s+NOT\s+NULL", text, re.IGNORECASE):
            continue
        filters.append(f"{col} IS NOT NULL")

    if not filters:
        return text, rules

    predicate = " AND ".join(filters)
    if re.search(r"\bwhere\b", text, re.IGNORECASE):
        text = re.sub(r"\bwhere\b", f"WHERE {predicate} AND", text, count=1, flags=re.IGNORECASE)
    else:
        if re.search(r"\bgroup\s+by\b", text, re.IGNORECASE):
            text = re.sub(r"\bgroup\s+by\b", f"WHERE {predicate} GROUP BY", text, count=1, flags=re.IGNORECASE)
        elif re.search(r"\border\s+by\b", text, re.IGNORECASE):
            text = re.sub(r"\border\s+by\b", f"WHERE {predicate} ORDER BY", text, count=1, flags=re.IGNORECASE)
        else:
            text = text.rstrip(";") + f" WHERE {predicate}"
    rules.append("group_by_not_null")
    return text, rules


def _ensure_order_by_count(question: str, sql: str) -> tuple[str, list[str]]:
    rules: list[str] = []
    text = sql
    if "GROUP BY" not in text.upper() or "COUNT(" not in text.upper():
        return text, rules
    if re.search(r"\border\s+by\b", text, re.IGNORECASE):
        return text, rules
    q = question.lower()
    if "by" not in q and "top" not in q and "count" not in q and "most" not in q and "highest" not in q:
        return text, rules

    order_expr = "CNT"
    if not re.search(r"\bCNT\b", text, re.IGNORECASE):
        order_expr = "COUNT(*)"

    text = text.rstrip(";") + f" ORDER BY {order_expr} DESC"
    rules.append("order_by_count_desc")
    return text, rules


def _dedupe_table_alias(sql: str) -> tuple[str, list[str]]:
    rules: list[str] = []
    text = sql
    text2 = re.sub(r"\b(from|join)\s+([A-Za-z0-9_]+)\s+\2\b", r"\1 \2", text, flags=re.IGNORECASE)
    if text2 != text:
        rules.append("dedupe_table_alias")
    return text2, rules


def _rewrite_timestampdiff(sql: str) -> tuple[str, list[str]]:
    rules: list[str] = []
    text = sql

    def repl(match: re.Match) -> str:
        start = match.group(1)
        end = match.group(2)
        return f"CAST({end} AS DATE) - CAST({start} AS DATE)"

    new_text = _TS_DIFF_RE.sub(repl, text)
    if new_text != text:
        rules.append("timestampdiff_day_to_date_diff")
    return new_text, rules


def _rewrite_extract_year(sql: str) -> tuple[str, list[str]]:
    rules: list[str] = []
    text = sql

    def repl(match: re.Match) -> str:
        expr = match.group(1)
        col = expr.split(".")[-1].upper()
        if col in {"ANCHOR_YEAR", "ANCHOR_YEAR_GROUP"}:
            return expr
        return match.group(0)

    new_text = _EXTRACT_YEAR_RE.sub(repl, text)
    if new_text != text:
        rules.append("extract_year_on_anchor_year")
    return new_text, rules


def _rewrite_icu_stay(sql: str) -> tuple[str, list[str]]:
    rules: list[str] = []
    text = sql
    if not _ICU_STAY_RE.search(text):
        return text, rules

    alias = _find_table_alias(text, "ADMISSIONS")
    if alias is None:
        return text, rules

    replacement = f"{alias}.HADM_ID IN (SELECT HADM_ID FROM ICUSTAYS)"
    text = _ICU_STAY_RE.sub(replacement, text)
    rules.append("icu_stay_to_icustays")
    return text, rules


def _rewrite_icustays_flag(sql: str) -> tuple[str, list[str]]:
    rules: list[str] = []
    text = sql
    if not _ICUSTAYS_FLAG_RE.search(text):
        return text, rules

    alias = _find_table_alias(text, "ADMISSIONS")
    if alias is None:
        m = re.search(r"\bfrom\s+([A-Za-z0-9_]+)(?:\s+([A-Za-z0-9_]+))?", text, re.IGNORECASE)
        if m:
            base_table = m.group(1)
            base_alias = m.group(2) or base_table
            if base_alias.upper() in {"WHERE", "JOIN", "GROUP", "ORDER"}:
                base_alias = base_table
            if base_table.upper() in _tables_with_hadm_id():
                alias = base_alias

    replacement = "HADM_ID IN (SELECT HADM_ID FROM ICUSTAYS)"
    if alias:
        replacement = f"{alias}.HADM_ID IN (SELECT HADM_ID FROM ICUSTAYS)"
    text = _ICUSTAYS_FLAG_RE.sub(replacement, text)
    rules.append("icustays_flag_to_icustays")
    return text, rules


def _rewrite_icustays_not_null(sql: str) -> tuple[str, list[str]]:
    rules: list[str] = []
    text = sql
    if not _ICUSTAYS_NOT_NULL_RE.search(text):
        return text, rules

    alias = _find_table_alias(text, "ADMISSIONS")
    if alias is None:
        m = re.search(r"\bfrom\s+([A-Za-z0-9_]+)(?:\s+([A-Za-z0-9_]+))?", text, re.IGNORECASE)
        if m:
            base_table = m.group(1)
            base_alias = m.group(2) or base_table
            if base_alias.upper() in {"WHERE", "JOIN", "GROUP", "ORDER"}:
                base_alias = base_table
            if base_table.upper() in _tables_with_hadm_id():
                alias = base_alias

    replacement = "HADM_ID IN (SELECT HADM_ID FROM ICUSTAYS)"
    if alias:
        replacement = f"{alias}.HADM_ID IN (SELECT HADM_ID FROM ICUSTAYS)"
    text = _ICUSTAYS_NOT_NULL_RE.sub(replacement, text)
    rules.append("icustays_not_null_to_icustays")
    return text, rules


def _ensure_label_join(sql: str) -> tuple[str, list[str]]:
    rules: list[str] = []
    text = sql

    if not re.search(r"(?<!\.)\bLABEL\b", text, re.IGNORECASE):
        return text, rules

    # If label is already available via D_ITEMS or D_LABITEMS, skip
    if re.search(r"\bD_ITEMS\b", text, re.IGNORECASE) or re.search(r"\bD_LABITEMS\b", text, re.IGNORECASE):
        return text, rules

    injected, inject_rules = _inject_join_in_outer(
        text,
        "CHARTEVENTS",
        " JOIN D_ITEMS d ON {alias}.ITEMID = d.ITEMID",
        r"(?<!\.)\bLABEL\b",
        "d.LABEL",
    )
    if injected:
        rules.extend(inject_rules)
        rules.append("join_d_items_for_label")
        return injected, rules

    if re.search(r"\bCHARTEVENTS\b", text, re.IGNORECASE):
        alias = _find_table_alias(text, "CHARTEVENTS") or "CHARTEVENTS"
        join_clause = f" JOIN D_ITEMS d ON {alias}.ITEMID = d.ITEMID"
        text = _insert_join(text, join_clause)
        text = re.sub(r"(?<!\.)\bLABEL\b", "d.LABEL", text, flags=re.IGNORECASE)
        rules.append("join_d_items_for_label")
        return text, rules

    injected, inject_rules = _inject_join_in_outer(
        text,
        "LABEVENTS",
        " JOIN D_LABITEMS d ON {alias}.ITEMID = d.ITEMID",
        r"(?<!\.)\bLABEL\b",
        "d.LABEL",
    )
    if injected:
        rules.extend(inject_rules)
        rules.append("join_d_labitems_for_label")
        return injected, rules

    if re.search(r"\bLABEVENTS\b", text, re.IGNORECASE):
        alias = _find_table_alias(text, "LABEVENTS") or "LABEVENTS"
        join_clause = f" JOIN D_LABITEMS d ON {alias}.ITEMID = d.ITEMID"
        text = _insert_join(text, join_clause)
        text = re.sub(r"(?<!\.)\bLABEL\b", "d.LABEL", text, flags=re.IGNORECASE)
        rules.append("join_d_labitems_for_label")
        return text, rules

    return text, rules


def _ensure_long_title_join(sql: str) -> tuple[str, list[str]]:
    rules: list[str] = []
    text = sql

    if not re.search(r"(?<!\.)\bLONG_TITLE\b", text, re.IGNORECASE):
        return text, rules

    if re.search(r"\bD_ICD_DIAGNOSES\b", text, re.IGNORECASE) or re.search(r"\bD_ICD_PROCEDURES\b", text, re.IGNORECASE):
        return text, rules

    injected, inject_rules = _inject_join_in_outer(
        text,
        "DIAGNOSES_ICD",
        " JOIN D_ICD_DIAGNOSES d ON {alias}.ICD_CODE = d.ICD_CODE AND {alias}.ICD_VERSION = d.ICD_VERSION",
        r"(?<!\.)\bLONG_TITLE\b",
        "d.LONG_TITLE",
    )
    if injected:
        rules.extend(inject_rules)
        rules.append("join_d_icd_diagnoses_for_long_title")
        return injected, rules

    if re.search(r"\bDIAGNOSES_ICD\b", text, re.IGNORECASE):
        alias = _find_table_alias(text, "DIAGNOSES_ICD") or "DIAGNOSES_ICD"
        join_clause = f" JOIN D_ICD_DIAGNOSES d ON {alias}.ICD_CODE = d.ICD_CODE AND {alias}.ICD_VERSION = d.ICD_VERSION"
        text = _insert_join(text, join_clause)
        text = re.sub(r"(?<!\.)\bLONG_TITLE\b", "d.LONG_TITLE", text, flags=re.IGNORECASE)
        rules.append("join_d_icd_diagnoses_for_long_title")
        return text, rules

    injected, inject_rules = _inject_join_in_outer(
        text,
        "PROCEDURES_ICD",
        " JOIN D_ICD_PROCEDURES d ON {alias}.ICD_CODE = d.ICD_CODE AND {alias}.ICD_VERSION = d.ICD_VERSION",
        r"(?<!\.)\bLONG_TITLE\b",
        "d.LONG_TITLE",
    )
    if injected:
        rules.extend(inject_rules)
        rules.append("join_d_icd_procedures_for_long_title")
        return injected, rules

    if re.search(r"\bPROCEDURES_ICD\b", text, re.IGNORECASE):
        alias = _find_table_alias(text, "PROCEDURES_ICD") or "PROCEDURES_ICD"
        join_clause = f" JOIN D_ICD_PROCEDURES d ON {alias}.ICD_CODE = d.ICD_CODE AND {alias}.ICD_VERSION = d.ICD_VERSION"
        text = _insert_join(text, join_clause)
        text = re.sub(r"(?<!\.)\bLONG_TITLE\b", "d.LONG_TITLE", text, flags=re.IGNORECASE)
        rules.append("join_d_icd_procedures_for_long_title")
        return text, rules

    return text, rules


def _rewrite_has_icu_stay(sql: str) -> tuple[str, list[str]]:
    rules: list[str] = []
    text = sql
    if not _HAS_ICU_RE.search(text):
        return text, rules

    alias = _find_table_alias(text, "ADMISSIONS")

    if alias is None:
        return text, rules

    replacement = f"{alias}.HADM_ID IN (SELECT HADM_ID FROM ICUSTAYS)"
    text = _HAS_ICU_RE.sub(replacement, text)
    rules.append("has_icu_stay_to_icustays")
    return text, rules


def _normalize_timestamp_diffs(sql: str) -> tuple[str, list[str]]:
    rules: list[str] = []
    text = sql

    def repl(match: re.Match) -> str:
        a = match.group(1)
        b = match.group(2)
        a_col = a.split(".")[-1].upper()
        b_col = b.split(".")[-1].upper()
        if a_col in _timestamp_cols() and b_col in _timestamp_cols():
            return f"CAST({a} AS DATE) - CAST({b} AS DATE)"
        return match.group(0)

    new_text = _DIFF_RE.sub(repl, text)
    if new_text != text:
        rules.append("timestamp_diff_cast_to_date")
    return new_text, rules


def _rewrite_diagnosis_title_filter_with_icd_map(question: str, sql: str) -> tuple[str, list[str]]:
    rules: list[str] = []
    text = sql
    diagnosis_cfg = load_sql_postprocess_rules().get("diagnosis_rewrite", {})
    if not bool(diagnosis_cfg.get("enabled", True)):
        return text, rules

    table_name = str(diagnosis_cfg.get("table_name") or "DIAGNOSES_ICD").strip().upper()
    if not table_name:
        table_name = "DIAGNOSES_ICD"
    if not re.search(rf"\b{re.escape(table_name)}\b", text, re.IGNORECASE):
        return text, rules
    if not _DIAGNOSIS_TITLE_FILTER_RE.search(text):
        return text, rules

    matched = match_diagnosis_mappings(question)
    if not matched:
        return text, rules

    prefixes: list[str] = []
    for item in matched:
        for prefix in item.get("icd_prefixes", []):
            value = str(prefix).strip().upper()
            if not value or value in prefixes:
                continue
            prefixes.append(value)
    if not prefixes:
        return text, rules

    dx_alias = _find_table_alias(text, table_name) or table_name
    like_template = str(diagnosis_cfg.get("icd_like_template") or "{alias}.ICD_CODE LIKE '{prefix}%'")
    join_operator = str(diagnosis_cfg.get("join_operator") or " OR ")
    predicates = []
    for prefix in prefixes:
        try:
            predicates.append(like_template.format(alias=dx_alias, prefix=prefix))
        except Exception:
            predicates.append(f"{dx_alias}.ICD_CODE LIKE '{prefix}%'")
    icd_filter = "(" + join_operator.join(predicates) + ")"
    rewritten = _DIAGNOSIS_TITLE_FILTER_RE.sub(icd_filter, text)
    if rewritten != text:
        rules.append("diagnosis_title_filter_to_icd_prefix")
    return rewritten, rules


def _rewrite_mortality_avg_under_diagnosis_join(sql: str) -> tuple[str, list[str]]:
    rules: list[str] = []
    text = sql
    mortality_cfg = load_sql_postprocess_rules().get("mortality_rewrite", {})
    if not bool(mortality_cfg.get("enabled", True)):
        return text, rules

    join_table = str(mortality_cfg.get("join_table") or "DIAGNOSES_ICD").strip().upper() or "DIAGNOSES_ICD"
    admissions_table = str(mortality_cfg.get("admissions_table") or "ADMISSIONS").strip().upper() or "ADMISSIONS"
    outcome_column = str(mortality_cfg.get("outcome_column") or "HOSPITAL_EXPIRE_FLAG").strip().upper() or "HOSPITAL_EXPIRE_FLAG"
    key_column = str(mortality_cfg.get("key_column") or "HADM_ID").strip().upper() or "HADM_ID"
    numerator_template = str(
        mortality_cfg.get("numerator_template")
        or "COUNT(DISTINCT CASE WHEN {expire_ref} = 1 THEN {key_ref} END)"
    )
    denominator_template = str(
        mortality_cfg.get("denominator_template")
        or "NULLIF(COUNT(DISTINCT {key_ref}), 0)"
    )

    if not re.search(rf"\bJOIN\s+{re.escape(join_table)}\b", text, re.IGNORECASE):
        return text, rules
    if not re.search(r"\bAVG\s*\(", text, re.IGNORECASE):
        return text, rules
    if not re.search(rf"\b{re.escape(outcome_column)}\b", text, re.IGNORECASE):
        return text, rules

    adm_alias = _find_table_alias(text, admissions_table)
    key_ref = f"{adm_alias}.{key_column}" if adm_alias else key_column
    expire_ref = f"{adm_alias}.{outcome_column}" if adm_alias else outcome_column
    try:
        numerator_expr = numerator_template.format(expire_ref=expire_ref, key_ref=key_ref)
    except Exception:
        numerator_expr = f"COUNT(DISTINCT CASE WHEN {expire_ref} = 1 THEN {key_ref} END)"
    try:
        denominator_expr = denominator_template.format(key_ref=key_ref, expire_ref=expire_ref)
    except Exception:
        denominator_expr = f"NULLIF(COUNT(DISTINCT {key_ref}), 0)"
    ratio_expr = f"{numerator_expr} / {denominator_expr}"

    expire_ref_pattern = re.escape(expire_ref)
    changed = False

    direct_avg_re = re.compile(rf"AVG\s*\(\s*{expire_ref_pattern}\s*\)", re.IGNORECASE)
    rewritten = direct_avg_re.sub(ratio_expr, text)
    if rewritten != text:
        changed = True
        text = rewritten

    case_avg_re = re.compile(
        rf"AVG\s*\(\s*CASE\s+WHEN\s+{expire_ref_pattern}\s*=\s*1\s+THEN\s+1\s+ELSE\s+0\s+END\s*\)",
        re.IGNORECASE,
    )
    rewritten = case_avg_re.sub(ratio_expr, text)
    if rewritten != text:
        changed = True
        text = rewritten

    if changed:
        rules.append("mortality_avg_to_distinct_hadm_ratio")
    return text, rules


def _add_icd_version_for_prefix_filters(sql: str) -> tuple[str, list[str]]:
    rules: list[str] = []
    text = sql
    version_cfg = load_sql_postprocess_rules().get("icd_version_inference", {})
    if not bool(version_cfg.get("enabled", True)):
        return text, rules

    table_name = str(version_cfg.get("table_name") or "DIAGNOSES_ICD").strip().upper() or "DIAGNOSES_ICD"
    version_column = str(version_cfg.get("version_column") or "ICD_VERSION").strip().upper() or "ICD_VERSION"
    predicate_template = str(
        version_cfg.get("predicate_template")
        or "({version_col} = {version} AND {code_expr} LIKE '{prefix}%')"
    )
    try:
        letter_version = int(version_cfg.get("letter_prefix_version", 10))
    except Exception:
        letter_version = 10
    try:
        digit_version = int(version_cfg.get("digit_prefix_version", 9))
    except Exception:
        digit_version = 9

    if not re.search(rf"\b{re.escape(table_name)}\b", text, re.IGNORECASE):
        return text, rules
    if re.search(rf"\b{re.escape(version_column)}\b", text, re.IGNORECASE):
        return text, rules
    if not _ICD_CODE_LIKE_RE.search(text):
        return text, rules

    changed = False

    def repl(match: re.Match) -> str:
        nonlocal changed
        lhs = match.group("lhs")
        prefix = match.group("prefix").strip()
        if not prefix:
            return match.group(0)
        first = prefix[0]
        if first.isalpha():
            version = letter_version
        elif first.isdigit():
            version = digit_version
        else:
            return match.group(0)
        version_col = f"{lhs.rsplit('.', 1)[0]}.{version_column}" if "." in lhs else version_column
        changed = True
        try:
            return predicate_template.format(
                version_col=version_col,
                version=version,
                code_expr=lhs,
                prefix=prefix,
            )
        except Exception:
            return f"({version_col} = {version} AND {lhs} LIKE '{prefix}%')"

    rewritten = _ICD_CODE_LIKE_RE.sub(repl, text)
    if changed and rewritten != text:
        rules.append("add_icd_version_to_prefix_filters")
        return rewritten, rules
    return text, rules


def _quote_to_char_format_literals(sql: str) -> tuple[str, list[str]]:
    rules: list[str] = []
    text = sql
    rewritten = _TO_CHAR_BARE_FMT_RE.sub(
        lambda m: f"TO_CHAR({m.group('expr').strip()}, '{m.group('fmt').upper()}')",
        text,
    )
    if rewritten != text:
        rules.append("quote_to_char_format_literal")
        return rewritten, rules
    return text, rules


def postprocess_sql(question: str, sql: str) -> tuple[str, list[str]]:
    rules: list[str] = []
    q = question.strip()

    match = _COUNT_RE.match(q)
    if match:
        table = match.group(1)
        rules.append("count_rows_sampled_template")
        return f"SELECT COUNT(*) AS cnt FROM {table} WHERE ROWNUM <= 1000", rules

    match = _DISTINCT_RE.match(q)
    if match:
        col = match.group(1)
        table = match.group(2)
        rules.append("distinct_sample_template")
        return f"SELECT DISTINCT {col} FROM {table} WHERE {col} IS NOT NULL AND ROWNUM <= 50", rules

    match = _SAMPLE_RE.match(q)
    if match:
        table = match.group(1)
        cols = _parse_columns(match.group(2))
        first = _first(cols)
        if cols and first:
            cols_sql = ", ".join(cols)
            rules.append("sample_rows_template")
            return (
                f"SELECT {cols_sql} FROM {table} WHERE {first} IS NOT NULL AND ROWNUM <= 100",
                rules,
            )

    mapped, map_rules = _apply_schema_mappings(sql)
    rules.extend(map_rules)

    micro_fixed, micro_rules = _ensure_microbiology_table(mapped)
    rules.extend(micro_rules)

    micro_by_q, micro_q_rules = _ensure_microbiology_by_question(q, micro_fixed)
    rules.extend(micro_q_rules)

    icu_forced, icu_force_rules = _ensure_icustays_table(q, micro_by_q)
    rules.extend(icu_force_rules)

    chart_forced, chart_rules = _ensure_chartevents_table(q, icu_forced)
    rules.extend(chart_rules)

    lab_forced, lab_rules = _ensure_labevents_table(q, chart_forced)
    rules.extend(lab_rules)

    services_forced, services_rules = _ensure_services_table(q, lab_forced)
    rules.extend(services_rules)

    prescriptions_forced, prescriptions_rules = _ensure_prescriptions_table(q, services_forced)
    rules.extend(prescriptions_rules)

    input_forced, input_rules = _ensure_inputevents_table(q, prescriptions_forced)
    rules.extend(input_rules)

    output_forced, output_rules = _ensure_outputevents_table(q, input_forced)
    rules.extend(output_rules)

    emar_forced, emar_rules = _ensure_emar_table(q, output_forced)
    rules.extend(emar_rules)

    diagnoses_forced, diagnoses_rules = _ensure_diagnoses_icd_table(q, emar_forced)
    rules.extend(diagnoses_rules)

    procedures_forced, procedures_rules = _ensure_procedures_icd_table(q, diagnoses_forced)
    rules.extend(procedures_rules)

    prescriptions_field_fixed, prescriptions_field_rules = _rewrite_prescriptions_drug_field(q, procedures_forced)
    rules.extend(prescriptions_field_rules)

    prescriptions_col_fixed, prescriptions_col_rules = _rewrite_prescriptions_columns(prescriptions_field_fixed)
    rules.extend(prescriptions_col_rules)

    icd_code_fixed, icd_code_rules = _rewrite_icd_code_field(q, prescriptions_col_fixed)
    rules.extend(icd_code_rules)

    icd_itemid_fixed, icd_itemid_rules = _rewrite_itemid_in_icd_tables(icd_code_fixed)
    rules.extend(icd_itemid_rules)

    emar_field_fixed, emar_field_rules = _rewrite_emar_medication_field(q, icd_itemid_fixed)
    rules.extend(emar_field_rules)

    transfers_fixed, transfers_rules = _ensure_transfers_eventtype(q, emar_field_fixed)
    rules.extend(transfers_rules)

    transfers_careunit_fixed, transfers_careunit_rules = _rewrite_transfers_careunit_fields(transfers_fixed)
    rules.extend(transfers_careunit_rules)

    services_order_fixed, services_order_rules = _rewrite_services_order_type(q, transfers_careunit_fixed)
    rules.extend(services_order_rules)

    rewritten_icu, icu_rules = _rewrite_has_icu_stay(services_order_fixed)
    rules.extend(icu_rules)

    rewritten_icu2, icu2_rules = _rewrite_icu_stay(rewritten_icu)
    rules.extend(icu2_rules)

    rewritten_icu3, icu3_rules = _rewrite_icustays_flag(rewritten_icu2)
    rules.extend(icu3_rules)

    rewritten_icu4, icu4_rules = _rewrite_icustays_not_null(rewritten_icu3)
    rules.extend(icu4_rules)

    rewritten_adm_len, adm_len_rules = _rewrite_admission_length(rewritten_icu4)
    rules.extend(adm_len_rules)

    rewritten_dur, dur_rules = _rewrite_duration(rewritten_adm_len)
    rules.extend(dur_rules)

    rewritten_date_cast, date_cast_rules = _rewrite_to_date_cast(rewritten_dur)
    rules.extend(date_cast_rules)

    rewritten_extract_day, extract_day_rules = _rewrite_extract_day_diff(rewritten_date_cast)
    rules.extend(extract_day_rules)

    rewritten_ts, ts_rules = _rewrite_timestampdiff(rewritten_extract_day)
    rules.extend(ts_rules)

    rewritten_ext, ext_rules = _rewrite_extract_year(rewritten_ts)
    rules.extend(ext_rules)

    joined_adm, adm_rules = _ensure_admissions_join(rewritten_ext)
    rules.extend(adm_rules)

    year_range_fixed, year_range_rules = _rewrite_absolute_year_range(q, joined_adm)
    rules.extend(year_range_rules)

    age_from_diff_fixed, age_from_diff_rules = _rewrite_age_from_sysdate_diff(year_range_fixed)
    rules.extend(age_from_diff_rules)

    joined_patients, patient_rules = _ensure_patients_join(age_from_diff_fixed)
    rules.extend(patient_rules)

    rewritten_patients_id, patient_id_rules = _rewrite_patients_id(joined_patients)
    rules.extend(patient_id_rules)

    joined_icd, icd_rules = _ensure_icd_join(question, rewritten_patients_id)
    rules.extend(icd_rules)

    labeled, label_rules = _ensure_label_join(joined_icd)
    rules.extend(label_rules)

    diag_title_fixed, diag_title_rules = _ensure_diagnosis_title_join(q, labeled)
    rules.extend(diag_title_rules)

    proc_title_fixed, proc_title_rules = _ensure_procedure_title_join(q, diag_title_fixed)
    rules.extend(proc_title_rules)

    proc_cleanup_fixed, proc_cleanup_rules = _cleanup_procedure_title_joins(proc_title_fixed)
    rules.extend(proc_cleanup_rules)

    titled, title_rules = _ensure_long_title_join(proc_cleanup_fixed)
    rules.extend(title_rules)

    diagnosis_map_fixed, diagnosis_map_rules = _rewrite_diagnosis_title_filter_with_icd_map(q, titled)
    rules.extend(diagnosis_map_rules)

    icd_version_fixed, icd_version_rules = _add_icd_version_for_prefix_filters(diagnosis_map_fixed)
    rules.extend(icd_version_rules)

    mortality_rate_fixed, mortality_rate_rules = _rewrite_mortality_avg_under_diagnosis_join(icd_version_fixed)
    rules.extend(mortality_rate_rules)

    timed, time_rules = _normalize_timestamp_diffs(mortality_rate_fixed)
    rules.extend(time_rules)

    deduped, dedupe_rules = _dedupe_table_alias(timed)
    rules.extend(dedupe_rules)

    grouped, group_rules = _fix_orphan_by(deduped)
    rules.extend(group_rules)

    having_fixed, having_rules = _fix_having_where(grouped)
    rules.extend(having_rules)

    expire_fixed, expire_rules = _rewrite_hospital_expire_flag(having_fixed)
    rules.extend(expire_rules)

    age_fixed, age_rules = _rewrite_age_from_anchor(expire_fixed)
    rules.extend(age_rules)

    birth_fixed, birth_rules = _rewrite_age_from_birthdate(age_fixed)
    rules.extend(birth_rules)

    birth_col_fixed, birth_col_rules = _rewrite_birthdate_to_anchor_age(birth_fixed)
    rules.extend(birth_col_rules)

    birth_year_fixed, birth_year_rules = _rewrite_birth_year_age(birth_col_fixed)
    rules.extend(birth_year_rules)

    icu_careunit_fixed, icu_careunit_rules = _rewrite_icustays_careunit(q, birth_year_fixed)
    rules.extend(icu_careunit_rules)

    icu_los_fixed, icu_los_rules = _rewrite_icustays_los(icu_careunit_fixed)
    rules.extend(icu_los_rules)

    warning_fixed, warning_rules = _rewrite_warning_flag(q, icu_los_fixed)
    rules.extend(warning_rules)

    lab_priority_fixed, lab_priority_rules = _rewrite_lab_priority(q, warning_fixed)
    rules.extend(lab_priority_rules)

    micro_field_fixed, micro_field_rules = _rewrite_micro_count_field(q, lab_priority_fixed)
    rules.extend(micro_field_rules)

    chart_label_fixed, chart_label_rules = _ensure_chart_label(q, micro_field_fixed)
    rules.extend(chart_label_rules)

    lab_label_fixed, lab_label_rules = _ensure_lab_label(q, chart_label_fixed)
    rules.extend(lab_label_rules)

    label_field_fixed, label_field_rules = _rewrite_label_field(q, lab_label_fixed)
    rules.extend(label_field_rules)

    count_fixed, count_rules = _normalize_count_aliases(label_field_fixed)
    rules.extend(count_rules)

    avg_fixed, avg_rules = _rewrite_avg_count_alias(count_fixed)
    rules.extend(avg_rules)

    avg_alias_fixed, avg_alias_rules = _normalize_avg_aliases(avg_fixed)
    rules.extend(avg_alias_rules)

    time_stripped, time_rules = _strip_time_window_if_absent(q, avg_alias_fixed)
    rules.extend(time_rules)

    grouped_filtered, group_filter_rules = _ensure_group_by_not_null(q, time_stripped)
    rules.extend(group_filter_rules)

    avg_not_null_fixed, avg_not_null_rules = _ensure_avg_not_null(grouped_filtered)
    rules.extend(avg_not_null_rules)

    admission_type_fixed, admission_type_rules = _strip_inpatient_admission_type_filter(q, avg_not_null_fixed)
    rules.extend(admission_type_rules)

    ordered, order_rules = _ensure_order_by_count(q, admission_type_fixed)
    rules.extend(order_rules)

    ordered2, order_alias_rules = _fix_order_by_bad_alias(ordered)
    rules.extend(order_alias_rules)

    ordered3, order_suffix_rules = _fix_order_by_count_suffix(ordered2)
    rules.extend(order_suffix_rules)

    update_stripped, update_rules = _strip_for_update(ordered3)
    rules.extend(update_rules)

    # Do not replace whole SQL by keyword-triggered canonical templates here.
    # Postprocess should only normalize/fix generated SQL.
    reordered, reorder_rules = _reorder_count_select(update_stripped)
    rules.extend(reorder_rules)

    avg_reordered, avg_reorder_rules = _reorder_avg_select(reordered)
    rules.extend(avg_reorder_rules)

    wrapped, wrap_rules = _wrap_top_n(q, avg_reordered)
    rules.extend(wrap_rules)

    cap_value = get_settings().row_cap
    if cap_value <= 0:
        cap_value = 100000
    capped, cap_rules = _apply_rownum_cap(wrapped, cap=cap_value)
    rules.extend(cap_rules)

    micro_cap_fixed, micro_cap_rules = _strip_rownum_cap_for_micro_topk(capped)
    rules.extend(micro_cap_rules)

    group_cap_fixed, group_cap_rules = _strip_rownum_cap_for_grouped_tables(micro_cap_fixed)
    rules.extend(group_cap_rules)

    pushed_fixed, pushed_rules = _pushdown_outer_predicates(group_cap_fixed)
    rules.extend(pushed_rules)

    missing_where_fixed, missing_where_rules = _fix_missing_where_predicate(pushed_fixed)
    rules.extend(missing_where_rules)

    to_char_fixed, to_char_rules = _quote_to_char_format_literals(missing_where_fixed)
    rules.extend(to_char_rules)

    rewritten, rewrite_rules = _rewrite_oracle_syntax(to_char_fixed)
    rules.extend(rewrite_rules)
    return rewritten, rules
