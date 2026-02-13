from __future__ import annotations

from typing import Any
import re

from app.core.config import get_settings

_TIMEOUT_MARKERS = ("DPY-4024", "DPI-1067", "ORA-03156", "TIMEOUT")
_INVALID_IDENTIFIER_MARKERS = ("ORA-00904", "INVALID IDENTIFIER")
_INVALID_NUMBER_MARKERS = ("ORA-01722", "INVALID NUMBER")

_ERR_IDENT_RE = re.compile(
    r'ORA-00904:\s*"(?:(?P<alias>[A-Za-z0-9_]+)"\.)?"(?P<column>[A-Za-z0-9_]+)"',
    re.IGNORECASE,
)
_TABLE_ALIAS_RE = re.compile(
    r"\b(?:FROM|JOIN)\s+(?P<table>[A-Za-z_][A-Za-z0-9_$#]*)"
    r"(?:\s+(?:AS\s+)?(?P<alias>[A-Za-z_][A-Za-z0-9_$#]*))?",
    re.IGNORECASE,
)


def _contains_any(text: str, markers: tuple[str, ...]) -> bool:
    upper = str(text or "").upper()
    return any(marker in upper for marker in markers)


def _find_aliases(sql: str, table_name: str) -> set[str]:
    aliases: set[str] = set()
    target = table_name.upper()
    for match in _TABLE_ALIAS_RE.finditer(sql):
        table = str(match.group("table") or "").strip().upper()
        if table != target:
            continue
        alias = str(match.group("alias") or "").strip()
        if alias:
            aliases.add(alias)
    aliases.add(target)
    return aliases


def _replace_alias_col(sql: str, aliases: set[str], source_col: str, target_col: str) -> str:
    text = sql
    for alias in aliases:
        text = re.sub(
            rf"\b{re.escape(alias)}\.{re.escape(source_col)}\b",
            f"{alias}.{target_col}",
            text,
            flags=re.IGNORECASE,
        )
    return text


def _strip_top_level_order_by(sql: str) -> tuple[str, bool]:
    text = str(sql or "").strip().rstrip(";")
    if not text:
        return text, False
    upper = text.upper()
    depth = 0
    in_single = False
    order_pos = -1
    i = 0
    while i < len(upper):
        ch = upper[i]
        if in_single:
            if ch == "'":
                if i + 1 < len(upper) and upper[i + 1] == "'":
                    i += 2
                    continue
                in_single = False
            i += 1
            continue
        if ch == "'":
            in_single = True
            i += 1
            continue
        if ch == "(":
            depth += 1
            i += 1
            continue
        if ch == ")":
            depth = max(0, depth - 1)
            i += 1
            continue
        if depth == 0 and upper.startswith("ORDER BY", i):
            prev = upper[i - 1] if i > 0 else " "
            if not (prev.isalnum() or prev in {"_", "$", "#"}):
                order_pos = i
        i += 1

    if order_pos < 0:
        return text, False
    return text[:order_pos].rstrip(), True


def _repair_invalid_identifier(sql: str, error_message: str) -> tuple[str, list[str]]:
    rules: list[str] = []
    text = sql
    upper = text.upper()
    err_upper = str(error_message or "").upper()

    # 1) PRESCRIPTIONS.MEDICATION -> PRESCRIPTIONS.DRUG
    if "MEDICATION" in err_upper and "PRESCRIPTIONS" in upper:
        aliases = _find_aliases(text, "PRESCRIPTIONS")
        rewritten = _replace_alias_col(text, aliases, "MEDICATION", "DRUG")
        rewritten = re.sub(r"(?<!\.)\bMEDICATION\b", "DRUG", rewritten, flags=re.IGNORECASE)
        if rewritten != text:
            text = rewritten
            rules.append("template_00904_prescriptions_medication_to_drug")

    # 2) ORDERCATEGORYNAME -> ORDERCATEGORYDESCRIPTION
    if "ORDERCATEGORYNAME" in err_upper:
        rewritten = re.sub(
            r"\bORDERCATEGORYNAME\b",
            "ORDERCATEGORYDESCRIPTION",
            text,
            flags=re.IGNORECASE,
        )
        if rewritten != text:
            text = rewritten
            rules.append("template_00904_ordercategoryname_to_description")

    # 3) TRANSFERS FIRST/LAST_CAREUNIT -> CAREUNIT
    if ("FIRST_CAREUNIT" in err_upper or "LAST_CAREUNIT" in err_upper) and "TRANSFERS" in upper:
        aliases = _find_aliases(text, "TRANSFERS")
        rewritten = text
        rewritten = _replace_alias_col(rewritten, aliases, "FIRST_CAREUNIT", "CAREUNIT")
        rewritten = _replace_alias_col(rewritten, aliases, "LAST_CAREUNIT", "CAREUNIT")
        rewritten = re.sub(r"(?<!\.)\bFIRST_CAREUNIT\b", "CAREUNIT", rewritten, flags=re.IGNORECASE)
        rewritten = re.sub(r"(?<!\.)\bLAST_CAREUNIT\b", "CAREUNIT", rewritten, flags=re.IGNORECASE)
        if rewritten != text:
            text = rewritten
            rules.append("template_00904_transfers_careunit_fix")

    # 4) D_ITEMS/D_LABITEMS LONG_TITLE -> LABEL
    if "LONG_TITLE" in err_upper and ("D_ITEMS" in upper or "D_LABITEMS" in upper):
        rewritten = re.sub(r"\bLONG_TITLE\b", "LABEL", text, flags=re.IGNORECASE)
        if rewritten != text:
            text = rewritten
            rules.append("template_00904_long_title_to_label")

    # 5) ITEMID/ICD_CODE mismatch on item dimensions
    if "ICD_CODE" in err_upper and ("D_ITEMS" in upper or "D_LABITEMS" in upper):
        rewritten = re.sub(
            r"(\b[A-Za-z_][A-Za-z0-9_$#]*\.)ICD_CODE\b",
            r"\1ITEMID",
            text,
            flags=re.IGNORECASE,
        )
        if rewritten != text:
            text = rewritten
            rules.append("template_00904_itemid_icd_code_mismatch_fix")

    # 6) projection alias fallback: INSERTIONS -> CNT
    if "INSERTIONS" in err_upper and re.search(r"\bAS\s+CNT\b", text, re.IGNORECASE):
        rewritten = re.sub(r"\bINSERTIONS\b", "CNT", text, flags=re.IGNORECASE)
        if rewritten != text:
            text = rewritten
            rules.append("template_00904_projection_alias_to_cnt")

    # 7) generic identifier fallback from error payload.
    match = _ERR_IDENT_RE.search(error_message or "")
    if match:
        err_col = str(match.group("column") or "").strip().upper()
        if err_col == "MEDICATION" and "PRESCRIPTIONS" in upper and "template_00904_prescriptions_medication_to_drug" not in rules:
            rewritten = re.sub(r"\bMEDICATION\b", "DRUG", text, flags=re.IGNORECASE)
            if rewritten != text:
                text = rewritten
                rules.append("template_00904_generic_medication_to_drug")

    return text, rules


def _repair_invalid_number(sql: str, error_message: str) -> tuple[str, list[str]]:
    rules: list[str] = []
    text = sql
    upper = text.upper()
    if "D_ICD_DIAGNOSES" in upper and (
        re.search(r"\bPROCEDUREEVENTS\b", upper) or re.search(r"\bCHARTEVENTS\b", upper)
    ):
        rewritten = re.sub(r"\bD_ICD_DIAGNOSES\b", "D_ITEMS", text, flags=re.IGNORECASE)
        rewritten = re.sub(
            r"(\b[A-Za-z_][A-Za-z0-9_$#]*\.)ICD_CODE\b",
            r"\1ITEMID",
            rewritten,
            flags=re.IGNORECASE,
        )
        if rewritten != text:
            text = rewritten
            rules.append("template_01722_event_to_items_join_fix")

    if "D_ICD_PROCEDURES" in upper and (
        re.search(r"\bPROCEDUREEVENTS\b", upper) or re.search(r"\bCHARTEVENTS\b", upper)
    ):
        rewritten = re.sub(r"\bD_ICD_PROCEDURES\b", "D_ITEMS", text, flags=re.IGNORECASE)
        rewritten = re.sub(
            r"(\b[A-Za-z_][A-Za-z0-9_$#]*\.)ICD_CODE\b",
            r"\1ITEMID",
            rewritten,
            flags=re.IGNORECASE,
        )
        if rewritten != text:
            text = rewritten
            rules.append("template_01722_event_to_items_proc_fix")

    if "INVALID NUMBER" in str(error_message or "").upper():
        rewritten = re.sub(
            r"TO_NUMBER\s*\(\s*([A-Za-z_][A-Za-z0-9_$#]*\.[A-Za-z_][A-Za-z0-9_$#]*)\s*\)",
            r"\1",
            text,
            flags=re.IGNORECASE,
        )
        if rewritten != text:
            text = rewritten
            rules.append("template_01722_strip_unnecessary_to_number")

    return text, rules


def _repair_timeout(question: str, sql: str) -> tuple[str, list[str]]:
    rules: list[str] = []
    text = str(sql or "").strip().rstrip(";")
    if not text:
        return text, rules

    topn_intent = bool(re.search(r"\btop\s+\d+\b|상위\s*\d+|탑\s*\d+", question or "", re.IGNORECASE))
    if not topn_intent:
        stripped, changed = _strip_top_level_order_by(text)
        if changed:
            text = stripped
            rules.append("template_timeout_strip_order_by")

    has_agg = bool(
        re.search(r"\bGROUP\s+BY\b|\bCOUNT\s*\(|\bAVG\s*\(|\bSUM\s*\(|\bMIN\s*\(|\bMAX\s*\(", text, re.IGNORECASE)
    )
    has_rownum = bool(re.search(r"\bROWNUM\s*<=\s*\d+", text, re.IGNORECASE))
    if not has_agg and not has_rownum:
        cap = min(max(1000, int(get_settings().row_cap or 5000)), 5000)
        text = f"SELECT * FROM ({text}) WHERE ROWNUM <= {cap}"
        rules.append(f"template_timeout_apply_rownum_cap:{cap}")

    return text, rules


def apply_sql_error_templates(
    *,
    question: str,
    sql: str,
    error_message: str,
) -> tuple[str, list[str]]:
    text = str(sql or "").strip()
    if not text:
        return text, []

    rules: list[str] = []
    err = str(error_message or "")
    if _contains_any(err, _TIMEOUT_MARKERS):
        text, timeout_rules = _repair_timeout(question, text)
        rules.extend(timeout_rules)
    if _contains_any(err, _INVALID_IDENTIFIER_MARKERS):
        text, identifier_rules = _repair_invalid_identifier(text, err)
        rules.extend(identifier_rules)
    if _contains_any(err, _INVALID_NUMBER_MARKERS):
        text, number_rules = _repair_invalid_number(text, err)
        rules.extend(number_rules)

    return text, rules

