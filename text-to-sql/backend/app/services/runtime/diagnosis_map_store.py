from __future__ import annotations

from pathlib import Path
from typing import Any, Iterable
import json
import re


_DIAGNOSIS_MAP_PATH = Path("var/metadata/diagnosis_icd_map.jsonl")
_DIAGNOSIS_MAP_CACHE_MTIME: float = -1.0
_DIAGNOSIS_MAP_CACHE: list[dict[str, Any]] = []


def _normalize_match_text(text: str) -> str:
    return re.sub(r"\s+", "", text.lower())


def load_diagnosis_icd_map() -> list[dict[str, Any]]:
    global _DIAGNOSIS_MAP_CACHE_MTIME
    global _DIAGNOSIS_MAP_CACHE

    if not _DIAGNOSIS_MAP_PATH.exists():
        _DIAGNOSIS_MAP_CACHE_MTIME = -1.0
        _DIAGNOSIS_MAP_CACHE = []
        return []

    mtime = _DIAGNOSIS_MAP_PATH.stat().st_mtime
    if _DIAGNOSIS_MAP_CACHE and _DIAGNOSIS_MAP_CACHE_MTIME == mtime:
        return _DIAGNOSIS_MAP_CACHE

    entries: list[dict[str, Any]] = []
    for line in _DIAGNOSIS_MAP_PATH.read_text(encoding="utf-8").splitlines():
        raw = line.strip()
        if not raw:
            continue
        try:
            item = json.loads(raw)
        except json.JSONDecodeError:
            continue
        if not isinstance(item, dict):
            continue

        term = str(item.get("term") or "").strip()
        if not term:
            continue
        aliases_raw = item.get("aliases") or []
        if isinstance(aliases_raw, list):
            aliases = [str(alias).strip() for alias in aliases_raw if str(alias).strip()]
        else:
            aliases = []
        prefixes_raw = item.get("icd_prefixes") or item.get("prefixes") or []
        if isinstance(prefixes_raw, list):
            prefixes = [str(prefix).strip().upper().replace(".", "") for prefix in prefixes_raw if str(prefix).strip()]
        else:
            prefixes = []
        # Deduplicate while keeping order.
        dedup_prefixes: list[str] = []
        for prefix in prefixes:
            if prefix and prefix not in dedup_prefixes:
                dedup_prefixes.append(prefix)
        if not dedup_prefixes:
            continue
        entries.append({
            "term": term,
            "aliases": aliases,
            "icd_prefixes": dedup_prefixes,
        })

    _DIAGNOSIS_MAP_CACHE_MTIME = mtime
    _DIAGNOSIS_MAP_CACHE = entries
    return entries


def match_diagnosis_mappings(question: str, diagnosis_map: list[dict[str, Any]] | None = None) -> list[dict[str, Any]]:
    normalized_question = _normalize_match_text(question)
    if not normalized_question:
        return []

    matched: list[dict[str, Any]] = []
    source = diagnosis_map if diagnosis_map is not None else load_diagnosis_icd_map()
    for item in source:
        term = str(item.get("term") or "").strip()
        aliases = [str(alias).strip() for alias in item.get("aliases", []) if str(alias).strip()]
        prefixes = [str(prefix).strip().upper().replace(".", "") for prefix in item.get("icd_prefixes", []) if str(prefix).strip()]
        if not term or not prefixes:
            continue
        candidates = [term, *aliases]
        hit_keywords = [candidate for candidate in candidates if _normalize_match_text(candidate) in normalized_question]
        if not hit_keywords:
            continue
        hit_score = max(len(keyword) for keyword in hit_keywords)
        matched.append({
            "term": term,
            "aliases": aliases,
            "icd_prefixes": prefixes,
            "_score": hit_score,
        })
    matched.sort(key=lambda entry: int(entry.get("_score", 0)), reverse=True)
    return matched


def map_prefixes_for_terms(diagnosis_map: list[dict[str, Any]], terms: Iterable[str]) -> list[str]:
    normalized_terms = {_normalize_match_text(str(term)) for term in terms if str(term).strip()}
    if not normalized_terms:
        return []
    prefixes: list[str] = []
    for item in diagnosis_map:
        candidates = [str(item.get("term") or ""), *[str(alias) for alias in item.get("aliases", [])]]
        normalized_candidates = {_normalize_match_text(candidate) for candidate in candidates if candidate}
        if not normalized_candidates.intersection(normalized_terms):
            continue
        for prefix in item.get("icd_prefixes", []):
            value = str(prefix).strip().upper().replace(".", "")
            if value and value not in prefixes:
                prefixes.append(value)
    return prefixes
