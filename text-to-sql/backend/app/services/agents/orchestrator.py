from __future__ import annotations

from typing import Any
from datetime import datetime
import math
import json
import re
from pathlib import Path

from app.core.config import get_settings
from app.services.agents.clarifier import evaluate_question_clarity
from app.services.agents.sql_engineer import generate_sql
from app.services.agents.sql_expert import review_sql
from app.services.agents.sql_postprocess import postprocess_sql
from app.services.agents.translator import contains_korean, translate_to_english
from app.services.cost_tracker import get_cost_tracker
from app.services.policy.gate import precheck_sql
from app.services.runtime.context_builder import build_context_payload, build_context_payload_multi
from app.services.runtime.risk_classifier import classify


def _load_demo_cache(path: str) -> dict[str, Any]:
    file_path = Path(path)
    if not file_path.exists():
        return {}
    try:
        return json.loads(file_path.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        return {}


def _normalize_question(text: str) -> str:
    cleaned = text.lower()
    cleaned = re.sub(r"[^a-z0-9\\s]", " ", cleaned)
    cleaned = re.sub(r"[\\uac00-\\ud7a3]", " ", cleaned)
    cleaned = re.sub(r"\\s+", " ", cleaned).strip()
    return cleaned


def _lookup_demo_cache(cache: dict[str, Any], question: str) -> dict[str, Any] | None:
    if question in cache:
        return {
            "mode": "demo",
            "question": question,
            "result": cache[question],
        }
    alias_map = cache.get("_aliases", {}) if isinstance(cache, dict) else {}
    if isinstance(alias_map, dict):
        aliased_key = alias_map.get(question)
        if aliased_key and aliased_key in cache:
            return {
                "mode": "demo",
                "question": question,
                "result": cache[aliased_key],
                "matched": aliased_key,
            }
    normalized = _normalize_question(question)
    if normalized:
        index = {_normalize_question(k): k for k in cache.keys() if k != "_aliases"}
        matched_key = index.get(normalized)
        if matched_key:
            return {
                "mode": "demo",
                "question": question,
                "result": cache[matched_key],
                "matched": matched_key,
            }
    return None


def _add_llm_cost(usage: dict[str, Any], stage: str) -> None:
    settings = get_settings()
    total_tokens = int(usage.get("total_tokens") or 0)
    if settings.llm_cost_per_1k_tokens_krw > 0 and total_tokens > 0:
        cost = int(math.ceil((total_tokens / 1000) * settings.llm_cost_per_1k_tokens_krw))
        get_cost_tracker().add_cost(cost, {"usage": usage, "stage": stage, "source": "llm"})


def _normalize_string_list(value: Any, *, limit: int) -> list[str]:
    if not isinstance(value, list):
        return []
    items: list[str] = []
    for item in value:
        text = str(item).strip()
        if not text:
            continue
        if text in items:
            continue
        items.append(text)
        if len(items) >= limit:
            break
    return items


_ASCII_WORD_RE = re.compile(r"[A-Za-z][A-Za-z0-9_/-]*")
_MULTI_SPACE_RE = re.compile(r"\s+")
_CLARIFICATION_SLOT_ORDER = ("period", "cohort", "comparison", "metric")
_CURRENT_CALENDAR_YEAR_KO = f"{datetime.now().year}년 전체"
_CURRENT_CALENDAR_YEAR_EN = f"Calendar year {datetime.now().year}"
_SLOT_LABELS_KO = {
    "period": "기간",
    "cohort": "대상 환자",
    "comparison": "비교 기준",
    "metric": "지표",
}
_SLOT_PATTERNS: dict[str, tuple[re.Pattern[str], ...]] = {
    "period": (
        re.compile(r"(최근|지난|작년|올해|전년|전년도|이번)\s*\d*\s*(일|주|개월|달|월|년)?"),
        re.compile(r"(월별|연도별|주별|일별|기간|time|date|month|year|week|day)", re.IGNORECASE),
        re.compile(r"(between|from|to)\s+\S+", re.IGNORECASE),
    ),
    "cohort": (
        re.compile(r"(환자|코호트|대상|집단|남성|여성|성별|연령|세\s*이상|세\s*이하|진단|질환)"),
        re.compile(r"(icu|입원|외래|subject|cohort|group|population|diagnos|disease)", re.IGNORECASE),
    ),
    "comparison": (
        re.compile(r"(비교|대비|전후|차이|증감|군간|대조군)"),
        re.compile(r"(vs|versus|comparison|compared|before|after)", re.IGNORECASE),
    ),
    "metric": (
        re.compile(r"(사망률|사망|생존율|생존|재입원율|재입원|비율|건수|평균|중앙|중위|재원일수)"),
        re.compile(r"(rate|ratio|count|mean|median|mortality|survival|readmission|length\s+of\s+stay)", re.IGNORECASE),
    ),
}
_SLOT_OPTION_SAMPLES_KO: dict[str, tuple[str, ...]] = {
    "period": ("최근 30일", _CURRENT_CALENDAR_YEAR_KO, "입원 후 30일"),
    "cohort": ("전체 환자", "65세 이상 환자", "여성 환자"),
    "comparison": ("비교 없음", "남성 대 여성 비교", "연도별 비교"),
    "metric": ("사망률", "사망 건수", "생존율"),
}
_PERIOD_ONLY_REASON_KO = "질문에 기간 정보가 없어 데이터 범위를 먼저 정해야 합니다."
_PERIOD_ONLY_REASON_EN = "A time range is required before generating SQL."
_PERIOD_ONLY_QUESTION_KO = "어떤 기간으로 분석할까요?"
_PERIOD_ONLY_QUESTION_EN = "What time period should be used?"
_SLOT_LABEL_ALIASES: dict[str, tuple[str, ...]] = {
    "period": ("기간", "시간", "date", "time", "날짜"),
    "cohort": ("대상환자", "대상", "환자군", "코호트", "cohort", "population", "group"),
    "comparison": ("비교기준", "비교", "대조군", "비교군", "comparison"),
    "metric": ("지표", "메트릭", "metric", "결과지표", "평가지표"),
}
_METRIC_HINTS = (
    "사망률",
    "사망 건수",
    "사망",
    "생존율",
    "생존",
    "재입원율",
    "재입원",
    "재원일수",
    "비율",
    "건수",
    "평균",
    "중앙",
    "중위",
)
_TIME_GRAIN_PATTERN = re.compile(
    r"(연도별|월별|주별|일별|분기별|추이|시계열|by\s+year|by\s+month|by\s+week|by\s+day|yearly|monthly|weekly|daily|trend|over\s+time)",
    re.IGNORECASE,
)


def _strip_english_tokens_for_korean(text: str) -> str:
    # 한국어 문장을 유지하면서 영문 토큰을 제거한다.
    cleaned = _ASCII_WORD_RE.sub("", text)
    cleaned = re.sub(r"\s*[:：]\s*", ": ", cleaned)
    cleaned = re.sub(r"\s*[/|]\s*", " / ", cleaned)
    cleaned = _MULTI_SPACE_RE.sub(" ", cleaned).strip(" ,;")
    return cleaned.strip()


def _normalize_conversation(conversation: list[dict[str, Any]] | None) -> list[dict[str, str]]:
    if not conversation:
        return []
    normalized: list[dict[str, str]] = []
    for turn in conversation[-20:]:
        role = str(turn.get("role") or "").strip().lower()
        content = str(turn.get("content") or "").strip()
        if role not in {"user", "assistant"} or not content:
            continue
        normalized.append({"role": role, "content": content[:2000]})
    return normalized


def _is_clarification_prompt_text(text: str) -> bool:
    normalized = _MULTI_SPACE_RE.sub(" ", text).strip().lower()
    if not normalized:
        return False
    hints = (
        "추가로 아래 항목",
        "답변 예시",
        "질문 범위를 조금 더 좁혀",
        "질문 범위를 조금 더",
        "기간을 알려",
        "기간으로 분석",
        "clarify",
        "clarification",
        "what time period should be used",
    )
    return any(hint in normalized for hint in hints)


def _slice_active_clarification_turns(turns: list[dict[str, str]]) -> list[dict[str, str]]:
    if not turns:
        return []

    clarify_indices = [
        idx
        for idx, turn in enumerate(turns)
        if turn["role"] == "assistant" and _is_clarification_prompt_text(turn["content"])
    ]

    start_idx = 0
    if clarify_indices:
        chain_start = clarify_indices[-1]
        while True:
            prev_idx = next((idx for idx in reversed(clarify_indices) if idx < chain_start), None)
            if prev_idx is None:
                break
            has_non_clarify_assistant = any(
                turns[pos]["role"] == "assistant" and not _is_clarification_prompt_text(turns[pos]["content"])
                for pos in range(prev_idx + 1, chain_start)
            )
            if has_non_clarify_assistant:
                break
            chain_start = prev_idx

        base_idx = next((idx for idx in range(chain_start - 1, -1, -1) if turns[idx]["role"] == "user"), None)
        start_idx = base_idx if base_idx is not None else chain_start
    else:
        latest_user_idx = next((idx for idx in range(len(turns) - 1, -1, -1) if turns[idx]["role"] == "user"), None)
        start_idx = latest_user_idx if latest_user_idx is not None else 0

    return turns[start_idx:]


def _extract_requested_slots_from_assistant(text: str) -> set[str]:
    if not text:
        return set()

    requested: set[str] = set()
    in_requested_section = False
    for raw_line in text.splitlines():
        line = raw_line.strip()
        if not line:
            continue
        if "추가로 아래 항목" in line:
            in_requested_section = True
            continue
        if not in_requested_section:
            continue
        if line.startswith("답변 예시") or line.startswith("선택 예시") or line.startswith("이유"):
            break
        bullet = re.match(r"^[-•]\s*(.+)$", line)
        if not bullet:
            continue
        slot = _slot_from_label(bullet.group(1))
        if slot:
            requested.add(slot)

    if requested:
        return requested
    return _extract_slots_from_text(text)


def _extract_slots_from_text(text: str) -> set[str]:
    if not text:
        return set()
    found: set[str] = set()
    for slot, patterns in _SLOT_PATTERNS.items():
        for pattern in patterns:
            if pattern.search(text):
                found.add(slot)
                break
    return found


def _has_time_grain_intent(text: str) -> bool:
    if not text:
        return False
    return bool(_TIME_GRAIN_PATTERN.search(text))


def _truncate_slot_answer(text: str, *, limit: int = 80) -> str:
    normalized = _MULTI_SPACE_RE.sub(" ", text).strip()
    if len(normalized) <= limit:
        return normalized
    return f"{normalized[: limit - 1].rstrip()}…"


def _normalize_slot_label(label: str) -> str:
    return re.sub(r"\s+", "", label).strip().lower()


def _slot_from_label(label: str) -> str | None:
    normalized = _normalize_slot_label(label)
    if not normalized:
        return None
    for slot, aliases in _SLOT_LABEL_ALIASES.items():
        for alias in aliases:
            alias_normalized = _normalize_slot_label(alias)
            if normalized == alias_normalized or normalized.startswith(alias_normalized):
                return slot
    return None


def _push_slot_answer(slot_answers: dict[str, list[str]], slot: str, value: str) -> None:
    normalized = _truncate_slot_answer(value)
    if not normalized:
        return
    if normalized not in slot_answers[slot]:
        slot_answers[slot].append(normalized)


def _extract_labeled_slot_values(text: str) -> dict[str, str]:
    values: dict[str, str] = {}
    chunks = [chunk.strip() for chunk in re.split(r"[\n/]+", text) if chunk.strip()]
    for chunk in chunks:
        match = re.match(r"^([^:：]{1,20})\s*[:：]\s*(.+)$", chunk)
        if not match:
            continue
        slot = _slot_from_label(match.group(1))
        if not slot:
            continue
        value = match.group(2).strip()
        if value:
            values[slot] = value
    return values


def _extract_slot_value_from_free_text(slot: str, text: str) -> str:
    compact = _MULTI_SPACE_RE.sub(" ", text).strip(" .")
    lowered = compact.lower()
    if not compact:
        return ""

    if slot == "period":
        if compact in {"전체", "전체 기간", "전체기간"}:
            return "전체"
        match = re.search(
            r"((최근|지난|작년|올해|전년|전년도|이번)\s*\d*\s*(일|주|개월|달|월|년)?)",
            compact,
        )
        if match:
            return match.group(1).strip()
        return ""

    if slot == "comparison":
        if any(keyword in lowered for keyword in ("비교 없음", "없음", "없다", "no comparison", "none")):
            return "비교 없음"
        vs_match = re.search(
            r"([가-힣A-Za-z0-9_]+)\s*(?:vs|VS|대)\s*([가-힣A-Za-z0-9_]+)",
            compact,
        )
        if vs_match:
            left = vs_match.group(1).strip()
            right = vs_match.group(2).strip()
            if left and right:
                return f"{left} vs {right}"
        compare_match = re.search(r"([가-힣A-Za-z0-9\s]{2,20}비교)", compact)
        if compare_match:
            return compare_match.group(1).strip()
        return ""

    if slot == "metric":
        for hint in _METRIC_HINTS:
            if hint in compact:
                if "icu" in lowered and not hint.lower().startswith("icu"):
                    return f"ICU {hint}"
                return hint
        return ""

    if slot == "cohort":
        if compact in {"전체 환자", "전체"}:
            return "전체 환자"
        age_match = re.search(r"(\d+\s*세\s*(이상|이하))", compact)
        if age_match:
            return f"{age_match.group(1).strip()} 환자"
        if "icu" in lowered or "중환자" in compact:
            return "ICU 환자"
        if "입원" in compact:
            return "입원 환자"
        if "외래" in compact:
            return "외래 환자"
        sex_patient_match = re.search(r"(남성|여성)\s*환자", compact)
        if sex_patient_match:
            return sex_patient_match.group(0).strip()
        simple_patient_match = re.search(r"([가-힣A-Za-z0-9\s]{1,18}환자)", compact)
        if simple_patient_match and "vs" not in lowered and "비교" not in compact:
            candidate = simple_patient_match.group(1).strip()
            if candidate:
                return candidate
        return ""

    return ""


def _is_specific_slot_answer(slot: str, value: str) -> bool:
    normalized = value.strip().lower()
    if not normalized:
        return False
    if slot == "comparison":
        if any(keyword in normalized for keyword in ("비교 없음", "없음", "없다", "no comparison", "none")):
            return True
        return slot in _extract_slots_from_text(value)
    if slot == "metric":
        return slot in _extract_slots_from_text(value)
    if slot == "period":
        if normalized in {"전체", "전체기간", "all", "all period"}:
            return True
        return slot in _extract_slots_from_text(value)
    if slot == "cohort":
        if any(hint in normalized for hint in _METRIC_HINTS):
            return False
        if " vs " in normalized or "비교" in normalized:
            return False
        if normalized in {"전체", "전체 환자", "all", "all patients"}:
            return True
        if any(
            keyword in normalized
            for keyword in (
                "남성",
                "여성",
                "연령",
                "세",
                "이상",
                "이하",
                "진단",
                "질환",
                "icu",
                "입원",
                "외래",
                "중환자",
                "male",
                "female",
                "age",
                "diagnos",
                "disease",
            )
        ):
            return True
        return "환자" in normalized and len(normalized) <= 12
    return bool(normalized)


def _collect_clarification_memory(
    question: str,
    conversation: list[dict[str, Any]] | None,
) -> tuple[str, set[str], dict[str, str]]:
    turns = _normalize_conversation(conversation)
    if question.strip():
        if not turns or turns[-1]["role"] != "user" or turns[-1]["content"] != question.strip():
            turns.append({"role": "user", "content": question.strip()})

    scoped_turns = _slice_active_clarification_turns(turns)

    base_question = question.strip()
    for turn in scoped_turns:
        if turn["role"] == "user":
            base_question = turn["content"]
            break

    asked_slots: set[str] = set()
    slot_answers: dict[str, list[str]] = {slot: [] for slot in _CLARIFICATION_SLOT_ORDER}
    pending_slots: set[str] = set()

    for turn in scoped_turns:
        text = turn["content"].strip()
        if not text:
            continue
        if turn["role"] == "assistant":
            pending_slots = _extract_requested_slots_from_assistant(text)
            asked_slots.update(pending_slots)
            continue

        assigned_slots: set[str] = set()
        labeled_values = _extract_labeled_slot_values(text)
        for slot, value in labeled_values.items():
            _push_slot_answer(slot_answers, slot, value)
            assigned_slots.add(slot)

        detected_slots = _extract_slots_from_text(text)
        for slot in detected_slots:
            if slot in assigned_slots:
                continue
            candidate = _extract_slot_value_from_free_text(slot, text)
            if candidate:
                _push_slot_answer(slot_answers, slot, candidate)
                assigned_slots.add(slot)

        if pending_slots:
            pending_list = [slot for slot in _CLARIFICATION_SLOT_ORDER if slot in pending_slots]
            if len(pending_list) == 1:
                slot = pending_list[0]
                if slot not in assigned_slots:
                    candidate = _extract_slot_value_from_free_text(slot, text)
                    if not candidate and not detected_slots and not labeled_values:
                        candidate = text
                    if candidate:
                        _push_slot_answer(slot_answers, slot, candidate)
            else:
                for slot in pending_list:
                    if slot in assigned_slots:
                        continue
                    candidate = _extract_slot_value_from_free_text(slot, text)
                    if candidate and _is_specific_slot_answer(slot, candidate):
                        _push_slot_answer(slot_answers, slot, candidate)
        pending_slots = set()

    latest_answers: dict[str, str] = {}
    for slot in _CLARIFICATION_SLOT_ORDER:
        values = slot_answers.get(slot) or []
        if not values:
            continue
        latest_answers[slot] = _truncate_slot_answer(values[-1])
    return base_question, asked_slots, latest_answers


def _infer_required_slots(
    *,
    question: str,
    reason: str,
    clarification_question: str,
    options: list[str],
    example_inputs: list[str],
    asked_slots: set[str],
) -> list[str]:
    required = set(asked_slots)
    # 기간은 항상 필수 슬롯으로 간주한다.
    required.add("period")
    for text in [question, reason, clarification_question, *options, *example_inputs]:
        required.update(_extract_slots_from_text(text))
    if not required:
        required.update({"cohort", "metric"})
    return [slot for slot in _CLARIFICATION_SLOT_ORDER if slot in required]


def _build_korean_examples(
    *,
    missing_slots: list[str],
) -> list[str]:
    if not missing_slots:
        return []
    if len(missing_slots) == 1:
        slot = missing_slots[0]
        sample_values = _SLOT_OPTION_SAMPLES_KO.get(slot) or ()
        return [item for item in sample_values[:3] if item]
    examples: list[str] = []
    for idx in range(2):
        parts: list[str] = []
        for slot in missing_slots:
            label = _SLOT_LABELS_KO.get(slot, slot)
            sample_values = _SLOT_OPTION_SAMPLES_KO.get(slot) or ()
            if sample_values:
                parts.append(f"{label}: {sample_values[idx % len(sample_values)]}")
        if parts:
            examples.append(" / ".join(parts))
    return examples[:3]


def _build_korean_consolidated_clarification(
    *,
    question: str,
    reason: str,
    clarification_question: str,
    options: list[str],
    example_inputs: list[str],
    conversation: list[dict[str, Any]] | None,
) -> tuple[str, list[str], list[str], dict[str, str], list[str], list[str], str]:
    base_question, asked_slots, raw_answers = _collect_clarification_memory(question, conversation)
    required_slots = _infer_required_slots(
        question=base_question,
        reason=reason,
        clarification_question=clarification_question,
        options=options,
        example_inputs=example_inputs,
        asked_slots=asked_slots,
    )
    if not required_slots:
        required_slots = ["period", "cohort", "metric"]

    known_answers: dict[str, str] = {}
    for slot in required_slots:
        value = raw_answers.get(slot)
        if value and _is_specific_slot_answer(slot, value):
            known_answers[slot] = value
    # 연도별/월별/추이 같은 질문은 기간을 전체로 기본 해석해 추가 질문을 생략한다.
    if "period" in required_slots and "period" not in known_answers and _has_time_grain_intent(base_question):
        known_answers["period"] = "전체"

    missing_slots = [slot for slot in required_slots if slot not in known_answers]

    lines: list[str] = []
    if known_answers:
        lines.append("현재까지 답변 정리:")
        for slot in required_slots:
            value = known_answers.get(slot)
            if not value:
                continue
            lines.append(f"- {_SLOT_LABELS_KO[slot]}: {value}")
    lines.append("추가로 아래 항목을 한 번에 알려주세요.")
    for slot in missing_slots:
        lines.append(f"- {_SLOT_LABELS_KO[slot]}")

    option_candidates: list[str] = []
    for slot in missing_slots:
        option_candidates.extend(_SLOT_OPTION_SAMPLES_KO.get(slot, ()))
    dedup_options: list[str] = []
    for item in option_candidates:
        if item not in dedup_options:
            dedup_options.append(item)
    examples = _build_korean_examples(
        missing_slots=missing_slots,
    )
    if not examples:
        if len(missing_slots) == 1:
            examples = [item for item in dedup_options[:3] if item]
        else:
            examples = [item for item in example_inputs if item][:3]

    return (
        "\n".join(lines),
        dedup_options[:5],
        examples[:3],
        known_answers,
        required_slots,
        missing_slots,
        base_question,
    )


def _compose_refined_question(
    *,
    base_question: str,
    required_slots: list[str],
    known_answers: dict[str, str],
) -> str:
    details = [
        f"{_SLOT_LABELS_KO[slot]}: {known_answers[slot]}"
        for slot in required_slots
        if slot in known_answers and known_answers[slot]
    ]
    if not details:
        return base_question.strip()
    suffix = " / ".join(details)
    prefix = base_question.strip()
    if not prefix:
        return suffix
    if suffix in prefix:
        return prefix
    return f"{prefix} ({suffix})"


def _default_korean_clarification(question: str) -> tuple[str, list[str], list[str]]:
    q = question.lower()
    if "약" in question or "med" in q or "drug" in q or "medication" in q:
        reason = "요청 범위가 넓어 약물군 또는 진료 영역을 먼저 좁혀야 합니다."
        options = ["심혈관 약물", "정신과 약물", "항생제", "모든 약물"]
        examples = [
            "심혈관 약물에 대한 정보가 필요해요",
            "정신과 약물로 좁혀주세요",
            "항생제만 보여주세요",
        ]
        return reason, options, examples
    reason = "질문 범위를 조금 더 좁혀야 정확한 SQL을 만들 수 있습니다."
    options = ["기간을 지정", "대상 집단을 지정", "지표를 지정"]
    examples = [
        "최근 1년 데이터로 보여주세요",
        "65세 이상 환자로 제한해 주세요",
        "재입원율 기준으로만 알려주세요",
    ]
    return reason, options, examples


def _normalize_clarifier_payload(
    payload: dict[str, Any],
    question: str,
    *,
    conversation: list[dict[str, Any]] | None = None,
) -> dict[str, Any]:
    base_question, _, raw_answers = _collect_clarification_memory(question, conversation)
    period_answer = raw_answers.get("period", "")
    has_period_answer = bool(period_answer and _is_specific_slot_answer("period", period_answer))
    has_time_grain = _has_time_grain_intent(base_question or question)
    must_ask_period = not has_period_answer and not has_time_grain

    need_clarification = bool(payload.get("need_clarification"))
    refined_question = str(payload.get("refined_question") or "").strip()
    reason = str(payload.get("reason") or "").strip()
    clarification_question = str(payload.get("clarification_question") or "").strip()
    if need_clarification and not clarification_question:
        clarification_question = "질문 범위를 조금 더 좁혀주세요."

    options = _normalize_string_list(payload.get("options"), limit=5)
    example_inputs = _normalize_string_list(payload.get("example_inputs"), limit=3)
    known_answers: dict[str, str] = {}

    if contains_korean(question):
        reason = _strip_english_tokens_for_korean(reason)
        clarification_question = _strip_english_tokens_for_korean(clarification_question)
        options = [_strip_english_tokens_for_korean(item) for item in options]
        options = [item for item in options if item]
        example_inputs = [_strip_english_tokens_for_korean(item) for item in example_inputs]
        example_inputs = [item for item in example_inputs if item]

        default_reason, default_options, default_examples = _default_korean_clarification(question)
        if not reason:
            reason = default_reason
        if not clarification_question:
            clarification_question = "어떤 기준으로 범위를 좁힐까요?"
        if not options:
            options = default_options
        if not example_inputs:
            example_inputs = default_examples
        if need_clarification or must_ask_period:
            (
                clarification_question,
                options,
                example_inputs,
                known_answers,
                required_slots,
                missing_slots,
                base_question,
            ) = _build_korean_consolidated_clarification(
                question=question,
                reason=reason,
                clarification_question=clarification_question,
                options=options,
                example_inputs=example_inputs,
                conversation=conversation,
            )
            if "period" in missing_slots:
                need_clarification = True
                reason = _PERIOD_ONLY_REASON_KO
            if not missing_slots:
                need_clarification = False
                if not refined_question:
                    refined_question = _compose_refined_question(
                        base_question=base_question,
                        required_slots=required_slots,
                        known_answers=known_answers,
                    )
                clarification_question = ""
                options = []
                example_inputs = []
    elif must_ask_period:
        need_clarification = True
        reason = reason or _PERIOD_ONLY_REASON_EN
        clarification_question = _PERIOD_ONLY_QUESTION_EN
        options = ["Last 30 days", "Last 12 months", _CURRENT_CALENDAR_YEAR_EN]
        example_inputs = ["Use the last 30 days", f"Use {_CURRENT_CALENDAR_YEAR_EN.lower()}"]

    return {
        "need_clarification": need_clarification,
        "reason": reason,
        "clarification_question": clarification_question,
        "options": options,
        "example_inputs": example_inputs,
        "known_answers": known_answers,
        "refined_question": refined_question,
        "usage": payload.get("usage", {}),
    }


def run_oneshot(
    question: str,
    *,
    skip_policy: bool = False,
    translate: bool | None = None,
    rag_multi: bool | None = None,
    conversation: list[dict[str, Any]] | None = None,
    enable_clarification: bool = False,
) -> dict[str, Any]:
    settings = get_settings()
    original_question = question
    translated_question = None
    use_translate = settings.translate_ko_to_en if translate is None else translate
    use_rag_multi = settings.rag_multi_query if rag_multi is None else rag_multi

    if settings.demo_mode or settings.demo_cache_always:
        cache = _load_demo_cache(settings.demo_cache_path)
        cached = _lookup_demo_cache(cache, question)
        if cached:
            return cached

    if enable_clarification:
        clarity: dict[str, Any] = {
            "need_clarification": False,
            "reason": "",
            "clarification_question": "",
            "options": [],
            "example_inputs": [],
            "refined_question": "",
            "usage": {},
        }
        try:
            clarity_raw = evaluate_question_clarity(original_question, conversation=conversation)
            clarity = _normalize_clarifier_payload(
                clarity_raw,
                original_question,
                conversation=conversation,
            )
            _add_llm_cost(clarity.get("usage", {}), "clarify")
        except Exception:
            clarity = {**clarity, "need_clarification": False}
        if clarity["need_clarification"]:
            return {
                "mode": "clarify",
                "question": original_question,
                "clarification": {
                    "reason": clarity.get("reason"),
                    "question": clarity.get("clarification_question"),
                    "options": clarity.get("options", []),
                    "example_inputs": clarity.get("example_inputs", []),
                    "known_answers": clarity.get("known_answers", {}),
                },
            }
        if clarity.get("refined_question"):
            original_question = clarity["refined_question"]
            question = clarity["refined_question"]
            if settings.demo_mode or settings.demo_cache_always:
                cache = _load_demo_cache(settings.demo_cache_path)
                cached = _lookup_demo_cache(cache, question)
                if cached:
                    return cached

    if use_translate and contains_korean(question):
        try:
            translated_question, usage = translate_to_english(question)
            _add_llm_cost(usage, "translate")
            if translated_question:
                question = translated_question
            else:
                translated_question = None
        except Exception:
            translated_question = None

    if (settings.demo_mode or settings.demo_cache_always) and translated_question:
        cache = _load_demo_cache(settings.demo_cache_path)
        cached = _lookup_demo_cache(cache, question)
        if cached:
            cached["question"] = original_question
            cached["question_en"] = question
            return cached

    risk_info = classify(question)
    if translated_question and use_rag_multi:
        context = build_context_payload_multi([question, original_question])
    else:
        context = build_context_payload(question)

    attempt = 0
    last_error: Exception | None = None
    while attempt <= settings.max_retry_attempts:
        attempt += 1
        try:
            engineer = generate_sql(question, context)
            # LLM 경고 문구는 사용하지 않도록 제거
            engineer.pop("warnings", None)
            final_payload = engineer

            if settings.expert_trigger_mode == "score" and risk_info["risk"] >= settings.expert_score_threshold:
                expert = review_sql(question, context, engineer)
                final_payload = expert
                # LLM 경고 문구는 사용하지 않도록 제거
                final_payload.pop("warnings", None)

            usage = final_payload.get("usage", {})
            _add_llm_cost(usage, "oneshot")

            final_sql = final_payload.get("final_sql") or ""
            if final_sql:
                final_sql, rules = postprocess_sql(question, final_sql)
                if rules:
                    final_payload["final_sql"] = final_sql
                    final_payload["postprocess"] = rules

            policy_result = None
            if not skip_policy and final_sql:
                policy_result = precheck_sql(final_sql, original_question)
            return {
                "mode": "advanced",
                "question": original_question,
                "question_en": question if translated_question else None,
                "risk": risk_info,
                "policy": policy_result,
                "context": context,
                "draft": engineer,
                "final": final_payload,
            }
        except Exception as exc:  # pragma: no cover - depends on LLM
            last_error = exc
            if attempt > settings.max_retry_attempts:
                raise
    raise last_error if last_error else RuntimeError("Unknown error")
