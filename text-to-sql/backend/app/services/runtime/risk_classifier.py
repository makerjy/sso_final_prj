from __future__ import annotations

import re
from typing import Any


_WRITE_KEYWORDS = re.compile(r"\b(delete|update|insert|merge|drop|alter|truncate)\b", re.IGNORECASE)


def classify(question: str) -> dict[str, Any]:
    text = question.lower()
    risk = 0
    complexity = 0

    if _WRITE_KEYWORDS.search(text):
        risk += 5
    if "join" in text:
        complexity += text.count("join")
    if len(text) > 120:
        complexity += 1
    if "all" in text or "everything" in text:
        risk += 1

    intent = "read"
    if risk >= 3:
        intent = "risky"

    return {
        "intent": intent,
        "complexity": complexity,
        "risk": risk,
    }
