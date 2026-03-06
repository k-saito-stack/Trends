"""Heuristics for behavior-style topic extraction."""

from __future__ import annotations

import re

from packages.core.topic_normalize import normalize_topic_text, should_keep_topic

BEHAVIOR_PATTERNS = (
    re.compile(r"([^\s、。!！?？]{1,20}?する)"),
    re.compile(r"([^\s、。!！?？]{1,20}?した)"),
    re.compile(r"([^\s、。!！?？]{1,20}?してる)"),
    re.compile(r"([^\s、。!！?？]{1,20}?を(?:付ける|つける))"),
    re.compile(r"([^\s、。!！?？]{1,20}?を持つ)"),
    re.compile(r"([^\s、。!！?？]{1,20}?交換)"),
    re.compile(r"([^\s、。!！?？]{1,20}?コーデ)"),
    re.compile(r"([^\s、。!！?？]{1,20}?メイク)"),
    re.compile(r"([^\s、。!！?？]{1,20}?界隈)"),
    re.compile(r"([^\s、。!！?？]{1,20}?活)"),
    re.compile(r"([^\s、。!！?？]{1,20}?チャレンジ)"),
    re.compile(r"([^\s、。!！?？]{1,20}?現象)"),
)


def extract_behavior_phrases(text: str) -> list[str]:
    found: list[str] = []
    for pattern in BEHAVIOR_PATTERNS:
        for match in pattern.findall(text or ""):
            normalized = normalize_topic_text(match)
            if should_keep_topic(normalized) and normalized not in found:
                found.append(normalized)
    return found
