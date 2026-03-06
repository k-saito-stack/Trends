"""Phrase and hashtag extraction for topic candidates."""

from __future__ import annotations

import re

from packages.core.behavior_patterns import extract_behavior_phrases
from packages.core.models import CandidateType, ExtractionConfidence, RawCandidate
from packages.core.topic_normalize import normalize_topic_text, should_keep_topic

HASHTAG_RE = re.compile(r"(#[\wぁ-んァ-ン一-龥ー]+)")
KATAKANA_COMPOUND_RE = re.compile(r"([ァ-ヴー]{3,}(?:[ァ-ヴー]+)*)")
PHRASE_RE = re.compile(r"([^\s、。!！?？]{2,20})")


def extract_hashtags(text: str) -> list[str]:
    results: list[str] = []
    for raw in HASHTAG_RE.findall(text or ""):
        normalized = _trim_hashtag_tail(normalize_topic_text(raw))
        if should_keep_topic(normalized) and normalized not in results:
            results.append(normalized)
    return results


def extract_topic_phrases(text: str, max_candidates: int = 8) -> list[str]:
    results: list[str] = []

    for hashtag in extract_hashtags(text):
        results.append(hashtag)

    for phrase in extract_behavior_phrases(text):
        if phrase not in results:
            results.append(phrase)

    for raw in KATAKANA_COMPOUND_RE.findall(text or ""):
        normalized = normalize_topic_text(raw)
        if should_keep_topic(normalized) and normalized not in results:
            results.append(normalized)

    if len(results) < max_candidates:
        for raw in PHRASE_RE.findall(text or ""):
            normalized = normalize_topic_text(raw)
            if should_keep_topic(normalized) and normalized not in results:
                results.append(normalized)
            if len(results) >= max_candidates:
                break

    return results[:max_candidates]


def extract_topic_raw_candidates(
    text: str,
    source_id: str,
    metric_value: float = 1.0,
) -> list[RawCandidate]:
    candidates: list[RawCandidate] = []
    for phrase in extract_topic_phrases(text):
        candidate_type = CandidateType.HASHTAG if phrase.startswith("#") else CandidateType.PHRASE
        if any(token in phrase for token in ("界隈", "活", "チャレンジ", "する", "つける", "付ける", "交換", "コーデ", "メイク")):
            candidate_type = CandidateType.BEHAVIOR
        candidates.append(
            RawCandidate(
                name=phrase,
                type=candidate_type,
                source_id=source_id,
                metric_value=metric_value,
                extraction_confidence=ExtractionConfidence.MEDIUM,
            )
        )
    return candidates


def _trim_hashtag_tail(text: str) -> str:
    if not text.startswith("#"):
        return text
    trimmed = re.sub(r"(が|を|で|と|は|に|も).*$", "", text)
    return trimmed or text
