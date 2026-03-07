"""Rule-based topic extraction for topic-style candidates.

This module keeps phrase / behavior / style / product extraction separate
from entity-oriented NER so discovery sources can produce richer topic
candidates without overloading GiNZA.
"""

from __future__ import annotations

import re
from collections.abc import Iterable
from typing import Any

from packages.core.behavior_patterns import extract_behavior_phrases
from packages.core.domain_classifier import classify_domain
from packages.core.models import CandidateType, Evidence, ExtractionConfidence, RawCandidate
from packages.core.topic_normalize import normalize_topic_text, should_keep_topic

HASHTAG_RE = re.compile(r"(#[\wぁ-んァ-ン一-龥ー]+)")
QUOTED_PHRASE_RE = re.compile(r"[「『“\"]([^」』”\"]{2,24})[」』”\"]")
PHRASE_RE = re.compile(r"([^\s、。!！?？]{2,24})")

STYLE_TOKENS = (
    "コーデ",
    "メイク",
    "ネイル",
    "ヘア",
    "レイヤード",
    "着回し",
    "春服",
    "秋服",
    "冬服",
    "夏服",
)
PRODUCT_TOKENS = (
    "バッグ",
    "スニーカー",
    "サングラス",
    "スカート",
    "ジャケット",
    "デニム",
    "キャップ",
    "リップ",
    "ネックレス",
    "ブーツ",
)
BRANDISH_TOKENS = ("コラボ", "別注", "限定")


def extract_topic_candidates(
    text: str,
    source_id: str,
    metadata: dict[str, Any] | None = None,
    *,
    metric_value: float = 1.0,
    evidence: Evidence | None = None,
    max_candidates: int = 10,
) -> list[RawCandidate]:
    """Extract topic-style raw candidates from a text snippet."""
    metadata = dict(metadata or {})
    candidates: list[RawCandidate] = []
    seen: set[tuple[CandidateType, str]] = set()

    for candidate_type, surface in _iter_topic_surfaces(text):
        normalized = normalize_topic_text(surface)
        if not should_keep_topic(normalized):
            continue
        key = (candidate_type, normalized)
        if key in seen:
            continue
        seen.add(key)
        candidates.append(
            RawCandidate(
                name=surface if candidate_type == CandidateType.HASHTAG else normalized,
                type=candidate_type,
                source_id=source_id,
                metric_value=metric_value,
                evidence=evidence,
                extraction_confidence=_confidence_for(candidate_type),
                domain_class=classify_domain(candidate_type, source_id, text=normalized),
                extra=dict(metadata),
            )
        )
        if len(candidates) >= max_candidates:
            break
    return candidates


def extract_hashtag_candidates(
    text: str,
    source_id: str,
    *,
    metric_value: float = 1.0,
    evidence: Evidence | None = None,
    metadata: dict[str, Any] | None = None,
) -> list[RawCandidate]:
    return [
        RawCandidate(
            name=hashtag,
            type=CandidateType.HASHTAG,
            source_id=source_id,
            metric_value=metric_value,
            evidence=evidence,
            extraction_confidence=ExtractionConfidence.HIGH,
            domain_class=classify_domain(CandidateType.HASHTAG, source_id, text=hashtag),
            extra=dict(metadata or {}),
        )
        for hashtag in _extract_hashtags(text)
    ]


def _iter_topic_surfaces(text: str) -> Iterable[tuple[CandidateType, str]]:
    for hashtag in _extract_hashtags(text):
        yield CandidateType.HASHTAG, hashtag

    for phrase in extract_behavior_phrases(text):
        yield CandidateType.BEHAVIOR, phrase

    for quoted in QUOTED_PHRASE_RE.findall(text or ""):
        candidate_type = _classify_topic_surface(quoted)
        yield candidate_type, quoted

    for raw in PHRASE_RE.findall(text or ""):
        candidate_type = _classify_topic_surface(raw)
        yield candidate_type, raw


def _extract_hashtags(text: str) -> list[str]:
    found: list[str] = []
    for raw in HASHTAG_RE.findall(text or ""):
        normalized = normalize_topic_text(raw)
        surface = normalized if normalized.startswith("#") else f"#{normalized}"
        if should_keep_topic(surface) and surface not in found:
            found.append(surface)
    return found


def _classify_topic_surface(surface: str) -> CandidateType:
    normalized = normalize_topic_text(surface)
    if any(token in normalized for token in STYLE_TOKENS):
        return CandidateType.STYLE
    if any(token in normalized for token in PRODUCT_TOKENS):
        return CandidateType.PRODUCT
    if any(token in normalized for token in BRANDISH_TOKENS):
        return CandidateType.BRAND
    if any(
        token in normalized
        for token in ("界隈", "活", "チャレンジ", "する", "つける", "付ける", "交換", "現象")
    ):
        return CandidateType.BEHAVIOR
    return CandidateType.PHRASE


def _confidence_for(candidate_type: CandidateType) -> ExtractionConfidence:
    if candidate_type == CandidateType.HASHTAG:
        return ExtractionConfidence.HIGH
    if candidate_type in {CandidateType.BEHAVIOR, CandidateType.STYLE, CandidateType.PRODUCT}:
        return ExtractionConfidence.MEDIUM
    return ExtractionConfidence.LOW
