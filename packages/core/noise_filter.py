"""Public-ranking noise filters for topic-heavy candidates."""

from __future__ import annotations

import json
from functools import lru_cache
from pathlib import Path
from typing import Any, cast

from packages.core.models import Candidate, CandidateKind, DailySourceFeature
from packages.core.topic_normalize import (
    behavior_objectness,
    contains_finance_print_word,
    contains_generic_event_word,
    contains_live_playbyplay_word,
    normalize_topic_text,
    topic_specificity,
)

CONFIG_PATH = Path(__file__).resolve().parents[2] / "configs" / "domain_filters.yaml"


@lru_cache(maxsize=1)
def _load_filters() -> dict[str, list[str]]:
    return cast(dict[str, list[str]], json.loads(CONFIG_PATH.read_text(encoding="utf-8")))


def source_locality_penalty(feature_list: list[DailySourceFeature]) -> float:
    if not feature_list:
        return 1.0
    family_count = len({feature.family_primary.value for feature in feature_list})
    source_count = len({feature.source_id for feature in feature_list})
    if family_count <= 1 and source_count <= 1:
        return 1.0
    if family_count <= 1:
        return 0.6
    if source_count <= 2:
        return 0.35
    return 0.1


def genericity_penalty(text: str) -> float:
    normalized = normalize_topic_text(text)
    if not normalized:
        return 1.0
    filters = _load_filters()
    lowered = normalized.lower()
    if normalized in filters["generic_phrase_blacklist"]:
        return 1.0
    if contains_generic_event_word(normalized):
        return 0.8
    if any(token.lower() in lowered for token in filters["generic_phrase_blacklist"]):
        return 0.75
    return max(0.0, 1.0 - topic_specificity(normalized))


def live_event_bias(text: str, metadata: dict[str, Any] | None = None) -> float:
    metadata = metadata or {}
    combined = " ".join(
        [
            normalize_topic_text(text),
            normalize_topic_text(str(metadata.get("title", ""))),
            normalize_topic_text(str(metadata.get("surfaceType", ""))),
        ]
    )
    if contains_live_playbyplay_word(combined):
        return 1.0
    if contains_finance_print_word(combined):
        return 0.9
    return 0.0


def compute_public_noise_penalty(
    candidate: Candidate,
    feature_list: list[DailySourceFeature],
) -> tuple[float, float, float]:
    if (candidate.kind or candidate.type.default_kind) != CandidateKind.TOPIC:
        return 0.05, topic_specificity(candidate.display_name), behavior_objectness(
            candidate.display_name
        )

    specificity = topic_specificity(candidate.display_name)
    objectness = behavior_objectness(candidate.display_name)
    locality = source_locality_penalty(feature_list)
    live_bias = max(
        live_event_bias(candidate.display_name, feature.metadata) for feature in feature_list
    ) if feature_list else 0.0
    genericity = genericity_penalty(candidate.display_name)

    penalty = (
        0.34 * locality
        + 0.28 * genericity
        + 0.22 * live_bias
        + 0.12 * (1.0 - specificity)
    )
    if candidate.type.value == "BEHAVIOR":
        penalty += 0.14 * (1.0 - objectness)
    return round(max(0.0, min(1.0, penalty)), 4), specificity, objectness
