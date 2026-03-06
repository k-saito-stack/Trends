"""Offline evaluation helpers for v2 ranking quality."""

from __future__ import annotations

from collections import Counter

from packages.core.models import DailyCandidateFeature, RankingLane


def lead_spread_at_k(features: list[DailyCandidateFeature], k: int = 20) -> float:
    relevant = features[:k]
    if not relevant:
        return 0.0
    hits = sum(1 for feature in relevant if feature.cross_family_confirm > 0)
    return hits / len(relevant)


def cross_family_presence_at_k(features: list[DailyCandidateFeature], k: int = 20) -> float:
    relevant = features[:k]
    if not relevant:
        return 0.0
    return sum(len(feature.source_families) for feature in relevant) / len(relevant)


def novelty_precision(features: list[DailyCandidateFeature], threshold: float = 0.5) -> float:
    if not features:
        return 0.0
    flagged = [feature for feature in features if feature.novelty >= threshold]
    if not flagged:
        return 0.0
    hits = sum(1 for feature in flagged if feature.primary_score >= threshold)
    return hits / len(flagged)


def type_diversity_at_k(features: list[DailyCandidateFeature], k: int = 20) -> float:
    relevant = features[:k]
    if not relevant:
        return 0.0
    counts = Counter(feature.candidate_type.value for feature in relevant)
    return len(counts) / len(relevant)


def lane_mix_at_k(features: list[DailyCandidateFeature], k: int = 20) -> dict[str, int]:
    relevant = features[:k]
    counts = Counter(feature.lane.value for feature in relevant)
    return {
        lane.value: counts.get(lane.value, 0) for lane in RankingLane if lane != RankingLane.SHADOW
    }
