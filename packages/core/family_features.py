"""Aggregation helpers from source-local features to candidate-level families."""

from __future__ import annotations

from collections import defaultdict
from math import log1p
from typing import TypedDict

from packages.core.models import DailySourceFeature, SourceFamily, SourceRole


class FamilyAggregateMetrics(TypedDict):
    family_scores: dict[str, float]
    role_scores: dict[str, float]
    family_counts: dict[str, float]
    discovery_rise: float
    broad_confirmation: float
    editorial_support: float
    commerce_support: float
    cross_family_confirm: float
    lead_lag_bonus: float
    redundancy_penalty: float
    source_families: list[str]
    has_discovery: float
    has_confirmation: float
    music_confirmation: float
    show_confirmation: float


def aggregate_family_metrics(
    features: list[DailySourceFeature],
) -> FamilyAggregateMetrics:
    family_scores: dict[str, float] = defaultdict(float)
    role_scores: dict[str, float] = defaultdict(float)
    family_counts: dict[str, int] = defaultdict(int)

    for feature in features:
        family = feature.family_primary.value
        role = feature.source_role.value
        family_scores[family] += feature.surprise01
        role_scores[role] += feature.surprise01
        family_counts[family] += 1

    discovery_families = {
        feature.family_primary.value
        for feature in features
        if feature.source_role == SourceRole.DISCOVERY
    }
    confirmation_families = {
        feature.family_primary.value
        for feature in features
        if feature.source_role
        in {SourceRole.CONFIRMATION, SourceRole.EDITORIAL, SourceRole.COMMERCE}
    }

    redundancy_penalty = sum(max(0, count - 1) * 0.08 for count in family_counts.values())
    cross_family_confirm = log1p(len(confirmation_families))
    lead_lag_bonus = 0.25 if discovery_families and confirmation_families else 0.0

    return {
        "family_scores": dict(family_scores),
        "role_scores": dict(role_scores),
        "family_counts": {key: float(value) for key, value in family_counts.items()},
        "discovery_rise": sum(
            feature.surprise01
            for feature in features
            if feature.source_role == SourceRole.DISCOVERY
        ),
        "broad_confirmation": sum(
            feature.surprise01
            for feature in features
            if feature.source_role == SourceRole.CONFIRMATION
        ),
        "editorial_support": sum(
            feature.surprise01
            for feature in features
            if feature.source_role == SourceRole.EDITORIAL
        ),
        "commerce_support": sum(
            feature.surprise01 for feature in features if feature.source_role == SourceRole.COMMERCE
        ),
        "cross_family_confirm": cross_family_confirm,
        "lead_lag_bonus": lead_lag_bonus,
        "redundancy_penalty": redundancy_penalty,
        "source_families": sorted(family_scores.keys()),
        "has_discovery": 1.0 if discovery_families else 0.0,
        "has_confirmation": 1.0 if confirmation_families else 0.0,
        "music_confirmation": family_scores.get(SourceFamily.MUSIC_CHART.value, 0.0),
        "show_confirmation": family_scores.get(SourceFamily.SHOW_CHART.value, 0.0),
    }
