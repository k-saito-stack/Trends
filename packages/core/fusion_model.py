"""Candidate-level fusion/calibration helpers.

This module keeps the candidate-level scoring logic separate from the
source-local anomaly stage in ``scoring_v2.py``. The current
implementation uses deterministic logistic-style feature fusion with a
monotonic calibration layer so the model remains inspectable and easy to
replace with learned weights later.
"""

from __future__ import annotations

import math
from typing import Any

from packages.core.family_features import FamilyAggregateMetrics
from packages.core.models import (
    Candidate,
    CandidateKind,
    CandidateType,
    DailySourceFeature,
    DomainClass,
)


def build_candidate_feature_vector(
    candidate: Candidate,
    aggregate: FamilyAggregateMetrics,
    source_features: list[DailySourceFeature],
    *,
    novelty: float,
    domain_fit: float,
    extraction_confidence: float,
    maturity_penalty: float,
    sustained_presence: float,
) -> dict[str, float]:
    """Build a stable candidate-level feature vector for fusion.

    The vector is intentionally simple and derived from the already
    persisted v2 features so it can be logged, tested, and later used by a
    learned calibrator without changing upstream extraction code.
    """

    discovery_sources = [
        feature for feature in source_features if feature.source_role.value == "DISCOVERY"
    ]
    confirmation_sources = [
        feature for feature in source_features if feature.source_role.value == "CONFIRMATION"
    ]
    editorial_sources = [
        feature for feature in source_features if feature.source_role.value == "EDITORIAL"
    ]
    commerce_sources = [
        feature for feature in source_features if feature.source_role.value == "COMMERCE"
    ]

    posterior_reliability = _mean(
        feature.posterior_reliability
        for feature in source_features
        if feature.posterior_reliability
    )
    posterior_lead = _mean(feature.posterior_lead for feature in source_features)
    posterior_persistence = _mean(feature.posterior_persistence for feature in source_features)
    regional_overlap = _regional_overlap(source_features)
    regional_priority = 1.0 if _has_priority_asia_signal(source_features) else 0.0
    tiktok_priority = 1.0 if _has_priority_tiktok_signal(source_features) else 0.0

    candidate_kind = candidate.kind or candidate.type.default_kind
    family_counts = aggregate.get("family_counts", {})
    role_scores = aggregate.get("role_scores", {})
    family_scores = aggregate.get("family_scores", {})

    return {
        "candidate_kind_topic": 1.0 if candidate_kind == CandidateKind.TOPIC else 0.0,
        "candidate_kind_entity": 1.0 if candidate_kind == CandidateKind.ENTITY else 0.0,
        "discovery_rise": float(aggregate["discovery_rise"]),
        "broad_confirmation": float(aggregate["broad_confirmation"]),
        "editorial_support": float(aggregate["editorial_support"]),
        "commerce_support": float(aggregate["commerce_support"]),
        "cross_family_confirm": float(aggregate["cross_family_confirm"]),
        "lead_lag_bonus": float(aggregate["lead_lag_bonus"]),
        "redundancy_penalty": float(aggregate["redundancy_penalty"]),
        "has_discovery": float(aggregate["has_discovery"]),
        "has_confirmation": float(aggregate["has_confirmation"]),
        "music_confirmation": float(aggregate["music_confirmation"]),
        "show_confirmation": float(aggregate["show_confirmation"]),
        "novelty": novelty,
        "domain_fit": domain_fit,
        "extraction_confidence": extraction_confidence,
        "maturity_penalty": maturity_penalty,
        "sustained_presence": sustained_presence,
        "source_count": float(len(source_features)),
        "family_count": float(len(aggregate["source_families"])),
        "discovery_source_count": float(len(discovery_sources)),
        "confirmation_source_count": float(len(confirmation_sources)),
        "editorial_source_count": float(len(editorial_sources)),
        "commerce_source_count": float(len(commerce_sources)),
        "posterior_reliability": posterior_reliability,
        "posterior_lead": posterior_lead,
        "posterior_persistence": posterior_persistence,
        "regional_overlap": regional_overlap,
        "regional_priority": regional_priority,
        "tiktok_priority": tiktok_priority,
        "search_family_score": float(family_scores.get("SEARCH", 0.0)),
        "social_family_score": float(family_scores.get("SOCIAL_DISCOVERY", 0.0)),
        "music_family_score": float(family_scores.get("MUSIC_CHART", 0.0)),
        "show_family_score": float(family_scores.get("SHOW_CHART", 0.0)),
        "editorial_family_score": float(family_scores.get("EDITORIAL", 0.0)),
        "commerce_family_score": float(family_scores.get("COMMERCE", 0.0)),
        "discovery_role_score": float(role_scores.get("DISCOVERY", 0.0)),
        "confirmation_role_score": float(role_scores.get("CONFIRMATION", 0.0)),
        "editorial_role_score": float(role_scores.get("EDITORIAL", 0.0)),
        "commerce_role_score": float(role_scores.get("COMMERCE", 0.0)),
        "search_family_count": float(family_counts.get("SEARCH", 0.0)),
        "social_family_count": float(family_counts.get("SOCIAL_DISCOVERY", 0.0)),
        "music_family_count": float(family_counts.get("MUSIC_CHART", 0.0)),
        "show_family_count": float(family_counts.get("SHOW_CHART", 0.0)),
        "editorial_family_count": float(family_counts.get("EDITORIAL", 0.0)),
        "commerce_family_count": float(family_counts.get("COMMERCE", 0.0)),
    }


def predict_breakout_prob(feature_vector: dict[str, float], horizon_days: int = 7) -> float:
    """Predict calibrated breakout probability for a future horizon."""

    horizon_bias = {
        1: -0.42,
        3: -0.18,
        7: 0.0,
        14: 0.14,
    }.get(horizon_days, 0.0)

    logit = (
        1.9 * feature_vector["discovery_rise"]
        + 0.42 * feature_vector["broad_confirmation"]
        + 0.38 * feature_vector["cross_family_confirm"]
        + 0.34 * feature_vector["lead_lag_bonus"]
        + 0.52 * feature_vector["novelty"]
        + 0.28 * feature_vector["domain_fit"]
        + 0.22 * feature_vector["extraction_confidence"]
        + 0.44 * feature_vector["posterior_reliability"]
        + 0.18 * feature_vector["posterior_lead"]
        + 0.12 * feature_vector["posterior_persistence"]
        + 0.22 * feature_vector["regional_overlap"]
        + 0.18 * feature_vector["regional_priority"]
        + 0.25 * feature_vector["tiktok_priority"]
        + 0.12 * feature_vector["family_count"]
        - 0.84 * feature_vector["maturity_penalty"]
        - 0.72 * feature_vector["redundancy_penalty"]
        - 1.34
        + horizon_bias
    )
    probability = _logistic(logit)
    return _apply_monotonic_calibration(probability, BREAKOUT_CALIBRATION_POINTS)


def predict_mass_prob(feature_vector: dict[str, float]) -> float:
    """Predict calibrated mass-adoption probability."""

    logit = (
        0.42 * feature_vector["discovery_rise"]
        + 1.35 * feature_vector["broad_confirmation"]
        + 0.62 * feature_vector["editorial_support"]
        + 0.54 * feature_vector["commerce_support"]
        + 0.58 * feature_vector["sustained_presence"]
        + 0.34 * feature_vector["posterior_reliability"]
        + 0.26 * feature_vector["posterior_persistence"]
        + 0.18 * feature_vector["regional_overlap"]
        + 0.24 * feature_vector["has_confirmation"]
        + 0.12 * feature_vector["music_confirmation"]
        + 0.12 * feature_vector["show_confirmation"]
        - 0.38 * feature_vector["novelty"]
        - 0.28 * feature_vector["redundancy_penalty"]
        - 0.92
    )
    probability = _logistic(logit)
    return _apply_monotonic_calibration(probability, MASS_CALIBRATION_POINTS)


def compute_primary_score(
    candidate_type: CandidateType,
    domain_class: DomainClass,
    breakout_prob: float,
    mass_prob: float,
) -> float:
    """Map breakout/mass probabilities onto the published score scale."""

    gamma = _mass_gamma(candidate_type, domain_class)
    return round(4.0 * breakout_prob + 4.0 * gamma * mass_prob, 4)


def _mass_gamma(candidate_type: CandidateType, domain_class: DomainClass) -> float:
    gamma = 0.24
    if candidate_type in {
        CandidateType.PHRASE,
        CandidateType.HASHTAG,
        CandidateType.BEHAVIOR,
        CandidateType.STYLE,
    }:
        gamma = 0.18
    elif candidate_type in {
        CandidateType.SHOW,
        CandidateType.REALITY_SHOW,
        CandidateType.WORK,
        CandidateType.MUSIC_TRACK,
    }:
        gamma = 0.31
    elif candidate_type in {
        CandidateType.PERSON,
        CandidateType.GROUP,
        CandidateType.MUSIC_ARTIST,
    }:
        gamma = 0.22

    if domain_class == DomainClass.ENTERTAINMENT:
        gamma += 0.03
    elif domain_class == DomainClass.BUSINESS_PROFESSIONAL:
        gamma -= 0.02
    return max(0.12, min(0.38, gamma))


def _regional_overlap(features: list[DailySourceFeature]) -> float:
    countries: set[str] = set()
    region_groups: set[str] = set()
    for feature in features:
        metadata = feature.metadata or {}
        for country in metadata.get("countries", []):
            if isinstance(country, str) and country:
                countries.add(country)
        for region in metadata.get("regions", []):
            if isinstance(region, str) and region:
                region_groups.add(region)
    overlap_count = len(countries) + len(region_groups)
    return min(1.0, overlap_count / 4.0)


def _has_priority_asia_signal(features: list[DailySourceFeature]) -> bool:
    asia_markets = {"JP", "KR", "TW", "HK", "TH", "VN", "ID", "PH", "MY", "SG"}
    for feature in features:
        countries = {
            str(country)
            for country in feature.metadata.get("countries", [])
            if isinstance(country, str) and country
        }
        if "JP" in countries:
            return True
        if len(countries & asia_markets) >= 2:
            return True
    return False


def _has_priority_tiktok_signal(features: list[DailySourceFeature]) -> bool:
    for feature in features:
        if feature.source_role.value != "DISCOVERY":
            continue
        if not feature.source_id.startswith("TIKTOK_CREATIVE_CENTER"):
            continue
        if _has_priority_asia_signal([feature]) and feature.extraction_confidence.value == "HIGH":
            return True
    return False


def _mean(values: Any) -> float:
    numbers = [float(value) for value in values]
    if not numbers:
        return 0.0
    return sum(numbers) / len(numbers)


def _logistic(value: float) -> float:
    return 1.0 / (1.0 + math.exp(-value))


def _apply_monotonic_calibration(
    probability: float,
    knots: tuple[tuple[float, float], ...],
) -> float:
    if probability <= knots[0][0]:
        return knots[0][1]
    if probability >= knots[-1][0]:
        return knots[-1][1]

    for (x0, y0), (x1, y1) in zip(knots, knots[1:], strict=False):
        if x0 <= probability <= x1:
            if x1 == x0:
                return y1
            ratio = (probability - x0) / (x1 - x0)
            return y0 + ratio * (y1 - y0)
    return probability


BREAKOUT_CALIBRATION_POINTS: tuple[tuple[float, float], ...] = (
    (0.0, 0.0),
    (0.15, 0.08),
    (0.35, 0.27),
    (0.55, 0.56),
    (0.75, 0.82),
    (1.0, 1.0),
)

MASS_CALIBRATION_POINTS: tuple[tuple[float, float], ...] = (
    (0.0, 0.0),
    (0.2, 0.12),
    (0.4, 0.31),
    (0.6, 0.57),
    (0.8, 0.84),
    (1.0, 1.0),
)
