"""Scoring logic for the v2 two-axis ranking model."""

from __future__ import annotations

import math
from collections import defaultdict
from collections.abc import Iterable

from packages.core.family_features import FamilyAggregateMetrics, aggregate_family_metrics
from packages.core.models import (
    AlgorithmConfig,
    Candidate,
    CandidateKind,
    CandidateType,
    DailyCandidateFeature,
    DailySourceFeature,
    DomainClass,
    ExtractionConfidence,
    RankingLane,
    SourceFamily,
    SourceRole,
)
from packages.core.scoring import momentum, update_source_state

DOMAIN_FIT_MATRIX: dict[tuple[CandidateType, SourceFamily], float] = {}
for source_family in SourceFamily:
    DOMAIN_FIT_MATRIX[(CandidateType.PERSON, source_family)] = 0.2
    DOMAIN_FIT_MATRIX[(CandidateType.GROUP, source_family)] = 0.2
    DOMAIN_FIT_MATRIX[(CandidateType.MUSIC_ARTIST, source_family)] = 0.2
    DOMAIN_FIT_MATRIX[(CandidateType.MUSIC_TRACK, source_family)] = 0.0
    DOMAIN_FIT_MATRIX[(CandidateType.SHOW, source_family)] = 0.0
    DOMAIN_FIT_MATRIX[(CandidateType.REALITY_SHOW, source_family)] = 0.0
    DOMAIN_FIT_MATRIX[(CandidateType.PHRASE, source_family)] = 0.1
    DOMAIN_FIT_MATRIX[(CandidateType.HASHTAG, source_family)] = 0.1
    DOMAIN_FIT_MATRIX[(CandidateType.BEHAVIOR, source_family)] = 0.1
    DOMAIN_FIT_MATRIX[(CandidateType.STYLE, source_family)] = 0.1
    DOMAIN_FIT_MATRIX[(CandidateType.PRODUCT, source_family)] = 0.1
    DOMAIN_FIT_MATRIX[(CandidateType.BRAND, source_family)] = 0.1
    DOMAIN_FIT_MATRIX[(CandidateType.KEYWORD, source_family)] = 0.1

for candidate_type in (CandidateType.PERSON, CandidateType.GROUP, CandidateType.MUSIC_ARTIST):
    DOMAIN_FIT_MATRIX[(candidate_type, SourceFamily.SEARCH)] = 1.0
    DOMAIN_FIT_MATRIX[(candidate_type, SourceFamily.SOCIAL_DISCOVERY)] = 1.0
    DOMAIN_FIT_MATRIX[(candidate_type, SourceFamily.MUSIC_CHART)] = 1.0
    DOMAIN_FIT_MATRIX[(candidate_type, SourceFamily.SHOW_CHART)] = 0.5
    DOMAIN_FIT_MATRIX[(candidate_type, SourceFamily.EDITORIAL)] = 0.8
    DOMAIN_FIT_MATRIX[(candidate_type, SourceFamily.FASHION_STYLE)] = 0.4
    DOMAIN_FIT_MATRIX[(candidate_type, SourceFamily.COMMERCE)] = 0.2

DOMAIN_FIT_MATRIX[(CandidateType.MUSIC_TRACK, SourceFamily.SEARCH)] = 0.8
DOMAIN_FIT_MATRIX[(CandidateType.MUSIC_TRACK, SourceFamily.SOCIAL_DISCOVERY)] = 0.8
DOMAIN_FIT_MATRIX[(CandidateType.MUSIC_TRACK, SourceFamily.MUSIC_CHART)] = 1.0
DOMAIN_FIT_MATRIX[(CandidateType.MUSIC_TRACK, SourceFamily.EDITORIAL)] = 0.4

for candidate_type in (CandidateType.SHOW, CandidateType.REALITY_SHOW, CandidateType.WORK):
    DOMAIN_FIT_MATRIX[(candidate_type, SourceFamily.SEARCH)] = 1.0
    DOMAIN_FIT_MATRIX[(candidate_type, SourceFamily.SOCIAL_DISCOVERY)] = 1.0
    DOMAIN_FIT_MATRIX[(candidate_type, SourceFamily.SHOW_CHART)] = 1.0
    DOMAIN_FIT_MATRIX[(candidate_type, SourceFamily.EDITORIAL)] = 0.8
    DOMAIN_FIT_MATRIX[(candidate_type, SourceFamily.FASHION_STYLE)] = 0.2

for candidate_type in (
    CandidateType.PHRASE,
    CandidateType.HASHTAG,
    CandidateType.BEHAVIOR,
    CandidateType.KEYWORD,
):
    DOMAIN_FIT_MATRIX[(candidate_type, SourceFamily.SEARCH)] = 1.0
    DOMAIN_FIT_MATRIX[(candidate_type, SourceFamily.SOCIAL_DISCOVERY)] = 1.0
    DOMAIN_FIT_MATRIX[(candidate_type, SourceFamily.FASHION_STYLE)] = 0.8
    DOMAIN_FIT_MATRIX[(candidate_type, SourceFamily.EDITORIAL)] = 0.6
    DOMAIN_FIT_MATRIX[(candidate_type, SourceFamily.COMMERCE)] = 0.6
    DOMAIN_FIT_MATRIX[(candidate_type, SourceFamily.SHOW_CHART)] = 0.3
    DOMAIN_FIT_MATRIX[(candidate_type, SourceFamily.MUSIC_CHART)] = 0.1

for candidate_type in (CandidateType.STYLE, CandidateType.PRODUCT, CandidateType.BRAND):
    DOMAIN_FIT_MATRIX[(candidate_type, SourceFamily.SEARCH)] = 0.8
    DOMAIN_FIT_MATRIX[(candidate_type, SourceFamily.SOCIAL_DISCOVERY)] = 0.8
    DOMAIN_FIT_MATRIX[(candidate_type, SourceFamily.FASHION_STYLE)] = 1.0
    DOMAIN_FIT_MATRIX[(candidate_type, SourceFamily.EDITORIAL)] = 0.7
    DOMAIN_FIT_MATRIX[(candidate_type, SourceFamily.COMMERCE)] = 1.0


def compute_source_feature_score(
    candidate: Candidate,
    source_id: str,
    signal_value: float,
    algo_config: AlgorithmConfig,
    target_date: str,
    family: SourceFamily,
) -> tuple[float, float]:
    params = algo_config.family_params.get(family.value, {})
    local_algo = AlgorithmConfig(
        half_life_days=float(params.get("halfLifeDays", algo_config.half_life_days)),
        beta=algo_config.beta,
        warmup_days=int(params.get("warmupDays", algo_config.warmup_days)),
        min_sig=algo_config.min_sig,
        multi_weight=algo_config.multi_weight,
        momentum_lambda=float(params.get("momentumLambda", algo_config.momentum_lambda)),
        max_x_clip=algo_config.max_x_clip,
        power_weight=algo_config.power_weight,
        ranking_gate_discovery_threshold=algo_config.ranking_gate_discovery_threshold,
        mass_heat_weight=algo_config.mass_heat_weight,
        source_weight_floor=algo_config.source_weight_floor,
        family_params=algo_config.family_params,
    )

    state = candidate.source_state.get(source_id)
    if state is None:
        from packages.core.models import SourceState

        state = SourceState()

    updated_state, anomaly = update_source_state(state, signal_value, local_algo, target_date)
    candidate.source_state[source_id] = updated_state
    sig_history = [anomaly, *updated_state.sig_history[:2]]
    updated_state.sig_history = sig_history[:3]
    local_momentum = momentum(sig_history, local_algo.momentum_lambda)
    surprise01 = normalize_surprise(anomaly)
    return anomaly, surprise01 + min(0.25, local_momentum * 0.08)


def normalize_surprise(anomaly_score: float) -> float:
    if anomaly_score <= 0:
        return 0.0
    return min(1.0, 1.0 / (1.0 + math.exp(-(anomaly_score - 1.0))))


def compute_candidate_feature(
    date: str,
    candidate: Candidate,
    lane: RankingLane,
    domain_class: DomainClass,
    source_features: Iterable[DailySourceFeature],
    algo_config: AlgorithmConfig,
) -> DailyCandidateFeature:
    feature_list = list(source_features)
    aggregate = aggregate_family_metrics(feature_list)

    novelty = _compute_novelty(candidate)
    domain_fit = _compute_domain_fit(candidate.type, feature_list)
    extraction_confidence = _confidence_score(feature_list)
    maturity_penalty = candidate.maturity * 0.6 + (0.25 if not aggregate["has_discovery"] else 0.0)
    coming_score = max(
        0.0,
        float(aggregate["discovery_rise"])
        + float(aggregate["cross_family_confirm"])
        + float(aggregate["lead_lag_bonus"])
        + novelty
        + domain_fit
        + extraction_confidence
        - maturity_penalty
        - float(aggregate["redundancy_penalty"]),
    )

    mass_heat = max(
        0.0,
        float(aggregate["broad_confirmation"])
        + float(aggregate["editorial_support"]) * 0.5
        + float(aggregate["commerce_support"]) * 0.35
        + _sustained_presence(candidate)
        + (0.35 if candidate.maturity > 0.8 else 0.0),
    )

    ranking_gate_passed = passes_ranking_gate(
        candidate,
        feature_list,
        aggregate,
        coming_score,
        novelty,
        algo_config,
    )

    primary_score = coming_score + algo_config.mass_heat_weight * mass_heat
    if not ranking_gate_passed:
        primary_score *= _ungated_primary_multiplier(candidate, aggregate)

    evidence = []
    seen_titles: set[tuple[str, str]] = set()
    for item in feature_list:
        for ev in item.evidence:
            key = (ev.source_id, ev.title)
            if key in seen_titles:
                continue
            seen_titles.add(key)
            evidence.append(ev)

    return DailyCandidateFeature(
        date=date,
        candidate_id=candidate.candidate_id,
        display_name=candidate.display_name,
        candidate_type=candidate.type,
        candidate_kind=candidate.kind or candidate.type.default_kind,
        lane=lane,
        domain_class=domain_class,
        source_families=list(aggregate["source_families"]),
        discovery_rise=float(aggregate["discovery_rise"]),
        cross_family_confirm=float(aggregate["cross_family_confirm"]),
        lead_lag_bonus=float(aggregate["lead_lag_bonus"]),
        novelty=novelty,
        domain_fit=domain_fit,
        extraction_confidence=extraction_confidence,
        maturity_penalty=maturity_penalty,
        redundancy_penalty=float(aggregate["redundancy_penalty"]),
        broad_confirmation=float(aggregate["broad_confirmation"]),
        sustained_presence=_sustained_presence(candidate),
        mainstream_reach=float(aggregate["music_confirmation"])
        + float(aggregate["show_confirmation"]),
        coming_score=coming_score,
        mass_heat=mass_heat,
        primary_score=primary_score,
        ranking_gate_passed=ranking_gate_passed,
        related_entity_ids=list(candidate.related_entity_ids),
        evidence=evidence[:5],
        metadata={
            "familyScores": aggregate["family_scores"],
            "roleScores": aggregate["role_scores"],
        },
    )


def passes_ranking_gate(
    candidate: Candidate,
    feature_list: list[DailySourceFeature],
    aggregate: FamilyAggregateMetrics,
    coming_score: float,
    novelty: float,
    algo_config: AlgorithmConfig,
) -> bool:
    has_discovery = bool(aggregate["has_discovery"])
    support_families = len(aggregate["source_families"])

    candidate_kind = candidate.kind or candidate.type.default_kind

    if candidate_kind == CandidateKind.TOPIC and has_discovery and support_families >= 2:
        return True

    if has_discovery:
        if candidate_kind == CandidateKind.TOPIC and support_families == 1:
            min_threshold = 0.75 if _has_priority_regional_tiktok_signal(feature_list) else 0.9
            return float(aggregate["discovery_rise"]) >= max(
                algo_config.ranking_gate_discovery_threshold,
                min_threshold,
            )
        if coming_score >= algo_config.ranking_gate_discovery_threshold:
            return True

    return candidate.type in {
        CandidateType.MUSIC_ARTIST,
        CandidateType.MUSIC_TRACK,
        CandidateType.SHOW,
        CandidateType.REALITY_SHOW,
        CandidateType.WORK,
    } and (
        novelty >= 0.4
        and len(feature_list) >= 2
        and (
            aggregate["music_confirmation"] > 0.35
            or aggregate["show_confirmation"] > 0.35
        )
    )


def _ungated_primary_multiplier(
    candidate: Candidate,
    aggregate: FamilyAggregateMetrics,
) -> float:
    support_families = len(aggregate["source_families"])
    candidate_kind = candidate.kind or candidate.type.default_kind

    if (
        candidate_kind == CandidateKind.TOPIC
        and bool(aggregate["has_discovery"])
        and support_families == 1
    ):
        return 0.25
    if (
        not bool(aggregate["has_discovery"])
        and support_families == 1
        and (aggregate["music_confirmation"] > 0 or aggregate["show_confirmation"] > 0)
    ):
        return 0.2
    if support_families >= 2:
        return 0.45
    return 0.35


def _has_priority_regional_tiktok_signal(features: list[DailySourceFeature]) -> bool:
    for feature in features:
        if (
            feature.source_id != "TIKTOK_CREATIVE_CENTER"
            or feature.source_role != SourceRole.DISCOVERY
            or feature.extraction_confidence != ExtractionConfidence.HIGH
        ):
            continue

        countries = [
            str(country)
            for country in feature.metadata.get("countries", [])
            if isinstance(country, str) and country
        ]
        country_ranks = feature.metadata.get("countryRanks", {})
        jp_present = "JP" in countries or (
            isinstance(country_ranks, dict) and "JP" in country_ranks
        )
        multi_market_overlap = len(countries) >= 2

        if jp_present or multi_market_overlap:
            return True
    return False


def _compute_novelty(candidate: Candidate) -> float:
    history_len = len([score for score in candidate.trend_history_7d if score > 0])
    base = 1.0 / (1.0 + math.log1p(history_len + candidate.maturity * 5.0))
    return max(0.0, min(1.0, base))


def _compute_domain_fit(candidate_type: CandidateType, features: list[DailySourceFeature]) -> float:
    if not features:
        return 0.0
    values = [
        DOMAIN_FIT_MATRIX.get((candidate_type, feature.family_primary), 0.0) for feature in features
    ]
    return sum(values) / len(values)


def _confidence_score(features: list[DailySourceFeature]) -> float:
    if not features:
        return 0.0
    return sum(feature.extraction_confidence.weight for feature in features) / len(features)


def _sustained_presence(candidate: Candidate) -> float:
    if not candidate.trend_history_7d:
        return 0.0
    positives = [value for value in candidate.trend_history_7d if value > 0]
    if not positives:
        return 0.0
    return min(1.0, sum(positives) / max(1.0, len(positives) * 4.0))


def group_features_by_candidate(
    features: Iterable[DailySourceFeature],
) -> dict[str, list[DailySourceFeature]]:
    grouped: dict[str, list[DailySourceFeature]] = defaultdict(list)
    for feature in features:
        grouped[feature.candidate_id].append(feature)
    return grouped
