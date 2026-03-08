"""Scoring logic for the v2 two-axis ranking model."""

from __future__ import annotations

import math
from collections import defaultdict
from collections.abc import Iterable

from packages.core.evidence import dedupe_evidence
from packages.core.family_features import FamilyAggregateMetrics, aggregate_family_metrics
from packages.core.fusion_model import (
    build_candidate_feature_vector,
    build_public_feature_vector,
    compute_primary_score,
    compute_public_score,
    predict_breakout_prob,
    predict_mass_prob,
    predict_public_rankability_prob,
)
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
from packages.core.noise_filter import compute_public_noise_penalty
from packages.core.public_rank_rules import load_public_rank_rules
from packages.core.realism_features import (
    compute_constrained_trends_support,
    compute_jp_relevance,
    compute_mature_mass_only_penalty,
    compute_yahoo_realtime_support,
)
from packages.core.scoring import momentum, update_source_state
from packages.core.source_availability import adjust_threshold_for_availability

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
    posterior_multiplier: float = 1.0,
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
    composite = surprise01 + min(0.25, local_momentum * 0.08)
    return anomaly, min(1.0, composite * posterior_multiplier)


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
    relation_support: dict[str, float] | None = None,
    source_availability_snapshot: dict[str, float] | None = None,
) -> DailyCandidateFeature:
    feature_list = list(source_features)
    aggregate = aggregate_family_metrics(feature_list)
    rules = load_public_rank_rules()
    source_availability_snapshot = source_availability_snapshot or {}

    novelty = _compute_novelty(candidate)
    domain_fit = _compute_domain_fit(candidate.type, feature_list)
    extraction_confidence = _confidence_score(feature_list)
    maturity_penalty = candidate.maturity * 0.6 + (0.25 if not aggregate["has_discovery"] else 0.0)
    relation_support = relation_support or {}
    support_metrics = _compute_support_metrics(candidate, feature_list, relation_support)
    heuristic_coming_score = max(
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

    heuristic_mass_heat = max(
        0.0,
        float(aggregate["broad_confirmation"])
        + float(aggregate["editorial_support"]) * 0.5
        + float(aggregate["commerce_support"]) * 0.35
        + _sustained_presence(candidate)
        + (0.35 if candidate.maturity > 0.8 else 0.0),
    )

    sustained_presence = _sustained_presence(candidate)
    jp_relevance = compute_jp_relevance(feature_list, candidate)
    constrained_trends_ent_support, constrained_trends_beauty_support = (
        compute_constrained_trends_support(feature_list, domain_class)
    )
    yahoo_realtime_support = compute_yahoo_realtime_support(feature_list)
    public_noise_penalty, topic_specificity, behavior_objectness = (
        compute_public_noise_penalty(candidate, feature_list)
    )
    mature_mass_only_penalty = compute_mature_mass_only_penalty(candidate, aggregate)
    relation_support_total = float(support_metrics["relation_support_total"])
    relation_confirmed_support = float(support_metrics["relation_confirmed_support"])
    tiktok_metrics = _compute_tiktok_priority_metrics(feature_list)
    jp_credibility = max(
        constrained_trends_ent_support,
        constrained_trends_beauty_support,
        yahoo_realtime_support,
    )
    availability_adjusted_jp_credibility = _compute_availability_adjusted_jp_credibility(
        jp_credibility,
        source_availability_snapshot,
    )
    relation_only_penalty = 1.0 if bool(support_metrics["relation_only_flag"]) else 0.0
    same_work_cluster_penalty = min(
        1.0,
        max(0, int(support_metrics["same_work_relation_count"]) - 1) * 0.5
        + float(support_metrics["dominant_work_ratio"]) * 0.35,
    )
    fusion_vector = build_candidate_feature_vector(
        candidate,
        aggregate,
        feature_list,
        novelty=novelty,
        domain_fit=domain_fit,
        extraction_confidence=extraction_confidence,
        maturity_penalty=maturity_penalty,
        sustained_presence=sustained_presence,
        direct_support_total=float(support_metrics["direct_support_total"]),
        direct_confirmation_support=float(support_metrics["direct_confirmation_support"]),
        relation_support_total=relation_support_total,
        relation_confirmed_support=relation_confirmed_support,
        tver_relation_support=float(support_metrics["tver_relation_support"]),
        relation_only_penalty=relation_only_penalty,
        same_work_cluster_penalty=same_work_cluster_penalty,
        omnipresent_talent_penalty=float(support_metrics["omnipresent_talent_penalty"]),
        tiktok_priority_score=float(tiktok_metrics["tiktok_priority_score"]),
        availability_adjusted_jp_credibility=availability_adjusted_jp_credibility,
    )
    fusion_vector.update(
        {
            "jp_relevance": jp_relevance,
            "constrained_trends_support": max(
                constrained_trends_ent_support,
                constrained_trends_beauty_support,
            ),
            "yahoo_realtime_support": yahoo_realtime_support,
            "public_noise_penalty": public_noise_penalty,
            "relation_confirmed_support": relation_confirmed_support,
        }
    )
    breakout_prob_1d = predict_breakout_prob(fusion_vector, horizon_days=1)
    breakout_prob_3d = predict_breakout_prob(fusion_vector, horizon_days=3)
    breakout_prob_7d = predict_breakout_prob(fusion_vector, horizon_days=7)
    mass_prob = predict_mass_prob(fusion_vector)
    coming_score = breakout_prob_7d * 4.0
    mass_heat = mass_prob * 4.0

    public_vector = build_public_feature_vector(
        breakout_prob=breakout_prob_7d,
        mass_prob=mass_prob,
        jp_relevance=jp_relevance,
        constrained_trends_ent_support=constrained_trends_ent_support,
        constrained_trends_beauty_support=constrained_trends_beauty_support,
        yahoo_realtime_support=yahoo_realtime_support,
        posterior_reliability=fusion_vector["posterior_reliability"],
        extraction_confidence=extraction_confidence,
        relation_confirmed_support=relation_confirmed_support,
        direct_confirmation_support=float(support_metrics["direct_confirmation_support"]),
        tver_relation_support=float(support_metrics["tver_relation_support"]),
        tiktok_priority_score=float(tiktok_metrics["tiktok_priority_score"]),
        availability_adjusted_jp_credibility=availability_adjusted_jp_credibility,
        public_noise_penalty=public_noise_penalty,
        mature_mass_only_penalty=mature_mass_only_penalty,
        relation_only_penalty=relation_only_penalty,
        same_work_cluster_penalty=same_work_cluster_penalty,
        omnipresent_talent_penalty=float(support_metrics["omnipresent_talent_penalty"]),
        family_count=fusion_vector["family_count"],
        source_count=fusion_vector["source_count"],
    )
    public_rankability_prob = predict_public_rankability_prob(public_vector)
    public_score = compute_public_score(
        coming_score=coming_score,
        mass_heat=mass_heat,
        breakout_prob=breakout_prob_7d,
        mass_prob=mass_prob,
        public_rankability_prob=public_rankability_prob,
        same_work_cluster_penalty=same_work_cluster_penalty,
        omnipresent_talent_penalty=float(support_metrics["omnipresent_talent_penalty"]),
    )

    ranking_gate_passed = passes_ranking_gate(
        candidate,
        feature_list,
        aggregate,
        breakout_prob_7d,
        novelty,
        algo_config,
        public_rankability_prob=public_rankability_prob,
        public_noise_penalty=public_noise_penalty,
        mature_mass_only_penalty=mature_mass_only_penalty,
        constrained_trends_ent_support=constrained_trends_ent_support,
        constrained_trends_beauty_support=constrained_trends_beauty_support,
        yahoo_realtime_support=yahoo_realtime_support,
        relation_support_total=relation_support_total,
        direct_support_total=float(support_metrics["direct_support_total"]),
        direct_confirmation_support=float(support_metrics["direct_confirmation_support"]),
        relation_only_flag=bool(support_metrics["relation_only_flag"]),
        tver_relation_support=float(support_metrics["tver_relation_support"]),
        topic_specificity=topic_specificity,
        behavior_objectness=behavior_objectness,
        tiktok_metrics=tiktok_metrics,
        availability_adjusted_jp_credibility=availability_adjusted_jp_credibility,
        source_availability_snapshot=source_availability_snapshot,
        rules=rules,
    )
    public_gate_passed = ranking_gate_passed and public_rankability_prob >= float(
        rules["public_rankability_min"]
    )

    model_primary_score = compute_primary_score(
        candidate.type,
        domain_class,
        breakout_prob_7d,
        mass_prob,
    )
    primary_score = coming_score + algo_config.mass_heat_weight * mass_heat
    if not ranking_gate_passed:
        primary_score *= _ungated_primary_multiplier(candidate, aggregate)

    evidence = []
    for item in feature_list:
        for ev in item.evidence:
            evidence.append(ev)
    evidence = dedupe_evidence(evidence)

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
        sustained_presence=sustained_presence,
        mainstream_reach=float(aggregate["music_confirmation"])
        + float(aggregate["show_confirmation"]),
        jp_relevance=jp_relevance,
        constrained_trends_ent_support=constrained_trends_ent_support,
        constrained_trends_beauty_support=constrained_trends_beauty_support,
        yahoo_realtime_support=yahoo_realtime_support,
        topic_specificity=topic_specificity,
        behavior_objectness=behavior_objectness,
        public_noise_penalty=public_noise_penalty,
        mature_mass_only_penalty=mature_mass_only_penalty,
        direct_support_total=float(support_metrics["direct_support_total"]),
        direct_confirmation_support=float(support_metrics["direct_confirmation_support"]),
        relation_support_total=relation_support_total,
        relation_confirmed_support=relation_confirmed_support,
        tver_relation_support=float(support_metrics["tver_relation_support"]),
        relation_only_flag=bool(support_metrics["relation_only_flag"]),
        work_cluster_id=str(support_metrics["work_cluster_id"]),
        relation_cluster_id=str(support_metrics["relation_cluster_id"]),
        same_work_relation_count=int(support_metrics["same_work_relation_count"]),
        dominant_work_ratio=float(support_metrics["dominant_work_ratio"]),
        omnipresent_talent_penalty=float(support_metrics["omnipresent_talent_penalty"]),
        tiktok_primary_jp=bool(tiktok_metrics["tiktok_primary_jp"]),
        tiktok_country_count=int(tiktok_metrics["tiktok_country_count"]),
        tiktok_multi_asia_count=int(tiktok_metrics["tiktok_multi_asia_count"]),
        tiktok_weighted_region_score=float(tiktok_metrics["tiktok_weighted_region_score"]),
        tiktok_cross_surface_count=int(tiktok_metrics["tiktok_cross_surface_count"]),
        tiktok_priority_score=float(tiktok_metrics["tiktok_priority_score"]),
        availability_adjusted_jp_credibility=availability_adjusted_jp_credibility,
        public_rankability_prob=public_rankability_prob,
        public_score=public_score,
        breakout_prob_1d=breakout_prob_1d,
        breakout_prob_3d=breakout_prob_3d,
        breakout_prob_7d=breakout_prob_7d,
        mass_prob=mass_prob,
        coming_score=coming_score,
        mass_heat=mass_heat,
        primary_score=primary_score,
        ranking_gate_passed=ranking_gate_passed,
        public_gate_passed=public_gate_passed,
        related_entity_ids=list(candidate.related_entity_ids),
        related_candidate_ids=list(candidate.related_candidate_ids),
        source_contrib={
            feature.source_id: round(feature.surprise01, 4) for feature in feature_list
        },
        evidence=evidence[:5],
        metadata={
            "familyScores": aggregate["family_scores"],
            "roleScores": aggregate["role_scores"],
            "fusionModel": {
                "vector": {key: round(value, 4) for key, value in fusion_vector.items()},
                "heuristicComing": round(heuristic_coming_score, 4),
                "heuristicMass": round(heuristic_mass_heat, 4),
                "breakoutProb1d": round(breakout_prob_1d, 4),
                "breakoutProb3d": round(breakout_prob_3d, 4),
                "breakoutProb7d": round(breakout_prob_7d, 4),
                "massProb": round(mass_prob, 4),
                "publicRankabilityProb": round(public_rankability_prob, 4),
                "publicScore": round(public_score, 4),
                "modelPrimaryScore": round(model_primary_score, 4),
                "publicVector": {key: round(value, 4) for key, value in public_vector.items()},
                "relationSupport": {
                    key: round(float(value), 4) for key, value in relation_support.items()
                },
                "sourceAvailabilitySnapshot": {
                    key: round(value, 4) if isinstance(value, float) else value
                    for key, value in source_availability_snapshot.items()
                },
            },
        },
    )


def passes_ranking_gate(
    candidate: Candidate,
    feature_list: list[DailySourceFeature],
    aggregate: FamilyAggregateMetrics,
    breakout_prob_7d: float,
    novelty: float,
    algo_config: AlgorithmConfig,
    *,
    public_rankability_prob: float,
    public_noise_penalty: float,
    mature_mass_only_penalty: float,
    constrained_trends_ent_support: float,
    constrained_trends_beauty_support: float,
    yahoo_realtime_support: float,
    relation_support_total: float,
    direct_support_total: float,
    direct_confirmation_support: float,
    relation_only_flag: bool,
    tver_relation_support: float,
    topic_specificity: float,
    behavior_objectness: float,
    tiktok_metrics: dict[str, float | int | bool | str],
    availability_adjusted_jp_credibility: float,
    source_availability_snapshot: dict[str, float],
    rules: dict[str, float],
) -> bool:
    has_discovery = bool(aggregate["has_discovery"])
    support_families = len(aggregate["source_families"])
    constrained_support = max(
        constrained_trends_ent_support,
        constrained_trends_beauty_support,
    )
    jp_credibility = max(constrained_support, yahoo_realtime_support)
    search_family_availability_ratio = float(
        source_availability_snapshot.get("searchFamilyAvailabilityRatio", 1.0)
    )

    candidate_kind = candidate.kind or candidate.type.default_kind
    if public_noise_penalty >= float(rules["low_value_penalty_block_threshold"]):
        return False
    if public_rankability_prob < float(rules["public_rankability_min"]):
        return False
    if mature_mass_only_penalty > 0.85:
        return False
    if relation_only_flag and candidate.type in {
        CandidateType.PERSON,
        CandidateType.GROUP,
        CandidateType.MUSIC_ARTIST,
    }:
        return False
    if (
        tver_relation_support > float(rules.get("tver_relation_support_cap", 0.18))
        and direct_support_total < 0.08
    ):
        return False

    if candidate_kind == CandidateKind.TOPIC:
        if has_discovery and support_families >= 2:
            return (
                breakout_prob_7d >= float(rules["topic_multi_family_public_threshold"])
                and (availability_adjusted_jp_credibility > 0 or relation_support_total > 0.18)
            )
        if has_discovery and support_families == 1:
            if not _has_priority_regional_tiktok_signal(feature_list):
                return False
            surface = str(tiktok_metrics.get("best_surface", ""))
            if surface == "creator":
                return False
            adjusted_topic_threshold = adjust_threshold_for_availability(
                _single_family_topic_threshold(surface, rules),
                search_family_availability_ratio,
            )
            if breakout_prob_7d < adjusted_topic_threshold:
                return False
            if availability_adjusted_jp_credibility <= _minimum_adjusted_jp_credibility(
                surface, rules
            ):
                return False
            if surface == "song":
                return (
                    bool(tiktok_metrics.get("tiktok_primary_jp", False))
                    and float(tiktok_metrics.get("tiktok_weighted_region_score", 0.0)) >= 2.2
                    and public_noise_penalty <= 0.25
                    and (
                        _has_direct_source_support(
                            feature_list,
                            {"APPLE_MUSIC_JP", "YOUTUBE_TREND_JP"},
                        )
                        or constrained_trends_ent_support > 0
                    )
                )
            if surface in {"hashtag", "video"}:
                min_region_score = float(
                    rules.get(
                        "tiktok_video_region_score_min"
                        if surface == "video"
                        else "tiktok_hashtag_region_score_min",
                        2.6 if surface == "video" else 1.6,
                    )
                )
                if (
                    float(tiktok_metrics.get("tiktok_weighted_region_score", 0.0))
                    < min_region_score
                ):
                    return False
                if topic_specificity < 0.55:
                    return False
                if candidate.type == CandidateType.BEHAVIOR and behavior_objectness < 0.45:
                    return False
                noise_threshold = float(
                    rules.get(
                        "tiktok_video_noise_penalty_max"
                        if surface == "video"
                        else "tiktok_topic_noise_penalty_max",
                        0.34 if surface == "video" else 0.40,
                    )
                )
                return public_noise_penalty <= noise_threshold and (
                    constrained_support > 0 or yahoo_realtime_support > 0
                )
            return False
        return False

    if relation_support_total > 0 and not has_discovery and jp_credibility <= 0:
        return public_rankability_prob >= float(rules["relation_only_public_threshold"]) and (
            relation_support_total >= 0.25 and direct_support_total >= 0.08
        )

    if has_discovery and breakout_prob_7d >= algo_config.ranking_gate_discovery_threshold:
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
        and public_noise_penalty <= 0.5
        and (
            aggregate["music_confirmation"] > 0.35
            or aggregate["show_confirmation"] > 0.35
            or direct_confirmation_support > 0.2
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
            not _is_tiktok_discovery_source(feature.source_id)
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


def _is_tiktok_discovery_source(source_id: str) -> bool:
    return source_id == "TIKTOK_CREATIVE_CENTER" or source_id.startswith(
        "TIKTOK_CREATIVE_CENTER_"
    )


def _compute_support_metrics(
    candidate: Candidate,
    features: list[DailySourceFeature],
    relation_support: dict[str, float],
) -> dict[str, float | bool | str]:
    direct_support_total = 0.0
    direct_confirmation_support = 0.0
    direct_non_relation_families: set[str] = set()
    work_cluster_counts: dict[str, int] = defaultdict(int)
    derived_feature_count = 0
    primary_work_cluster_id = ""

    for feature in features:
        derived_from_work = bool(feature.metadata.get("derivedFromWork", False))
        if derived_from_work:
            derived_feature_count += 1
            work_cluster_id = str(feature.metadata.get("workClusterId", "")).strip()
            if work_cluster_id:
                work_cluster_counts[work_cluster_id] += 1
                if not primary_work_cluster_id:
                    primary_work_cluster_id = work_cluster_id
            continue

        direct_support_total += feature.surprise01
        direct_non_relation_families.add(feature.family_primary.value)
        if feature.source_role == SourceRole.CONFIRMATION:
            direct_confirmation_support += feature.surprise01

    same_work_relation_count = max(work_cluster_counts.values(), default=0)
    distinct_work_clusters = len(work_cluster_counts)
    dominant_work_ratio = (
        same_work_relation_count / derived_feature_count if derived_feature_count else 0.0
    )
    relation_support_total = float(relation_support.get("relation_support_total", 0.0))
    relation_confirmed_support = float(relation_support.get("relation_confirmed_support", 0.0))
    tver_relation_support = float(relation_support.get("tver_relation_support", 0.0))

    person_like = candidate.type in {
        CandidateType.PERSON,
        CandidateType.GROUP,
        CandidateType.MUSIC_ARTIST,
    }
    relation_only_flag = bool(
        person_like
        and direct_support_total < 0.08
        and not direct_non_relation_families
        and (relation_support_total > 0 or derived_feature_count > 0)
    )
    omnipresent_talent_penalty = 0.0
    if person_like:
        omnipresent_talent_penalty = min(
            0.35,
            max(0, distinct_work_clusters - 1) * 0.12
            + max(0.0, candidate.maturity - 0.70) * 0.25,
        )

    return {
        "direct_support_total": round(direct_support_total, 4),
        "direct_confirmation_support": round(direct_confirmation_support, 4),
        "relation_support_total": round(relation_support_total, 4),
        "relation_confirmed_support": round(relation_confirmed_support, 4),
        "tver_relation_support": round(tver_relation_support, 4),
        "relation_only_flag": relation_only_flag,
        "work_cluster_id": primary_work_cluster_id,
        "relation_cluster_id": primary_work_cluster_id,
        "same_work_relation_count": same_work_relation_count,
        "dominant_work_ratio": round(dominant_work_ratio, 4),
        "omnipresent_talent_penalty": round(omnipresent_talent_penalty, 4),
    }


def _compute_tiktok_priority_metrics(
    features: list[DailySourceFeature],
) -> dict[str, float | int | bool | str]:
    countries: set[str] = set()
    surfaces: set[str] = set()
    weighted_region_score = 0.0
    best_priority_score = 0.0
    best_surface = ""
    primary_jp = False

    for feature in features:
        if not _is_tiktok_discovery_source(feature.source_id):
            continue
        source_countries = {
            str(country)
            for country in feature.metadata.get("countries", [])
            if isinstance(country, str) and country
        }
        country_ranks = feature.metadata.get("countryRanks", {})
        if isinstance(country_ranks, dict):
            source_countries.update(str(country) for country in country_ranks if country)
        countries.update(source_countries)
        has_jp = "JP" in source_countries
        primary_jp = primary_jp or has_jp
        secondary_country_count = len([country for country in source_countries if country != "JP"])
        feature_region_score = (1.0 if has_jp else 0.0) + 0.6 * secondary_country_count
        weighted_region_score = max(weighted_region_score, feature_region_score)
        surface = _tiktok_surface(feature)
        if surface:
            surfaces.add(surface)
        best_rank = 50
        if isinstance(country_ranks, dict):
            numeric_ranks = []
            for raw_rank in country_ranks.values():
                try:
                    numeric_ranks.append(int(raw_rank))
                except (TypeError, ValueError):
                    continue
            if numeric_ranks:
                best_rank = min(numeric_ranks)
        priority_score = (
            _tiktok_surface_weight(surface)
            * feature_region_score
            * _rank_norm(best_rank)
            * feature.extraction_confidence.weight
        )
        if priority_score > best_priority_score:
            best_priority_score = priority_score
            best_surface = surface

    return {
        "tiktok_primary_jp": primary_jp,
        "tiktok_country_count": len(countries),
        "tiktok_multi_asia_count": len([country for country in countries if country != "JP"]),
        "tiktok_weighted_region_score": round(weighted_region_score, 4),
        "tiktok_cross_surface_count": len(surfaces),
        "tiktok_priority_score": round(best_priority_score, 4),
        "best_surface": best_surface,
    }


def _compute_availability_adjusted_jp_credibility(
    jp_credibility: float,
    source_availability_snapshot: dict[str, float],
) -> float:
    search_family_availability_ratio = float(
        source_availability_snapshot.get("searchFamilyAvailabilityRatio", 1.0)
    )
    return round(jp_credibility / max(0.5, search_family_availability_ratio), 4)


def _has_direct_source_support(
    feature_list: list[DailySourceFeature],
    source_ids: set[str],
) -> bool:
    for feature in feature_list:
        if feature.source_id not in source_ids:
            continue
        if bool(feature.metadata.get("derivedFromWork", False)):
            continue
        return True
    return False


def _minimum_adjusted_jp_credibility(surface: str, rules: dict[str, float]) -> float:
    return {
        "song": float(rules.get("tiktok_song_adjusted_jp_credibility_min", 0.08)),
        "hashtag": float(rules.get("tiktok_topic_adjusted_jp_credibility_min", 0.08)),
        "video": float(rules.get("tiktok_video_adjusted_jp_credibility_min", 0.1)),
        "creator": float(rules.get("tiktok_creator_adjusted_jp_credibility_min", 0.12)),
    }.get(surface, 0.08)


def _single_family_topic_threshold(surface: str, rules: dict[str, float]) -> float:
    base_threshold = float(rules["topic_single_family_public_threshold"])
    return {
        "song": float(rules.get("tiktok_single_source_song_threshold", base_threshold)),
        "hashtag": float(rules.get("tiktok_single_source_topic_threshold", base_threshold)),
        "video": float(rules.get("tiktok_single_source_video_threshold", base_threshold)),
        "creator": 1.0,
    }.get(surface, base_threshold)


def _tiktok_surface(feature: DailySourceFeature) -> str:
    surface = str(
        feature.metadata.get("surface") or feature.metadata.get("sourceSurface") or ""
    ).strip().lower()
    if surface:
        return surface
    if feature.source_id.endswith("_SONGS"):
        return "song"
    if feature.source_id.endswith("_HASHTAGS"):
        return "hashtag"
    if feature.source_id.endswith("_VIDEOS"):
        return "video"
    if feature.source_id.endswith("_CREATORS"):
        return "creator"
    if feature.source_id == "TIKTOK_CREATIVE_CENTER":
        return "hashtag"
    return ""


def _tiktok_surface_weight(surface: str) -> float:
    return {
        "song": 1.15,
        "hashtag": 1.0,
        "video": 0.85,
        "creator": 0.65,
    }.get(surface, 1.0)


def _rank_norm(rank: int) -> float:
    return 1.0 / math.log2(max(rank, 1) + 1)


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
