from __future__ import annotations

from packages.core.family_features import aggregate_family_metrics
from packages.core.fusion_model import (
    build_candidate_feature_vector,
    compute_primary_score,
    predict_breakout_prob,
    predict_mass_prob,
)
from packages.core.models import (
    Candidate,
    CandidateKind,
    CandidateType,
    DailySourceFeature,
    DomainClass,
    ExtractionConfidence,
    SourceFamily,
    SourceRole,
)


def _build_feature(
    source_id: str,
    *,
    source_role: SourceRole,
    family_primary: SourceFamily,
    surprise01: float,
    candidate_type: CandidateType = CandidateType.HASHTAG,
    candidate_kind: CandidateKind = CandidateKind.TOPIC,
    countries: list[str] | None = None,
    posterior_reliability: float = 1.0,
    posterior_lead: float = 0.0,
    posterior_persistence: float = 0.0,
) -> DailySourceFeature:
    return DailySourceFeature(
        date="2026-03-07",
        source_id=source_id,
        candidate_id="cand",
        candidate_type=candidate_type,
        candidate_kind=candidate_kind,
        source_role=source_role,
        family_primary=family_primary,
        surprise01=surprise01,
        momentum=surprise01,
        extraction_confidence=ExtractionConfidence.HIGH,
        posterior_reliability=posterior_reliability,
        posterior_lead=posterior_lead,
        posterior_persistence=posterior_persistence,
        metadata={"countries": countries or []},
    )


def test_breakout_probability_prefers_regional_tiktok_discovery_signal() -> None:
    candidate = Candidate(
        candidate_id="cand",
        type=CandidateType.HASHTAG,
        kind=CandidateKind.TOPIC,
        canonical_name="#tag",
        display_name="#tag",
        domain_class=DomainClass.CONSUMER_CULTURE,
    )
    tiktok_features = [
        _build_feature(
            "TIKTOK_CREATIVE_CENTER_HASHTAGS",
            source_role=SourceRole.DISCOVERY,
            family_primary=SourceFamily.SOCIAL_DISCOVERY,
            surprise01=0.92,
            countries=["JP", "KR"],
            posterior_reliability=0.88,
            posterior_lead=0.35,
        ),
        _build_feature(
            "YOUTUBE_TREND_JP",
            source_role=SourceRole.CONFIRMATION,
            family_primary=SourceFamily.VIDEO_CONFIRM,
            surprise01=0.35,
            candidate_type=CandidateType.HASHTAG,
            candidate_kind=CandidateKind.TOPIC,
        ),
    ]
    chart_only_features = [
        _build_feature(
            "APPLE_MUSIC_JP",
            source_role=SourceRole.CONFIRMATION,
            family_primary=SourceFamily.MUSIC_CHART,
            surprise01=0.7,
            candidate_type=CandidateType.MUSIC_TRACK,
            candidate_kind=CandidateKind.ENTITY,
            posterior_persistence=0.5,
        )
    ]

    tiktok_vector = build_candidate_feature_vector(
        candidate,
        aggregate_family_metrics(tiktok_features),
        tiktok_features,
        novelty=0.82,
        domain_fit=0.9,
        extraction_confidence=1.0,
        maturity_penalty=0.1,
        sustained_presence=0.15,
    )
    chart_vector = build_candidate_feature_vector(
        candidate,
        aggregate_family_metrics(chart_only_features),
        chart_only_features,
        novelty=0.35,
        domain_fit=0.65,
        extraction_confidence=1.0,
        maturity_penalty=0.45,
        sustained_presence=0.4,
    )

    assert predict_breakout_prob(tiktok_vector, horizon_days=7) > predict_breakout_prob(
        chart_vector, horizon_days=7
    )


def test_mass_probability_prefers_confirmation_and_persistence() -> None:
    candidate = Candidate(
        candidate_id="cand",
        type=CandidateType.WORK,
        kind=CandidateKind.ENTITY,
        canonical_name="show",
        display_name="Show",
        domain_class=DomainClass.ENTERTAINMENT,
    )
    light_features = [
        _build_feature(
            "TRENDS",
            source_role=SourceRole.DISCOVERY,
            family_primary=SourceFamily.SEARCH,
            surprise01=0.5,
            candidate_type=CandidateType.WORK,
            candidate_kind=CandidateKind.ENTITY,
        )
    ]
    heavy_features = light_features + [
        _build_feature(
            "NETFLIX_TV_JP",
            source_role=SourceRole.CONFIRMATION,
            family_primary=SourceFamily.SHOW_CHART,
            surprise01=0.7,
            candidate_type=CandidateType.WORK,
            candidate_kind=CandidateKind.ENTITY,
            posterior_persistence=0.6,
        ),
        _build_feature(
            "EDITORIAL_FASHIONSNAP",
            source_role=SourceRole.EDITORIAL,
            family_primary=SourceFamily.EDITORIAL,
            surprise01=0.45,
            candidate_type=CandidateType.WORK,
            candidate_kind=CandidateKind.ENTITY,
        ),
    ]

    light_vector = build_candidate_feature_vector(
        candidate,
        aggregate_family_metrics(light_features),
        light_features,
        novelty=0.7,
        domain_fit=0.8,
        extraction_confidence=1.0,
        maturity_penalty=0.1,
        sustained_presence=0.1,
    )
    heavy_vector = build_candidate_feature_vector(
        candidate,
        aggregate_family_metrics(heavy_features),
        heavy_features,
        novelty=0.55,
        domain_fit=0.8,
        extraction_confidence=1.0,
        maturity_penalty=0.18,
        sustained_presence=0.7,
    )

    assert predict_mass_prob(heavy_vector) > predict_mass_prob(light_vector)


def test_primary_score_keeps_phrase_mass_weight_below_show_weight() -> None:
    phrase_score = compute_primary_score(
        CandidateType.HASHTAG,
        DomainClass.CONSUMER_CULTURE,
        breakout_prob=0.6,
        mass_prob=0.8,
    )
    show_score = compute_primary_score(
        CandidateType.WORK,
        DomainClass.ENTERTAINMENT,
        breakout_prob=0.6,
        mass_prob=0.8,
    )

    assert show_score > phrase_score
