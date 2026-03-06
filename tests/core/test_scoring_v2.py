from __future__ import annotations

from packages.core.models import (
    AlgorithmConfig,
    Candidate,
    CandidateKind,
    CandidateType,
    DailySourceFeature,
    DomainClass,
    ExtractionConfidence,
    RankingLane,
    SourceFamily,
    SourceRole,
)
from packages.core.scoring_v2 import compute_candidate_feature


def _candidate() -> Candidate:
    return Candidate(
        candidate_id="cand_1",
        type=CandidateType.BEHAVIOR,
        kind=CandidateKind.TOPIC,
        canonical_name="シール交換",
        display_name="シール交換",
        domain_class=DomainClass.CONSUMER_CULTURE,
    )


def test_compute_candidate_feature_prefers_discovery_plus_confirmation() -> None:
    candidate = _candidate()
    features = [
        DailySourceFeature(
            date="2026-03-06",
            source_id="YAHOO_REALTIME",
            candidate_id="cand_1",
            candidate_type=CandidateType.BEHAVIOR,
            candidate_kind=CandidateKind.TOPIC,
            source_role=SourceRole.DISCOVERY,
            family_primary=SourceFamily.SOCIAL_DISCOVERY,
            signal_value=1.0,
            anomaly_score=3.5,
            surprise01=0.9,
            extraction_confidence=ExtractionConfidence.HIGH,
            domain_class=DomainClass.CONSUMER_CULTURE,
        ),
        DailySourceFeature(
            date="2026-03-06",
            source_id="WEAR_WORDS",
            candidate_id="cand_1",
            candidate_type=CandidateType.BEHAVIOR,
            candidate_kind=CandidateKind.TOPIC,
            source_role=SourceRole.EDITORIAL,
            family_primary=SourceFamily.FASHION_STYLE,
            signal_value=0.8,
            anomaly_score=2.5,
            surprise01=0.7,
            extraction_confidence=ExtractionConfidence.MEDIUM,
            domain_class=DomainClass.CONSUMER_CULTURE,
        ),
    ]

    feature = compute_candidate_feature(
        date="2026-03-06",
        candidate=candidate,
        lane=RankingLane.WORDS_BEHAVIORS,
        domain_class=DomainClass.CONSUMER_CULTURE,
        source_features=features,
        algo_config=AlgorithmConfig(),
    )

    assert feature.coming_score > 0.5
    assert feature.ranking_gate_passed is True
    assert feature.primary_score >= feature.coming_score
