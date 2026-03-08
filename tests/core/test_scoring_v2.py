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


def test_single_family_discovery_topic_requires_stronger_signal() -> None:
    candidate = _candidate()
    features = [
        DailySourceFeature(
            date="2026-03-06",
            source_id="TIKTOK_CREATIVE_CENTER",
            candidate_id="cand_1",
            candidate_type=CandidateType.BEHAVIOR,
            candidate_kind=CandidateKind.TOPIC,
            source_role=SourceRole.DISCOVERY,
            family_primary=SourceFamily.SOCIAL_DISCOVERY,
            signal_value=1.0,
            anomaly_score=3.2,
            surprise01=0.86,
            extraction_confidence=ExtractionConfidence.HIGH,
            domain_class=DomainClass.CONSUMER_CULTURE,
        )
    ]

    feature = compute_candidate_feature(
        date="2026-03-06",
        candidate=candidate,
        lane=RankingLane.WORDS_BEHAVIORS,
        domain_class=DomainClass.CONSUMER_CULTURE,
        source_features=features,
        algo_config=AlgorithmConfig(),
    )

    assert feature.ranking_gate_passed is False
    assert feature.primary_score < 1.0


def test_regional_tiktok_topic_can_pass_with_japan_signal_and_jp_confirmation() -> None:
    candidate = Candidate(
        candidate_id="cand_regional",
        type=CandidateType.HASHTAG,
        kind=CandidateKind.TOPIC,
        canonical_name="#メガ割購入品レビュー",
        display_name="#メガ割購入品レビュー",
        domain_class=DomainClass.CONSUMER_CULTURE,
    )
    features = [
        DailySourceFeature(
            date="2026-03-06",
            source_id="TIKTOK_CREATIVE_CENTER",
            candidate_id="cand_1",
            candidate_type=CandidateType.HASHTAG,
            candidate_kind=CandidateKind.TOPIC,
            source_role=SourceRole.DISCOVERY,
            family_primary=SourceFamily.SOCIAL_DISCOVERY,
            signal_value=1.0,
            anomaly_score=3.0,
            surprise01=0.78,
            extraction_confidence=ExtractionConfidence.HIGH,
            domain_class=DomainClass.CONSUMER_CULTURE,
            metadata={"countries": ["JP", "KR"], "countryRanks": {"JP": 2, "KR": 5}},
        ),
        DailySourceFeature(
            date="2026-03-06",
            source_id="YAHOO_REALTIME",
            candidate_id="cand_regional",
            candidate_type=CandidateType.HASHTAG,
            candidate_kind=CandidateKind.TOPIC,
            source_role=SourceRole.DISCOVERY,
            family_primary=SourceFamily.SOCIAL_DISCOVERY,
            signal_value=0.6,
            anomaly_score=1.8,
            surprise01=0.32,
            extraction_confidence=ExtractionConfidence.HIGH,
            domain_class=DomainClass.CONSUMER_CULTURE,
            metadata={"countryCode": "JP"},
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

    assert feature.ranking_gate_passed is True
    assert feature.primary_score >= feature.coming_score


def test_ungated_multi_family_candidate_keeps_more_score_than_single_family_topic() -> None:
    candidate = Candidate(
        candidate_id="cand_2",
        type=CandidateType.MUSIC_ARTIST,
        kind=CandidateKind.ENTITY,
        canonical_name="Snow Man",
        display_name="Snow Man",
        domain_class=DomainClass.ENTERTAINMENT,
        maturity=1.0,
    )
    features = [
        DailySourceFeature(
            date="2026-03-06",
            source_id="APPLE_MUSIC_JP",
            candidate_id="cand_2",
            candidate_type=CandidateType.MUSIC_ARTIST,
            candidate_kind=CandidateKind.ENTITY,
            source_role=SourceRole.CONFIRMATION,
            family_primary=SourceFamily.MUSIC_CHART,
            signal_value=1.0,
            anomaly_score=2.0,
            surprise01=0.7,
            extraction_confidence=ExtractionConfidence.HIGH,
            domain_class=DomainClass.ENTERTAINMENT,
        ),
        DailySourceFeature(
            date="2026-03-06",
            source_id="TVER_RANKING_JP",
            candidate_id="cand_2",
            candidate_type=CandidateType.MUSIC_ARTIST,
            candidate_kind=CandidateKind.ENTITY,
            source_role=SourceRole.CONFIRMATION,
            family_primary=SourceFamily.SHOW_CHART,
            signal_value=1.0,
            anomaly_score=1.8,
            surprise01=0.6,
            extraction_confidence=ExtractionConfidence.HIGH,
            domain_class=DomainClass.ENTERTAINMENT,
        ),
    ]

    feature = compute_candidate_feature(
        date="2026-03-06",
        candidate=candidate,
        lane=RankingLane.PEOPLE_MUSIC,
        domain_class=DomainClass.ENTERTAINMENT,
        source_features=features,
        algo_config=AlgorithmConfig(),
    )

    expected_raw_primary = (
        feature.coming_score + AlgorithmConfig().mass_heat_weight * feature.mass_heat
    )
    assert feature.ranking_gate_passed is False
    assert abs(feature.primary_score - (expected_raw_primary * 0.45)) < 1e-9


def test_relation_only_tver_person_is_not_public_rankable() -> None:
    candidate = Candidate(
        candidate_id="cand_actor",
        type=CandidateType.PERSON,
        kind=CandidateKind.ENTITY,
        canonical_name="俳優A",
        display_name="俳優A",
        domain_class=DomainClass.ENTERTAINMENT,
    )
    features = [
        DailySourceFeature(
            date="2026-03-06",
            source_id="TVER_RANKING_JP",
            candidate_id="cand_actor",
            candidate_type=CandidateType.PERSON,
            candidate_kind=CandidateKind.ENTITY,
            source_role=SourceRole.CONFIRMATION,
            family_primary=SourceFamily.SHOW_CHART,
            signal_value=0.22,
            anomaly_score=1.2,
            surprise01=0.18,
            extraction_confidence=ExtractionConfidence.MEDIUM,
            domain_class=DomainClass.ENTERTAINMENT,
            metadata={
                "derivedFromWork": True,
                "workClusterId": "tver:1:show-a",
                "relationClusterId": "tver:1:show-a",
                "relationType": "features_in",
            },
        )
    ]

    feature = compute_candidate_feature(
        date="2026-03-06",
        candidate=candidate,
        lane=RankingLane.PEOPLE_MUSIC,
        domain_class=DomainClass.ENTERTAINMENT,
        source_features=features,
        algo_config=AlgorithmConfig(),
        relation_support={"relation_support_total": 0.22, "tver_relation_support": 0.22},
    )

    assert feature.relation_only_flag is True
    assert feature.public_gate_passed is False


def test_single_chart_confirmation_requires_second_source() -> None:
    candidate = Candidate(
        candidate_id="cand_music_1",
        type=CandidateType.MUSIC_ARTIST,
        kind=CandidateKind.ENTITY,
        canonical_name="Hearts2Hearts",
        display_name="Hearts2Hearts",
        domain_class=DomainClass.ENTERTAINMENT,
    )
    features = [
        DailySourceFeature(
            date="2026-03-06",
            source_id="APPLE_MUSIC_KR",
            candidate_id="cand_music_1",
            candidate_type=CandidateType.MUSIC_ARTIST,
            candidate_kind=CandidateKind.ENTITY,
            source_role=SourceRole.CONFIRMATION,
            family_primary=SourceFamily.MUSIC_CHART,
            signal_value=1.0,
            anomaly_score=2.4,
            surprise01=0.82,
            extraction_confidence=ExtractionConfidence.HIGH,
            domain_class=DomainClass.ENTERTAINMENT,
            metadata={"regions": ["KR"]},
        )
    ]

    feature = compute_candidate_feature(
        date="2026-03-06",
        candidate=candidate,
        lane=RankingLane.PEOPLE_MUSIC,
        domain_class=DomainClass.ENTERTAINMENT,
        source_features=features,
        algo_config=AlgorithmConfig(),
    )

    expected_raw_primary = (
        feature.coming_score + AlgorithmConfig().mass_heat_weight * feature.mass_heat
    )
    assert feature.ranking_gate_passed is False
    assert abs(feature.primary_score - (expected_raw_primary * 0.2)) < 1e-9


def test_multi_region_music_chart_confirmation_can_pass_without_discovery() -> None:
    candidate = Candidate(
        candidate_id="cand_music_2",
        type=CandidateType.MUSIC_ARTIST,
        kind=CandidateKind.ENTITY,
        canonical_name="Snow Man",
        display_name="Snow Man",
        domain_class=DomainClass.ENTERTAINMENT,
    )
    features = [
        DailySourceFeature(
            date="2026-03-06",
            source_id="APPLE_MUSIC_JP",
            candidate_id="cand_music_2",
            candidate_type=CandidateType.MUSIC_ARTIST,
            candidate_kind=CandidateKind.ENTITY,
            source_role=SourceRole.CONFIRMATION,
            family_primary=SourceFamily.MUSIC_CHART,
            signal_value=1.0,
            anomaly_score=2.2,
            surprise01=0.76,
            extraction_confidence=ExtractionConfidence.HIGH,
            domain_class=DomainClass.ENTERTAINMENT,
            metadata={"regions": ["JP"]},
        ),
        DailySourceFeature(
            date="2026-03-06",
            source_id="APPLE_MUSIC_KR",
            candidate_id="cand_music_2",
            candidate_type=CandidateType.MUSIC_ARTIST,
            candidate_kind=CandidateKind.ENTITY,
            source_role=SourceRole.CONFIRMATION,
            family_primary=SourceFamily.MUSIC_CHART,
            signal_value=1.0,
            anomaly_score=2.0,
            surprise01=0.71,
            extraction_confidence=ExtractionConfidence.HIGH,
            domain_class=DomainClass.ENTERTAINMENT,
            metadata={"regions": ["KR"]},
        ),
    ]

    feature = compute_candidate_feature(
        date="2026-03-06",
        candidate=candidate,
        lane=RankingLane.PEOPLE_MUSIC,
        domain_class=DomainClass.ENTERTAINMENT,
        source_features=features,
        algo_config=AlgorithmConfig(),
    )

    assert feature.ranking_gate_passed is True
