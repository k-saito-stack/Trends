from __future__ import annotations

from packages.core.models import (
    CandidateKind,
    CandidateRelation,
    CandidateType,
    DailySourceFeature,
    DomainClass,
    ExtractionConfidence,
    SourceFamily,
    SourceRole,
)
from packages.core.relation_propagation import build_relation_support_features


def _feature(candidate_id: str, source_id: str) -> DailySourceFeature:
    return DailySourceFeature(
        date="2026-03-07",
        source_id=source_id,
        candidate_id=candidate_id,
        candidate_type=CandidateType.SHOW,
        candidate_kind=CandidateKind.ENTITY,
        source_role=SourceRole.CONFIRMATION,
        family_primary=SourceFamily.SHOW_CHART,
        signal_value=1.0,
        anomaly_score=2.0,
        surprise01=0.8,
        extraction_confidence=ExtractionConfidence.HIGH,
        domain_class=DomainClass.ENTERTAINMENT,
    )


def test_build_relation_support_features_propagates_netflix_confirmation() -> None:
    feature_map = {"show_1": [_feature("show_1", "NETFLIX_TV_JP")]}
    relations = [
        CandidateRelation(
            src_candidate_id="show_1",
            relation_type="features_in",
            dst_candidate_id="person_1",
            confidence=0.9,
            source="netflix:test",
        )
    ]

    support = build_relation_support_features(feature_map, relations)

    assert support["person_1"]["relation_support_total"] > 0
    assert support["person_1"]["netflix_relation_support_people"] > 0
    assert support["person_1"]["relation_confirmed_support"] > 0
