from __future__ import annotations

from batch.run import _pick_domain, _pick_feature_domain
from packages.core.models import (
    Candidate,
    CandidateKind,
    CandidateType,
    DailySourceFeature,
    DomainClass,
    Evidence,
    ExtractionConfidence,
    RawCandidate,
    SourceFamily,
    SourceRole,
)


def test_pick_domain_infers_music_from_source_when_raw_candidate_is_other() -> None:
    candidate = Candidate(
        candidate_id="cand_music",
        type=CandidateType.MUSIC_ARTIST,
        kind=CandidateKind.ENTITY,
        canonical_name="official髭男dism",
        display_name="Official髭男dism",
        domain_class=DomainClass.OTHER,
    )
    raw_items = [
        RawCandidate(
            name="Official髭男dism",
            type=CandidateType.MUSIC_ARTIST,
            source_id="APPLE_MUSIC_JP",
            domain_class=DomainClass.OTHER,
        )
    ]

    assert _pick_domain(candidate, raw_items) == DomainClass.ENTERTAINMENT


def test_pick_feature_domain_infers_show_from_confirmation_source() -> None:
    candidate = Candidate(
        candidate_id="cand_show",
        type=CandidateType.WORK,
        kind=CandidateKind.ENTITY,
        canonical_name="under ninja",
        display_name="Under Ninja",
        domain_class=DomainClass.OTHER,
    )
    features = [
        DailySourceFeature(
            date="2026-03-06",
            source_id="NETFLIX_FILMS_JP",
            candidate_id="cand_show",
            candidate_type=CandidateType.WORK,
            candidate_kind=CandidateKind.ENTITY,
            source_role=SourceRole.CONFIRMATION,
            family_primary=SourceFamily.SHOW_CHART,
            signal_value=1.0,
            anomaly_score=1.0,
            surprise01=0.5,
            extraction_confidence=ExtractionConfidence.HIGH,
            domain_class=DomainClass.OTHER,
            evidence=[
                Evidence(
                    source_id="NETFLIX_FILMS_JP",
                    title="Under Ninja",
                    url="https://example.com",
                )
            ],
        )
    ]

    assert _pick_feature_domain(candidate, features) == DomainClass.ENTERTAINMENT
