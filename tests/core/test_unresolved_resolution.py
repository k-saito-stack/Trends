from __future__ import annotations

from packages.core.models import (
    Candidate,
    CandidateKind,
    CandidateType,
    DailyCandidateFeature,
    DomainClass,
    RankingLane,
)
from packages.core.unresolved_resolution import (
    apply_resolution_results,
    build_unresolved_pairs,
    max_llm_judgments_for_date,
)


def _candidate(candidate_id: str, name: str, candidate_type: CandidateType) -> Candidate:
    candidate_kind = (
        CandidateKind.ENTITY
        if candidate_type.default_kind == CandidateKind.ENTITY
        else CandidateKind.TOPIC
    )
    return Candidate(
        candidate_id=candidate_id,
        type=candidate_type,
        kind=candidate_kind,
        canonical_name=name,
        display_name=name,
        aliases=[],
        domain_class=DomainClass.ENTERTAINMENT,
    )


def _feature(candidate_id: str, name: str, score: float) -> DailyCandidateFeature:
    return DailyCandidateFeature(
        date="2026-03-07",
        candidate_id=candidate_id,
        display_name=name,
        candidate_type=CandidateType.MUSIC_ARTIST,
        candidate_kind=CandidateKind.ENTITY,
        lane=RankingLane.PEOPLE_MUSIC,
        domain_class=DomainClass.ENTERTAINMENT,
        source_families=["SOCIAL_DISCOVERY", "MUSIC_CHART"],
        primary_score=score,
    )


def test_build_unresolved_pairs_returns_similar_top_candidates() -> None:
    candidates = {
        "cand_1": _candidate("cand_1", "Snow Man", CandidateType.GROUP),
        "cand_2": _candidate("cand_2", "SnowMan", CandidateType.GROUP),
    }
    features = [
        DailyCandidateFeature(
            date="2026-03-07",
            candidate_id="cand_1",
            display_name="Snow Man",
            candidate_type=CandidateType.GROUP,
            candidate_kind=CandidateKind.ENTITY,
            lane=RankingLane.PEOPLE_MUSIC,
            domain_class=DomainClass.ENTERTAINMENT,
            source_families=["SOCIAL_DISCOVERY"],
            primary_score=3.8,
        ),
        DailyCandidateFeature(
            date="2026-03-07",
            candidate_id="cand_2",
            display_name="SnowMan",
            candidate_type=CandidateType.GROUP,
            candidate_kind=CandidateKind.ENTITY,
            lane=RankingLane.PEOPLE_MUSIC,
            domain_class=DomainClass.ENTERTAINMENT,
            source_families=["MUSIC_CHART"],
            primary_score=3.6,
        ),
    ]

    pairs = build_unresolved_pairs(features, candidates, top_window=200, max_pairs=30)

    assert len(pairs) == 1
    assert pairs[0]["mergeRecommended"] is True
    assert "sequence_ratio" in pairs[0]["reasons"]


def test_apply_resolution_results_creates_duplicate_relation_for_merge_recommendation() -> None:
    pairs = [
        {
            "pairId": "pair_1",
            "left": {"candidateId": "cand_1", "name": "Snow Man"},
            "right": {"candidateId": "cand_2", "name": "SnowMan"},
            "priority": 1.2,
            "reasons": ["sequence_ratio"],
            "deterministicSupportCount": 1,
            "mergeRecommended": True,
        }
    ]
    decisions = [
        {
            "decision": "merge",
            "confidence": 0.92,
            "provider": "stub",
            "model": "stub-model",
            "reason": "same group",
            "cacheHit": False,
        }
    ]

    queue_items, relations = apply_resolution_results(
        pairs,
        decisions,
        created_at="2026-03-07T07:00:00+09:00",
    )

    assert queue_items[0]["finalAction"] == "MERGE_RECOMMENDED"
    assert len(relations) == 2
    assert relations[0].relation_type == "possible_duplicate"


def test_max_llm_judgments_for_date_distinguishes_backfill() -> None:
    assert max_llm_judgments_for_date("2026-03-07", "2026-03-07") == 30
    assert max_llm_judgments_for_date("2026-03-06", "2026-03-07") == 100
