from __future__ import annotations

from packages.core.manual_resolution import apply_manual_resolution
from packages.core.models import Candidate, CandidateKind, CandidateStatus, CandidateType


def _candidate(candidate_id: str, name: str) -> Candidate:
    return Candidate(
        candidate_id=candidate_id,
        type=CandidateType.GROUP,
        kind=CandidateKind.ENTITY,
        canonical_name=name,
        display_name=name,
        aliases=[],
        status=CandidateStatus.ACTIVE,
    )


def test_apply_manual_resolution_merge_marks_loser_merged_and_moves_aliases() -> None:
    left = _candidate("cand_1", "Snow Man")
    right = _candidate("cand_2", "SnowMan")
    queue_item = {
        "date": "2026-03-07",
        "pairId": "pair_1",
        "leftCandidateId": "cand_1",
        "rightCandidateId": "cand_2",
    }

    result = apply_manual_resolution(
        queue_item,
        {"cand_1": left, "cand_2": right},
        action="merge",
        winner_candidate_id="cand_1",
        changed_by="tester@example.com",
        changed_at="2026-03-07T07:00:00+09:00",
    )

    winner = next(
        candidate for candidate in result["updatedCandidates"] if candidate.candidate_id == "cand_1"
    )
    loser = next(
        candidate for candidate in result["updatedCandidates"] if candidate.candidate_id == "cand_2"
    )

    assert loser.status == CandidateStatus.MERGED
    assert loser.metadata["mergedIntoCandidateId"] == "cand_1"
    assert "SnowMan" in winner.aliases
    assert result["queueItem"]["appliedAction"] == "merge"
    assert result["deleteAliasCandidateIds"] == ["cand_2"]


def test_apply_manual_resolution_separate_adds_resolution_exclusion() -> None:
    left = _candidate("cand_1", "Ado")
    right = _candidate("cand_2", "adoメイク")
    queue_item = {
        "date": "2026-03-07",
        "pairId": "pair_2",
        "leftCandidateId": "cand_1",
        "rightCandidateId": "cand_2",
    }

    result = apply_manual_resolution(
        queue_item,
        {"cand_1": left, "cand_2": right},
        action="separate",
        changed_by="tester@example.com",
        changed_at="2026-03-07T07:00:00+09:00",
    )

    assert result["relations"] == []
    assert left.metadata["resolutionExcludeIds"] == ["cand_2"]
    assert right.metadata["resolutionExcludeIds"] == ["cand_1"]
    assert result["queueItem"]["appliedAction"] == "separate"


def test_apply_manual_resolution_link_adds_manual_relation() -> None:
    left = _candidate("cand_1", "曲A")
    right = _candidate("cand_2", "アーティストB")
    queue_item = {
        "date": "2026-03-07",
        "pairId": "pair_3",
        "leftCandidateId": "cand_1",
        "rightCandidateId": "cand_2",
    }

    result = apply_manual_resolution(
        queue_item,
        {"cand_1": left, "cand_2": right},
        action="link",
        changed_by="tester@example.com",
        changed_at="2026-03-07T07:00:00+09:00",
    )

    assert len(result["relations"]) == 2
    assert result["relations"][0].relation_type == "manual_related"
    assert "cand_2" in left.related_candidate_ids
    assert result["queueItem"]["appliedAction"] == "link"
