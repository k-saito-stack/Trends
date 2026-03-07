from __future__ import annotations

from packages.core.models import Candidate, CandidateKind, CandidateType, DomainClass
from packages.core.noise_filter import compute_public_noise_penalty


def _topic(name: str, candidate_type: CandidateType = CandidateType.PHRASE) -> Candidate:
    return Candidate(
        candidate_id=name,
        type=candidate_type,
        kind=CandidateKind.TOPIC,
        canonical_name=name,
        display_name=name,
        domain_class=DomainClass.CONSUMER_CULTURE,
    )


def test_compute_public_noise_penalty_penalizes_generic_phrase_more_than_specific_tag() -> None:
    generic_penalty, _, _ = compute_public_noise_penalty(_topic("ランキング"), [])
    specific_penalty, _, _ = compute_public_noise_penalty(
        _topic("#メガ割", CandidateType.HASHTAG),
        [],
    )

    assert generic_penalty > specific_penalty
