from __future__ import annotations

from packages.core.models import CandidateType
from packages.core.topic_extract import extract_topic_candidates


def test_extract_topic_candidates_splits_hashtag_behavior_style_and_product() -> None:
    candidates = extract_topic_candidates(
        "#ラブブ バッグにつける 春服コーデ スニーカー",
        "WEAR_WORDS",
    )

    pairs = {(candidate.type, candidate.name) for candidate in candidates}

    assert (CandidateType.HASHTAG, "#ラブブ") in pairs
    assert any(candidate_type == CandidateType.BEHAVIOR for candidate_type, _ in pairs)
    assert any(candidate_type == CandidateType.STYLE for candidate_type, _ in pairs)
    assert any(candidate_type == CandidateType.PRODUCT for candidate_type, _ in pairs)
