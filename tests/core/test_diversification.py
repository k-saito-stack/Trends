from __future__ import annotations

from packages.core.diversification import infer_lane, interleave_ranked_items
from packages.core.models import CandidateType, RankingLane


def test_infer_lane() -> None:
    assert infer_lane(CandidateType.MUSIC_ARTIST) == RankingLane.PEOPLE_MUSIC
    assert infer_lane(CandidateType.BEHAVIOR) == RankingLane.WORDS_BEHAVIORS
    assert infer_lane(CandidateType.BRAND) == RankingLane.STYLE_PRODUCTS


def test_interleave_ranked_items_protects_topic_lanes() -> None:
    items = [
        {"candidate_id": "1", "lane": "people_music", "primary_score": 10.0},
        {"candidate_id": "2", "lane": "people_music", "primary_score": 9.5},
        {"candidate_id": "3", "lane": "people_music", "primary_score": 9.0},
        {"candidate_id": "4", "lane": "words_behaviors", "primary_score": 8.9},
        {"candidate_id": "5", "lane": "style_products", "primary_score": 8.8},
    ]

    ranked = interleave_ranked_items(items, top_k=5)
    lanes = {item["lane"] for item in ranked}
    assert "words_behaviors" in lanes
    assert "style_products" in lanes


def test_interleave_ranked_items_does_not_underfill_when_lane_missing() -> None:
    items = [
        {"candidate_id": "1", "lane": "people_music", "primary_score": 10.0},
        {"candidate_id": "2", "lane": "people_music", "primary_score": 9.5},
        {"candidate_id": "3", "lane": "people_music", "primary_score": 9.0},
        {"candidate_id": "4", "lane": "people_music", "primary_score": 8.5},
        {"candidate_id": "5", "lane": "words_behaviors", "primary_score": 8.4},
    ]

    ranked = interleave_ranked_items(items, top_k=5)

    assert len(ranked) == 5
    assert ranked[-1]["candidate_id"] == "4"
