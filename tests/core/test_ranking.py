"""Tests for ranking engine (TrendScore aggregation + Top-K)."""

from __future__ import annotations

from packages.core.models import AlgorithmConfig, MusicConfig
from packages.core.ranking import compute_candidate_score, select_top_k


class TestComputeCandidateScore:
    def test_single_source(self) -> None:
        sig_by_source = {
            "YOUTUBE_TREND_JP": [3.0, 2.0, 1.0],
        }
        algo = AlgorithmConfig()
        music = MusicConfig()

        score, breakdown, mb = compute_candidate_score(sig_by_source, algo, music)

        assert score > 0
        assert mb == 0.0  # only 1 source
        assert len(breakdown) >= 1

    def test_multi_source_bonus(self) -> None:
        sig_by_source = {
            "YOUTUBE_TREND_JP": [3.0, 2.0, 1.0],
            "APPLE_MUSIC_JP": [4.0, 3.0, 2.0],
            "TRENDS": [2.0, 1.0, 0.5],
        }
        algo = AlgorithmConfig(min_sig=0.0)
        music = MusicConfig()

        score, breakdown, mb = compute_candidate_score(sig_by_source, algo, music)

        assert mb > 0  # 3 active sources
        assert score > 0

    def test_music_global_weight_applied(self) -> None:
        sig_by_source_jp = {
            "APPLE_MUSIC_JP": [5.0, 3.0, 1.0],
        }
        sig_by_source_global = {
            "APPLE_MUSIC_GLOBAL": [5.0, 3.0, 1.0],
        }
        algo = AlgorithmConfig()
        music = MusicConfig(weights={"JP": 1.0, "GLOBAL": 0.25})

        score_jp, _, _ = compute_candidate_score(sig_by_source_jp, algo, music)
        score_global, _, _ = compute_candidate_score(sig_by_source_global, algo, music)

        # JP score should be higher (weight 1.0 vs 0.25)
        assert score_jp > score_global

    def test_source_weights_override_legacy_music_weights(self) -> None:
        sig_by_source_jp = {
            "APPLE_MUSIC_JP": [5.0, 3.0, 1.0],
        }
        sig_by_source_global = {
            "APPLE_MUSIC_GLOBAL": [5.0, 3.0, 1.0],
        }
        algo = AlgorithmConfig()
        music = MusicConfig(weights={"JP": 1.0, "GLOBAL": 0.25})
        source_weights = {"APPLE_MUSIC_JP": 1.0, "APPLE_MUSIC_GLOBAL": 0.25}

        score_jp, _, _ = compute_candidate_score(
            sig_by_source_jp, algo, music, source_weights=source_weights
        )
        score_global, _, _ = compute_candidate_score(
            sig_by_source_global, algo, music, source_weights=source_weights
        )

        assert abs(score_global - (score_jp * 0.25)) < 1e-10

    def test_empty_sources(self) -> None:
        score, breakdown, mb = compute_candidate_score({}, AlgorithmConfig(), MusicConfig())
        assert score == 0.0
        assert breakdown == []
        assert mb == 0.0

    def test_trend_score_equals_sum_plus_bonus(self) -> None:
        """Verify TrendScore == sum(bucket_scores) + multiBonus."""
        sig_by_source = {
            "YOUTUBE_TREND_JP": [5.0, 3.0, 1.0],
            "TRENDS": [3.0, 2.0, 1.0],
        }
        algo = AlgorithmConfig(min_sig=0.0)
        music = MusicConfig()

        score, breakdown, mb = compute_candidate_score(sig_by_source, algo, music)

        bucket_sum = sum(b.score for b in breakdown)
        assert abs(score - (bucket_sum + mb)) < 1e-10

    def test_breakdown_details_include_weighted_contribution(self) -> None:
        sig_by_source = {
            "YOUTUBE_TREND_JP": [5.0, 2.0, 1.0],
        }
        algo = AlgorithmConfig(min_sig=0.0)
        music = MusicConfig()

        _, breakdown, _ = compute_candidate_score(
            sig_by_source,
            algo,
            music,
            source_weights={"YOUTUBE_TREND_JP": 1.5},
        )

        assert breakdown[0].details[0]["sourceId"] == "YOUTUBE_TREND_JP"
        assert breakdown[0].details[0]["weight"] == 1.5


class TestSelectTopK:
    def test_selects_top_k(self) -> None:
        candidates = [
            {"trend_score": 10.0, "name": "A"},
            {"trend_score": 30.0, "name": "B"},
            {"trend_score": 20.0, "name": "C"},
        ]
        result = select_top_k(candidates, top_k=2)
        assert len(result) == 2
        assert result[0]["name"] == "B"
        assert result[1]["name"] == "C"

    def test_tiebreaker_multi_bonus(self) -> None:
        candidates = [
            {"trend_score": 10.0, "multi_bonus": 2.0, "name": "A"},
            {"trend_score": 10.0, "multi_bonus": 3.0, "name": "B"},
        ]
        result = select_top_k(candidates, top_k=2)
        assert result[0]["name"] == "B"

    def test_fewer_than_k(self) -> None:
        candidates = [
            {"trend_score": 5.0, "name": "A"},
        ]
        result = select_top_k(candidates, top_k=15)
        assert len(result) == 1

    def test_empty_list(self) -> None:
        assert select_top_k([], top_k=15) == []
