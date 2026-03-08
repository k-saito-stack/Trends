"""Tests for ranking engine (TrendScore aggregation + Top-K)."""

from __future__ import annotations

from packages.core.models import (
    AlgorithmConfig,
    Candidate,
    CandidateKind,
    CandidateType,
    DailyCandidateFeature,
    DomainClass,
    MusicConfig,
    RankingLane,
)
from packages.core.ranking import build_ranked_candidates_v2, compute_candidate_score, select_top_k


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

    def test_music_kr_weight_applied_below_jp(self) -> None:
        sig_by_source_jp = {
            "APPLE_MUSIC_JP": [5.0, 3.0, 1.0],
        }
        sig_by_source_kr = {
            "APPLE_MUSIC_KR": [5.0, 3.0, 1.0],
        }
        algo = AlgorithmConfig()
        music = MusicConfig(weights={"JP": 1.0, "KR": 0.85, "GLOBAL": 0.1})

        score_jp, _, _ = compute_candidate_score(sig_by_source_jp, algo, music)
        score_kr, _, _ = compute_candidate_score(sig_by_source_kr, algo, music)

        assert score_jp > score_kr

    def test_source_weights_override_legacy_music_weights(self) -> None:
        sig_by_source_jp = {
            "APPLE_MUSIC_JP": [5.0, 3.0, 1.0],
        }
        sig_by_source_kr = {
            "APPLE_MUSIC_KR": [5.0, 3.0, 1.0],
        }
        algo = AlgorithmConfig()
        music = MusicConfig(weights={"JP": 1.0, "KR": 0.85, "GLOBAL": 0.1})
        source_weights = {"APPLE_MUSIC_JP": 1.0, "APPLE_MUSIC_KR": 0.85}

        score_jp, _, _ = compute_candidate_score(
            sig_by_source_jp, algo, music, source_weights=source_weights
        )
        score_kr, _, _ = compute_candidate_score(
            sig_by_source_kr, algo, music, source_weights=source_weights
        )

        assert abs(score_kr - (score_jp * 0.85)) < 1e-10

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


class TestBuildRankedCandidatesV2:
    def test_backfills_main_domains_when_gate_passed_items_are_short(self) -> None:
        candidates = {
            "cand_a": Candidate(
                candidate_id="cand_a",
                type=CandidateType.PHRASE,
                kind=CandidateKind.TOPIC,
                canonical_name="a",
                display_name="A",
                domain_class=DomainClass.CONSUMER_CULTURE,
            ),
            "cand_b": Candidate(
                candidate_id="cand_b",
                type=CandidateType.WORK,
                kind=CandidateKind.ENTITY,
                canonical_name="b",
                display_name="B",
                domain_class=DomainClass.ENTERTAINMENT,
            ),
        }
        features = [
            DailyCandidateFeature(
                date="2026-03-06",
                candidate_id="cand_a",
                display_name="A",
                candidate_type=CandidateType.PHRASE,
                candidate_kind=CandidateKind.TOPIC,
                lane=RankingLane.WORDS_BEHAVIORS,
                domain_class=DomainClass.CONSUMER_CULTURE,
                coming_score=2.0,
                primary_score=2.0,
                ranking_gate_passed=True,
            ),
            DailyCandidateFeature(
                date="2026-03-06",
                candidate_id="cand_b",
                display_name="B",
                candidate_type=CandidateType.WORK,
                candidate_kind=CandidateKind.ENTITY,
                lane=RankingLane.SHOWS_FORMATS,
                domain_class=DomainClass.ENTERTAINMENT,
                coming_score=0.3,
                primary_score=0.4,
                ranking_gate_passed=False,
            ),
        ]

        ranked = build_ranked_candidates_v2(features, candidates, top_k=2)

        assert [item.candidate_id for item in ranked] == ["cand_a", "cand_b"]

    def test_backfill_prefers_discovery_topics_over_single_chart_confirmation(self) -> None:
        candidates = {
            "cand_gate": Candidate(
                candidate_id="cand_gate",
                type=CandidateType.PHRASE,
                kind=CandidateKind.TOPIC,
                canonical_name="gate",
                display_name="Gate",
                domain_class=DomainClass.CONSUMER_CULTURE,
            ),
            "cand_tiktok": Candidate(
                candidate_id="cand_tiktok",
                type=CandidateType.HASHTAG,
                kind=CandidateKind.TOPIC,
                canonical_name="#tag",
                display_name="#tag",
                domain_class=DomainClass.CONSUMER_CULTURE,
            ),
            "cand_apple": Candidate(
                candidate_id="cand_apple",
                type=CandidateType.MUSIC_ARTIST,
                kind=CandidateKind.ENTITY,
                canonical_name="Apple Artist",
                display_name="Apple Artist",
                domain_class=DomainClass.ENTERTAINMENT,
            ),
        }
        features = [
            DailyCandidateFeature(
                date="2026-03-06",
                candidate_id="cand_gate",
                display_name="Gate",
                candidate_type=CandidateType.PHRASE,
                candidate_kind=CandidateKind.TOPIC,
                lane=RankingLane.WORDS_BEHAVIORS,
                domain_class=DomainClass.CONSUMER_CULTURE,
                source_families=["SOCIAL_DISCOVERY", "SEARCH"],
                coming_score=1.8,
                primary_score=1.8,
                ranking_gate_passed=True,
                metadata={"roleScores": {"DISCOVERY": 0.9}},
            ),
            DailyCandidateFeature(
                date="2026-03-06",
                candidate_id="cand_tiktok",
                display_name="#tag",
                candidate_type=CandidateType.HASHTAG,
                candidate_kind=CandidateKind.TOPIC,
                lane=RankingLane.WORDS_BEHAVIORS,
                domain_class=DomainClass.CONSUMER_CULTURE,
                source_families=["SOCIAL_DISCOVERY"],
                coming_score=0.72,
                primary_score=0.68,
                ranking_gate_passed=False,
                metadata={"roleScores": {"DISCOVERY": 0.78}},
            ),
            DailyCandidateFeature(
                date="2026-03-06",
                candidate_id="cand_apple",
                display_name="Apple Artist",
                candidate_type=CandidateType.MUSIC_ARTIST,
                candidate_kind=CandidateKind.ENTITY,
                lane=RankingLane.PEOPLE_MUSIC,
                domain_class=DomainClass.ENTERTAINMENT,
                source_families=["MUSIC_CHART"],
                coming_score=0.3,
                primary_score=0.92,
                ranking_gate_passed=False,
                metadata={"roleScores": {"CONFIRMATION": 0.92}},
            ),
        ]

        ranked = build_ranked_candidates_v2(features, candidates, top_k=3)

        assert [item.candidate_id for item in ranked] == [
            "cand_gate",
            "cand_tiktok",
            "cand_apple",
        ]

    def test_relation_only_people_from_same_work_cluster_are_capped(self) -> None:
        candidates = {
            "actor_1": Candidate(
                candidate_id="actor_1",
                type=CandidateType.PERSON,
                kind=CandidateKind.ENTITY,
                canonical_name="actor_1",
                display_name="Actor 1",
                domain_class=DomainClass.ENTERTAINMENT,
            ),
            "actor_2": Candidate(
                candidate_id="actor_2",
                type=CandidateType.PERSON,
                kind=CandidateKind.ENTITY,
                canonical_name="actor_2",
                display_name="Actor 2",
                domain_class=DomainClass.ENTERTAINMENT,
            ),
            "show_1": Candidate(
                candidate_id="show_1",
                type=CandidateType.SHOW,
                kind=CandidateKind.ENTITY,
                canonical_name="show_1",
                display_name="Show 1",
                domain_class=DomainClass.ENTERTAINMENT,
            ),
        }
        features = [
            DailyCandidateFeature(
                date="2026-03-06",
                candidate_id="actor_1",
                display_name="Actor 1",
                candidate_type=CandidateType.PERSON,
                candidate_kind=CandidateKind.ENTITY,
                lane=RankingLane.PEOPLE_MUSIC,
                domain_class=DomainClass.ENTERTAINMENT,
                primary_score=3.0,
                public_score=3.0,
                public_rankability_prob=0.7,
                public_gate_passed=True,
                relation_only_flag=True,
                work_cluster_id="cluster_show_1",
            ),
            DailyCandidateFeature(
                date="2026-03-06",
                candidate_id="actor_2",
                display_name="Actor 2",
                candidate_type=CandidateType.PERSON,
                candidate_kind=CandidateKind.ENTITY,
                lane=RankingLane.PEOPLE_MUSIC,
                domain_class=DomainClass.ENTERTAINMENT,
                primary_score=2.9,
                public_score=2.9,
                public_rankability_prob=0.68,
                public_gate_passed=True,
                relation_only_flag=True,
                work_cluster_id="cluster_show_1",
            ),
            DailyCandidateFeature(
                date="2026-03-06",
                candidate_id="show_1",
                display_name="Show 1",
                candidate_type=CandidateType.SHOW,
                candidate_kind=CandidateKind.ENTITY,
                lane=RankingLane.SHOWS_FORMATS,
                domain_class=DomainClass.ENTERTAINMENT,
                primary_score=3.2,
                public_score=3.2,
                public_rankability_prob=0.75,
                public_gate_passed=True,
            ),
        ]

        ranked = build_ranked_candidates_v2(features, candidates, top_k=20)

        ranked_ids = [item.candidate_id for item in ranked]
        assert "actor_1" in ranked_ids
        assert "actor_2" not in ranked_ids
