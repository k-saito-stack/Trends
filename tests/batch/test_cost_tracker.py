"""Tests for cost tracker."""

from __future__ import annotations

from batch.cost_tracker import estimate_run_cost


class TestEstimateRunCost:
    def test_free_sources_zero_cost(self) -> None:
        cost = estimate_run_cost(
            ["YOUTUBE_TREND_JP", "APPLE_MUSIC_JP", "TRENDS"]
        )
        assert cost == 0.0

    def test_x_search_costs(self) -> None:
        cost = estimate_run_cost([], x_search_calls=3)
        assert cost == 15.0  # 3 * 5.0

    def test_llm_summary_costs(self) -> None:
        cost = estimate_run_cost([], llm_summary_calls=5)
        assert cost == 15.0  # 5 * 3.0

    def test_combined_costs(self) -> None:
        cost = estimate_run_cost(
            ["YOUTUBE_TREND_JP"],
            x_search_calls=2,
            llm_summary_calls=3,
            llm_resolution_calls=2,
        )
        # 0 (youtube) + 2*5 (x_search) + 3*3 (summary) + 2*2 (resolution) = 23.0
        assert cost == 23.0

    def test_empty_run(self) -> None:
        cost = estimate_run_cost([])
        assert cost == 0.0
