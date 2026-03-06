"""Tests for cost degradation logic."""

from __future__ import annotations

from batch.degrade import compute_degrade_state
from packages.core.models import AppConfig


class TestComputeDegradeState:
    def test_full_mode_under_60_percent(self) -> None:
        state = compute_degrade_state(0.3, AppConfig())
        assert state.summary_mode == "LLM"
        assert state.x_search_enabled is True
        assert state.x_search_max == 20

    def test_template_mode_at_60_percent(self) -> None:
        state = compute_degrade_state(0.65, AppConfig())
        assert state.summary_mode == "TEMPLATE"
        assert state.x_search_enabled is True
        assert state.x_search_max == 20

    def test_reduced_x_search_at_80_percent(self) -> None:
        state = compute_degrade_state(0.85, AppConfig())
        assert state.summary_mode == "TEMPLATE"
        assert state.x_search_enabled is True
        assert state.x_search_max == 5

    def test_all_disabled_at_100_percent(self) -> None:
        state = compute_degrade_state(1.0, AppConfig())
        assert state.summary_mode == "OFF"
        assert state.x_search_enabled is False
        assert state.x_search_max == 0

    def test_over_budget(self) -> None:
        state = compute_degrade_state(1.5, AppConfig())
        assert state.summary_mode == "OFF"
        assert state.x_search_enabled is False

    def test_zero_budget(self) -> None:
        # Should be full mode at 0%
        state = compute_degrade_state(0.0, AppConfig())
        assert state.summary_mode == "LLM"

    def test_custom_thresholds(self) -> None:
        config = AppConfig(template_at_ratio=0.5, x_search_reduce_at_ratio=0.7)
        state = compute_degrade_state(0.55, config)
        assert state.summary_mode == "TEMPLATE"

    def test_degrade_state_to_dict(self) -> None:
        state = compute_degrade_state(0.5, AppConfig())
        d = state.to_dict()
        assert "summaryMode" in d
        assert "xSearchEnabled" in d

    def test_x_search_max_tracks_top_k_in_full_mode(self) -> None:
        state = compute_degrade_state(0.3, AppConfig(top_k=50))
        assert state.x_search_max == 50

    def test_x_search_max_is_capped_at_five_in_reduced_mode(self) -> None:
        state = compute_degrade_state(0.85, AppConfig(top_k=50))
        assert state.x_search_max == 5
