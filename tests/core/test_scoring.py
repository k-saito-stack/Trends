"""Tests for scoring engine (EWMA / sig / momentum)."""

from __future__ import annotations

import math

from packages.core.models import AlgorithmConfig, SourceState
from packages.core.scoring import (
    alpha_from_half_life,
    ewma_update,
    ewmvar_update,
    momentum,
    multi_source_bonus,
    rank_exposure,
    sig_beta,
    update_source_state,
)


class TestAlphaFromHalfLife:
    def test_half_life_7(self) -> None:
        alpha = alpha_from_half_life(7.0)
        # alpha = 1 - exp(log(0.5)/7) ≈ 0.0943
        assert abs(alpha - 0.0943) < 0.001

    def test_half_life_0_returns_1(self) -> None:
        assert alpha_from_half_life(0) == 1.0

    def test_half_life_negative_returns_1(self) -> None:
        assert alpha_from_half_life(-5) == 1.0


class TestEwmaUpdate:
    def test_basic(self) -> None:
        # m_t = (1-0.1)*5.0 + 0.1*10.0 = 4.5 + 1.0 = 5.5
        result = ewma_update(5.0, 10.0, 0.1)
        assert abs(result - 5.5) < 1e-10

    def test_alpha_1_equals_latest(self) -> None:
        # alpha=1 -> m_t = x_t
        result = ewma_update(100.0, 42.0, 1.0)
        assert abs(result - 42.0) < 1e-10


class TestEwmvarUpdate:
    def test_basic(self) -> None:
        # v_t = (1-0.1)*(1.0 + 0.1*(10.0-5.0)^2) = 0.9*(1.0 + 2.5) = 3.15
        result = ewmvar_update(1.0, 10.0, 5.0, 0.1)
        assert abs(result - 3.15) < 1e-10


class TestSigBeta:
    def test_above_baseline(self) -> None:
        # sig = (10 - max(5, 0.1)) / (sqrt(1) + 0.1) = 5.0 / 1.1 ≈ 4.545
        result = sig_beta(10.0, 5.0, 1.0, 0.1)
        assert abs(result - 5.0 / 1.1) < 0.01

    def test_below_baseline(self) -> None:
        # x < m -> negative sig
        result = sig_beta(1.0, 5.0, 1.0, 0.1)
        assert result < 0

    def test_beta_floor(self) -> None:
        # When m < beta, use beta as floor
        result = sig_beta(5.0, 0.01, 0.01, 0.1)
        # sig = (5 - 0.1) / (sqrt(0.01) + 0.1) = 4.9 / 0.2 = 24.5
        assert abs(result - 24.5) < 0.1


class TestMomentum:
    def test_single_day(self) -> None:
        result = momentum([3.0])
        assert abs(result - 3.0) < 1e-10

    def test_three_days(self) -> None:
        # mom = max(0,3) + 0.7*max(0,2) + 0.49*max(0,1) = 3+1.4+0.49 = 4.89
        result = momentum([3.0, 2.0, 1.0], lam=0.7)
        assert abs(result - 4.89) < 0.01

    def test_negative_clamped_to_zero(self) -> None:
        # Negative sig values don't contribute
        result = momentum([-5.0, -3.0, 2.0], lam=0.7)
        # Only s2 contributes: 0 + 0 + 0.49*2 = 0.98
        assert abs(result - 0.98) < 0.01

    def test_empty_history(self) -> None:
        assert momentum([]) == 0.0


class TestMultiSourceBonus:
    def test_single_source_no_bonus(self) -> None:
        assert multi_source_bonus(1) == 0.0

    def test_two_sources(self) -> None:
        assert multi_source_bonus(2) == 1.0

    def test_cap_at_3(self) -> None:
        # clamp(5-1, 0, 3) = 3
        assert multi_source_bonus(5) == 3.0

    def test_custom_weight(self) -> None:
        assert multi_source_bonus(3, multi_weight=2.0) == 4.0


class TestUpdateSourceState:
    def test_warmup_returns_zero_sig(self) -> None:
        state = SourceState()
        config = AlgorithmConfig(warmup_days=3)

        new_state, sig = update_source_state(state, 5.0, config, "2025-01-01")
        assert sig == 0.0
        assert new_state.observation_count == 1
        assert new_state.last_sig == 0.0

    def test_missing_data_skips_update(self) -> None:
        state = SourceState(m=5.0, v=1.0, observation_count=5)
        config = AlgorithmConfig()

        new_state, sig = update_source_state(state, None, config, "2025-01-01")
        assert sig == 0.0
        assert new_state.m == 5.0  # unchanged
        assert new_state.v == 1.0  # unchanged

    def test_after_warmup_produces_sig(self) -> None:
        state = SourceState(m=2.0, v=0.5, observation_count=3)
        config = AlgorithmConfig(warmup_days=3)

        new_state, sig = update_source_state(state, 10.0, config, "2025-01-04")
        assert new_state.observation_count == 4
        assert sig != 0.0
        assert new_state.last_sig == sig

    def test_clips_extreme_values(self) -> None:
        state = SourceState(m=1.0, v=0.1, observation_count=5)
        config = AlgorithmConfig(max_x_clip=50.0)

        # x=1000 should be clipped to 50
        new_state1, _ = update_source_state(state, 1000.0, config, "2025-01-01")
        new_state2, _ = update_source_state(state, 50.0, config, "2025-01-01")
        assert abs(new_state1.m - new_state2.m) < 1e-10


class TestRankExposure:
    def test_rank_1(self) -> None:
        assert abs(rank_exposure(1) - 1.0) < 1e-10

    def test_rank_2(self) -> None:
        expected = 1.0 / math.log2(3)
        assert abs(rank_exposure(2) - expected) < 1e-10

    def test_rank_10(self) -> None:
        expected = 1.0 / math.log2(11)
        assert abs(rank_exposure(10) - expected) < 1e-10
