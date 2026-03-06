"""Tests for source weighting."""

from __future__ import annotations

from packages.core.models import (
    AlgorithmConfig,
    SourceDailySnapshot,
    SourceTopItem,
    SourceWeightingConfig,
)
from packages.core import source_weighting
from packages.core.source_weighting import (
    compute_C_predictive_f1,
    compute_I_independence,
    compute_S_stability,
    compute_prior_weights,
    load_current_source_weights,
)


class TestComputePriorWeights:
    def test_prior_order_matches_freshness_and_resolution(self) -> None:
        source_cfgs = {
            "YOUTUBE_TREND_JP": {
                "regionWeightR": 1.0,
                "avgLagDaysDelta": 0.25,
                "granularityN": 50,
                "topMForStats": 20,
            },
            "TRENDS": {
                "regionWeightR": 1.0,
                "avgLagDaysDelta": 0.5,
                "granularityN": 20,
                "topMForStats": 20,
            },
            "NETFLIX_TV_JP": {
                "regionWeightR": 1.0,
                "avgLagDaysDelta": 3.5,
                "granularityN": 10,
                "topMForStats": 10,
            },
            "APPLE_MUSIC_GLOBAL": {
                "regionWeightR": 0.25,
                "avgLagDaysDelta": 0.5,
                "granularityN": 50,
                "topMForStats": 20,
            },
        }
        weights = compute_prior_weights(
            source_cfgs=source_cfgs,
            source_ids=list(source_cfgs),
            algo_cfg=AlgorithmConfig(half_life_days=7.0),
            weighting_cfg=SourceWeightingConfig(n_ref=50),
        )

        assert weights["YOUTUBE_TREND_JP"] > weights["TRENDS"]
        assert weights["TRENDS"] > weights["NETFLIX_TV_JP"]
        assert weights["NETFLIX_TV_JP"] > weights["APPLE_MUSIC_GLOBAL"]

    def test_normalized_average_is_one(self) -> None:
        source_cfgs = {
            "A": {"regionWeightR": 1.0, "avgLagDaysDelta": 0.25, "granularityN": 50},
            "B": {"regionWeightR": 1.0, "avgLagDaysDelta": 0.5, "granularityN": 20},
            "C": {"regionWeightR": 1.0, "avgLagDaysDelta": 3.5, "granularityN": 10},
        }

        weights = compute_prior_weights(
            source_cfgs=source_cfgs,
            source_ids=list(source_cfgs),
            algo_cfg=AlgorithmConfig(),
            weighting_cfg=SourceWeightingConfig(),
        )

        assert abs(sum(weights.values()) - len(weights)) < 1e-9


class TestIndependenceAndPredictive:
    def test_independence_penalizes_redundant_sources(self) -> None:
        weighting_cfg = SourceWeightingConfig(window_days=2, i_min=0.2)
        records = [
            SourceDailySnapshot(
                date="2026-03-01",
                source_id="A",
                ok=True,
                item_count=2,
                top_m=[
                    SourceTopItem(candidate_id="x", momentum=3.0),
                    SourceTopItem(candidate_id="y", momentum=2.0),
                ],
            ),
            SourceDailySnapshot(
                date="2026-03-01",
                source_id="B",
                ok=True,
                item_count=2,
                top_m=[
                    SourceTopItem(candidate_id="x", momentum=3.0),
                    SourceTopItem(candidate_id="y", momentum=2.0),
                ],
            ),
            SourceDailySnapshot(
                date="2026-03-01",
                source_id="C",
                ok=True,
                item_count=2,
                top_m=[
                    SourceTopItem(candidate_id="z", momentum=3.0),
                    SourceTopItem(candidate_id="w", momentum=2.0),
                ],
            ),
            SourceDailySnapshot(
                date="2026-03-02",
                source_id="A",
                ok=True,
                item_count=2,
                top_m=[SourceTopItem(candidate_id="x", momentum=3.0)],
            ),
            SourceDailySnapshot(
                date="2026-03-02",
                source_id="B",
                ok=True,
                item_count=2,
                top_m=[SourceTopItem(candidate_id="x", momentum=3.0)],
            ),
            SourceDailySnapshot(
                date="2026-03-02",
                source_id="C",
                ok=True,
                item_count=2,
                top_m=[SourceTopItem(candidate_id="z", momentum=3.0)],
            ),
        ]

        independence = compute_I_independence(
            source_ids=["A", "B", "C"],
            weighting_cfg=weighting_cfg,
            source_daily_records=records,
            as_of_date="2026-03-02",
        )

        assert independence["A"] < independence["C"]
        assert independence["B"] < independence["C"]

    def test_predictive_f1_rewards_future_hit_sources(self) -> None:
        source_cfgs = {
            "A": {"regionWeightR": 1.0, "avgLagDaysDelta": 0.5, "granularityN": 20},
            "B": {"regionWeightR": 1.0, "avgLagDaysDelta": 0.5, "granularityN": 20},
            "C": {"regionWeightR": 1.0, "avgLagDaysDelta": 0.5, "granularityN": 20},
        }
        weighting_cfg = SourceWeightingConfig(window_days=1, horizon_days=1, top_k_for_future=1)
        records = [
            SourceDailySnapshot(
                date="2026-03-01",
                source_id="A",
                ok=True,
                item_count=1,
                top_m=[SourceTopItem(candidate_id="x", momentum=3.0)],
            ),
            SourceDailySnapshot(
                date="2026-03-01",
                source_id="B",
                ok=True,
                item_count=1,
                top_m=[SourceTopItem(candidate_id="y", momentum=3.0)],
            ),
            SourceDailySnapshot(
                date="2026-03-01",
                source_id="C",
                ok=True,
                item_count=1,
                top_m=[SourceTopItem(candidate_id="z", momentum=3.0)],
            ),
            SourceDailySnapshot(
                date="2026-03-02",
                source_id="A",
                ok=True,
                item_count=1,
                top_m=[SourceTopItem(candidate_id="x", momentum=3.0)],
            ),
            SourceDailySnapshot(
                date="2026-03-02",
                source_id="B",
                ok=True,
                item_count=1,
                top_m=[SourceTopItem(candidate_id="x", momentum=3.0)],
            ),
            SourceDailySnapshot(
                date="2026-03-02",
                source_id="C",
                ok=True,
                item_count=1,
                top_m=[SourceTopItem(candidate_id="x", momentum=3.0)],
            ),
        ]

        predictive = compute_C_predictive_f1(
            source_cfgs=source_cfgs,
            source_ids=["A", "B", "C"],
            algo_cfg=AlgorithmConfig(),
            weighting_cfg=weighting_cfg,
            source_daily_records=records,
        )

        assert predictive["A"] > predictive["B"]
        assert predictive["A"] > 0
        assert predictive["B"] == 0.0


class TestStabilityAndFallback:
    def test_stability_tracks_fail_rate(self) -> None:
        weighting_cfg = SourceWeightingConfig(window_days=2, s_min=0.5)
        records = [
            SourceDailySnapshot(date="2026-03-01", source_id="A", ok=True, item_count=1),
            SourceDailySnapshot(date="2026-03-02", source_id="A", ok=True, item_count=1),
            SourceDailySnapshot(date="2026-03-01", source_id="B", ok=False, item_count=0),
            SourceDailySnapshot(date="2026-03-02", source_id="B", ok=True, item_count=1),
        ]

        stability = compute_S_stability(
            source_ids=["A", "B"],
            weighting_cfg=weighting_cfg,
            source_daily_records=records,
            as_of_date="2026-03-02",
        )

        assert stability["A"] == 1.0
        assert stability["B"] == 0.5

    def test_missing_source_weights_current_falls_back(self, monkeypatch) -> None:
        source_cfgs = {
            "YOUTUBE_TREND_JP": {
                "regionWeightR": 1.0,
                "avgLagDaysDelta": 0.25,
                "granularityN": 50,
            },
            "TRENDS": {
                "regionWeightR": 1.0,
                "avgLagDaysDelta": 0.5,
                "granularityN": 20,
            },
        }

        class FakeFirestoreClient:
            @staticmethod
            def get_document(*args, **kwargs):
                return None

            @staticmethod
            def get_collection(*args, **kwargs):
                return []

        monkeypatch.setattr(
            source_weighting,
            "_get_firestore_client",
            lambda: FakeFirestoreClient,
        )

        weights = load_current_source_weights(
            target_date="2026-03-03",
            source_cfgs=source_cfgs,
            source_ids=["YOUTUBE_TREND_JP", "TRENDS"],
            algo_cfg=AlgorithmConfig(),
            weighting_cfg=SourceWeightingConfig(),
        )

        assert weights["YOUTUBE_TREND_JP"] > weights["TRENDS"]
