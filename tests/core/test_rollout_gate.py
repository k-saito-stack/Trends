from __future__ import annotations

from packages.core.models import RankingEvaluation
from packages.core.rollout_gate import evaluate_shadow_rollout


def _report(
    date: str,
    variant: str,
    *,
    breakout_precision: float,
    future_spread: float,
    mature_ratio: float,
    write_ops: int = 3200,
    healthy_core_sources: int = 6,
    discovery_families: int = 2,
    confirmation_families: int = 2,
    words_behaviors: int = 3,
) -> RankingEvaluation:
    return RankingEvaluation(
        date=date,
        variant=variant,
        top_k=20,
        breakout_horizon_days=7,
        metrics={
            "breakoutPrecisionAt20_7d": breakout_precision,
            "futureSpreadAt20_7d": future_spread,
            "maturePeopleMusicRatioAt20": mature_ratio,
        },
        publish_health={
            "metrics": {
                "healthyCoreSourceCount": healthy_core_sources,
                "discoveryFamilyCount": discovery_families,
                "confirmationFamilyCount": confirmation_families,
                "wordsBehaviorsTop20Count": words_behaviors,
            }
        },
        metadata={"writeOpsEstimate": write_ops},
    )


def test_evaluate_shadow_rollout_marks_ready_when_window_beats_public() -> None:
    reports = []
    for day in range(1, 15):
        date = f"2026-03-{day:02d}"
        reports.append(
            _report(
                date,
                "shadow_v2",
                breakout_precision=0.65,
                future_spread=1.1,
                mature_ratio=0.2,
            )
        )
        reports.append(
            _report(
                date,
                "public_main",
                breakout_precision=0.55,
                future_spread=0.9,
                mature_ratio=0.35,
            )
        )

    summary = evaluate_shadow_rollout(reports, window_days=14, top_k=20)

    assert summary["ready"] is True
    assert summary["reasons"] == []
    assert summary["metrics"]["avgShadowBreakoutPrecisionAt20_7d"] == 0.65
    assert summary["metrics"]["avgPublicMaturePeopleMusicRatioAt20"] == 0.35


def test_evaluate_shadow_rollout_requires_enough_history_and_public_baseline() -> None:
    reports = [
        _report(
            "2026-03-01",
            "shadow_v2",
            breakout_precision=0.55,
            future_spread=0.8,
            mature_ratio=0.4,
            healthy_core_sources=5,
        )
    ]

    summary = evaluate_shadow_rollout(reports, window_days=14, top_k=20)

    assert summary["ready"] is False
    assert "insufficient_shadow_history:1/14" in summary["reasons"]
    assert "missing_public_baseline:0/1" in summary["reasons"]
    assert "healthy_core_sources<6:5" in summary["reasons"]


def test_evaluate_shadow_rollout_fails_when_write_ops_are_too_high() -> None:
    reports = []
    for day in range(1, 15):
        date = f"2026-03-{day:02d}"
        reports.append(
            _report(
                date,
                "shadow_v2",
                breakout_precision=0.7,
                future_spread=1.1,
                mature_ratio=0.2,
                write_ops=6200,
            )
        )
        reports.append(
            _report(
                date,
                "public_main",
                breakout_precision=0.5,
                future_spread=0.9,
                mature_ratio=0.35,
                write_ops=2800,
            )
        )

    summary = evaluate_shadow_rollout(reports, window_days=14, top_k=20)

    assert summary["ready"] is False
    assert "write_ops>5000:6200" in summary["reasons"]
    assert summary["metrics"]["maxWriteOpsEstimate"] == 6200
