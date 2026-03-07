from __future__ import annotations

from packages.core.evaluation import (
    build_ranked_entries_from_features,
    compare_variant_metrics,
    evaluate_ranked_entries,
)
from packages.core.models import (
    CandidateKind,
    CandidateType,
    DailyCandidateFeature,
    DomainClass,
    HindsightLabel,
    RankingLane,
)


def _feature(
    *,
    date: str,
    candidate_id: str,
    source_families: list[str],
    breakout_prob_7d: float,
    mass_prob: float = 0.0,
    primary_score: float = 3.0,
    maturity: float = 0.2,
    lane: RankingLane = RankingLane.WORDS_BEHAVIORS,
) -> DailyCandidateFeature:
    return DailyCandidateFeature(
        date=date,
        candidate_id=candidate_id,
        display_name=candidate_id,
        candidate_type=CandidateType.HASHTAG,
        candidate_kind=CandidateKind.TOPIC,
        lane=lane,
        domain_class=DomainClass.CONSUMER_CULTURE,
        source_families=source_families,
        breakout_prob_7d=breakout_prob_7d,
        mass_prob=mass_prob,
        primary_score=primary_score,
        mass_heat=maturity,
        novelty=0.7,
    )


def test_evaluate_ranked_entries_computes_breakout_and_future_spread() -> None:
    anchor = _feature(
        date="2026-03-01",
        candidate_id="cand_1",
        source_families=["SOCIAL_DISCOVERY"],
        breakout_prob_7d=0.8,
    )
    future = _feature(
        date="2026-03-08",
        candidate_id="cand_1",
        source_families=["SOCIAL_DISCOVERY", "MUSIC_CHART"],
        breakout_prob_7d=0.9,
    )
    label = HindsightLabel(
        date="2026-03-01",
        candidate_id="cand_1",
        breakout_7d=True,
        mass_7d=True,
        new_confirmation_families=["MUSIC_CHART"],
        lead_days=7,
        available_breakout_horizons=[7],
        available_mass_horizons=[7],
        created_at="2026-03-08T07:00:00+09:00",
    )

    entries = build_ranked_entries_from_features([anchor])
    metrics = evaluate_ranked_entries(
        entries,
        {"cand_1": label},
        {
            "2026-03-01": {"cand_1": anchor},
            "2026-03-08": {"cand_1": future},
        },
        anchor_date="2026-03-01",
        top_k=20,
    )

    assert metrics["breakoutPrecisionAt20_7d"] == 1.0
    assert metrics["futureSpreadAt20_7d"] == 1.0
    assert metrics["leadGainAt20"] == 7.0
    assert metrics["laneCoverageAt20"] == 0.25
    calibration = metrics["calibration"]
    assert isinstance(calibration, dict)
    assert calibration["breakout7dCount"] == 1
    assert calibration["breakout7dBrier"] == 0.04


def test_compare_variant_metrics_returns_shadow_deltas() -> None:
    shadow = {
        "breakoutPrecisionAt20_7d": 0.6,
        "futureSpreadAt20_7d": 1.2,
        "maturePeopleMusicRatioAt20": 0.15,
        "laneCoverageAt20": 1.0,
    }
    public = {
        "breakoutPrecisionAt20_7d": 0.5,
        "futureSpreadAt20_7d": 0.9,
        "maturePeopleMusicRatioAt20": 0.35,
        "laneCoverageAt20": 0.75,
    }

    comparison = compare_variant_metrics(shadow, public, top_k=20)

    assert comparison["breakoutPrecisionDeltaAt20_7d"] == 0.1
    assert comparison["futureSpreadDeltaAt20_7d"] == 0.3
    assert comparison["maturePeopleMusicRatioDeltaAt20"] == -0.2
    assert comparison["laneCoverageDeltaAt20"] == 0.25
