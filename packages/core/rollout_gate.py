"""Rolling rollout readiness checks for shadow ranking promotion."""

from __future__ import annotations

from collections import defaultdict
from typing import Any

from packages.core.models import RankingEvaluation

PUBLIC_CORE_HEALTHY_MIN = 6
PUBLIC_DISCOVERY_FAMILY_MIN = 2
PUBLIC_CONFIRMATION_FAMILY_MIN = 2
PUBLIC_WORDS_BEHAVIORS_MIN = 3
MAX_ACCEPTABLE_WRITE_OPS_PER_RUN = 5000


def evaluate_shadow_rollout(
    evaluations: list[RankingEvaluation],
    *,
    window_days: int = 14,
    top_k: int = 20,
) -> dict[str, Any]:
    by_date_variant: dict[str, dict[str, RankingEvaluation]] = defaultdict(dict)
    for evaluation in evaluations:
        if evaluation.date and evaluation.variant:
            by_date_variant[evaluation.date][evaluation.variant] = evaluation

    shadow_dates = sorted(
        date_key
        for date_key, variants in by_date_variant.items()
        if "shadow_v2" in variants
    )
    window_dates = shadow_dates[-window_days:]

    reasons: list[str] = []
    if len(window_dates) < window_days:
        reasons.append(f"insufficient_shadow_history:{len(window_dates)}/{window_days}")

    shadow_reports = [by_date_variant[date_key]["shadow_v2"] for date_key in window_dates]
    public_reports = [
        by_date_variant[date_key]["public_main"]
        for date_key in window_dates
        if "public_main" in by_date_variant[date_key]
    ]

    if window_dates and len(public_reports) < len(window_dates):
        reasons.append(f"missing_public_baseline:{len(public_reports)}/{len(window_dates)}")

    healthy_core_counts = [
        _publish_metric(report, "healthyCoreSourceCount") for report in shadow_reports
    ]
    discovery_family_counts = [
        _publish_metric(report, "discoveryFamilyCount") for report in shadow_reports
    ]
    confirmation_family_counts = [
        _publish_metric(report, "confirmationFamilyCount") for report in shadow_reports
    ]
    words_behaviors_counts = [
        _publish_metric(report, "wordsBehaviorsTop20Count") for report in shadow_reports
    ]
    write_ops_estimates = [
        _metadata_metric(report, "writeOpsEstimate") for report in shadow_reports
    ]

    if shadow_reports and min(healthy_core_counts) < PUBLIC_CORE_HEALTHY_MIN:
        reasons.append(
            f"healthy_core_sources<{PUBLIC_CORE_HEALTHY_MIN}:{min(healthy_core_counts)}"
        )
    if shadow_reports and min(discovery_family_counts) < PUBLIC_DISCOVERY_FAMILY_MIN:
        reasons.append(
            f"discovery_families<{PUBLIC_DISCOVERY_FAMILY_MIN}:{min(discovery_family_counts)}"
        )
    if shadow_reports and min(confirmation_family_counts) < PUBLIC_CONFIRMATION_FAMILY_MIN:
        reasons.append(
            "confirmation_families"
            f"<{PUBLIC_CONFIRMATION_FAMILY_MIN}:{min(confirmation_family_counts)}"
        )
    if shadow_reports and min(words_behaviors_counts) < PUBLIC_WORDS_BEHAVIORS_MIN:
        reasons.append(
            f"words_behaviors_top{top_k}<{PUBLIC_WORDS_BEHAVIORS_MIN}:{min(words_behaviors_counts)}"
        )
    if shadow_reports and max(write_ops_estimates) > MAX_ACCEPTABLE_WRITE_OPS_PER_RUN:
        reasons.append(
            f"write_ops>{MAX_ACCEPTABLE_WRITE_OPS_PER_RUN}:{max(write_ops_estimates)}"
        )

    avg_shadow_breakout = _average_metric(
        shadow_reports, f"breakoutPrecisionAt{top_k}_7d"
    )
    avg_public_breakout = _average_metric(
        public_reports, f"breakoutPrecisionAt{top_k}_7d"
    )
    avg_shadow_future_spread = _average_metric(
        shadow_reports, f"futureSpreadAt{top_k}_7d"
    )
    avg_public_future_spread = _average_metric(
        public_reports, f"futureSpreadAt{top_k}_7d"
    )
    avg_shadow_mature_ratio = _average_metric(
        shadow_reports, f"maturePeopleMusicRatioAt{top_k}"
    )
    avg_public_mature_ratio = _average_metric(
        public_reports, f"maturePeopleMusicRatioAt{top_k}"
    )

    if public_reports and avg_shadow_breakout < avg_public_breakout:
        reasons.append(
            "shadow_breakout_precision<"
            f"{round(avg_public_breakout, 4)}:{round(avg_shadow_breakout, 4)}"
        )
    if public_reports and avg_shadow_mature_ratio > avg_public_mature_ratio:
        reasons.append(
            "shadow_mature_ratio>"
            f"{round(avg_public_mature_ratio, 4)}:{round(avg_shadow_mature_ratio, 4)}"
        )

    ready = not reasons
    return {
        "windowDays": window_days,
        "datesUsed": window_dates,
        "ready": ready,
        "reasons": reasons,
        "metrics": {
            "windowSize": len(window_dates),
            "avgShadowBreakoutPrecisionAt20_7d": round(avg_shadow_breakout, 4),
            "avgPublicBreakoutPrecisionAt20_7d": round(avg_public_breakout, 4),
            "avgShadowFutureSpreadAt20_7d": round(avg_shadow_future_spread, 4),
            "avgPublicFutureSpreadAt20_7d": round(avg_public_future_spread, 4),
            "avgShadowMaturePeopleMusicRatioAt20": round(avg_shadow_mature_ratio, 4),
            "avgPublicMaturePeopleMusicRatioAt20": round(avg_public_mature_ratio, 4),
            "minHealthyCoreSourceCount": min(healthy_core_counts) if healthy_core_counts else 0,
            "minDiscoveryFamilyCount": min(discovery_family_counts)
            if discovery_family_counts
            else 0,
            "minConfirmationFamilyCount": min(confirmation_family_counts)
            if confirmation_family_counts
            else 0,
            "minWordsBehaviorsTop20Count": min(words_behaviors_counts)
            if words_behaviors_counts
            else 0,
            "avgWriteOpsEstimate": round(_average_values(write_ops_estimates), 2),
            "maxWriteOpsEstimate": max(write_ops_estimates) if write_ops_estimates else 0,
            "writeVolumeThreshold": MAX_ACCEPTABLE_WRITE_OPS_PER_RUN,
        },
    }


def _publish_metric(report: RankingEvaluation, key: str) -> int:
    publish_metrics = report.publish_health.get("metrics", {})
    value = publish_metrics.get(key, 0)
    return int(value) if isinstance(value, (int, float)) else 0


def _average_metric(reports: list[RankingEvaluation], key: str) -> float:
    values = []
    for report in reports:
        value = report.metrics.get(key, 0.0)
        if isinstance(value, (int, float)):
            values.append(float(value))
    if not values:
        return 0.0
    return sum(values) / len(values)


def _metadata_metric(report: RankingEvaluation, key: str) -> int:
    value = report.metadata.get(key, 0)
    return int(value) if isinstance(value, (int, float)) else 0


def _average_values(values: list[int]) -> float:
    if not values:
        return 0.0
    return sum(values) / len(values)
