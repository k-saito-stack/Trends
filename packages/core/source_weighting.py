"""Source weighting for TrendScore aggregation.

Weights are computed from source metadata and recent source_daily history.
The current run uses config/source_weights_current when present, while the
weights computed from today's data are stored for the next run.
"""

from __future__ import annotations

import math
from collections import defaultdict
from datetime import date, timedelta
from typing import Any

from packages.core.models import (
    AlgorithmConfig,
    SourceDailySnapshot,
    SourceTopItem,
    SourceWeightSnapshot,
    SourceWeightingConfig,
)

NEUTRAL_WEIGHT = 1.0
EXCLUDED_WEIGHTED_SOURCES = {"WIKI_PAGEVIEWS", "X_SEARCH", "IG_BOOST"}


def filter_weighted_source_ids(source_ids: list[str]) -> list[str]:
    """Filter out sources that do not contribute to TrendScore."""
    return [
        source_id
        for source_id in source_ids
        if source_id and source_id not in EXCLUDED_WEIGHTED_SOURCES
    ]


def compute_prior_weights(
    source_cfgs: dict[str, dict[str, Any]],
    source_ids: list[str],
    algo_cfg: AlgorithmConfig,
    weighting_cfg: SourceWeightingConfig,
    source_daily_records: list[SourceDailySnapshot] | None = None,
) -> dict[str, float]:
    """Compute normalized prior-only weights (R * F * G * S)."""
    _, priors, _, _, _, _ = _compute_components(
        source_cfgs=source_cfgs,
        source_ids=source_ids,
        algo_cfg=algo_cfg,
        weighting_cfg=weighting_cfg,
        source_daily_records=source_daily_records or [],
    )
    return normalize_average_to_one(priors)


def load_source_daily(
    as_of_date: str,
    weighting_cfg: SourceWeightingConfig,
) -> dict[tuple[str, str], SourceDailySnapshot]:
    """Load recent source_daily history needed for weight computation."""
    if not as_of_date:
        return {}

    end_date = date.fromisoformat(as_of_date)
    lookback = max(weighting_cfg.window_days + weighting_cfg.horizon_days - 1, 1)
    start_date = end_date - timedelta(days=lookback - 1)

    firestore_client = _get_firestore_client()
    docs = firestore_client.get_collection("source_daily")
    result: dict[tuple[str, str], SourceDailySnapshot] = {}
    for doc in docs:
        record = SourceDailySnapshot.from_dict(doc)
        if not record.date or not record.source_id:
            continue
        try:
            record_date = date.fromisoformat(record.date)
        except ValueError:
            continue
        if start_date <= record_date <= end_date:
            result[(record.date, record.source_id)] = record
    return result


def compute_C_predictive_f1(
    source_cfgs: dict[str, dict[str, Any]],
    source_ids: list[str],
    algo_cfg: AlgorithmConfig,
    weighting_cfg: SourceWeightingConfig,
    source_daily_records: list[SourceDailySnapshot],
    stability_factors: dict[str, float] | None = None,
) -> dict[str, float]:
    """Compute predictive F1 using leave-one-out future aggregate."""
    (
        records_by_date,
        prior_raw,
        _stability,
        _factors,
        prediction_dates,
        _recent_dates,
    ) = _compute_components(
        source_cfgs=source_cfgs,
        source_ids=source_ids,
        algo_cfg=algo_cfg,
        weighting_cfg=weighting_cfg,
        source_daily_records=source_daily_records,
        stability_override=stability_factors,
    )

    predictive: dict[str, float] = {}
    for source_id in source_ids:
        precisions: list[float] = []
        recalls: list[float] = []

        for anchor_date in prediction_dates:
            anchor_record = records_by_date.get(anchor_date, {}).get(source_id)
            anchor_set = _top_candidate_ids(anchor_record)
            if not anchor_set:
                continue

            future_date = (date.fromisoformat(anchor_date) + timedelta(
                days=weighting_cfg.horizon_days
            )).isoformat()
            future_top_k = _aggregate_future_top_k(
                day_records=records_by_date.get(future_date, {}),
                exclude_source_id=source_id,
                prior_raw=prior_raw,
                top_k=weighting_cfg.top_k_for_future,
            )
            if not future_top_k:
                continue

            overlap = len(anchor_set & future_top_k)
            precisions.append(overlap / len(anchor_set))
            recalls.append(overlap / len(future_top_k))

        if precisions and recalls:
            precision = sum(precisions) / len(precisions)
            recall = sum(recalls) / len(recalls)
            predictive[source_id] = (
                2.0 * precision * recall
            ) / (precision + recall + weighting_cfg.epsilon)
        else:
            predictive[source_id] = NEUTRAL_WEIGHT

    return predictive


def compute_I_independence(
    source_ids: list[str],
    weighting_cfg: SourceWeightingConfig,
    source_daily_records: list[SourceDailySnapshot],
    as_of_date: str,
) -> dict[str, float]:
    """Compute independence factor from average Jaccard overlap."""
    if not as_of_date:
        return {source_id: NEUTRAL_WEIGHT for source_id in source_ids}

    recent_start = (
        date.fromisoformat(as_of_date)
        - timedelta(days=max(weighting_cfg.window_days - 1, 0))
    )
    records_by_date = _index_records_by_date(source_daily_records)
    overlap_values: dict[str, list[float]] = {source_id: [] for source_id in source_ids}

    for day, day_records in records_by_date.items():
        try:
            day_value = date.fromisoformat(day)
        except ValueError:
            continue
        if day_value < recent_start or day_value > date.fromisoformat(as_of_date):
            continue

        source_sets = {
            source_id: _top_candidate_ids(day_records.get(source_id))
            for source_id in source_ids
        }
        for source_id in source_ids:
            if not source_sets[source_id]:
                continue
            jaccards: list[float] = []
            for other_id in source_ids:
                if other_id == source_id or not source_sets[other_id]:
                    continue
                union = source_sets[source_id] | source_sets[other_id]
                if not union:
                    continue
                intersection = source_sets[source_id] & source_sets[other_id]
                jaccards.append(len(intersection) / len(union))
            if jaccards:
                overlap_values[source_id].append(sum(jaccards) / len(jaccards))

    independence: dict[str, float] = {}
    for source_id in source_ids:
        redundancy = (
            sum(overlap_values[source_id]) / len(overlap_values[source_id])
            if overlap_values[source_id]
            else 0.0
        )
        independence[source_id] = max(weighting_cfg.i_min, 1.0 - redundancy)
    return independence


def compute_S_stability(
    source_ids: list[str],
    weighting_cfg: SourceWeightingConfig,
    source_daily_records: list[SourceDailySnapshot],
    as_of_date: str,
) -> dict[str, float]:
    """Compute stability from recent success rate."""
    if not as_of_date:
        return {source_id: NEUTRAL_WEIGHT for source_id in source_ids}

    recent_start = (
        date.fromisoformat(as_of_date)
        - timedelta(days=max(weighting_cfg.window_days - 1, 0))
    )
    records_by_source = _index_records_by_source(source_daily_records)
    stability: dict[str, float] = {}
    for source_id in source_ids:
        relevant = [
            record
            for record in records_by_source.get(source_id, [])
            if _within_date_range(record.date, recent_start, date.fromisoformat(as_of_date))
        ]
        if not relevant:
            stability[source_id] = NEUTRAL_WEIGHT
            continue
        fail_rate = sum(1 for record in relevant if not record.ok) / len(relevant)
        stability[source_id] = max(weighting_cfg.s_min, 1.0 - fail_rate)
    return stability


def combine_and_normalize(
    priors: dict[str, float],
    predictive: dict[str, float],
    independence: dict[str, float],
) -> dict[str, float]:
    """Combine factors and normalize to mean 1."""
    raw_weights = {
        source_id: priors.get(source_id, 1.0)
        * predictive.get(source_id, 1.0)
        * independence.get(source_id, 1.0)
        for source_id in priors
    }
    return normalize_average_to_one(raw_weights)


def normalize_average_to_one(raw_weights: dict[str, float]) -> dict[str, float]:
    """Normalize weights so the mean becomes exactly 1."""
    if not raw_weights:
        return {}
    total = sum(raw_weights.values())
    source_count = len(raw_weights)
    if total <= 0:
        return {source_id: 1.0 for source_id in raw_weights}
    return {
        source_id: source_count * weight / total
        for source_id, weight in raw_weights.items()
    }


def build_source_daily_snapshots(
    target_date: str,
    generated_at: str,
    source_ids: list[str],
    source_ok: dict[str, bool],
    source_item_count: dict[str, int],
    source_momentum: dict[str, list[tuple[str, float]]],
    source_cfgs: dict[str, dict[str, Any]],
    weighting_cfg: SourceWeightingConfig,
) -> list[SourceDailySnapshot]:
    """Build daily source snapshots from per-source candidate momentum."""
    snapshots: list[SourceDailySnapshot] = []
    for source_id in source_ids:
        cfg = source_cfgs.get(source_id, {})
        top_m_limit = infer_top_m_for_stats(cfg, weighting_cfg)
        top_items = sorted(
            (
                SourceTopItem(candidate_id=candidate_id, momentum=momentum)
                for candidate_id, momentum in source_momentum.get(source_id, [])
                if momentum > 0
            ),
            key=lambda item: -item.momentum,
        )[:top_m_limit]
        snapshots.append(
            SourceDailySnapshot(
                date=target_date,
                source_id=source_id,
                ok=source_ok.get(source_id, False),
                item_count=source_item_count.get(source_id, 0),
                top_m=top_items,
                generated_at=generated_at,
            )
        )
    return snapshots


def compute_weight_snapshot(
    target_date: str,
    generated_at: str,
    source_cfgs: dict[str, dict[str, Any]],
    source_ids: list[str],
    algo_cfg: AlgorithmConfig,
    weighting_cfg: SourceWeightingConfig,
    source_daily_records: list[SourceDailySnapshot],
) -> SourceWeightSnapshot:
    """Compute the full source weight snapshot for a given day."""
    (
        _records_by_date,
        priors,
        stability,
        factor_base,
        _prediction_dates,
        _recent_dates,
    ) = _compute_components(
        source_cfgs=source_cfgs,
        source_ids=source_ids,
        algo_cfg=algo_cfg,
        weighting_cfg=weighting_cfg,
        source_daily_records=source_daily_records,
    )
    predictive = compute_C_predictive_f1(
        source_cfgs=source_cfgs,
        source_ids=source_ids,
        algo_cfg=algo_cfg,
        weighting_cfg=weighting_cfg,
        source_daily_records=source_daily_records,
        stability_factors=stability,
    )
    independence = compute_I_independence(
        source_ids=source_ids,
        weighting_cfg=weighting_cfg,
        source_daily_records=source_daily_records,
        as_of_date=target_date,
    )
    weights = combine_and_normalize(priors, predictive, independence)

    factors: dict[str, dict[str, Any]] = {}
    for source_id in source_ids:
        source_factors = dict(factor_base[source_id])
        source_factors["C"] = predictive.get(source_id, 1.0)
        source_factors["I"] = independence.get(source_id, 1.0)
        source_factors["S"] = stability.get(source_id, 1.0)
        source_factors["prior"] = priors.get(source_id, 1.0)
        source_factors["weight"] = weights.get(source_id, 1.0)
        factors[source_id] = source_factors

    return SourceWeightSnapshot(
        date=target_date,
        generated_at=generated_at,
        window_days=weighting_cfg.window_days,
        horizon_days=weighting_cfg.horizon_days,
        half_life_days=algo_cfg.half_life_days,
        n_ref=weighting_cfg.n_ref,
        weights=weights,
        factors=factors,
    )


def load_current_source_weights(
    target_date: str,
    source_cfgs: dict[str, dict[str, Any]],
    source_ids: list[str],
    algo_cfg: AlgorithmConfig,
    weighting_cfg: SourceWeightingConfig,
) -> dict[str, float]:
    """Load next-run weights from config/source_weights_current or fall back."""
    weighted_source_ids = filter_weighted_source_ids(source_ids)
    if not weighting_cfg.enabled or not weighted_source_ids:
        return {}

    prev_date = (date.fromisoformat(target_date) - timedelta(days=1)).isoformat()
    history = list(load_source_daily(prev_date, weighting_cfg).values())
    prior_weights = compute_prior_weights(
        source_cfgs=source_cfgs,
        source_ids=weighted_source_ids,
        algo_cfg=algo_cfg,
        weighting_cfg=weighting_cfg,
        source_daily_records=history,
    )

    firestore_client = _get_firestore_client()
    pointer_doc = firestore_client.get_document("config", "source_weights_current")
    if pointer_doc:
        stored_weights = {
            str(source_id): float(weight)
            for source_id, weight in pointer_doc.get("weights", {}).items()
        }
        merged = {
            source_id: stored_weights.get(source_id, prior_weights.get(source_id, 1.0))
            for source_id in weighted_source_ids
        }
        return normalize_average_to_one(merged)

    if history:
        snapshot = compute_weight_snapshot(
            target_date=prev_date,
            generated_at="",
            source_cfgs=source_cfgs,
            source_ids=weighted_source_ids,
            algo_cfg=algo_cfg,
            weighting_cfg=weighting_cfg,
            source_daily_records=history,
        )
        return snapshot.weights

    return prior_weights


def infer_region_weight(source_cfg: dict[str, Any], source_id: str) -> float:
    """Infer the regional prior weight."""
    value = source_cfg.get("regionWeightR")
    if value is not None:
        return float(value)
    if source_id == "APPLE_MUSIC_GLOBAL":
        return 0.25
    return 1.0


def infer_avg_lag_days(
    source_cfg: dict[str, Any],
    source_id: str,
) -> float:
    """Infer delta(days) for freshness factor."""
    value = source_cfg.get("avgLagDaysDelta")
    if value is not None:
        return float(value)

    if source_id.startswith("NETFLIX_"):
        return 3.5
    if source_id == "YOUTUBE_TREND_JP":
        return 0.25
    return 0.5


def infer_granularity_n(
    source_cfg: dict[str, Any],
    weighting_cfg: SourceWeightingConfig,
) -> int:
    """Infer N for granularity factor."""
    value = source_cfg.get("granularityN")
    if value is not None:
        return max(int(value), 1)

    fetch_limit = source_cfg.get("fetchLimit")
    if fetch_limit:
        return max(int(fetch_limit), 1)

    return max(weighting_cfg.n_ref, 1)


def infer_top_m_for_stats(
    source_cfg: dict[str, Any],
    weighting_cfg: SourceWeightingConfig,
) -> int:
    """Infer topM size for source_daily and stats calculations."""
    value = source_cfg.get("topMForStats")
    if value is not None:
        return max(int(value), 1)
    return min(weighting_cfg.top_m_default, infer_granularity_n(source_cfg, weighting_cfg))


def _compute_components(
    source_cfgs: dict[str, dict[str, Any]],
    source_ids: list[str],
    algo_cfg: AlgorithmConfig,
    weighting_cfg: SourceWeightingConfig,
    source_daily_records: list[SourceDailySnapshot],
    stability_override: dict[str, float] | None = None,
) -> tuple[
    dict[str, dict[str, SourceDailySnapshot]],
    dict[str, float],
    dict[str, float],
    dict[str, dict[str, Any]],
    list[str],
    list[str],
]:
    weighted_source_ids = filter_weighted_source_ids(source_ids)
    records_by_date = _index_records_by_date(source_daily_records)
    all_dates = sorted(records_by_date)
    as_of_date = max(all_dates) if all_dates else ""

    stability = (
        stability_override
        if stability_override is not None
        else compute_S_stability(
            source_ids=weighted_source_ids,
            weighting_cfg=weighting_cfg,
            source_daily_records=source_daily_records,
            as_of_date=as_of_date,
        )
    )

    priors: dict[str, float] = {}
    factor_base: dict[str, dict[str, Any]] = {}
    for source_id in weighted_source_ids:
        source_cfg = source_cfgs.get(source_id, {})
        region = infer_region_weight(source_cfg, source_id)
        lag_days = infer_avg_lag_days(source_cfg, source_id)
        freshness = math.pow(2.0, -lag_days / algo_cfg.half_life_days)
        granularity_n = infer_granularity_n(source_cfg, weighting_cfg)
        granularity = min(
            1.0,
            math.log1p(granularity_n) / math.log1p(max(weighting_cfg.n_ref, 1)),
        )
        prior = region * freshness * granularity * stability.get(source_id, 1.0)
        priors[source_id] = prior
        factor_base[source_id] = {
            "R": region,
            "F": freshness,
            "G": granularity,
            "lagDays": lag_days,
            "granularityN": granularity_n,
        }

    prediction_dates: list[str] = []
    recent_dates: list[str] = []
    if as_of_date:
        as_of = date.fromisoformat(as_of_date)
        recent_start = as_of - timedelta(days=max(weighting_cfg.window_days - 1, 0))
        prediction_start = as_of - timedelta(
            days=max(weighting_cfg.window_days + weighting_cfg.horizon_days - 1, 0)
        )
        prediction_end = as_of - timedelta(days=weighting_cfg.horizon_days)

        for day in all_dates:
            day_value = date.fromisoformat(day)
            if recent_start <= day_value <= as_of:
                recent_dates.append(day)
            if prediction_start <= day_value <= prediction_end:
                prediction_dates.append(day)

    return records_by_date, priors, stability, factor_base, prediction_dates, recent_dates


def _aggregate_future_top_k(
    day_records: dict[str, SourceDailySnapshot],
    exclude_source_id: str,
    prior_raw: dict[str, float],
    top_k: int,
) -> set[str]:
    scores: dict[str, float] = defaultdict(float)
    for source_id, record in day_records.items():
        if source_id == exclude_source_id or not record.ok:
            continue
        weight = prior_raw.get(source_id, 1.0)
        for top_item in record.top_m:
            scores[top_item.candidate_id] += weight * top_item.momentum

    ranked_ids = sorted(scores, key=lambda candidate_id: -scores[candidate_id])[:top_k]
    return set(ranked_ids)


def _top_candidate_ids(record: SourceDailySnapshot | None) -> set[str]:
    if record is None or not record.ok:
        return set()
    return {item.candidate_id for item in record.top_m if item.candidate_id}


def _index_records_by_date(
    source_daily_records: list[SourceDailySnapshot],
) -> dict[str, dict[str, SourceDailySnapshot]]:
    records_by_date: dict[str, dict[str, SourceDailySnapshot]] = defaultdict(dict)
    for record in source_daily_records:
        if record.date and record.source_id:
            records_by_date[record.date][record.source_id] = record
    return dict(records_by_date)


def _index_records_by_source(
    source_daily_records: list[SourceDailySnapshot],
) -> dict[str, list[SourceDailySnapshot]]:
    records_by_source: dict[str, list[SourceDailySnapshot]] = defaultdict(list)
    for record in source_daily_records:
        if record.source_id:
            records_by_source[record.source_id].append(record)
    return dict(records_by_source)


def _within_date_range(value: str, start: date, end: date) -> bool:
    if not value:
        return False
    try:
        parsed = date.fromisoformat(value)
    except ValueError:
        return False
    return start <= parsed <= end


def _get_firestore_client() -> Any:
    from packages.core import firestore_client

    return firestore_client
