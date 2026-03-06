"""Ranking: aggregate scores and select Top-K candidates.

Combines per-source momentum scores into TrendScore,
applies multi-source bonus, and selects top candidates.

Spec reference: Section 10.7 (Integrated TrendScore)
"""

from __future__ import annotations

from typing import Any

from packages.core.models import (
    AlgorithmConfig,
    BucketScore,
    DisplayBucket,
    MusicConfig,
)
from packages.core.scoring import momentum, multi_source_bonus

# Maps source_id -> display bucket
SOURCE_TO_BUCKET: dict[str, str] = {
    "YOUTUBE_TREND_JP": DisplayBucket.YOUTUBE,
    "APPLE_MUSIC_JP": DisplayBucket.MUSIC,
    "APPLE_MUSIC_GLOBAL": DisplayBucket.MUSIC,
    "TRENDS": DisplayBucket.TRENDS,
    "NEWS_RSS": DisplayBucket.NEWS_RSS,
    "RAKUTEN_MAG": DisplayBucket.MAGAZINES,
    "X_SEARCH": DisplayBucket.X,
    "X_TRENDING": DisplayBucket.X,
    "NETFLIX_TV_JP": DisplayBucket.RANKINGS_STREAM,
    "NETFLIX_FILMS_JP": DisplayBucket.RANKINGS_STREAM,
    "TVER_RANKING_JP": DisplayBucket.RANKINGS_STREAM,
    "IG_BOOST": DisplayBucket.INSTAGRAM_BOOST,
}


def compute_candidate_score(
    sig_by_source: dict[str, list[float]],
    algo_config: AlgorithmConfig,
    music_config: MusicConfig,
    source_weights: dict[str, float] | None = None,
) -> tuple[float, list[BucketScore], float]:
    """Compute TrendScore for a single candidate.

    Args:
        sig_by_source: Map of source_id -> [sig_t, sig_{t-1}, sig_{t-2}]
        algo_config: Algorithm parameters
        music_config: Music regional weights

    Returns:
        (trend_score, breakdown_buckets, multi_bonus)
    """
    bucket_scores: dict[str, float] = {}
    bucket_details: dict[str, list[dict[str, Any]]] = {}
    active_sources = 0

    for source_id, sig_hist in sig_by_source.items():
        # Compute momentum for this source
        mom = momentum(sig_hist, algo_config.momentum_lambda)

        source_multiplier = _resolve_source_multiplier(
            source_id=source_id,
            music_config=music_config,
            source_weights=source_weights,
        )
        weighted_momentum = mom * source_multiplier

        # Aggregate into display bucket
        bucket = SOURCE_TO_BUCKET.get(source_id, DisplayBucket.TRENDS)
        bucket_scores[bucket] = bucket_scores.get(bucket, 0.0) + weighted_momentum
        bucket_details.setdefault(bucket, []).append({
            "sourceId": source_id,
            "weight": source_multiplier,
            "momentum": mom,
            "weightedMomentum": weighted_momentum,
        })

        # Count active sources (sig_t >= minSig)
        if len(sig_hist) > 0 and sig_hist[0] >= algo_config.min_sig:
            active_sources += 1

    # Multi-source bonus
    mb = multi_source_bonus(active_sources, algo_config.multi_weight)

    # TrendScore = sum(bucket_scores) + multiBonus
    # MVP: all bucket weights = 1.0
    total = sum(bucket_scores.values()) + mb

    # Build breakdown list
    breakdown = [
        BucketScore(bucket=b, score=s, details=bucket_details.get(b, []))
        for b, s in sorted(bucket_scores.items(), key=lambda x: -x[1])
        if s > 0
    ]

    return total, breakdown, mb


def _resolve_source_multiplier(
    source_id: str,
    music_config: MusicConfig,
    source_weights: dict[str, float] | None,
) -> float:
    """Resolve the multiplier applied to a source momentum value."""
    if source_weights is not None and source_id in source_weights:
        return source_weights[source_id]

    if source_id == "APPLE_MUSIC_GLOBAL":
        return music_config.weights.get("GLOBAL", 0.25)
    if source_id == "APPLE_MUSIC_JP":
        return music_config.weights.get("JP", 1.0)
    return 1.0


def compute_final_score(
    candidate_scores: list[dict[str, Any]],
    power_weight: float = 0.15,
) -> None:
    """Compute final_score incorporating Wikipedia power.

    Formula:
      power_norm = normalize power across current batch to 0..1
      power_boost = trend_score * power_weight * power_norm
      final_score = trend_score + power_boost

    This avoids "always-strong" candidates dominating by making
    power a multiplier of trend (not an independent additive term).
    Modifies candidate_scores in-place.
    """
    import math

    # Collect raw power values for normalization
    powers = [c.get("power", 0) or 0 for c in candidate_scores]
    max_power = max(powers) if powers else 0

    for entry in candidate_scores:
        trend = entry.get("trend_score", 0)
        raw_power = entry.get("power", 0) or 0

        if max_power > 0 and raw_power > 0:
            power_norm = math.log1p(raw_power) / math.log1p(max_power)
            power_boost = trend * power_weight * power_norm
        else:
            power_boost = 0.0

        entry["final_score"] = trend + power_boost


def select_top_k(
    candidate_scores: list[dict[str, Any]],
    top_k: int = 20,
) -> list[dict[str, Any]]:
    """Select top K candidates by final_score (or trend_score as fallback).

    final_score incorporates Wikipedia power as a boost proportional to trend.
    Tiebreaker: multiBonus.
    Returns sorted list of candidate score dicts.
    """
    sorted_candidates = sorted(
        candidate_scores,
        key=lambda c: (
            -c.get("final_score", c.get("trend_score", 0)),
            -c.get("multi_bonus", 0),
        ),
    )
    return sorted_candidates[:top_k]
