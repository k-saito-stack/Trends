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
    "IG_BOOST": DisplayBucket.INSTAGRAM_BOOST,
}


def compute_candidate_score(
    sig_by_source: dict[str, list[float]],
    algo_config: AlgorithmConfig,
    music_config: MusicConfig,
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
    active_sources = 0

    for source_id, sig_hist in sig_by_source.items():
        # Compute momentum for this source
        mom = momentum(sig_hist, algo_config.momentum_lambda)

        # Apply music regional weights
        if source_id == "APPLE_MUSIC_GLOBAL":
            mom *= music_config.weights.get("GLOBAL", 0.25)
        elif source_id == "APPLE_MUSIC_JP":
            mom *= music_config.weights.get("JP", 1.0)

        # Aggregate into display bucket
        bucket = SOURCE_TO_BUCKET.get(source_id, DisplayBucket.TRENDS)
        bucket_scores[bucket] = bucket_scores.get(bucket, 0.0) + mom

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
        BucketScore(bucket=b, score=s)
        for b, s in sorted(bucket_scores.items(), key=lambda x: -x[1])
        if s > 0
    ]

    return total, breakdown, mb


def select_top_k(
    candidate_scores: list[dict[str, Any]],
    top_k: int = 15,
) -> list[dict[str, Any]]:
    """Select top K candidates by TrendScore.

    Tiebreaker: multiBonus -> power (if available).
    Returns sorted list of candidate score dicts.
    """
    sorted_candidates = sorted(
        candidate_scores,
        key=lambda c: (
            -c.get("trend_score", 0),
            -c.get("multi_bonus", 0),
            -c.get("power", 0),
        ),
    )
    return sorted_candidates[:top_k]
