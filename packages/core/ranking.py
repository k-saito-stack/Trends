"""Ranking helpers for both the legacy score and the v2 lane-based ranking."""

from __future__ import annotations

from typing import Any, TypedDict, cast

from packages.core.diversification import interleave_ranked_items
from packages.core.domain_classifier import is_main_ranking_domain
from packages.core.models import (
    AlgorithmConfig,
    BucketScore,
    CandidateKind,
    CandidateType,
    DailyCandidateFeature,
    DisplayBucket,
    DomainClass,
    Evidence,
    MusicConfig,
    RankedCandidateV2,
    RankingLane,
)
from packages.core.scoring import momentum, multi_source_bonus

# Maps source_id -> display bucket
SOURCE_TO_BUCKET: dict[str, str] = {
    "YOUTUBE_TREND_JP": DisplayBucket.YOUTUBE,
    "APPLE_MUSIC_JP": DisplayBucket.MUSIC,
    "APPLE_MUSIC_KR": DisplayBucket.MUSIC,
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

APPLE_MUSIC_SOURCE_REGIONS = {
    "APPLE_MUSIC_JP": "JP",
    "APPLE_MUSIC_KR": "KR",
    "APPLE_MUSIC_GLOBAL": "GLOBAL",
}
APPLE_MUSIC_FALLBACK_WEIGHTS = {
    "JP": 1.0,
    "KR": 0.85,
    "GLOBAL": 0.1,
}


class _PublishEntry(TypedDict):
    candidate_id: str
    display_name: str
    candidate_type: CandidateType
    candidate_kind: CandidateKind
    lane: str
    domain_class: DomainClass
    coming_score: float
    mass_heat: float
    primary_score: float
    source_families: list[str]
    evidence: list[Evidence]
    summary: str
    feature: DailyCandidateFeature
    selection_score: float


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
        bucket_details.setdefault(bucket, []).append(
            {
                "sourceId": source_id,
                "weight": source_multiplier,
                "momentum": mom,
                "weightedMomentum": weighted_momentum,
            }
        )

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

    region = APPLE_MUSIC_SOURCE_REGIONS.get(source_id)
    if region is not None:
        return music_config.weights.get(region, APPLE_MUSIC_FALLBACK_WEIGHTS[region])
    return 1.0


def compute_final_score(
    candidate_scores: list[dict[str, Any]],
    power_weight: float = 0.15,
) -> None:
    """Compatibility helper.

    v2 no longer boosts by Wikipedia power. If `primary_score` is already
    present we preserve it; otherwise we carry forward the legacy score.
    """
    del power_weight
    for entry in candidate_scores:
        if "primary_score" in entry:
            entry["final_score"] = entry["primary_score"]
        else:
            entry["final_score"] = entry.get("trend_score", 0.0)


def select_top_k(
    candidate_scores: list[dict[str, Any]],
    top_k: int = 20,
) -> list[dict[str, Any]]:
    """Select top K candidates by v2 primary/final score with legacy fallback."""
    sorted_candidates = sorted(
        candidate_scores,
        key=lambda c: (
            -c.get("primary_score", c.get("final_score", c.get("trend_score", 0))),
            -c.get("multi_bonus", 0),
        ),
    )
    return sorted_candidates[:top_k]


def _selection_score(feature: DailyCandidateFeature) -> float:
    base_score = feature.primary_score
    role_scores = feature.metadata.get("roleScores", {})
    discovery_score = float(role_scores.get("DISCOVERY", 0.0))
    breakout_prob = feature.breakout_prob_7d
    mass_prob = feature.mass_prob
    chart_only_confirmation = (
        discovery_score == 0.0
        and len(feature.source_families) == 1
        and feature.source_families[0] in {"MUSIC_CHART", "SHOW_CHART"}
    )

    if feature.ranking_gate_passed:
        return base_score + 0.8 + breakout_prob
    if discovery_score > 0 and feature.candidate_kind == CandidateKind.TOPIC:
        return base_score + 0.2 + breakout_prob * 0.4
    if chart_only_confirmation:
        return max(0.0, base_score - 0.35 + mass_prob * 0.1)
    return base_score + breakout_prob * 0.15


def build_ranked_candidates_v2(
    candidate_features: list[DailyCandidateFeature],
    candidates_by_id: dict[str, Any],
    top_k: int = 20,
) -> list[RankedCandidateV2]:
    main_domain_entries: list[_PublishEntry] = [
        {
            "candidate_id": feature.candidate_id,
            "display_name": feature.display_name,
            "candidate_type": feature.candidate_type,
            "candidate_kind": feature.candidate_kind,
            "lane": feature.lane.value,
            "domain_class": feature.domain_class,
            "coming_score": feature.coming_score,
            "mass_heat": feature.mass_heat,
            "primary_score": feature.primary_score,
            "source_families": feature.source_families,
            "evidence": feature.evidence,
            "summary": "",
            "feature": feature,
            "selection_score": _selection_score(feature),
        }
        for feature in candidate_features
        if is_main_ranking_domain(feature.domain_class)
    ]
    eligible = [
        entry for entry in main_domain_entries if entry["feature"].ranking_gate_passed
    ]
    if len(eligible) < top_k:
        seen_candidate_ids = {entry["candidate_id"] for entry in eligible}
        for entry in main_domain_entries:
            if entry["candidate_id"] in seen_candidate_ids:
                continue
            eligible.append(entry)
            seen_candidate_ids.add(entry["candidate_id"])
            if len(eligible) >= top_k:
                break

    interleaved = cast(
        list[_PublishEntry],
        interleave_ranked_items(cast(list[dict[str, Any]], eligible), top_k=top_k),
    )
    ranked: list[RankedCandidateV2] = []
    for rank, entry in enumerate(interleaved, start=1):
        candidate = candidates_by_id[entry["candidate_id"]]
        ranked.append(
            RankedCandidateV2(
                rank=rank,
                candidate_id=entry["candidate_id"],
                display_name=entry["display_name"],
                candidate_type=entry["candidate_type"],
                candidate_kind=entry["candidate_kind"],
                lane=RankingLane(entry["lane"]),
                domain_class=entry["domain_class"],
                coming_score=float(entry["coming_score"]),
                mass_heat=float(entry["mass_heat"]),
                primary_score=float(entry["primary_score"]),
                maturity=float(getattr(candidate, "maturity", 0.0)),
                source_families=list(entry["source_families"]),
                evidence=list(entry["evidence"])[:5],
                summary=str(entry.get("summary", "")),
                metadata={"feature": entry["feature"].to_dict()},
            )
        )
    return ranked
