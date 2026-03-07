"""Offline evaluation helpers for shadow/public ranking quality."""

from __future__ import annotations

from collections import Counter
from dataclasses import dataclass
from math import log2

from packages.core.labels import compute_new_confirmation_families
from packages.core.models import (
    DailyCandidateFeature,
    DailyRankingItem,
    HindsightLabel,
    RankingLane,
)

MATURE_PEOPLE_MUSIC_TYPES = {"PERSON", "GROUP", "MUSIC_ARTIST", "MUSIC_TRACK"}
LANE_TARGETS = (
    RankingLane.PEOPLE_MUSIC.value,
    RankingLane.SHOWS_FORMATS.value,
    RankingLane.WORDS_BEHAVIORS.value,
    RankingLane.STYLE_PRODUCTS.value,
)
BREAKOUT_HORIZON_ATTRS = {
    1: "breakout_1d",
    3: "breakout_3d",
    7: "breakout_7d",
    14: "breakout_14d",
}
MASS_HORIZON_ATTRS = {
    3: "mass_3d",
    7: "mass_7d",
}


@dataclass(frozen=True)
class RankedEvaluationEntry:
    candidate_id: str
    candidate_type: str
    lane: str
    source_families: tuple[str, ...]
    maturity: float
    breakout_prob_1d: float = 0.0
    breakout_prob_3d: float = 0.0
    breakout_prob_7d: float = 0.0
    mass_prob: float = 0.0
    cross_family_confirm: float = 0.0
    novelty: float = 0.0
    primary_score: float = 0.0
    anchor_feature: DailyCandidateFeature | None = None


def build_ranked_entries_from_features(
    features: list[DailyCandidateFeature],
) -> list[RankedEvaluationEntry]:
    ranked = sorted(features, key=lambda feature: (-feature.primary_score, feature.candidate_id))
    return [_entry_from_feature(feature) for feature in ranked]


def build_ranked_entries_from_items(
    items: list[DailyRankingItem],
    feature_by_candidate: dict[str, DailyCandidateFeature],
) -> list[RankedEvaluationEntry]:
    ranked = sorted(items, key=lambda item: (item.rank, item.candidate_id))
    entries: list[RankedEvaluationEntry] = []
    for item in ranked:
        feature = feature_by_candidate.get(item.candidate_id)
        if feature is not None:
            entries.append(
                RankedEvaluationEntry(
                    candidate_id=item.candidate_id,
                    candidate_type=item.candidate_type or feature.candidate_type.value,
                    lane=item.lane or feature.lane.value,
                    source_families=tuple(item.source_families or feature.source_families),
                    maturity=float(
                        item.maturity if item.maturity is not None else feature.mass_heat
                    ),
                    breakout_prob_1d=float(feature.breakout_prob_1d),
                    breakout_prob_3d=float(feature.breakout_prob_3d),
                    breakout_prob_7d=float(feature.breakout_prob_7d),
                    mass_prob=float(feature.mass_prob),
                    cross_family_confirm=float(feature.cross_family_confirm),
                    novelty=float(feature.novelty),
                    primary_score=float(
                        item.primary_score or feature.primary_score or item.trend_score
                    ),
                    anchor_feature=feature,
                )
            )
            continue
        entries.append(
            RankedEvaluationEntry(
                candidate_id=item.candidate_id,
                candidate_type=item.candidate_type,
                lane=item.lane or RankingLane.SHADOW.value,
                source_families=tuple(item.source_families),
                maturity=float(item.maturity or 0.0),
                primary_score=float(item.primary_score or item.trend_score),
            )
        )
    return entries


def lead_spread_at_k(features: list[DailyCandidateFeature], k: int = 20) -> float:
    relevant = features[:k]
    if not relevant:
        return 0.0
    hits = sum(1 for feature in relevant if feature.cross_family_confirm > 0)
    return hits / len(relevant)


def cross_family_presence_at_k(features: list[DailyCandidateFeature], k: int = 20) -> float:
    relevant = features[:k]
    if not relevant:
        return 0.0
    return sum(len(feature.source_families) for feature in relevant) / len(relevant)


def novelty_precision(features: list[DailyCandidateFeature], threshold: float = 0.5) -> float:
    if not features:
        return 0.0
    flagged = [feature for feature in features if feature.novelty >= threshold]
    if not flagged:
        return 0.0
    hits = sum(1 for feature in flagged if feature.primary_score >= threshold)
    return hits / len(flagged)


def type_diversity_at_k(features: list[DailyCandidateFeature], k: int = 20) -> float:
    relevant = features[:k]
    if not relevant:
        return 0.0
    counts = Counter(feature.candidate_type.value for feature in relevant)
    return len(counts) / len(relevant)


def lane_mix_at_k(features: list[DailyCandidateFeature], k: int = 20) -> dict[str, int]:
    relevant = features[:k]
    counts = Counter(feature.lane.value for feature in relevant)
    return {
        lane.value: counts.get(lane.value, 0) for lane in RankingLane if lane != RankingLane.SHADOW
    }


def breakout_precision_at_k(
    entries: list[RankedEvaluationEntry],
    labels_by_candidate: dict[str, HindsightLabel],
    *,
    k: int = 20,
    horizon_days: int = 7,
) -> tuple[float, int]:
    relevant = entries[:k]
    label_attr = BREAKOUT_HORIZON_ATTRS.get(horizon_days)
    if label_attr is None:
        return 0.0, 0
    outcomes: list[bool] = []
    for entry in relevant:
        label = labels_by_candidate.get(entry.candidate_id)
        if label is None or horizon_days not in label.available_breakout_horizons:
            continue
        outcomes.append(bool(getattr(label, label_attr)))
    if not outcomes:
        return 0.0, 0
    positives = sum(1 for value in outcomes if value)
    return positives / len(outcomes), len(outcomes)


def future_spread_at_k(
    entries: list[RankedEvaluationEntry],
    labels_by_candidate: dict[str, HindsightLabel],
    feature_map: dict[str, dict[str, DailyCandidateFeature]],
    *,
    anchor_date: str,
    k: int = 20,
    horizon_days: int = 7,
) -> float:
    relevant = entries[:k]
    if not relevant:
        return 0.0

    spread_counts: list[int] = []
    for entry in relevant:
        if entry.anchor_feature is not None:
            spread_counts.append(
                len(
                    compute_new_confirmation_families(
                        entry.anchor_feature,
                        feature_map,
                        anchor_date,
                        horizon_days,
                    )
                )
            )
            continue

        label = labels_by_candidate.get(entry.candidate_id)
        if label is None:
            spread_counts.append(0)
            continue
        if horizon_days >= 14:
            spread_counts.append(len(label.new_confirmation_families))
        elif label.lead_days is not None and label.lead_days <= horizon_days:
            spread_counts.append(max(1, len(label.new_confirmation_families)))
        else:
            spread_counts.append(0)
    return sum(spread_counts) / len(spread_counts)


def lead_gain_at_k(
    entries: list[RankedEvaluationEntry],
    labels_by_candidate: dict[str, HindsightLabel],
    *,
    k: int = 20,
) -> tuple[float, float, int]:
    relevant = entries[:k]
    lead_days = [
        float(label.lead_days)
        for entry in relevant
        if (label := labels_by_candidate.get(entry.candidate_id)) is not None
        and label.lead_days is not None
    ]
    if not lead_days:
        return 0.0, 0.0, 0
    return sum(lead_days) / len(lead_days), len(lead_days) / len(relevant), len(lead_days)


def novelty_adjusted_ndcg_at_k(
    entries: list[RankedEvaluationEntry],
    labels_by_candidate: dict[str, HindsightLabel],
    feature_map: dict[str, dict[str, DailyCandidateFeature]],
    *,
    anchor_date: str,
    k: int = 20,
    horizon_days: int = 7,
) -> float:
    relevant = entries[:k]
    if not relevant:
        return 0.0

    relevances = [
        _relevance_score(entry, labels_by_candidate, feature_map, anchor_date, horizon_days)
        for entry in relevant
    ]
    dcg = _dcg(relevances)
    ideal = sorted(relevances, reverse=True)
    idcg = _dcg(ideal)
    if idcg <= 0:
        return 0.0
    return dcg / idcg


def lane_mix_for_entries(entries: list[RankedEvaluationEntry], k: int = 20) -> dict[str, int]:
    relevant = entries[:k]
    counts = Counter(entry.lane for entry in relevant if entry.lane)
    return {lane: counts.get(lane, 0) for lane in LANE_TARGETS}


def lane_coverage_at_k(entries: list[RankedEvaluationEntry], k: int = 20) -> float:
    lane_mix = lane_mix_for_entries(entries, k=k)
    covered = sum(1 for count in lane_mix.values() if count > 0)
    return covered / len(LANE_TARGETS)


def type_diversity_for_entries(entries: list[RankedEvaluationEntry], k: int = 20) -> float:
    relevant = entries[:k]
    if not relevant:
        return 0.0
    counts = Counter(entry.candidate_type for entry in relevant)
    return len(counts) / len(relevant)


def mature_people_music_ratio_at_k(entries: list[RankedEvaluationEntry], k: int = 20) -> float:
    relevant = entries[:k]
    if not relevant:
        return 0.0
    mature_count = sum(
        1
        for entry in relevant
        if entry.candidate_type in MATURE_PEOPLE_MUSIC_TYPES and entry.maturity >= 0.8
    )
    return mature_count / len(relevant)


def brier_score(
    probabilities: list[float],
    outcomes: list[bool],
) -> float:
    if not probabilities or len(probabilities) != len(outcomes):
        return 0.0
    squared_error = sum(
        (probability - float(outcome)) ** 2
        for probability, outcome in zip(probabilities, outcomes, strict=True)
    )
    return squared_error / len(probabilities)


def reliability_diagram(
    probabilities: list[float],
    outcomes: list[bool],
    *,
    bins: int = 5,
) -> list[dict[str, float | int]]:
    if not probabilities or len(probabilities) != len(outcomes):
        return []

    grouped: list[list[tuple[float, bool]]] = [[] for _ in range(bins)]
    for probability, outcome in zip(probabilities, outcomes, strict=True):
        idx = min(int(max(probability, 0.0) * bins), bins - 1)
        grouped[idx].append((probability, outcome))

    results: list[dict[str, float | int]] = []
    for idx, bucket in enumerate(grouped):
        lower = idx / bins
        upper = (idx + 1) / bins
        if not bucket:
            results.append(
                {
                    "lower": round(lower, 4),
                    "upper": round(upper, 4),
                    "count": 0,
                    "avgPred": 0.0,
                    "avgOutcome": 0.0,
                }
            )
            continue
        avg_pred = sum(item[0] for item in bucket) / len(bucket)
        avg_outcome = sum(1.0 if item[1] else 0.0 for item in bucket) / len(bucket)
        results.append(
            {
                "lower": round(lower, 4),
                "upper": round(upper, 4),
                "count": len(bucket),
                "avgPred": round(avg_pred, 4),
                "avgOutcome": round(avg_outcome, 4),
            }
        )
    return results


def evaluate_ranked_entries(
    entries: list[RankedEvaluationEntry],
    labels_by_candidate: dict[str, HindsightLabel],
    feature_map: dict[str, dict[str, DailyCandidateFeature]],
    *,
    anchor_date: str,
    top_k: int = 20,
) -> dict[str, object]:
    metrics: dict[str, object] = {
        "itemCount": len(entries),
        f"futureSpreadAt{top_k}_7d": round(
            future_spread_at_k(
                entries,
                labels_by_candidate,
                feature_map,
                anchor_date=anchor_date,
                k=top_k,
                horizon_days=7,
            ),
            4,
        ),
        f"futureSpreadAt{top_k}_14d": round(
            future_spread_at_k(
                entries,
                labels_by_candidate,
                feature_map,
                anchor_date=anchor_date,
                k=top_k,
                horizon_days=14,
            ),
            4,
        ),
        f"leadGainAt{top_k}": 0.0,
        f"leadGainCoverageAt{top_k}": 0.0,
        f"leadGainCountAt{top_k}": 0,
        f"noveltyAdjustedNdcgAt{top_k}_7d": round(
            novelty_adjusted_ndcg_at_k(
                entries,
                labels_by_candidate,
                feature_map,
                anchor_date=anchor_date,
                k=top_k,
                horizon_days=7,
            ),
            4,
        ),
        f"laneCoverageAt{top_k}": round(lane_coverage_at_k(entries, k=top_k), 4),
        f"laneMixAt{top_k}": lane_mix_for_entries(entries, k=top_k),
        f"typeDiversityAt{top_k}": round(type_diversity_for_entries(entries, k=top_k), 4),
        f"maturePeopleMusicRatioAt{top_k}": round(
            mature_people_music_ratio_at_k(entries, k=top_k),
            4,
        ),
    }

    lead_gain, lead_coverage, lead_count = lead_gain_at_k(
        entries,
        labels_by_candidate,
        k=top_k,
    )
    metrics[f"leadGainAt{top_k}"] = round(lead_gain, 4)
    metrics[f"leadGainCoverageAt{top_k}"] = round(lead_coverage, 4)
    metrics[f"leadGainCountAt{top_k}"] = lead_count

    for horizon_days in (1, 3, 7, 14):
        precision, evaluated = breakout_precision_at_k(
            entries,
            labels_by_candidate,
            k=top_k,
            horizon_days=horizon_days,
        )
        metrics[f"breakoutPrecisionAt{top_k}_{horizon_days}d"] = round(precision, 4)
        metrics[f"breakoutEvaluatedAt{top_k}_{horizon_days}d"] = evaluated

    breakout_pairs = _probability_pairs(
        entries,
        labels_by_candidate,
        horizon_days=7,
        probability_attr="breakout_prob_7d",
    )
    mass_pairs = _probability_pairs(
        entries,
        labels_by_candidate,
        horizon_days=7,
        probability_attr="mass_prob",
        mass_labels=True,
    )
    metrics["calibration"] = {
        "breakout7dBrier": round(
            brier_score(
                [pair[0] for pair in breakout_pairs],
                [pair[1] for pair in breakout_pairs],
            ),
            4,
        ),
        "breakout7dCount": len(breakout_pairs),
        "breakout7dReliability": reliability_diagram(
            [pair[0] for pair in breakout_pairs],
            [pair[1] for pair in breakout_pairs],
        ),
        "mass7dBrier": round(
            brier_score(
                [pair[0] for pair in mass_pairs],
                [pair[1] for pair in mass_pairs],
            ),
            4,
        ),
        "mass7dCount": len(mass_pairs),
        "mass7dReliability": reliability_diagram(
            [pair[0] for pair in mass_pairs],
            [pair[1] for pair in mass_pairs],
        ),
    }
    return metrics


def compare_variant_metrics(
    shadow_metrics: dict[str, object],
    public_metrics: dict[str, object],
    *,
    top_k: int = 20,
) -> dict[str, float]:
    def _metric(name: str, metrics: dict[str, object]) -> float:
        value = metrics.get(name, 0.0)
        return float(value) if isinstance(value, (int, float)) else 0.0

    return {
        f"breakoutPrecisionDeltaAt{top_k}_7d": round(
            _metric(f"breakoutPrecisionAt{top_k}_7d", shadow_metrics)
            - _metric(f"breakoutPrecisionAt{top_k}_7d", public_metrics),
            4,
        ),
        f"futureSpreadDeltaAt{top_k}_7d": round(
            _metric(f"futureSpreadAt{top_k}_7d", shadow_metrics)
            - _metric(f"futureSpreadAt{top_k}_7d", public_metrics),
            4,
        ),
        f"maturePeopleMusicRatioDeltaAt{top_k}": round(
            _metric(f"maturePeopleMusicRatioAt{top_k}", shadow_metrics)
            - _metric(f"maturePeopleMusicRatioAt{top_k}", public_metrics),
            4,
        ),
        f"laneCoverageDeltaAt{top_k}": round(
            _metric(f"laneCoverageAt{top_k}", shadow_metrics)
            - _metric(f"laneCoverageAt{top_k}", public_metrics),
            4,
        ),
    }


def _entry_from_feature(feature: DailyCandidateFeature) -> RankedEvaluationEntry:
    return RankedEvaluationEntry(
        candidate_id=feature.candidate_id,
        candidate_type=feature.candidate_type.value,
        lane=feature.lane.value,
        source_families=tuple(feature.source_families),
        maturity=float(feature.mass_heat),
        breakout_prob_1d=float(feature.breakout_prob_1d),
        breakout_prob_3d=float(feature.breakout_prob_3d),
        breakout_prob_7d=float(feature.breakout_prob_7d),
        mass_prob=float(feature.mass_prob),
        cross_family_confirm=float(feature.cross_family_confirm),
        novelty=float(feature.novelty),
        primary_score=float(feature.primary_score),
        anchor_feature=feature,
    )


def _probability_pairs(
    entries: list[RankedEvaluationEntry],
    labels_by_candidate: dict[str, HindsightLabel],
    *,
    horizon_days: int,
    probability_attr: str,
    mass_labels: bool = False,
) -> list[tuple[float, bool]]:
    pairs: list[tuple[float, bool]] = []
    for entry in entries:
        label = labels_by_candidate.get(entry.candidate_id)
        if label is None:
            continue
        if mass_labels:
            label_attr = MASS_HORIZON_ATTRS.get(horizon_days)
            if label_attr is None or horizon_days not in label.available_mass_horizons:
                continue
        else:
            label_attr = BREAKOUT_HORIZON_ATTRS.get(horizon_days)
            if label_attr is None or horizon_days not in label.available_breakout_horizons:
                continue
        pairs.append((float(getattr(entry, probability_attr)), bool(getattr(label, label_attr))))
    return pairs


def _relevance_score(
    entry: RankedEvaluationEntry,
    labels_by_candidate: dict[str, HindsightLabel],
    feature_map: dict[str, dict[str, DailyCandidateFeature]],
    anchor_date: str,
    horizon_days: int,
) -> float:
    label = labels_by_candidate.get(entry.candidate_id)
    breakout_attr = BREAKOUT_HORIZON_ATTRS.get(horizon_days, "breakout_7d")
    breakout_hit = float(bool(label and getattr(label, breakout_attr, False)))
    if entry.anchor_feature is not None:
        spread = len(
            compute_new_confirmation_families(
                entry.anchor_feature,
                feature_map,
                anchor_date,
                horizon_days,
            )
        )
    elif label is not None:
        spread = len(label.new_confirmation_families) if horizon_days >= 14 else int(
            label.lead_days is not None and label.lead_days <= horizon_days
        )
    else:
        spread = 0
    maturity_penalty = (
        entry.maturity * 0.35
        if entry.candidate_type in MATURE_PEOPLE_MUSIC_TYPES
        else 0.0
    )
    novelty_bonus = entry.novelty * 0.15
    return max(0.0, breakout_hit + min(spread, 3) * 0.25 + novelty_bonus - maturity_penalty)


def _dcg(relevances: list[float]) -> float:
    return sum(value / log2(index + 2) for index, value in enumerate(relevances) if value > 0)
