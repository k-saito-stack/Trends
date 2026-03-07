"""Weak-supervision hindsight labels for breakout and mass confirmation."""

from __future__ import annotations

from datetime import date, timedelta

from packages.core.models import (
    CandidateKind,
    DailyCandidateFeature,
    HindsightLabel,
    SourceFamily,
)

CONFIRMATION_FAMILIES = {
    SourceFamily.MUSIC_CHART.value,
    SourceFamily.SHOW_CHART.value,
    SourceFamily.VIDEO_CONFIRM.value,
}
MASS_FAMILIES = CONFIRMATION_FAMILIES | {
    SourceFamily.EDITORIAL.value,
    SourceFamily.COMMERCE.value,
}
BREAKOUT_MASS_THRESHOLD = 0.8
MASS_NOW_THRESHOLD = 0.9


def build_hindsight_labels(
    anchor_date: str,
    anchor_features: list[DailyCandidateFeature],
    feature_map: dict[str, dict[str, DailyCandidateFeature]],
    *,
    available_breakout_horizons: list[int],
    available_mass_horizons: list[int],
    created_at: str,
) -> list[HindsightLabel]:
    """Build hindsight labels for one anchor date using currently available future days."""
    labels: list[HindsightLabel] = []
    for anchor in anchor_features:
        labels.append(
            HindsightLabel(
                date=anchor_date,
                candidate_id=anchor.candidate_id,
                breakout_1d=_compute_breakout_labels(
                    anchor, feature_map, anchor_date, 1
                )
                if 1 in available_breakout_horizons
                else False,
                breakout_3d=_compute_breakout_labels(
                    anchor, feature_map, anchor_date, 3
                )
                if 3 in available_breakout_horizons
                else False,
                breakout_7d=_compute_breakout_labels(
                    anchor, feature_map, anchor_date, 7
                )
                if 7 in available_breakout_horizons
                else False,
                breakout_14d=_compute_breakout_labels(
                    anchor, feature_map, anchor_date, 14
                )
                if 14 in available_breakout_horizons
                else False,
                mass_now=compute_mass_now(anchor),
                mass_3d=compute_mass_labels(anchor, feature_map, anchor_date, 3)
                if 3 in available_mass_horizons
                else False,
                mass_7d=compute_mass_labels(anchor, feature_map, anchor_date, 7)
                if 7 in available_mass_horizons
                else False,
                jp_confirm_3d=compute_jp_confirm(anchor, feature_map, anchor_date, 3),
                jp_confirm_7d=compute_jp_confirm(anchor, feature_map, anchor_date, 7),
                public_confirm_7d=compute_public_confirm(anchor, feature_map, anchor_date, 7),
                trivial_noise_7d=compute_trivial_noise(anchor, feature_map, anchor_date, 7),
                new_confirmation_families=sorted(
                    compute_new_confirmation_families(anchor, feature_map, anchor_date, 14)
                ),
                lead_days=compute_lead_days(anchor, feature_map, anchor_date, 14),
                available_breakout_horizons=sorted(set(available_breakout_horizons)),
                available_mass_horizons=sorted(set(available_mass_horizons)),
                created_at=created_at,
            )
        )
    return labels


def compute_breakout_labels(
    anchor: DailyCandidateFeature,
    feature_map: dict[str, dict[str, DailyCandidateFeature]],
    anchor_date: str,
    horizon_days: int,
) -> bool:
    return _compute_breakout_labels(anchor, feature_map, anchor_date, horizon_days)


def compute_mass_labels(
    anchor: DailyCandidateFeature,
    feature_map: dict[str, dict[str, DailyCandidateFeature]],
    anchor_date: str,
    horizon_days: int,
) -> bool:
    for feature in _iter_future_features(
        feature_map, anchor_date, anchor.candidate_id, horizon_days
    ):
        if _is_mass_feature(feature):
            return True
    return False


def compute_mass_now(anchor: DailyCandidateFeature) -> bool:
    return _is_mass_feature(anchor)


def compute_new_confirmation_families(
    anchor: DailyCandidateFeature,
    feature_map: dict[str, dict[str, DailyCandidateFeature]],
    anchor_date: str,
    horizon_days: int,
) -> set[str]:
    current = _confirmation_family_set(anchor)
    future: set[str] = set()
    for feature in _iter_future_features(
        feature_map, anchor_date, anchor.candidate_id, horizon_days
    ):
        future.update(_confirmation_family_set(feature))
    return future - current


def compute_lead_days(
    anchor: DailyCandidateFeature,
    feature_map: dict[str, dict[str, DailyCandidateFeature]],
    anchor_date: str,
    max_horizon_days: int,
) -> int | None:
    for offset in range(1, max_horizon_days + 1):
        future_feature = _future_feature(feature_map, anchor_date, anchor.candidate_id, offset)
        if future_feature is None:
            continue
        if (
            compute_new_confirmation_families(anchor, feature_map, anchor_date, offset)
            or _is_mass_feature(future_feature)
        ):
            return offset
    return None


def compute_jp_confirm(
    anchor: DailyCandidateFeature,
    feature_map: dict[str, dict[str, DailyCandidateFeature]],
    anchor_date: str,
    horizon_days: int,
) -> bool:
    future_features = _iter_future_features(
        feature_map,
        anchor_date,
        anchor.candidate_id,
        horizon_days,
    )
    for feature in future_features:
        if (
            feature.constrained_trends_ent_support > 0
            or feature.constrained_trends_beauty_support > 0
            or feature.yahoo_realtime_support > 0
        ):
            return True
    return False


def compute_public_confirm(
    anchor: DailyCandidateFeature,
    feature_map: dict[str, dict[str, DailyCandidateFeature]],
    anchor_date: str,
    horizon_days: int,
) -> bool:
    future_features = _iter_future_features(
        feature_map,
        anchor_date,
        anchor.candidate_id,
        horizon_days,
    )
    for feature in future_features:
        if feature.public_gate_passed or feature.public_rankability_prob >= 0.5:
            return True
    return False


def compute_trivial_noise(
    anchor: DailyCandidateFeature,
    feature_map: dict[str, dict[str, DailyCandidateFeature]],
    anchor_date: str,
    horizon_days: int,
) -> bool:
    if anchor.candidate_kind != CandidateKind.TOPIC:
        return False
    if compute_jp_confirm(anchor, feature_map, anchor_date, horizon_days):
        return False
    if compute_public_confirm(anchor, feature_map, anchor_date, horizon_days):
        return False
    return (
        len(set(anchor.source_families)) <= 1
        and anchor.public_noise_penalty >= 0.55
        and anchor.public_rankability_prob < 0.5
    )


def _compute_breakout_labels(
    anchor: DailyCandidateFeature,
    feature_map: dict[str, dict[str, DailyCandidateFeature]],
    anchor_date: str,
    horizon_days: int,
) -> bool:
    if compute_new_confirmation_families(anchor, feature_map, anchor_date, horizon_days):
        return True
    if compute_mass_labels(anchor, feature_map, anchor_date, horizon_days):
        return True
    if anchor.candidate_kind == CandidateKind.TOPIC:
        for feature in _iter_future_features(
            feature_map, anchor_date, anchor.candidate_id, horizon_days
        ):
            if len(set(feature.source_families)) >= 2:
                return True
    return False


def _iter_future_features(
    feature_map: dict[str, dict[str, DailyCandidateFeature]],
    anchor_date: str,
    candidate_id: str,
    horizon_days: int,
) -> list[DailyCandidateFeature]:
    results: list[DailyCandidateFeature] = []
    for offset in range(1, horizon_days + 1):
        feature = _future_feature(feature_map, anchor_date, candidate_id, offset)
        if feature is not None:
            results.append(feature)
    return results


def _future_feature(
    feature_map: dict[str, dict[str, DailyCandidateFeature]],
    anchor_date: str,
    candidate_id: str,
    offset_days: int,
) -> DailyCandidateFeature | None:
    target = (date.fromisoformat(anchor_date) + timedelta(days=offset_days)).isoformat()
    return feature_map.get(target, {}).get(candidate_id)


def _confirmation_family_set(feature: DailyCandidateFeature) -> set[str]:
    return {
        family
        for family in feature.source_families
        if family in CONFIRMATION_FAMILIES
    }


def _mass_family_set(feature: DailyCandidateFeature) -> set[str]:
    return {
        family
        for family in feature.source_families
        if family in MASS_FAMILIES
    }


def _is_mass_feature(feature: DailyCandidateFeature) -> bool:
    return (
        feature.mass_heat >= MASS_NOW_THRESHOLD
        or feature.broad_confirmation >= 0.6
        or len(_mass_family_set(feature)) >= 2
    )
