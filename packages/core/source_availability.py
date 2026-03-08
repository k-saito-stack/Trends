"""Run-level source availability snapshot helpers."""

from __future__ import annotations

from typing import Any

JP_CREDIBILITY_SOURCE_IDS = {
    "TRENDS_JP_24H_ENT",
    "TRENDS_JP_24H_BEAUTY_FASHION",
    "YAHOO_REALTIME",
}
TIKTOK_SOURCE_IDS = {
    "TIKTOK_CREATIVE_CENTER_HASHTAGS",
    "TIKTOK_CREATIVE_CENTER_SONGS",
    "TIKTOK_CREATIVE_CENTER_CREATORS",
    "TIKTOK_CREATIVE_CENTER_VIDEOS",
}


def compute_source_availability_snapshot(
    *,
    source_ok: dict[str, bool],
    source_item_count: dict[str, int],
    source_plan: list[dict[str, Any]],
) -> dict[str, Any]:
    enabled_entries = [entry for entry in source_plan if entry.get("enabled", True)]
    available_source_ids = sorted(
        source_id
        for source_id, ok in source_ok.items()
        if ok and source_item_count.get(source_id, 0) >= 0
    )
    non_zero_source_ids = sorted(
        source_id
        for source_id, ok in source_ok.items()
        if ok and source_item_count.get(source_id, 0) > 0
    )

    planned_families = {
        str(entry.get("familyPrimary", ""))
        for entry in enabled_entries
        if entry.get("familyPrimary")
    }
    non_zero_families = {
        str(entry.get("familyPrimary", ""))
        for entry in enabled_entries
        if str(entry.get("sourceId", "")) in non_zero_source_ids and entry.get("familyPrimary")
    }

    planned_core_ids = {
        str(entry.get("sourceId", ""))
        for entry in enabled_entries
        if str(entry.get("availabilityTier", "")) == "core"
    }
    planned_jp_credibility = {
        str(entry.get("sourceId", ""))
        for entry in enabled_entries
        if str(entry.get("sourceId", "")) in JP_CREDIBILITY_SOURCE_IDS
    }
    planned_music_confirmation = {
        str(entry.get("sourceId", ""))
        for entry in enabled_entries
        if str(entry.get("role", "")) == "CONFIRMATION"
        and str(entry.get("familyPrimary", "")) == "MUSIC_CHART"
    }
    planned_show_confirmation = {
        str(entry.get("sourceId", ""))
        for entry in enabled_entries
        if str(entry.get("role", "")) == "CONFIRMATION"
        and str(entry.get("familyPrimary", "")) == "SHOW_CHART"
    }
    planned_tiktok = {
        str(entry.get("sourceId", ""))
        for entry in enabled_entries
        if str(entry.get("sourceId", "")) in TIKTOK_SOURCE_IDS
    }

    return {
        "availableSourceIds": available_source_ids,
        "nonZeroSourceIds": non_zero_source_ids,
        "familyAvailabilityRatio": _safe_ratio(len(non_zero_families), len(planned_families)),
        "healthyCoreAvailabilityRatio": _match_ratio(non_zero_source_ids, planned_core_ids),
        "jpCredibilityAvailabilityRatio": _match_ratio(non_zero_source_ids, planned_jp_credibility),
        "musicConfirmationAvailabilityRatio": _match_ratio(
            non_zero_source_ids, planned_music_confirmation
        ),
        "showConfirmationAvailabilityRatio": _match_ratio(
            non_zero_source_ids, planned_show_confirmation
        ),
        "tiktokAvailabilityRatio": _match_ratio(non_zero_source_ids, planned_tiktok),
        "searchFamilyAvailabilityRatio": _match_ratio(
            non_zero_families,
            {"SEARCH", "SOCIAL_DISCOVERY"},
        ),
    }


def adjust_threshold_for_availability(
    base_threshold: float,
    availability_ratio: float,
    *,
    min_ratio: float = 0.5,
) -> float:
    return round(base_threshold * max(min_ratio, availability_ratio), 4)


def _match_ratio(observed: list[str] | set[str], planned: set[str]) -> float:
    observed_set = set(observed)
    if not planned:
        return 1.0
    matched = len(observed_set & planned)
    return _safe_ratio(matched, len(planned))


def _safe_ratio(numerator: int, denominator: int) -> float:
    if denominator <= 0:
        return 1.0
    return round(numerator / denominator, 4)
