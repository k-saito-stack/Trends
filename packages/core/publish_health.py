"""Publish health gate for public ranking promotion."""

from __future__ import annotations

from collections import Counter
from typing import Any

from packages.core.models import DailyRankingItem
from packages.core.source_health import SourceHealthRecord

PUBLIC_CORE_HEALTHY_MIN = 6
PUBLIC_DISCOVERY_FAMILY_MIN = 2
PUBLIC_CONFIRMATION_FAMILY_MIN = 2
PUBLIC_WORDS_BEHAVIORS_MIN = 3
HEALTHY_FAILURE_CLASSES = {"healthy"}
CONFIRMATION_ROLES = {"CONFIRMATION", "EDITORIAL", "COMMERCE"}
MATURE_PEOPLE_MUSIC_TYPES = {"PERSON", "GROUP", "MUSIC_ARTIST", "MUSIC_TRACK"}


def evaluate_publish_health(
    source_health_records: list[SourceHealthRecord],
    source_plan: list[dict[str, Any]],
    ranking_items: list[DailyRankingItem],
    *,
    top_window: int = 20,
) -> dict[str, Any]:
    plan_by_id = {
        str(entry.get("sourceId")): entry for entry in source_plan if entry.get("sourceId")
    }
    healthy_records = [
        record
        for record in source_health_records
        if record.failure_class in HEALTHY_FAILURE_CLASSES
    ]
    healthy_core_source_ids = sorted(
        record.source_id
        for record in healthy_records
        if str(plan_by_id.get(record.source_id, {}).get("availabilityTier", "")) == "core"
    )
    healthy_discovery_families = sorted(
        {
            str(plan_by_id[record.source_id].get("familyPrimary", ""))
            for record in healthy_records
            if record.source_id in plan_by_id
            and str(plan_by_id[record.source_id].get("role", "")) == "DISCOVERY"
            and plan_by_id[record.source_id].get("familyPrimary")
        }
    )
    healthy_confirmation_families = sorted(
        {
            str(plan_by_id[record.source_id].get("familyPrimary", ""))
            for record in healthy_records
            if record.source_id in plan_by_id
            and str(plan_by_id[record.source_id].get("role", "")) in CONFIRMATION_ROLES
            and plan_by_id[record.source_id].get("familyPrimary")
        }
    )

    top_items = sorted(ranking_items, key=lambda item: item.rank)[:top_window]
    lane_counts = Counter(item.lane or "" for item in top_items if item.lane)
    single_family_counts = Counter(
        item.source_families[0]
        for item in top_items
        if len(item.source_families) == 1 and item.source_families[0]
    )
    mature_people_music_count = sum(
        1
        for item in top_items
        if item.candidate_type in MATURE_PEOPLE_MUSIC_TYPES and float(item.maturity or 0.0) >= 0.8
    )
    top_item_count = len(top_items)
    mature_people_music_ratio = (
        mature_people_music_count / top_item_count if top_item_count else 0.0
    )
    single_family_dominance = (
        max(single_family_counts.values()) / top_item_count
        if top_item_count and single_family_counts
        else 0.0
    )

    reasons: list[str] = []
    if len(healthy_core_source_ids) < PUBLIC_CORE_HEALTHY_MIN:
        reasons.append(
            f"healthy_core_sources<{PUBLIC_CORE_HEALTHY_MIN}:{len(healthy_core_source_ids)}"
        )
    if len(healthy_discovery_families) < PUBLIC_DISCOVERY_FAMILY_MIN:
        reasons.append(
            f"discovery_families<{PUBLIC_DISCOVERY_FAMILY_MIN}:{len(healthy_discovery_families)}"
        )
    if len(healthy_confirmation_families) < PUBLIC_CONFIRMATION_FAMILY_MIN:
        reasons.append(
            "confirmation_families"
            f"<{PUBLIC_CONFIRMATION_FAMILY_MIN}:{len(healthy_confirmation_families)}"
        )
    if lane_counts.get("words_behaviors", 0) < PUBLIC_WORDS_BEHAVIORS_MIN:
        reasons.append(
            f"words_behaviors_top{top_window}<{PUBLIC_WORDS_BEHAVIORS_MIN}:"
            f"{lane_counts.get('words_behaviors', 0)}"
        )

    public_eligible = not reasons
    return {
        "publicEligible": public_eligible,
        "shadowOnly": not public_eligible,
        "reasons": reasons,
        "metrics": {
            "healthyCoreSourceCount": len(healthy_core_source_ids),
            "discoveryFamilyCount": len(healthy_discovery_families),
            "confirmationFamilyCount": len(healthy_confirmation_families),
            "wordsBehaviorsTop20Count": lane_counts.get("words_behaviors", 0),
            "maturePeopleMusicRatioTop20": round(mature_people_music_ratio, 4),
            "singleFamilyDominanceTop20": round(single_family_dominance, 4),
            "top20ItemCount": top_item_count,
        },
        "healthyCoreSources": healthy_core_source_ids,
        "healthyDiscoveryFamilies": healthy_discovery_families,
        "healthyConfirmationFamilies": healthy_confirmation_families,
        "laneCountsTop20": dict(lane_counts),
        "singleFamilyCountsTop20": dict(single_family_counts),
    }
