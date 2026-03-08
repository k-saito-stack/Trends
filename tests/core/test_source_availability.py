from __future__ import annotations

from packages.core.source_availability import (
    adjust_threshold_for_availability,
    compute_source_availability_snapshot,
)


def test_compute_source_availability_snapshot_counts_non_zero_family_coverage() -> None:
    snapshot = compute_source_availability_snapshot(
        source_ok={
            "TRENDS_JP_24H_ENT": True,
            "YAHOO_REALTIME": True,
            "TIKTOK_CREATIVE_CENTER_HASHTAGS": True,
            "APPLE_MUSIC_JP": False,
        },
        source_item_count={
            "TRENDS_JP_24H_ENT": 10,
            "YAHOO_REALTIME": 0,
            "TIKTOK_CREATIVE_CENTER_HASHTAGS": 20,
            "APPLE_MUSIC_JP": 0,
        },
        source_plan=[
            {
                "sourceId": "TRENDS_JP_24H_ENT",
                "enabled": True,
                "familyPrimary": "SEARCH",
                "availabilityTier": "core",
                "role": "DISCOVERY",
            },
            {
                "sourceId": "YAHOO_REALTIME",
                "enabled": True,
                "familyPrimary": "SOCIAL_DISCOVERY",
                "availabilityTier": "core",
                "role": "DISCOVERY",
            },
            {
                "sourceId": "TIKTOK_CREATIVE_CENTER_HASHTAGS",
                "enabled": True,
                "familyPrimary": "SOCIAL_DISCOVERY",
                "availabilityTier": "core",
                "role": "DISCOVERY",
            },
            {
                "sourceId": "APPLE_MUSIC_JP",
                "enabled": True,
                "familyPrimary": "MUSIC_CHART",
                "availabilityTier": "core",
                "role": "CONFIRMATION",
            },
        ],
    )

    assert snapshot["familyAvailabilityRatio"] > 0
    assert snapshot["healthyCoreAvailabilityRatio"] == 0.5
    assert "TRENDS_JP_24H_ENT" in snapshot["nonZeroSourceIds"]


def test_adjust_threshold_for_availability_uses_floor() -> None:
    assert adjust_threshold_for_availability(0.8, 0.2) == 0.4
