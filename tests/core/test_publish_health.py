from __future__ import annotations

from packages.core.models import DailyRankingItem
from packages.core.publish_health import evaluate_publish_health
from packages.core.source_health import SourceHealthRecord


def _record(
    source_id: str,
    *,
    failure_class: str = "healthy",
) -> SourceHealthRecord:
    return SourceHealthRecord(
        date="2026-03-07",
        source_id=source_id,
        ok=failure_class == "healthy",
        raw_item_count=10,
        kept_item_count=5 if failure_class == "healthy" else 0,
        failure_class=failure_class,
    )


def _item(
    rank: int,
    lane: str,
    source_families: list[str],
    maturity: float = 0.0,
) -> DailyRankingItem:
    return DailyRankingItem(
        rank=rank,
        candidate_id=f"cand_{rank}",
        candidate_type="BEHAVIOR" if lane == "words_behaviors" else "MUSIC_ARTIST",
        display_name=f"item-{rank}",
        trend_score=3.0,
        lane=lane,
        source_families=source_families,
        maturity=maturity,
    )


def test_evaluate_publish_health_passes_when_core_coverage_and_lane_mix_are_healthy() -> None:
    source_plan = [
        {
            "sourceId": "TRENDS",
            "availabilityTier": "core",
            "role": "DISCOVERY",
            "familyPrimary": "SEARCH",
        },
        {
            "sourceId": "YAHOO_REALTIME",
            "availabilityTier": "core",
            "role": "DISCOVERY",
            "familyPrimary": "SOCIAL_DISCOVERY",
        },
        {
            "sourceId": "WEAR_WORDS",
            "availabilityTier": "core",
            "role": "DISCOVERY",
            "familyPrimary": "FASHION_STYLE",
        },
        {
            "sourceId": "APPLE_MUSIC_JP",
            "availabilityTier": "core",
            "role": "CONFIRMATION",
            "familyPrimary": "MUSIC_CHART",
        },
        {
            "sourceId": "YOUTUBE_TREND_JP",
            "availabilityTier": "core",
            "role": "CONFIRMATION",
            "familyPrimary": "VIDEO_CONFIRM",
        },
        {
            "sourceId": "EDITORIAL_MODELPRESS",
            "availabilityTier": "core",
            "role": "EDITORIAL",
            "familyPrimary": "EDITORIAL",
        },
    ]
    records = [
        _record("TRENDS"),
        _record("YAHOO_REALTIME"),
        _record("WEAR_WORDS"),
        _record("APPLE_MUSIC_JP"),
        _record("YOUTUBE_TREND_JP"),
        _record("EDITORIAL_MODELPRESS"),
    ]
    ranking_items = [
        _item(1, "words_behaviors", ["SEARCH"]),
        _item(2, "words_behaviors", ["SOCIAL_DISCOVERY"]),
        _item(3, "words_behaviors", ["FASHION_STYLE"]),
        _item(4, "people_music", ["MUSIC_CHART"], maturity=0.4),
    ]

    report = evaluate_publish_health(records, source_plan, ranking_items)

    assert report["publicEligible"] is True
    assert report["shadowOnly"] is False
    assert report["metrics"]["healthyCoreSourceCount"] == 6


def test_evaluate_publish_health_falls_back_to_shadow_when_coverage_is_weak() -> None:
    source_plan = [
        {
            "sourceId": "TRENDS",
            "availabilityTier": "core",
            "role": "DISCOVERY",
            "familyPrimary": "SEARCH",
        },
        {
            "sourceId": "APPLE_MUSIC_JP",
            "availabilityTier": "core",
            "role": "CONFIRMATION",
            "familyPrimary": "MUSIC_CHART",
        },
    ]
    records = [
        _record("TRENDS"),
        _record("APPLE_MUSIC_JP", failure_class="zero_kept"),
    ]
    ranking_items = [
        _item(1, "people_music", ["MUSIC_CHART"], maturity=0.9),
        _item(2, "people_music", ["MUSIC_CHART"], maturity=0.85),
    ]

    report = evaluate_publish_health(records, source_plan, ranking_items)

    assert report["publicEligible"] is False
    assert report["shadowOnly"] is True
    assert "healthy_core_sources<6:1" in report["reasons"]
    assert "words_behaviors_top20<3:0" in report["reasons"]
