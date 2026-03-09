from __future__ import annotations

from batch import run as run_module


def test_build_source_plan_applies_include_exclude_and_skip_slow() -> None:
    source_cfgs = [
        {"sourceId": "TRENDS_JP_24H_ENT", "enabled": True},
        {"sourceId": "YAHOO_REALTIME", "enabled": True},
        {"sourceId": "APPLE_MUSIC_JP", "enabled": True},
    ]
    options = run_module.BatchRuntimeOptions(
        skip_slow_sources=True,
        source_include=("TRENDS_JP_24H_ENT", "YAHOO_REALTIME", "APPLE_MUSIC_JP"),
        source_exclude=("YAHOO_REALTIME",),
    )

    plan = run_module.build_source_plan(source_cfgs, options)

    assert [entry["sourceId"] for entry in plan] == ["APPLE_MUSIC_JP"]


def test_build_publish_collections_keeps_public_collection_for_shadow_only() -> None:
    assert run_module._build_publish_collections(
        light_publish=False,
        shadow_only=True,
    ) == ("daily_rankings", "daily_rankings_v2_shadow")


def test_parse_csv_arg_dedupes_values() -> None:
    parsed = run_module._parse_csv_arg(["TRENDS,APPLE_MUSIC_JP", "APPLE_MUSIC_JP", "YOUTUBE"])

    assert parsed == ("TRENDS", "APPLE_MUSIC_JP", "YOUTUBE")
