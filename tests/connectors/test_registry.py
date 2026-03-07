from __future__ import annotations

from packages.connectors.registry import (
    build_source_plan_from_catalog,
    validate_runtime_source_cfgs,
)


def test_build_source_plan_excludes_evidence_only_and_includes_split_tiktok_sources() -> None:
    plan = build_source_plan_from_catalog(
        [
            {
                "sourceId": "TIKTOK_CREATIVE_CENTER_HASHTAGS",
                "enabled": True,
                "countryCodes": ["JP", "KR"],
            },
            {"sourceId": "X_SEARCH", "enabled": True},
        ]
    )

    source_ids = {entry["sourceId"] for entry in plan}

    assert "X_SEARCH" not in source_ids
    assert "TIKTOK_CREATIVE_CENTER_HASHTAGS" in source_ids


def test_validate_runtime_source_cfgs_detects_unknown_and_stale_ids() -> None:
    validation = validate_runtime_source_cfgs(
        [
            {"sourceId": "NOT_A_SOURCE"},
            {"sourceId": "X_SEARCH"},
        ]
    )

    assert "NOT_A_SOURCE" in validation["unknownConfigSourceIds"]
    assert "X_SEARCH" in validation["staleConfigSourceIds"]
