from __future__ import annotations

from batch import run as run_module


def test_build_publish_collections_keeps_public_even_when_gate_blocks_public() -> None:
    assert run_module._build_publish_collections(
        light_publish=False,
        shadow_only=True,
    ) == ("daily_rankings", "daily_rankings_v2_shadow")


def test_build_publish_collections_keeps_public_updates_in_light_shadow_mode() -> None:
    assert run_module._build_publish_collections(
        light_publish=True,
        shadow_only=True,
    ) == ("daily_rankings",)
