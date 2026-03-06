from __future__ import annotations

from pathlib import Path

from packages.connectors.tiktok_creative_center import TikTokCreativeCenterConnector
from packages.core.models import CandidateType


def test_parse_tiktok_creative_center_fixture() -> None:
    html = Path("tests/fixtures/html/tiktok_creative_center/sample.html").read_text(
        encoding="utf-8"
    )
    connector = TikTokCreativeCenterConnector()
    items = connector.parse_items(html)
    candidates = connector.extract_candidates(items)

    assert len(items) == 3
    assert any(candidate.type == CandidateType.HASHTAG for candidate in candidates)
    assert any(candidate.name == "#ラブブチャレンジ" for candidate in candidates)
