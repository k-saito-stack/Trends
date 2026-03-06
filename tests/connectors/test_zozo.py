from __future__ import annotations

from pathlib import Path

from packages.connectors.zozo import ZozoConnector
from packages.core.models import CandidateType


def test_parse_zozo_fixture() -> None:
    html = Path("tests/fixtures/html/zozo/sample.html").read_text(encoding="utf-8")
    connector = ZozoConnector()
    items = connector.parse_items(html)
    candidates = connector.extract_candidates(items)

    assert items[0]["item_name"] == "ラインストーンバッグ"
    assert any(candidate.type == CandidateType.BRAND for candidate in candidates)
    assert any(candidate.name == "LOWRYS FARM" for candidate in candidates)
