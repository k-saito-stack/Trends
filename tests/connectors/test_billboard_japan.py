from __future__ import annotations

from pathlib import Path

from packages.connectors.billboard_japan import BillboardJapanConnector
from packages.core.models import CandidateType


def test_parse_billboard_fixture() -> None:
    html = Path("tests/fixtures/html/billboard_japan/sample.html").read_text(encoding="utf-8")
    connector = BillboardJapanConnector()
    items = connector.parse_items(html)
    candidates = connector.extract_candidates(items)

    assert items[0]["track"] == "BOW AND ARROW"
    assert any(candidate.type == CandidateType.MUSIC_TRACK for candidate in candidates)
    assert any(candidate.name == "米津玄師" for candidate in candidates)
