from __future__ import annotations

from pathlib import Path

from packages.connectors.rakuten_fashion import RakutenFashionConnector
from packages.core.models import CandidateType


def test_parse_rakuten_fashion_fixture() -> None:
    html = Path("tests/fixtures/html/rakuten_fashion/sample.html").read_text(encoding="utf-8")
    connector = RakutenFashionConnector()
    items = connector.parse_items(html)
    candidates = connector.extract_candidates(items)

    assert items[0]["brand"] == "RANDA"
    assert any(candidate.type == CandidateType.PRODUCT for candidate in candidates)
    assert any(candidate.name == "VIS" for candidate in candidates)
