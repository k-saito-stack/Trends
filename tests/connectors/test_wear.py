from __future__ import annotations

from pathlib import Path

from packages.connectors.wear import WearConnector
from packages.core.models import CandidateType


def test_parse_wear_fixture() -> None:
    html = Path("tests/fixtures/html/wear/sample.html").read_text(encoding="utf-8")
    connector = WearConnector()
    items = connector.parse_items(html)
    candidates = connector.extract_candidates(items)

    assert len(items) == 3
    assert any(candidate.type == CandidateType.STYLE for candidate in candidates)
    assert any(candidate.name == "スポーツサングラス" for candidate in candidates)
