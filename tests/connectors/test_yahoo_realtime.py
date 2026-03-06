from __future__ import annotations

from pathlib import Path

from packages.connectors.yahoo_realtime import YahooRealtimeConnector
from packages.core.models import CandidateType


def test_parse_yahoo_realtime_fixture() -> None:
    html = Path("tests/fixtures/html/yahoo_realtime/sample.html").read_text(encoding="utf-8")
    connector = YahooRealtimeConnector()
    items = connector.parse_items(html)
    candidates = connector.extract_candidates(items)

    assert items[0]["keyword"] == "#平成レトロ"
    assert any(candidate.type == CandidateType.HASHTAG for candidate in candidates)
    assert any(candidate.name == "ラブブをつける" for candidate in candidates)
