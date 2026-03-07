from __future__ import annotations

from pathlib import Path

from packages.connectors.spotify_charts import SpotifyChartsConnector
from packages.core.models import CandidateType


def test_parse_spotify_charts_fixture() -> None:
    html = Path("tests/fixtures/html/spotify_charts/sample.html").read_text(encoding="utf-8")
    connector = SpotifyChartsConnector()
    items = connector.parse_items(html)
    candidates = connector.extract_candidates(items)

    assert items[0]["track"] == "ダーリン"
    assert items[1]["artist"] == "米津玄師"
    assert any(candidate.type == CandidateType.MUSIC_TRACK for candidate in candidates)
    assert any(candidate.name == "Mrs. GREEN APPLE" for candidate in candidates)
