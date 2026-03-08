"""Tests for TVer ranking connector."""

from __future__ import annotations

from pathlib import Path
from unittest.mock import MagicMock, patch

from packages.connectors.tver import (
    TVerRankingConnector,
    _rank_exposure,
    parse_tver_ranking_html,
)

FIXTURES_DIR = Path(__file__).parent / "fixtures"


class TestParseTVerRankingHtml:
    def _load_fixture(self) -> str:
        return (FIXTURES_DIR / "tver_ranking.html").read_text(encoding="utf-8")

    def test_parses_three_items(self) -> None:
        html = self._load_fixture()
        items = parse_tver_ranking_html(html)
        assert len(items) == 3

    def test_rank_and_title(self) -> None:
        html = self._load_fixture()
        items = parse_tver_ranking_html(html)
        assert items[0]["rank"] == 1
        assert items[0]["title"] == "リブート"
        assert items[1]["rank"] == 2
        assert items[1]["title"] == "未来のムスコ"
        assert items[2]["rank"] == 3
        assert items[2]["title"] == "月曜から夜ふかし"

    def test_cast_extraction(self) -> None:
        html = self._load_fixture()
        items = parse_tver_ranking_html(html)
        assert "鈴木亮平" in items[0]["cast"]
        assert "戸田恵梨香" in items[0]["cast"]
        assert "永瀬廉" in items[0]["cast"]
        assert "村上信五" in items[2]["cast"]
        assert "マツコ・デラックス" in items[2]["cast"]

    def test_points(self) -> None:
        html = self._load_fixture()
        items = parse_tver_ranking_html(html)
        assert items[0]["points"] == 588
        assert items[1]["points"] == 579
        assert items[2]["points"] == 521


class TestTVerRankingConnector:
    def _load_fixture(self) -> str:
        return (FIXTURES_DIR / "tver_ranking.html").read_text(encoding="utf-8")

    def test_source_id(self) -> None:
        c = TVerRankingConnector()
        assert c.source_id == "TVER_RANKING_JP"

    def test_extract_candidates(self) -> None:
        connector = TVerRankingConnector()
        html = self._load_fixture()
        items = parse_tver_ranking_html(html)
        candidates = connector.extract_candidates(items)

        works = [c for c in candidates if c.type.value == "SHOW"]
        persons = [c for c in candidates if c.type.value == "PERSON"]
        assert len(works) == 3
        assert len(persons) == 0

        assert works[0].name == "リブート"
        assert works[0].extra.get("cast") is not None

    def test_compute_signals(self) -> None:
        connector = TVerRankingConnector()
        html = self._load_fixture()
        items = parse_tver_ranking_html(html)
        candidates = connector.extract_candidates(items)
        signals = connector.compute_signals(items, candidates)

        signal_names = {s.candidate_name for s in signals}
        assert "リブート" in signal_names
        assert "鈴木亮平" not in signal_names
        assert "マツコ・デラックス" not in signal_names

    def test_emit_cast_direct_can_be_enabled(self) -> None:
        connector = TVerRankingConnector(emit_cast_direct=True)
        html = self._load_fixture()
        items = parse_tver_ranking_html(html)
        candidates = connector.extract_candidates(items)

        persons = [c for c in candidates if c.type.value == "PERSON"]
        assert len(persons) >= 7
        assert persons[0].extra.get("derivedFromWork") is True

    def test_rank_exposure_ordering(self) -> None:
        assert _rank_exposure(1) > _rank_exposure(5)
        assert _rank_exposure(5) > _rank_exposure(10)

    @patch("packages.connectors.tver.requests.get")
    def test_fetch_success(self, mock_get: MagicMock) -> None:
        html = self._load_fixture()
        mock_response = MagicMock()
        mock_response.text = html
        mock_response.raise_for_status.return_value = None
        mock_get.return_value = mock_response

        connector = TVerRankingConnector()
        result = connector.fetch()

        assert result.error is None
        assert result.item_count == 3

    @patch("packages.connectors.tver.requests.get")
    def test_fetch_http_error(self, mock_get: MagicMock) -> None:
        import requests

        mock_get.side_effect = requests.RequestException("timeout")
        connector = TVerRankingConnector()
        result = connector.fetch()

        assert result.error is not None
        assert "timeout" in result.error

    @patch("packages.connectors.tver.requests.get")
    def test_full_run(self, mock_get: MagicMock) -> None:
        html = self._load_fixture()
        mock_response = MagicMock()
        mock_response.text = html
        mock_response.raise_for_status.return_value = None
        mock_get.return_value = mock_response

        connector = TVerRankingConnector()
        result = connector.run()

        assert result.ok is True
        assert len(result.candidates) >= 3
        assert len(result.signals) > 0

    def test_disabled_connector(self) -> None:
        connector = TVerRankingConnector(enabled=False)
        result = connector.run()
        assert result.ok is False
        assert result.candidates == []
