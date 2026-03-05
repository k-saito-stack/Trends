"""Tests for Netflix Top 10 connector."""

from __future__ import annotations

import math
from pathlib import Path
from unittest.mock import MagicMock, patch

from packages.connectors.netflix import (
    NetflixTop10Connector,
    _rank_exposure,
    parse_top10_html,
)

FIXTURES_DIR = Path(__file__).parent / "fixtures"


class TestParseTop10Html:
    def _load_fixture(self) -> str:
        return (FIXTURES_DIR / "netflix_top10_jp.html").read_text(encoding="utf-8")

    def test_parses_three_items(self) -> None:
        html = self._load_fixture()
        items = parse_top10_html(html)
        assert len(items) == 3

    def test_rank_and_title(self) -> None:
        html = self._load_fixture()
        items = parse_top10_html(html)
        assert items[0]["rank"] == 1
        assert items[0]["title"] == "Under Ninja"
        assert items[1]["rank"] == 2
        assert "ドラえもん" in items[1]["title"]
        assert items[2]["rank"] == 3
        assert items[2]["title"] == "怪獣8号"


class TestNetflixTop10Connector:
    def _load_fixture(self) -> str:
        return (FIXTURES_DIR / "netflix_top10_jp.html").read_text(encoding="utf-8")

    def test_source_id_films(self) -> None:
        c = NetflixTop10Connector(category="films")
        assert c.source_id == "NETFLIX_FILMS_JP"

    def test_source_id_tv(self) -> None:
        c = NetflixTop10Connector(category="tv")
        assert c.source_id == "NETFLIX_TV_JP"

    def test_extract_candidates(self) -> None:
        connector = NetflixTop10Connector(category="films")
        html = self._load_fixture()
        items = parse_top10_html(html)
        candidates = connector.extract_candidates(items)

        # At least 3 WORK candidates (one per title)
        works = [c for c in candidates if c.type.value == "WORK"]
        assert len(works) == 3
        assert works[0].name == "Under Ninja"
        assert works[0].source_id == "NETFLIX_FILMS_JP"
        assert works[0].rank == 1

    def test_compute_signals(self) -> None:
        connector = NetflixTop10Connector(category="tv")
        html = self._load_fixture()
        items = parse_top10_html(html)
        candidates = connector.extract_candidates(items)
        signals = connector.compute_signals(items, candidates)

        signal_names = {s.candidate_name for s in signals}
        assert "Under Ninja" in signal_names
        assert "怪獣8号" in signal_names

    def test_rank_exposure_ordering(self) -> None:
        assert _rank_exposure(1) > _rank_exposure(5)
        assert _rank_exposure(5) > _rank_exposure(10)

    @patch("packages.connectors.netflix.requests.get")
    def test_fetch_success(self, mock_get: MagicMock) -> None:
        html = self._load_fixture()
        mock_response = MagicMock()
        mock_response.text = html
        mock_response.raise_for_status.return_value = None
        mock_get.return_value = mock_response

        connector = NetflixTop10Connector(category="films")
        result = connector.fetch()

        assert result.error is None
        assert result.item_count == 3

    @patch("packages.connectors.netflix.requests.get")
    def test_fetch_http_error(self, mock_get: MagicMock) -> None:
        import requests

        mock_get.side_effect = requests.RequestException("timeout")
        connector = NetflixTop10Connector(category="tv")
        result = connector.fetch()

        assert result.error is not None
        assert "timeout" in result.error

    @patch("packages.connectors.netflix.requests.get")
    def test_full_run(self, mock_get: MagicMock) -> None:
        html = self._load_fixture()
        mock_response = MagicMock()
        mock_response.text = html
        mock_response.raise_for_status.return_value = None
        mock_get.return_value = mock_response

        connector = NetflixTop10Connector(category="films")
        result = connector.run()

        assert result.ok is True
        assert len(result.candidates) >= 3
        assert len(result.signals) > 0

    def test_disabled_connector(self) -> None:
        connector = NetflixTop10Connector(category="tv", enabled=False)
        result = connector.run()
        assert result.ok is False
        assert result.candidates == []
