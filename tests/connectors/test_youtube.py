"""Tests for YouTube connector."""

from __future__ import annotations

import json
import math
from pathlib import Path
from unittest.mock import MagicMock, patch

from packages.connectors.youtube import YouTubeConnector, _rank_exposure

FIXTURES_DIR = Path(__file__).parent / "fixtures"


class TestRankExposure:
    def test_rank_1(self) -> None:
        assert _rank_exposure(1) == 1.0 / math.log2(2)  # 1.0

    def test_rank_2(self) -> None:
        expected = 1.0 / math.log2(3)
        assert abs(_rank_exposure(2) - expected) < 1e-10

    def test_higher_rank_lower_exposure(self) -> None:
        assert _rank_exposure(1) > _rank_exposure(10)
        assert _rank_exposure(10) > _rank_exposure(50)


class TestYouTubeConnector:
    def _load_fixture(self) -> dict:
        with open(FIXTURES_DIR / "youtube_response.json", encoding="utf-8") as f:
            return json.load(f)

    def test_extract_candidates(self) -> None:
        connector = YouTubeConnector(api_key="test")
        data = self._load_fixture()
        candidates = connector.extract_candidates(data["items"])

        # 3 videos -> 3 channel-based candidates
        assert len(candidates) == 3

        # First candidate is YOASOBI (channel of video #1)
        assert candidates[0].name == "YOASOBI"
        assert candidates[0].source_id == "YOUTUBE_TREND_JP"
        assert candidates[0].rank == 1
        assert candidates[0].evidence is not None
        assert "abc123" in candidates[0].evidence.url

    def test_compute_signals_aggregates_same_channel(self) -> None:
        connector = YouTubeConnector(api_key="test")
        data = self._load_fixture()
        candidates = connector.extract_candidates(data["items"])
        signals = connector.compute_signals(data["items"], candidates)

        # YOASOBI appears twice (rank 1 and rank 3)
        yoasobi_signals = [s for s in signals if s.candidate_name == "YOASOBI"]
        assert len(yoasobi_signals) == 1

        expected = _rank_exposure(1) + _rank_exposure(3)
        assert abs(yoasobi_signals[0].signal_value - expected) < 1e-10

    def test_disabled_connector_returns_empty(self) -> None:
        connector = YouTubeConnector(api_key="test", enabled=False)
        candidates, signals = connector.run()
        assert candidates == []
        assert signals == []

    def test_missing_api_key_returns_error(self) -> None:
        connector = YouTubeConnector(api_key="")
        result = connector.fetch()
        assert result.error is not None
        assert "not set" in result.error

    @patch("packages.connectors.youtube.requests.get")
    def test_fetch_success(self, mock_get: MagicMock) -> None:
        data = self._load_fixture()
        mock_response = MagicMock()
        mock_response.json.return_value = data
        mock_response.raise_for_status.return_value = None
        mock_get.return_value = mock_response

        connector = YouTubeConnector(api_key="test_key")
        result = connector.fetch()

        assert result.error is None
        assert result.item_count == 3

    @patch("packages.connectors.youtube.requests.get")
    def test_fetch_http_error(self, mock_get: MagicMock) -> None:
        import requests

        mock_get.side_effect = requests.RequestException("timeout")
        connector = YouTubeConnector(api_key="test_key")
        result = connector.fetch()

        assert result.error is not None
        assert "timeout" in result.error
