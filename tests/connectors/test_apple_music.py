"""Tests for Apple Music RSS connector."""

from __future__ import annotations

import json
from pathlib import Path
from unittest.mock import MagicMock, patch

from packages.connectors.apple_music import AppleMusicConnector

FIXTURES_DIR = Path(__file__).parent / "fixtures"


class TestAppleMusicConnector:
    def _load_fixture(self) -> dict:
        with open(FIXTURES_DIR / "apple_rss_jp.json", encoding="utf-8") as f:
            return json.load(f)

    def test_source_id_includes_region(self) -> None:
        jp = AppleMusicConnector(region="JP")
        kr = AppleMusicConnector(region="KR")
        assert jp.source_id == "APPLE_MUSIC_JP"
        assert kr.source_id == "APPLE_MUSIC_KR"

    def test_extract_candidates(self) -> None:
        connector = AppleMusicConnector(region="JP")
        data = self._load_fixture()
        items = data["feed"]["results"]
        candidates = connector.extract_candidates(items)

        # 3 tracks -> 3 MUSIC_TRACK + 3 MUSIC_ARTIST = 6
        assert len(candidates) == 6

        tracks = [c for c in candidates if c.type.value == "MUSIC_TRACK"]
        artists = [c for c in candidates if c.type.value == "MUSIC_ARTIST"]
        assert len(tracks) == 3
        assert len(artists) == 3

        # First track
        assert tracks[0].name == "夜明け"
        assert tracks[0].extra.get("artist") == "YOASOBI"
        assert tracks[0].extra.get("region") == "JP"

    def test_compute_signals(self) -> None:
        connector = AppleMusicConnector(region="JP")
        data = self._load_fixture()
        items = data["feed"]["results"]
        candidates = connector.extract_candidates(items)
        signals = connector.compute_signals(items, candidates)

        # Each unique name gets one signal
        signal_names = {s.candidate_name for s in signals}
        assert "YOASOBI" in signal_names
        assert "ILLIT" in signal_names
        assert "Ado" in signal_names
        assert "夜明け" in signal_names

    @patch("packages.connectors.apple_music.requests.get")
    def test_fetch_success(self, mock_get: MagicMock) -> None:
        data = self._load_fixture()
        mock_response = MagicMock()
        mock_response.json.return_value = data
        mock_response.raise_for_status.return_value = None
        mock_get.return_value = mock_response

        connector = AppleMusicConnector(region="JP")
        result = connector.fetch()

        assert result.error is None
        assert result.item_count == 3

    @patch("packages.connectors.apple_music.requests.get")
    def test_full_run(self, mock_get: MagicMock) -> None:
        data = self._load_fixture()
        mock_response = MagicMock()
        mock_response.json.return_value = data
        mock_response.raise_for_status.return_value = None
        mock_get.return_value = mock_response

        connector = AppleMusicConnector(region="JP")
        result = connector.run()

        assert result.ok is True
        assert len(result.candidates) == 6
        assert len(result.signals) > 0
