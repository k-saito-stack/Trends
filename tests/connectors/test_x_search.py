"""Tests for X Search connector."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from unittest.mock import MagicMock, patch

from packages.connectors.x_search import (
    XSearchConnector,
    XTrendingConnector,
    _extract_json_array,
    _x_search_tool,
)


class TestExtractJsonArray:
    def test_direct_json(self) -> None:
        result = _extract_json_array('[{"url": "http://x.com/1", "summary": "test"}]')
        assert len(result) == 1
        assert result[0]["url"] == "http://x.com/1"

    def test_json_in_code_block(self) -> None:
        text = 'Here are the results:\n```json\n[{"url": "u1"}]\n```'
        result = _extract_json_array(text)
        assert len(result) == 1

    def test_invalid_json_returns_empty(self) -> None:
        result = _extract_json_array("not json at all")
        assert result == []

    def test_json_embedded_in_text(self) -> None:
        text = 'Found: [{"url": "u1", "summary": "s1"}] end.'
        result = _extract_json_array(text)
        assert len(result) == 1


class TestXSearchConnector:
    def test_no_api_key_returns_empty(self) -> None:
        connector = XSearchConnector(api_key="")
        result = connector.search_candidate("YOASOBI")
        assert result == []

    @patch("packages.core.llm_client.requests.post")
    def test_search_candidate_success(self, mock_post: MagicMock) -> None:
        mock_resp = MagicMock()
        mock_resp.json.return_value = {
            "output": [
                {
                    "type": "message",
                    "content": [
                        {
                            "type": "output_text",
                            "text": '[{"url": "https://x.com/post1", '
                            '"summary": "YOASOBI new song", '
                            '"likes": 5000, "retweets": 1200}]',
                        }
                    ],
                }
            ]
        }
        mock_resp.raise_for_status.return_value = None
        mock_post.return_value = mock_resp

        connector = XSearchConnector(api_key="test-key")
        results = connector.search_candidate("YOASOBI")

        assert len(results) == 1
        assert results[0].source_id == "X_SEARCH"
        assert "YOASOBI" in results[0].title or "x.com" in results[0].url
        tool = mock_post.call_args.kwargs["json"]["tools"][0]
        assert tool["type"] == "x_search"
        assert "from_date" in tool
        assert "to_date" in tool

    @patch("packages.core.llm_client.requests.post")
    def test_search_candidate_fallback_on_bad_json(self, mock_post: MagicMock) -> None:
        mock_resp = MagicMock()
        mock_resp.json.return_value = {
            "output": [
                {
                    "type": "message",
                    "content": [
                        {
                            "type": "output_text",
                            "text": "No structured data, just text about the topic.",
                        }
                    ],
                }
            ]
        }
        mock_resp.raise_for_status.return_value = None
        mock_post.return_value = mock_resp

        connector = XSearchConnector(api_key="test-key")
        results = connector.search_candidate("TestCandidate")

        # Should still return fallback evidence
        assert len(results) == 1
        assert results[0].source_id == "X_SEARCH"

    def test_parse_evidence_with_posts(self) -> None:
        connector = XSearchConnector(api_key="test-key")
        content = '[{"url": "u1", "summary": "s1", "likes": 100, "retweets": 50}]'
        results = connector._parse_evidence("Test", content)
        assert len(results) == 1
        assert results[0].metric == "likes:100,RT:50"

    def test_fetch_returns_empty(self) -> None:
        connector = XSearchConnector(api_key="")
        result = connector.fetch()
        assert result.items == []
        assert result.item_count == 0


class TestXTrendingConnector:
    @patch("packages.core.llm_client.requests.post")
    def test_fetch_success_uses_x_search(self, mock_post: MagicMock) -> None:
        mock_resp = MagicMock()
        mock_resp.json.return_value = {
            "output": [
                {
                    "type": "message",
                    "content": [
                        {
                            "type": "output_text",
                            "text": '[{"name": "YOASOBI", "type": "MUSIC_ARTIST", '
                            '"engagement": 5000, "summary": "new release"}]',
                        }
                    ],
                }
            ]
        }
        mock_resp.raise_for_status.return_value = None
        mock_post.return_value = mock_resp

        connector = XTrendingConnector(api_key="test-key", max_results=10)
        result = connector.fetch()

        assert result.error is None
        assert result.item_count == 1
        assert result.items[0]["name"] == "YOASOBI"
        tool = mock_post.call_args.kwargs["json"]["tools"][0]
        assert tool["type"] == "x_search"
        assert "from_date" in tool
        assert "to_date" in tool


class TestXSearchTool:
    def test_tool_contains_date_window(self) -> None:
        tool = _x_search_tool(days_back=3)
        today = datetime.now(timezone(timedelta(hours=9))).date()
        assert tool["type"] == "x_search"
        assert tool["to_date"] == today.isoformat()
        assert tool["from_date"] == (today - timedelta(days=3)).isoformat()
