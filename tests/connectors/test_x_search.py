"""Tests for X Search connector."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

from packages.connectors.x_search import XSearchConnector, _extract_json_array


class TestExtractJsonArray:
    def test_direct_json(self) -> None:
        result = _extract_json_array('[{"url": "http://x.com/1", "summary": "test"}]')
        assert len(result) == 1
        assert result[0]["url"] == "http://x.com/1"

    def test_json_in_code_block(self) -> None:
        text = "Here are the results:\n```json\n[{\"url\": \"u1\"}]\n```"
        result = _extract_json_array(text)
        assert len(result) == 1

    def test_invalid_json_returns_empty(self) -> None:
        result = _extract_json_array("not json at all")
        assert result == []

    def test_json_embedded_in_text(self) -> None:
        text = "Found: [{\"url\": \"u1\", \"summary\": \"s1\"}] end."
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
            "choices": [{
                "message": {
                    "content": '[{"url": "https://x.com/post1", '
                               '"summary": "YOASOBI new song", '
                               '"likes": 5000, "retweets": 1200}]'
                }
            }]
        }
        mock_resp.raise_for_status.return_value = None
        mock_post.return_value = mock_resp

        connector = XSearchConnector(api_key="test-key")
        results = connector.search_candidate("YOASOBI")

        assert len(results) == 1
        assert results[0].source_id == "X_SEARCH"
        assert "YOASOBI" in results[0].title or "x.com" in results[0].url

    @patch("packages.core.llm_client.requests.post")
    def test_search_candidate_fallback_on_bad_json(self, mock_post: MagicMock) -> None:
        mock_resp = MagicMock()
        mock_resp.json.return_value = {
            "choices": [{
                "message": {"content": "No structured data, just text about the topic."}
            }]
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
