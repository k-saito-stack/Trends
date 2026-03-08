"""Tests for LLM client."""

from __future__ import annotations

import logging
from unittest.mock import MagicMock, patch

from packages.core.llm_client import LLMClient


class TestLLMClient:
    def test_not_available_without_key(self) -> None:
        client = LLMClient(api_key="")
        assert client.available is False

    def test_available_with_key(self) -> None:
        client = LLMClient(api_key="test-key")
        assert client.available is True

    def test_chat_returns_none_without_key(self) -> None:
        client = LLMClient(api_key="")
        result = client.chat([{"role": "user", "content": "hello"}])
        assert result is None

    @patch("packages.core.llm_client.requests.post")
    def test_chat_success(self, mock_post: MagicMock) -> None:
        mock_resp = MagicMock()
        mock_resp.json.return_value = {"choices": [{"message": {"content": "Hello!"}}]}
        mock_resp.raise_for_status.return_value = None
        mock_post.return_value = mock_resp

        client = LLMClient(api_key="test-key")
        result = client.chat([{"role": "user", "content": "hi"}])
        assert result == "Hello!"

    @patch("packages.core.llm_client.requests.post")
    def test_chat_json_parses_response(self, mock_post: MagicMock) -> None:
        mock_resp = MagicMock()
        mock_resp.json.return_value = {"choices": [{"message": {"content": '{"key": "value"}'}}]}
        mock_resp.raise_for_status.return_value = None
        mock_post.return_value = mock_resp

        client = LLMClient(api_key="test-key")
        result = client.chat_json([{"role": "user", "content": "json please"}])
        assert result == {"key": "value"}

    @patch("packages.core.llm_client.requests.post")
    def test_chat_json_extracts_from_code_block(self, mock_post: MagicMock) -> None:
        mock_resp = MagicMock()
        mock_resp.json.return_value = {
            "choices": [{"message": {"content": "Here:\n```json\n[1, 2, 3]\n```"}}]
        }
        mock_resp.raise_for_status.return_value = None
        mock_post.return_value = mock_resp

        client = LLMClient(api_key="test-key")
        result = client.chat_json([{"role": "user", "content": "array"}])
        assert result == [1, 2, 3]

    @patch("packages.core.llm_client.requests.post")
    def test_chat_handles_api_error(self, mock_post: MagicMock) -> None:
        import requests

        mock_post.side_effect = requests.RequestException("timeout")

        client = LLMClient(api_key="test-key")
        result = client.chat([{"role": "user", "content": "hi"}])
        assert result is None

    @patch("packages.core.llm_client.requests.post")
    def test_responses_text_does_not_log_response_body(self, mock_post: MagicMock, caplog) -> None:
        import requests

        response = MagicMock()
        response.status_code = 400
        response.headers = {"x-request-id": "req-123"}
        response.text = "super sensitive provider body"
        error = requests.RequestException("bad request")
        error.response = response
        mock_post.side_effect = error

        client = LLMClient(api_key="test-key")
        with caplog.at_level(logging.WARNING):
            result = client.responses_text([{"role": "user", "content": "hi"}])

        assert result is None
        assert "super sensitive provider body" not in caplog.text
        assert "req-123" in caplog.text
