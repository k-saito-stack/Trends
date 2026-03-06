"""LLM client for xAI API (Grok).

Shared client used by:
- summary.py (candidate card summaries)
- x_search.py (evidence enrichment)

Spec reference: Section 10.9 (Summary), Section 8 Rule 7 (X Search)
"""

from __future__ import annotations

import json
import logging
import os
from typing import Any

import requests

logger = logging.getLogger(__name__)

XAI_API_URL = "https://api.x.ai/v1/chat/completions"


class LLMClient:
    """Client for xAI chat completions API."""

    def __init__(
        self,
        api_key: str | None = None,
        model: str = "grok-4-1-fast-non-reasoning",
        timeout: int = 60,
    ) -> None:
        self.api_key = api_key or os.environ.get("XAI_API_KEY", "")
        self.model = model
        self.timeout = timeout

    @property
    def available(self) -> bool:
        """Check if API key is configured."""
        return bool(self.api_key)

    def chat(
        self,
        messages: list[dict[str, str]],
        temperature: float = 0,
        tools: list[dict[str, str]] | None = None,
        max_tokens: int = 500,
    ) -> str | None:
        """Send a chat completion request.

        Args:
            messages: List of {"role": ..., "content": ...} dicts
            temperature: Sampling temperature (0 = deterministic)
            tools: Optional tools list (e.g. [{"type": "live_search"}])
            max_tokens: Maximum response tokens

        Returns:
            Assistant's response content, or None on failure
        """
        if not self.api_key:
            logger.warning("XAI_API_KEY not set")
            return None

        headers = {
            "Authorization": f"Bearer {self.api_key}",
            "Content-Type": "application/json",
        }

        payload: dict[str, Any] = {
            "model": self.model,
            "messages": messages,
            "temperature": temperature,
            "max_tokens": max_tokens,
        }
        if tools:
            payload["tools"] = tools

        try:
            resp = requests.post(
                XAI_API_URL, json=payload, headers=headers, timeout=self.timeout
            )
            resp.raise_for_status()
            data = resp.json()
        except requests.RequestException as e:
            logger.warning("xAI API call failed: %s", e)
            return None

        choices = data.get("choices", [])
        if not choices:
            return None

        message = choices[0].get("message", {})
        content: str | None = message.get("content")
        return content

    def chat_json(
        self,
        messages: list[dict[str, str]],
        temperature: float = 0,
        tools: list[dict[str, str]] | None = None,
    ) -> dict[str, Any] | list[Any] | None:
        """Send a chat request and parse the response as JSON.

        Returns parsed JSON, or None on failure.
        """
        content = self.chat(messages, temperature=temperature, tools=tools)
        if content is None:
            return None

        # Try to extract JSON from the response
        try:
            return json.loads(content)  # type: ignore[no-any-return]
        except json.JSONDecodeError:
            pass

        # Try to find JSON in markdown code blocks
        import re
        json_match = re.search(r"```(?:json)?\s*\n?([\s\S]*?)\n?```", content)
        if json_match:
            try:
                return json.loads(json_match.group(1))  # type: ignore[no-any-return]
            except json.JSONDecodeError:
                pass

        logger.warning("Failed to parse JSON from LLM response")
        return None
