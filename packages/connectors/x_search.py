"""X Search (xAI) connector.

Used for evidence enrichment of top candidates (NOT as a Discover source).
Searches X (Twitter) posts related to candidates for corroboration.

Spec reference: Section 8, Rule 7 (X Search)
API docs: https://docs.x.ai/developers/tools/x-search
"""

from __future__ import annotations

import logging
import os
from typing import Any

import requests

from packages.connectors.base import BaseConnector, FetchResult, SignalResult
from packages.core.models import RawCandidate

logger = logging.getLogger(__name__)

XAI_API_URL = "https://api.x.ai/v1/chat/completions"


class XSearchConnector(BaseConnector):
    """Connector for xAI X Search (evidence enrichment)."""

    def __init__(
        self,
        api_key: str | None = None,
        max_candidates: int = 30,
        **kwargs: Any,
    ) -> None:
        super().__init__(source_id="X_SEARCH", stability="B", **kwargs)
        self.api_key = api_key or os.environ.get("XAI_API_KEY", "")
        self.max_candidates = max_candidates

    def fetch(self) -> FetchResult:
        """Not used as a Discover source.

        X Search is called per-candidate via search_candidate().
        """
        return FetchResult(items=[], item_count=0)

    def search_candidate(self, candidate_name: str) -> dict[str, Any] | None:
        """Search X posts related to a candidate name.

        Uses xAI's chat completions with x_search tool to find
        related posts. Returns a summary dict or None on failure.
        """
        if not self.api_key:
            logger.warning("[%s] XAI_API_KEY not set", self.source_id)
            return None

        headers = {
            "Authorization": f"Bearer {self.api_key}",
            "Content-Type": "application/json",
        }

        payload = {
            "model": "grok-3-mini",
            "messages": [
                {
                    "role": "user",
                    "content": (
                        f"'{candidate_name}'について、"
                        "X(Twitter)で最近話題になっている投稿を3件まで教えてください。"
                        "各投稿のURL、概要、反応数（いいね/RT）を簡潔にJSON形式で返してください。"
                    ),
                }
            ],
            "tools": [{"type": "x_search"}],
            "temperature": 0,
        }

        try:
            resp = requests.post(
                XAI_API_URL, json=payload, headers=headers, timeout=60
            )
            resp.raise_for_status()
            data = resp.json()
        except requests.RequestException as e:
            logger.warning("[%s] Search failed for %s: %s",
                           self.source_id, candidate_name, e)
            return None

        # Extract the assistant's response
        choices = data.get("choices", [])
        if not choices:
            return None

        message = choices[0].get("message", {})
        content = message.get("content", "")

        return {
            "candidate_name": candidate_name,
            "content": content,
            "source_id": self.source_id,
        }

    def extract_candidates(self, items: list[dict[str, Any]]) -> list[RawCandidate]:
        """Not used (X Search is not a Discover source)."""
        return []

    def compute_signals(
        self, items: list[dict[str, Any]], candidates: list[RawCandidate]
    ) -> list[SignalResult]:
        """Not used (X Search is not a Discover source)."""
        return []
