"""X Search (xAI) connector.

Two modes:
1. XTrendingConnector: Discover trending topics on X (regular source)
2. XSearchConnector: Evidence enrichment per-candidate (Step 9)

Spec reference: Section 8, Rule 7 (X Search)
API docs: https://docs.x.ai/developers/tools/x-search
"""

from __future__ import annotations

import json
import logging
import math
import re
from typing import Any

from packages.connectors.base import BaseConnector, FetchResult, SignalResult
from packages.core.llm_client import LLMClient
from packages.core.models import CandidateType, Evidence, RawCandidate

logger = logging.getLogger(__name__)

# Valid CandidateType values for parsing LLM output
_VALID_TYPES = {t.value for t in CandidateType}


class XTrendingConnector(BaseConnector):
    """Discover trending topics on X via xAI x_search (regular source)."""

    def __init__(
        self,
        api_key: str | None = None,
        max_results: int = 20,
        **kwargs: Any,
    ) -> None:
        super().__init__(source_id="X_TRENDING", stability="C", **kwargs)
        self.llm = LLMClient(api_key=api_key)
        self.max_results = max_results

    def fetch(self) -> FetchResult:
        """Fetch trending topics from X via xAI x_search tool."""
        if not self.llm.available:
            return FetchResult(error="XAI_API_KEY not set")

        messages = [
            {
                "role": "user",
                "content": (
                    "日本のX(Twitter)で直近24時間に話題になっている"
                    "人物、グループ、アーティスト、作品、キーワードを"
                    f"最大{self.max_results}件教えてください。\n"
                    "以下のJSON配列形式で回答してください:\n"
                    '[{"name": "名前", '
                    '"type": "PERSON / GROUP / WORK / MUSIC_ARTIST / MUSIC_TRACK / KEYWORD", '
                    '"engagement": いいね+RT概算数値, '
                    '"summary": "なぜ話題か1行"}]'
                ),
            }
        ]

        result = self.llm.chat(
            messages,
            temperature=0,
            tools=[{"type": "x_search"}],
            max_tokens=1500,
        )

        if result is None:
            return FetchResult(error="X trending search returned no result")

        items = _extract_json_array(result)
        return FetchResult(items=items, item_count=len(items))

    def extract_candidates(self, items: list[dict[str, Any]]) -> list[RawCandidate]:
        """Extract candidates from X trending items."""
        candidates: list[RawCandidate] = []

        for i, item in enumerate(items):
            name = item.get("name", "")
            if not name:
                continue

            type_str = item.get("type", "KEYWORD").strip()
            cand_type = (
                CandidateType(type_str) if type_str in _VALID_TYPES
                else CandidateType.KEYWORD
            )

            engagement = item.get("engagement", 0) or 0
            summary = item.get("summary", "")
            rank = i + 1

            evidence = Evidence(
                source_id=self.source_id,
                title=name,
                url="",
                snippet=summary[:200] if summary else "",
                metric=f"rank:{rank},engagement:{engagement}",
            )

            candidates.append(RawCandidate(
                name=name,
                type=cand_type,
                source_id=self.source_id,
                rank=rank,
                metric_value=math.log1p(engagement) if engagement > 0 else 1.0,
                evidence=evidence,
            ))

        return candidates

    def compute_signals(
        self, items: list[dict[str, Any]], candidates: list[RawCandidate]
    ) -> list[SignalResult]:
        """Compute signals from X trending data."""
        signals: dict[str, SignalResult] = {}

        for cand in candidates:
            key = cand.name
            if key in signals:
                signals[key].signal_value += cand.metric_value
            else:
                signals[key] = SignalResult(
                    candidate_name=key,
                    signal_value=cand.metric_value,
                    evidence=cand.evidence,
                )

        return list(signals.values())


class XSearchConnector(BaseConnector):
    """Connector for xAI X Search (evidence enrichment)."""

    def __init__(
        self,
        api_key: str | None = None,
        max_candidates: int = 30,
        **kwargs: Any,
    ) -> None:
        super().__init__(source_id="X_SEARCH", stability="B", **kwargs)
        self.llm = LLMClient(api_key=api_key)
        self.max_candidates = max_candidates

    def fetch(self) -> FetchResult:
        """Not used as a Discover source.

        X Search is called per-candidate via search_candidate().
        """
        return FetchResult(items=[], item_count=0)

    def search_candidate(self, candidate_name: str) -> list[Evidence]:
        """Search X posts related to a candidate name.

        Uses xAI's chat completions with x_search tool to find
        related posts. Returns a list of Evidence items.
        """
        if not self.llm.available:
            logger.warning("[%s] XAI_API_KEY not set", self.source_id)
            return []

        messages = [
            {
                "role": "user",
                "content": (
                    f"'{candidate_name}'について、"
                    "X(Twitter)で最近話題になっている投稿を3件まで教えてください。\n"
                    "以下のJSON配列形式で返してください:\n"
                    '[{"url": "投稿URL", "summary": "概要", '
                    '"likes": 数値, "retweets": 数値}]'
                ),
            }
        ]

        result = self.llm.chat(
            messages,
            temperature=0,
            tools=[{"type": "x_search"}],
            max_tokens=800,
        )

        if result is None:
            return []

        return self._parse_evidence(candidate_name, result)

    def _parse_evidence(self, candidate_name: str, content: str) -> list[Evidence]:
        """Parse LLM response into Evidence items."""
        evidence_list: list[Evidence] = []

        posts = _extract_json_array(content)

        if posts:
            for post in posts[:3]:
                url = post.get("url", "")
                summary = post.get("summary", "")
                likes = post.get("likes", 0)
                retweets = post.get("retweets", 0)

                evidence_list.append(Evidence(
                    source_id=self.source_id,
                    title=summary[:100] if summary else f"X post about {candidate_name}",
                    url=url,
                    metric=f"likes:{likes},RT:{retweets}",
                    snippet=summary[:200] if summary else "",
                ))
        else:
            # Fallback: use the raw content as a single evidence
            evidence_list.append(Evidence(
                source_id=self.source_id,
                title=f"X posts about {candidate_name}",
                url="",
                snippet=content[:200],
            ))

        return evidence_list

    def extract_candidates(self, items: list[dict[str, Any]]) -> list[RawCandidate]:
        """Not used (X Search is not a Discover source)."""
        return []

    def compute_signals(
        self, items: list[dict[str, Any]], candidates: list[RawCandidate]
    ) -> list[SignalResult]:
        """Not used (X Search is not a Discover source)."""
        return []


def _extract_json_array(text: str) -> list[dict[str, Any]]:
    """Try to extract a JSON array from text."""
    # Direct parse
    try:
        parsed = json.loads(text)
        if isinstance(parsed, list):
            return parsed
    except json.JSONDecodeError:
        pass

    # Find JSON in markdown code blocks
    json_match = re.search(r"```(?:json)?\s*\n?([\s\S]*?)\n?```", text)
    if json_match:
        try:
            parsed = json.loads(json_match.group(1))
            if isinstance(parsed, list):
                return parsed
        except json.JSONDecodeError:
            pass

    # Find JSON array pattern in text
    arr_match = re.search(r"\[[\s\S]*?\]", text)
    if arr_match:
        try:
            parsed = json.loads(arr_match.group())
            if isinstance(parsed, list):
                return parsed
        except json.JSONDecodeError:
            pass

    return []
