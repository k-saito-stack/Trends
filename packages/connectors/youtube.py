"""YouTube mostPopular JP connector.

Fetches trending videos via YouTube Data API v3 videos.list
with chart=mostPopular, regionCode=JP.

Spec reference: Section 8, Rule 1 (YouTube mostPopular)
API docs: https://developers.google.com/youtube/v3/docs/videos/list
"""

from __future__ import annotations

import logging
import math
import os
from typing import Any

import requests

from packages.connectors.base import BaseConnector, FetchResult, SignalResult
from packages.core.models import CandidateType, Evidence, RawCandidate

logger = logging.getLogger(__name__)

YOUTUBE_API_URL = "https://www.googleapis.com/youtube/v3/videos"


class YouTubeConnector(BaseConnector):
    """Connector for YouTube mostPopular chart (JP)."""

    def __init__(
        self,
        api_key: str | None = None,
        max_results: int = 50,
        emit_channel_candidate: bool = False,
        **kwargs: Any,
    ) -> None:
        super().__init__(source_id="YOUTUBE_TREND_JP", stability="A", **kwargs)
        self.api_key = api_key or os.environ.get("YOUTUBE_API_KEY", "")
        self.max_results = max_results
        self.emit_channel_candidate = emit_channel_candidate or (
            os.environ.get("YOUTUBE_EMIT_CHANNEL_CANDIDATE", "0") == "1"
        )

    def fetch(self) -> FetchResult:
        """Fetch mostPopular videos from YouTube Data API."""
        if not self.api_key:
            return FetchResult(error="YOUTUBE_API_KEY not set")

        params: dict[str, str | int] = {
            "part": "snippet,statistics",
            "chart": "mostPopular",
            "regionCode": "JP",
            "maxResults": self.max_results,
            "key": self.api_key,
        }

        try:
            resp = requests.get(YOUTUBE_API_URL, params=params, timeout=30)
            resp.raise_for_status()
            data = resp.json()
        except requests.RequestException as e:
            return FetchResult(error=str(e))

        items = data.get("items", [])
        return FetchResult(items=items, item_count=len(items))

    def extract_candidates(self, items: list[dict[str, Any]]) -> list[RawCandidate]:
        """Extract candidate names from video titles.

        Uses NER to extract PERSON/GROUP/WORK entities from video titles.
        Channel names stay in evidence metadata unless explicitly enabled.
        """
        from packages.core.ner import extract_entities

        candidates: list[RawCandidate] = []

        for i, item in enumerate(items):
            snippet = item.get("snippet", {})
            rank = i + 1
            video_id = item.get("id", "")
            video_url = f"https://www.youtube.com/watch?v={video_id}"
            title = snippet.get("title", "")
            channel = snippet.get("channelTitle", "")
            stats = item.get("statistics", {})
            view_count = stats.get("viewCount", "0")

            evidence = Evidence(
                source_id=self.source_id,
                title=title,
                url=video_url,
                published_at=snippet.get("publishedAt", ""),
                metric=f"rank:{rank},views:{view_count},channel:{channel}",
            )

            if self.emit_channel_candidate and channel:
                candidates.append(
                    RawCandidate(
                        name=channel,
                        type=CandidateType.PERSON,
                        source_id=self.source_id,
                        rank=rank,
                        metric_value=_rank_exposure(rank),
                        evidence=evidence,
                    )
                )

            # NER: extract entities from video title
            ner_found = False
            if title:
                entities = extract_entities(title, max_entities=5)
                for ent_text, ent_type in entities:
                    try:
                        cand_type = CandidateType(ent_type)
                    except ValueError:
                        cand_type = CandidateType.KEYWORD
                    candidates.append(
                        RawCandidate(
                            name=ent_text,
                            type=cand_type,
                            source_id=self.source_id,
                            rank=rank,
                            metric_value=_rank_exposure(rank),
                            evidence=evidence,
                        )
                    )
                    ner_found = True

            # Fallback: if NER found nothing, keep title as KEYWORD
            if not ner_found and title:
                candidates.append(
                    RawCandidate(
                        name=title,
                        type=CandidateType.KEYWORD,
                        source_id=self.source_id,
                        rank=rank,
                        metric_value=_rank_exposure(rank),
                        evidence=evidence,
                    )
                )

        return candidates

    def compute_signals(
        self, items: list[dict[str, Any]], candidates: list[RawCandidate]
    ) -> list[SignalResult]:
        """Compute daily signal x(s,q,t) using rank exposure E(rank).

        Signal: x_rank(s,q,t) = sum of E(rank_i) for all entries
        where candidate q appears.
        """
        # Aggregate exposure by candidate name
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


def _rank_exposure(rank: int) -> float:
    """E(rank) = 1 / log2(rank + 1)

    Higher rank (lower number) = higher exposure.
    Spec reference: Section 10.1
    """
    return 1.0 / math.log2(rank + 1)
