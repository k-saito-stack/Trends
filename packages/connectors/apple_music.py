"""Apple Music RSS connector (regional Apple charts).

Fetches top songs/albums from Apple's RSS Generator JSON feed.

Spec reference: Section 8, Rule 2 (Apple RSS)
RSS Builder: https://rss.marketingtools.apple.com/
"""

from __future__ import annotations

import logging
import math
from typing import Any

import requests

from packages.connectors.base import BaseConnector, FetchResult, SignalResult
from packages.core.models import CandidateType, Evidence, RawCandidate

logger = logging.getLogger(__name__)

# Apple RSS Generator JSON endpoints
APPLE_RSS_URLS = {
    "JP": "https://rss.marketingtools.apple.com/api/v2/jp/music/most-played/50/songs.json",
    "KR": "https://rss.marketingtools.apple.com/api/v2/kr/music/most-played/50/songs.json",
    "GLOBAL": "https://rss.marketingtools.apple.com/api/v2/us/music/most-played/50/songs.json",
}


class AppleMusicConnector(BaseConnector):
    """Connector for Apple Music RSS (one region per instance)."""

    def __init__(
        self,
        region: str = "JP",
        max_results: int = 50,
        **kwargs: Any,
    ) -> None:
        source_id = f"APPLE_MUSIC_{region}"
        super().__init__(source_id=source_id, stability="A", **kwargs)
        self.region = region
        self.max_results = max_results
        self.feed_url = APPLE_RSS_URLS.get(region, APPLE_RSS_URLS["JP"])

    def fetch(self) -> FetchResult:
        """Fetch the Apple Music RSS JSON feed."""
        try:
            resp = requests.get(self.feed_url, timeout=30)
            resp.raise_for_status()
            data = resp.json()
        except requests.RequestException as e:
            return FetchResult(error=str(e))

        # Apple RSS structure: feed.results[]
        feed = data.get("feed", {})
        results = feed.get("results", [])
        items = results[: self.max_results]
        return FetchResult(items=items, item_count=len(items))

    def extract_candidates(self, items: list[dict[str, Any]]) -> list[RawCandidate]:
        """Extract track and artist candidates from RSS items."""
        candidates: list[RawCandidate] = []

        for i, item in enumerate(items):
            rank = i + 1
            track_name = item.get("name", "")
            artist_name = item.get("artistName", "")
            track_url = item.get("url", "")

            evidence = Evidence(
                source_id=self.source_id,
                title=f"{track_name} - {artist_name}",
                url=track_url,
                metric=f"rank:{rank}",
            )

            # Track as MUSIC_TRACK candidate
            if track_name:
                candidates.append(
                    RawCandidate(
                        name=track_name,
                        type=CandidateType.MUSIC_TRACK,
                        source_id=self.source_id,
                        rank=rank,
                        metric_value=_rank_exposure(rank),
                        evidence=evidence,
                        extra={"artist": artist_name, "region": self.region},
                    )
                )

            # Artist as MUSIC_ARTIST candidate
            if artist_name:
                candidates.append(
                    RawCandidate(
                        name=artist_name,
                        type=CandidateType.MUSIC_ARTIST,
                        source_id=self.source_id,
                        rank=rank,
                        metric_value=_rank_exposure(rank),
                        evidence=evidence,
                        extra={"track": track_name, "region": self.region},
                    )
                )

        return candidates

    def compute_signals(
        self, items: list[dict[str, Any]], candidates: list[RawCandidate]
    ) -> list[SignalResult]:
        """Compute daily signal using rank exposure.

        Aggregates E(rank) per candidate name.
        Regional weighting (for example JP/KR) is applied
        later in the scoring engine, NOT here.
        """
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
    """E(rank) = 1 / log2(rank + 1)"""
    return 1.0 / math.log2(rank + 1)
