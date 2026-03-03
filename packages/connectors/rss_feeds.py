"""RSS/News feed connector.

Fetches headlines from configured RSS feeds and extracts candidates
from article titles.

Spec reference: Section 8, Rule 4 (RSS/Official Feeds)
"""

from __future__ import annotations

import logging
import math
from typing import Any

import feedparser

from packages.connectors.base import BaseConnector, FetchResult, SignalResult
from packages.core.models import CandidateType, Evidence, RawCandidate

logger = logging.getLogger(__name__)

# Default RSS feeds (can be overridden via Firestore config)
DEFAULT_FEEDS = [
    "https://news.yahoo.co.jp/rss/topics/entertainment.xml",
    "https://news.yahoo.co.jp/rss/topics/it.xml",
]


class RSSFeedConnector(BaseConnector):
    """Connector for RSS/news feeds."""

    def __init__(
        self,
        feed_urls: list[str] | None = None,
        max_items_per_feed: int = 30,
        **kwargs: Any,
    ) -> None:
        super().__init__(source_id="NEWS_RSS", stability="B", **kwargs)
        self.feed_urls = feed_urls or DEFAULT_FEEDS
        self.max_items_per_feed = max_items_per_feed

    def fetch(self) -> FetchResult:
        """Fetch and parse all configured RSS feeds."""
        all_items: list[dict[str, Any]] = []

        for url in self.feed_urls:
            try:
                feed = feedparser.parse(url)
                if feed.bozo and not feed.entries:
                    logger.warning("[%s] Feed parse issue: %s", self.source_id, url)
                    continue

                for entry in feed.entries[: self.max_items_per_feed]:
                    all_items.append({
                        "title": entry.get("title", ""),
                        "url": entry.get("link", ""),
                        "published": entry.get("published", ""),
                        "feed_url": url,
                    })
            except Exception as e:
                logger.warning("[%s] Failed to fetch %s: %s", self.source_id, url, e)
                continue

        return FetchResult(items=all_items, item_count=len(all_items))

    def extract_candidates(self, items: list[dict[str, Any]]) -> list[RawCandidate]:
        """Extract candidates from article titles.

        For MVP, the full title is used as a KEYWORD candidate.
        The candidate engine's proper noun filter will handle noise.
        """
        candidates: list[RawCandidate] = []

        for item in items:
            title = item.get("title", "")
            url = item.get("url", "")

            if not title:
                continue

            evidence = Evidence(
                source_id=self.source_id,
                title=title,
                url=url,
                published_at=item.get("published", ""),
            )

            candidates.append(RawCandidate(
                name=title,
                type=CandidateType.KEYWORD,
                source_id=self.source_id,
                metric_value=1.0,  # count-based: each mention = 1
                evidence=evidence,
            ))

        return candidates

    def compute_signals(
        self, items: list[dict[str, Any]], candidates: list[RawCandidate]
    ) -> list[SignalResult]:
        """Compute daily signal: x_count(s,q,t) = log(1 + count).

        Counts how many times each candidate name appears across feeds.
        """
        mention_counts: dict[str, int] = {}
        evidence_map: dict[str, Evidence | None] = {}

        for cand in candidates:
            key = cand.name
            mention_counts[key] = mention_counts.get(key, 0) + 1
            if key not in evidence_map:
                evidence_map[key] = cand.evidence

        signals: list[SignalResult] = []
        for name, count in mention_counts.items():
            signals.append(SignalResult(
                candidate_name=name,
                signal_value=math.log1p(count),
                evidence=evidence_map.get(name),
            ))

        return signals
