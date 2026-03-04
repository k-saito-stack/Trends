"""Google Trends connector.

Priority: Trends API alpha (if available), fallback to public trends page.

Spec reference: Section 8, Rule 3 (Google Trends)
API alpha: https://developers.google.com/search/apis/trends
"""

from __future__ import annotations

import logging
import math
from typing import Any

import requests

from packages.connectors.base import BaseConnector, FetchResult, SignalResult
from packages.core.models import CandidateType, Evidence, RawCandidate

logger = logging.getLogger(__name__)

# Google Trends Daily Trends RSS (public, no auth needed)
# Note: Old URL (.../trendingsearches/daily/rss) was deprecated (404).
GOOGLE_TRENDS_RSS_URL = "https://trends.google.com/trending/rss?geo=JP"


class GoogleTrendsConnector(BaseConnector):
    """Connector for Google Trends (public daily trends)."""

    def __init__(self, **kwargs: Any) -> None:
        super().__init__(
            source_id="TRENDS", stability="B",
            max_consecutive_failures=5, **kwargs,
        )

    def fetch(self) -> FetchResult:
        """Fetch Google Trends daily trending searches."""
        try:
            resp = requests.get(GOOGLE_TRENDS_RSS_URL, timeout=30)
            resp.raise_for_status()
            content = resp.text
        except requests.RequestException as e:
            return FetchResult(error=str(e))

        # Parse the RSS XML to extract trending items
        items = _parse_trends_rss(content)
        return FetchResult(items=items, item_count=len(items))

    def extract_candidates(self, items: list[dict[str, Any]]) -> list[RawCandidate]:
        """Extract keyword candidates from trending searches."""
        candidates: list[RawCandidate] = []

        for i, item in enumerate(items):
            rank = i + 1
            title = item.get("title", "")
            url = item.get("url", "")
            traffic = item.get("approx_traffic", "")

            if not title:
                continue

            evidence = Evidence(
                source_id=self.source_id,
                title=title,
                url=url,
                metric=f"rank:{rank},traffic:{traffic}",
            )

            candidates.append(RawCandidate(
                name=title,
                type=CandidateType.KEYWORD,
                source_id=self.source_id,
                rank=rank,
                metric_value=_rank_exposure(rank),
                evidence=evidence,
            ))

        return candidates

    def compute_signals(
        self, items: list[dict[str, Any]], candidates: list[RawCandidate]
    ) -> list[SignalResult]:
        """Compute daily signal using rank exposure (fallback mode)."""
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


def _parse_trends_rss(xml_content: str) -> list[dict[str, Any]]:
    """Parse Google Trends RSS XML into a list of items.

    Uses simple XML parsing to avoid heavy dependencies.
    """
    import xml.etree.ElementTree as ET

    items: list[dict[str, Any]] = []
    try:
        root = ET.fromstring(xml_content)
    except ET.ParseError:
        logger.warning("Failed to parse Google Trends RSS XML")
        return items

    # RSS namespace handling
    ns = {"ht": "https://trends.google.com/trending/rss"}

    for item_el in root.iter("item"):
        title_el = item_el.find("title")
        traffic_el = item_el.find("ht:approx_traffic", ns)

        title = title_el.text if title_el is not None and title_el.text else ""
        traffic = traffic_el.text if traffic_el is not None and traffic_el.text else ""

        # Get URL from first news_item (link tag points to feed itself)
        url = ""
        news_item = item_el.find("ht:news_item", ns)
        if news_item is not None:
            url_el = news_item.find("ht:news_item_url", ns)
            url = url_el.text if url_el is not None and url_el.text else ""

        if title:
            items.append({
                "title": title,
                "url": url,
                "approx_traffic": traffic,
            })

    return items


def _rank_exposure(rank: int) -> float:
    """E(rank) = 1 / log2(rank + 1)"""
    return 1.0 / math.log2(rank + 1)
