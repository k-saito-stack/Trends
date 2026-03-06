"""Wikipedia Pageviews connector (Power score).

Fetches page view counts for known candidates to measure
baseline popularity ("power"). NOT used for TrendScore
calculation in MVP (display only).

Spec reference: Section 8, Rule 6 (Wikipedia Pageviews)
API docs: https://doc.wikimedia.org/generated-data-platform/aqs/analytics-api/reference/page-views.html
"""

from __future__ import annotations

import logging
import math
from typing import Any

import requests

from packages.connectors.base import BaseConnector, FetchResult, SignalResult
from packages.core.models import RawCandidate

logger = logging.getLogger(__name__)

WIKIMEDIA_PV_URL = (
    "https://wikimedia.org/api/rest_v1/metrics/pageviews/per-article"
    "/ja.wikipedia.org/all-access/all-agents"
)


class WikipediaConnector(BaseConnector):
    """Connector for Wikipedia Pageviews (power score, display only)."""

    def __init__(self, **kwargs: Any) -> None:
        super().__init__(source_id="WIKI_PAGEVIEWS", stability="A", **kwargs)

    def fetch(self) -> FetchResult:
        """Not used as a Discover source.

        Wikipedia is queried per-candidate after Top candidates
        are determined. This returns empty for the standard pipeline.
        """
        return FetchResult(items=[], item_count=0)

    def fetch_pageviews(self, wiki_title: str, start_date: str, end_date: str) -> int | None:
        """Fetch pageview count for a specific Wikipedia article.

        Args:
            wiki_title: Wikipedia article title (URL-encoded)
            start_date: YYYYMMDD format
            end_date: YYYYMMDD format

        Returns:
            Total pageviews in the period, or None on failure.
        """
        url = f"{WIKIMEDIA_PV_URL}/{wiki_title}/daily/{start_date}/{end_date}"
        headers = {
            "User-Agent": "TrendsBot/1.0 (trend detection platform)",
        }

        try:
            resp = requests.get(url, headers=headers, timeout=30)
            resp.raise_for_status()
            data = resp.json()
        except requests.RequestException as e:
            logger.warning("[%s] Failed for %s: %s", self.source_id, wiki_title, e)
            return None

        items = data.get("items", [])
        total: int = sum(item.get("views", 0) for item in items)
        return total

    def compute_power_score(self, pageviews: int) -> float:
        """power(q,t) = log(1 + pageviews)

        Spec: NOT added to TrendScore in MVP. Display only.
        """
        return math.log1p(pageviews)

    def extract_candidates(self, items: list[dict[str, Any]]) -> list[RawCandidate]:
        """Not used (Wikipedia is not a Discover source)."""
        return []

    def compute_signals(
        self, items: list[dict[str, Any]], candidates: list[RawCandidate]
    ) -> list[SignalResult]:
        """Not used (Wikipedia is not a Discover source)."""
        return []
