"""Rakuten Books magazine search connector.

Searches for recent magazines to detect cross-publisher signals.

Spec reference: Section 8, Rule 5 (Rakuten Books)
API docs: https://webservice.rakuten.co.jp/documentation/books-magazine-search
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

RAKUTEN_API_URL = "https://app.rakuten.co.jp/services/api/BooksMagazine/Search/20170404"


class RakutenMagazineConnector(BaseConnector):
    """Connector for Rakuten Books magazine search."""

    def __init__(
        self,
        app_id: str | None = None,
        max_results: int = 30,
        **kwargs: Any,
    ) -> None:
        super().__init__(source_id="RAKUTEN_MAG", stability="B", **kwargs)
        self.app_id = app_id or os.environ.get("RAKUTEN_APP_ID", "")
        self.max_results = max_results

    def fetch(self) -> FetchResult:
        """Fetch recent magazines from Rakuten Books API."""
        if not self.app_id:
            return FetchResult(error="RAKUTEN_APP_ID not set")

        params: dict[str, Any] = {
            "applicationId": self.app_id,
            "sort": "-releaseDate",
            "hits": self.max_results,
            "outOfStockFlag": 0,
        }

        try:
            resp = requests.get(RAKUTEN_API_URL, params=params, timeout=30)
            resp.raise_for_status()
            data = resp.json()
        except requests.RequestException as e:
            return FetchResult(error=str(e))

        items_raw = data.get("Items", [])
        items = [item.get("Item", item) for item in items_raw]
        return FetchResult(items=items, item_count=len(items))

    def extract_candidates(self, items: list[dict[str, Any]]) -> list[RawCandidate]:
        """Extract candidates from magazine titles and descriptions."""
        candidates: list[RawCandidate] = []

        for item in items:
            title = item.get("title", "")
            url = item.get("itemUrl", "")
            caption = item.get("itemCaption", "")

            if not title:
                continue

            evidence = Evidence(
                source_id=self.source_id,
                title=title,
                url=url,
                published_at=item.get("salesDate", ""),
            )

            # Magazine title as KEYWORD candidate
            candidates.append(RawCandidate(
                name=title,
                type=CandidateType.KEYWORD,
                source_id=self.source_id,
                metric_value=1.0,
                evidence=evidence,
                extra={"caption": caption[:200] if caption else ""},
            ))

        return candidates

    def compute_signals(
        self, items: list[dict[str, Any]], candidates: list[RawCandidate]
    ) -> list[SignalResult]:
        """Compute daily signal: x_count = log(1 + matchCount)."""
        mention_counts: dict[str, int] = {}
        evidence_map: dict[str, Evidence | None] = {}

        for cand in candidates:
            key = cand.name
            mention_counts[key] = mention_counts.get(key, 0) + 1
            if key not in evidence_map:
                evidence_map[key] = cand.evidence

        return [
            SignalResult(
                candidate_name=name,
                signal_value=math.log1p(count),
                evidence=evidence_map.get(name),
            )
            for name, count in mention_counts.items()
        ]
