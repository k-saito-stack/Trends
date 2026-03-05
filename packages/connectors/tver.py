"""TVer daily ranking connector.

Fetches TVer viewing-point rankings from achikochi-data.com
(third-party aggregation site, daily updated).

Source: https://achikochi-data.com/tver_daily_ranking_view_count_point_ranking_all/
"""

from __future__ import annotations

import logging
import math
import re
from typing import Any

import requests

from packages.connectors.base import BaseConnector, FetchResult, SignalResult
from packages.core.models import CandidateType, Evidence, RawCandidate

logger = logging.getLogger(__name__)

TVER_RANKING_URL = (
    "https://achikochi-data.com"
    "/tver_daily_ranking_view_count_point_ranking_all/"
)


def parse_tver_ranking_html(html: str) -> list[dict[str, Any]]:
    """Extract ranking items from achikochi-data TVer ranking page.

    Returns list of dicts with keys: rank, title, cast, points.
    """
    items: list[dict[str, Any]] = []
    rows = re.findall(r"<tr>(.*?)</tr>", html, re.DOTALL)

    for row in rows:
        rank_m = re.search(r"(\d+)位", row)
        if not rank_m:
            continue
        rank = int(rank_m.group(1))

        title_m = re.search(r"<h4[^>]*>(.*?)</h4>", row)
        title = title_m.group(1).strip() if title_m else ""
        if not title:
            continue

        # Extract cast names from links after "最新回出演者"
        cast: list[str] = []
        cast_section = re.search(r"最新回出演者.*?</div>", row, re.DOTALL)
        if cast_section:
            cast = re.findall(r">([^<]+)</a>", cast_section.group())
            cast = [c.strip() for c in cast if c.strip()]

        # Points (last numeric div in the row)
        points_matches = re.findall(r"<div>(\d+)</div>", row)
        points = int(points_matches[-1]) if points_matches else 0

        items.append({
            "rank": rank,
            "title": title,
            "cast": cast,
            "points": points,
        })

    return items


class TVerRankingConnector(BaseConnector):
    """Connector for TVer daily ranking (via achikochi-data.com)."""

    def __init__(
        self,
        max_results: int = 20,
        **kwargs: Any,
    ) -> None:
        super().__init__(source_id="TVER_RANKING_JP", stability="B", **kwargs)
        self.max_results = max_results

    def fetch(self) -> FetchResult:
        """Fetch and parse the TVer ranking page."""
        try:
            resp = requests.get(
                TVER_RANKING_URL,
                timeout=30,
                headers={"User-Agent": "Mozilla/5.0 (Trends Bot)"},
            )
            resp.raise_for_status()
        except requests.RequestException as e:
            return FetchResult(error=str(e))

        items = parse_tver_ranking_html(resp.text)
        items = items[: self.max_results]
        return FetchResult(items=items, item_count=len(items))

    def extract_candidates(
        self, items: list[dict[str, Any]]
    ) -> list[RawCandidate]:
        """Extract candidates from TVer ranking items.

        - Each show title -> WORK candidate
        - Each cast member -> PERSON candidate
        """
        candidates: list[RawCandidate] = []

        for item in items:
            rank = item.get("rank", 0)
            title = item.get("title", "")
            cast: list[str] = item.get("cast", [])
            points = item.get("points", 0)

            if not title:
                continue

            evidence = Evidence(
                source_id=self.source_id,
                title=title,
                url=TVER_RANKING_URL,
                metric=f"rank:{rank},points:{points}",
            )

            # Show title as WORK
            candidates.append(RawCandidate(
                name=title,
                type=CandidateType.WORK,
                source_id=self.source_id,
                rank=rank,
                metric_value=_rank_exposure(rank),
                evidence=evidence,
                extra={"cast": cast, "points": points},
            ))

            # Each cast member as PERSON
            for person in cast:
                candidates.append(RawCandidate(
                    name=person,
                    type=CandidateType.PERSON,
                    source_id=self.source_id,
                    rank=rank,
                    metric_value=_rank_exposure(rank),
                    evidence=evidence,
                    extra={"show": title, "points": points},
                ))

        return candidates

    def compute_signals(
        self, items: list[dict[str, Any]], candidates: list[RawCandidate]
    ) -> list[SignalResult]:
        """Compute daily signal using rank exposure."""
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
