"""FASHIONSNAP editorial connector."""

from __future__ import annotations

import math
import re
from typing import Any

import requests

from packages.connectors.base import BaseConnector, FetchResult, SignalResult
from packages.core.models import RawCandidate
from packages.core.phrase_mining import extract_topic_raw_candidates

URL = "https://www.fashionsnap.com/"
TITLE_RE = re.compile(r"<a[^>]*class=[\"'][^\"']*title[^\"']*[\"'][^>]*>([^<]+)</a>", re.IGNORECASE)


class EditorialFashionsnapConnector(BaseConnector):
    def __init__(self, **kwargs: Any) -> None:
        super().__init__(source_id="EDITORIAL_FASHIONSNAP", stability="B", **kwargs)

    def fetch(self) -> FetchResult:
        try:
            response = requests.get(URL, timeout=30)
            response.raise_for_status()
        except requests.RequestException as exc:
            return FetchResult(error=str(exc))
        items = [
            {"title": title.strip(), "rank": idx + 1}
            for idx, title in enumerate(TITLE_RE.findall(response.text))
        ]
        return FetchResult(items=items, item_count=len(items))

    def extract_candidates(self, items: list[dict[str, Any]]) -> list[RawCandidate]:
        candidates: list[RawCandidate] = []
        for item in items:
            title = str(item.get("title", ""))
            rank = int(item.get("rank", 0) or 0)
            for candidate in extract_topic_raw_candidates(
                title, self.source_id, metric_value=_rank_exposure(rank)
            ):
                candidates.append(candidate)
        return candidates

    def compute_signals(
        self,
        items: list[dict[str, Any]],
        candidates: list[RawCandidate],
    ) -> list[SignalResult]:
        del items
        return [
            SignalResult(
                candidate_name=candidate.name,
                signal_value=candidate.metric_value,
                evidence=candidate.evidence,
            )
            for candidate in candidates
        ]


def _rank_exposure(rank: int) -> float:
    return 1.0 / math.log2(max(rank, 1) + 1)
