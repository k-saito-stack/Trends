"""Curated magazine editorial connector."""

from __future__ import annotations

import json
import math
import re
from pathlib import Path
from typing import Any

import requests

from packages.connectors.base import BaseConnector, FetchResult, SignalResult
from packages.core.phrase_mining import extract_topic_raw_candidates

URL = "https://books.rakuten.co.jp/ranking/daily/"
TITLE_RE = re.compile(
    r"<li[^>]*data-title=[\"']([^\"']+)[\"'][^>]*data-publisher=[\"']([^\"']*)[\"'][^>]*>",
    re.IGNORECASE,
)
ALLOWLIST_PATH = Path(__file__).resolve().parents[2] / "configs" / "print_allowlist.yaml"


class EditorialMagazineConnector(BaseConnector):
    def __init__(self, **kwargs: Any) -> None:
        super().__init__(source_id="EDITORIAL_MAGAZINE", stability="C", **kwargs)
        allowlist = json.loads(ALLOWLIST_PATH.read_text(encoding="utf-8"))
        self.publishers = set(allowlist["publishers"])
        self.deny_terms = set(allowlist["deny_terms"])

    def fetch(self) -> FetchResult:
        try:
            response = requests.get(URL, timeout=30)
            response.raise_for_status()
        except requests.RequestException as exc:
            return FetchResult(error=str(exc))
        items = self.parse_items(response.text)
        return FetchResult(items=items, item_count=len(items))

    def parse_items(self, html: str) -> list[dict[str, Any]]:
        parsed = []
        for rank, match in enumerate(TITLE_RE.findall(html), start=1):
            title, publisher = match[0].strip(), match[1].strip()
            if publisher and publisher not in self.publishers:
                continue
            if any(term in title for term in self.deny_terms):
                continue
            parsed.append({"title": title, "publisher": publisher, "rank": rank})
        return parsed

    def extract_candidates(self, items: list[dict[str, Any]]):
        candidates = []
        for item in items:
            title = str(item.get("title", ""))
            rank = int(item.get("rank", 0) or 0)
            for candidate in extract_topic_raw_candidates(title, self.source_id, metric_value=_rank_exposure(rank)):
                candidates.append(candidate)
        return candidates

    def compute_signals(self, items: list[dict[str, Any]], candidates):
        return [SignalResult(candidate_name=candidate.name, signal_value=candidate.metric_value, evidence=candidate.evidence) for candidate in candidates]


def _rank_exposure(rank: int) -> float:
    return 1.0 / math.log2(max(rank, 1) + 1)
