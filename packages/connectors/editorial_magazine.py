"""Curated magazine editorial connector backed by Rakuten Books API."""

from __future__ import annotations

import json
import math
import os
from pathlib import Path
from typing import Any

import requests

from packages.connectors.base import BaseConnector, FetchResult, SignalResult
from packages.core.models import Evidence, RawCandidate
from packages.core.topic_extract import extract_topic_candidates

RAKUTEN_API_URL = "https://openapi.rakuten.co.jp/services/api/BooksMagazine/Search/20170404"
DEFAULT_MAGAZINE_GENRE = "007604"
ALLOWLIST_PATH = Path(__file__).resolve().parents[2] / "configs" / "print_allowlist.yaml"


class EditorialMagazineConnector(BaseConnector):
    def __init__(
        self,
        app_id: str | None = None,
        access_key: str | None = None,
        max_results: int = 30,
        **kwargs: Any,
    ) -> None:
        super().__init__(source_id="EDITORIAL_MAGAZINE", stability="C", **kwargs)
        allowlist = json.loads(ALLOWLIST_PATH.read_text(encoding="utf-8"))
        self.publishers = set(allowlist.get("publishers", []))
        self.titles = set(allowlist.get("titles", []))
        self.deny_terms = set(allowlist.get("deny_terms", []))
        self.app_id = app_id or os.environ.get("RAKUTEN_APP_ID", "")
        self.access_key = access_key or os.environ.get("RAKUTEN_ACCESS_KEY", "")
        self.max_results = max_results

    def fetch(self) -> FetchResult:
        if not self.app_id:
            return FetchResult(error="RAKUTEN_APP_ID not set")
        if not self.access_key:
            return FetchResult(error="RAKUTEN_ACCESS_KEY not set")

        params: dict[str, Any] = {
            "applicationId": self.app_id,
            "accessKey": self.access_key,
            "booksGenreId": DEFAULT_MAGAZINE_GENRE,
            "sort": "-releaseDate",
            "hits": self.max_results,
            "outOfStockFlag": 0,
        }
        headers = {"Origin": "https://k-saito-stack.github.io"}
        try:
            response = requests.get(RAKUTEN_API_URL, params=params, headers=headers, timeout=30)
            response.raise_for_status()
            payload = response.json()
        except requests.RequestException as exc:
            return FetchResult(error=str(exc))

        items: list[dict[str, Any]] = []
        for row in payload.get("Items", []):
            item = row.get("Item", row) if isinstance(row, dict) else {}
            title = str(item.get("title", "")).strip()
            publisher = str(item.get("publisherName", "")).strip()
            if not title:
                continue
            if any(term in title for term in self.deny_terms):
                continue
            if (
                self.publishers
                and publisher
                and publisher not in self.publishers
                and title not in self.titles
            ):
                continue
            items.append(item)

        return FetchResult(
            items=items,
            item_count=len(items),
            metadata={"provider": "rakuten_books"},
        )

    def extract_candidates(self, items: list[dict[str, Any]]) -> list[RawCandidate]:
        candidates: list[RawCandidate] = []
        for rank, item in enumerate(items, start=1):
            title = str(item.get("title", "")).strip()
            publisher = str(item.get("publisherName", "")).strip()
            url = str(item.get("itemUrl", "")).strip()
            caption = str(item.get("itemCaption", "")).strip()
            if not title:
                continue
            evidence = Evidence(
                source_id=self.source_id,
                title=title,
                url=url,
                metric=f"rank:{rank}",
                snippet=caption[:180],
                published_at=str(item.get("salesDate", "")),
            )
            candidates.extend(
                extract_topic_candidates(
                    f"{title} {caption[:200]}".strip(),
                    self.source_id,
                    {"publisher": publisher, "booksGenreId": item.get("booksGenreId", "")},
                    metric_value=_rank_exposure(rank),
                    evidence=evidence,
                    max_candidates=5,
                )
            )
        return candidates

    def compute_signals(
        self,
        items: list[dict[str, Any]],
        candidates: list[RawCandidate],
    ) -> list[SignalResult]:
        del items
        signals: dict[str, SignalResult] = {}
        for candidate in candidates:
            current = signals.get(candidate.name)
            if current is None:
                signals[candidate.name] = SignalResult(
                    candidate_name=candidate.name,
                    signal_value=candidate.metric_value,
                    evidence=candidate.evidence,
                )
            else:
                current.signal_value += candidate.metric_value
        return list(signals.values())


def _rank_exposure(rank: int) -> float:
    return 1.0 / math.log2(max(rank, 1) + 1)
