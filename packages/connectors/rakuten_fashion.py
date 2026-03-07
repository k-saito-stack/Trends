"""Rakuten Fashion ranking connector."""

from __future__ import annotations

import math
import re
from typing import Any

import requests

from packages.connectors.base import BaseConnector, FetchResult, SignalResult
from packages.core.domain_classifier import classify_domain
from packages.core.models import CandidateType, Evidence, ExtractionConfidence, RawCandidate

RAKUTEN_FASHION_URL = "https://brandavenue.rakuten.co.jp/ranking/"
RAKUTEN_FASHION_FALLBACK_URL = "https://brandavenue.rakuten.co.jp/"
REQUEST_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36"
    ),
    "Accept-Language": "ja,en-US;q=0.9,en;q=0.8",
}
ITEM_RE = re.compile(
    r"<li[^>]*data-item-name=[\"']([^\"']+)[\"'][^>]*data-brand=[\"']([^\"']*)[\"'][^>]*data-category=[\"']([^\"']*)[\"'][^>]*>",
    re.IGNORECASE,
)
SCRIPT_ITEM_RE = re.compile(
    r'"itemName"\s*:\s*"([^"]+)".*?"shopName"\s*:\s*"([^"]*)".*?"categoryName"\s*:\s*"([^"]*)"',
    re.IGNORECASE | re.DOTALL,
)


class RakutenFashionConnector(BaseConnector):
    def __init__(self, **kwargs: Any) -> None:
        super().__init__(source_id="RAKUTEN_FASHION", stability="A", **kwargs)

    def fetch(self) -> FetchResult:
        errors: list[str] = []
        for url, fallback_name in (
            (RAKUTEN_FASHION_URL, ""),
            (RAKUTEN_FASHION_FALLBACK_URL, "brandavenue_home"),
        ):
            try:
                response = requests.get(url, headers=REQUEST_HEADERS, timeout=30)
                response.raise_for_status()
            except requests.RequestException as exc:
                errors.append(f"{url}: {exc}")
                continue
            items = self.parse_items(response.text)
            if items:
                return FetchResult(
                    items=items,
                    item_count=len(items),
                    fallback_used=fallback_name,
                )
        return FetchResult(error=" | ".join(errors[-2:]) if errors else "no rakuten fashion data")

    def parse_items(self, html: str) -> list[dict[str, Any]]:
        items: list[dict[str, Any]] = []
        for rank, match in enumerate(ITEM_RE.findall(html), start=1):
            items.append(
                {
                    "item_name": match[0].strip(),
                    "brand": match[1].strip(),
                    "category": match[2].strip(),
                    "rank": rank,
                }
            )
        if items:
            return items
        for rank, match in enumerate(SCRIPT_ITEM_RE.findall(html), start=1):
            items.append(
                {
                    "item_name": match[0].strip(),
                    "brand": match[1].strip(),
                    "category": match[2].strip(),
                    "rank": rank,
                }
            )
        return items

    def extract_candidates(self, items: list[dict[str, Any]]) -> list[RawCandidate]:
        candidates: list[RawCandidate] = []
        for item in items:
            rank = int(item.get("rank", 0) or 0)
            item_name = str(item.get("item_name", "")).strip()
            brand = str(item.get("brand", "")).strip()
            category = str(item.get("category", "")).strip()
            metadata = {"category": category}
            if item_name:
                candidates.append(
                    RawCandidate(
                        name=item_name,
                        type=CandidateType.PRODUCT,
                        source_id=self.source_id,
                        rank=rank,
                        metric_value=_rank_exposure(rank),
                        evidence=Evidence(
                            source_id=self.source_id,
                            title=item_name,
                            url=RAKUTEN_FASHION_URL,
                            metric=f"rank:{rank}",
                        ),
                        extraction_confidence=ExtractionConfidence.MEDIUM,
                        domain_class=classify_domain(
                            CandidateType.PRODUCT,
                            self.source_id,
                            text=f"{item_name} {category}",
                            metadata=metadata,
                        ),
                        extra={"brand": brand, "category": category},
                    )
                )
            if brand:
                candidates.append(
                    RawCandidate(
                        name=brand,
                        type=CandidateType.BRAND,
                        source_id=self.source_id,
                        rank=rank,
                        metric_value=_rank_exposure(rank) * 0.7,
                        evidence=Evidence(
                            source_id=self.source_id,
                            title=f"{brand} / {item_name}",
                            url=RAKUTEN_FASHION_URL,
                            metric=f"rank:{rank}",
                        ),
                        extraction_confidence=ExtractionConfidence.MEDIUM,
                        domain_class=classify_domain(
                            CandidateType.BRAND, self.source_id, text=brand, metadata=metadata
                        ),
                    )
                )
        return candidates

    def compute_signals(
        self, items: list[dict[str, Any]], candidates: list[RawCandidate]
    ) -> list[SignalResult]:
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
