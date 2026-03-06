"""ZOZOTOWN ranking connector."""

from __future__ import annotations

import math
import re
from typing import Any

import requests

from packages.connectors.base import BaseConnector, FetchResult, SignalResult
from packages.core.domain_classifier import classify_domain
from packages.core.models import CandidateType, Evidence, ExtractionConfidence, RawCandidate

ZOZO_URL = "https://zozo.jp/ranking/"
ITEM_RE = re.compile(
    r"<li[^>]*data-item-name=[\"']([^\"']+)[\"'][^>]*data-brand=[\"']([^\"']*)[\"'][^>]*>",
    re.IGNORECASE,
)


class ZozoConnector(BaseConnector):
    def __init__(self, **kwargs: Any) -> None:
        super().__init__(source_id="ZOZO_RANKING", stability="A", **kwargs)

    def fetch(self) -> FetchResult:
        try:
            response = requests.get(ZOZO_URL, timeout=30)
            response.raise_for_status()
        except requests.RequestException as exc:
            return FetchResult(error=str(exc))
        items = self.parse_items(response.text)
        return FetchResult(items=items, item_count=len(items))

    def parse_items(self, html: str) -> list[dict[str, Any]]:
        items: list[dict[str, Any]] = []
        for rank, match in enumerate(ITEM_RE.findall(html), start=1):
            items.append({"item_name": match[0].strip(), "brand": match[1].strip(), "rank": rank})
        return items

    def extract_candidates(self, items: list[dict[str, Any]]) -> list[RawCandidate]:
        candidates: list[RawCandidate] = []
        for item in items:
            rank = int(item.get("rank", 0) or 0)
            item_name = str(item.get("item_name", "")).strip()
            brand = str(item.get("brand", "")).strip()
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
                            url=ZOZO_URL,
                            metric=f"rank:{rank}",
                        ),
                        extraction_confidence=ExtractionConfidence.MEDIUM,
                        domain_class=classify_domain(
                            CandidateType.PRODUCT, self.source_id, text=item_name
                        ),
                        extra={"brand": brand},
                    )
                )
            if brand:
                candidates.append(
                    RawCandidate(
                        name=brand,
                        type=CandidateType.BRAND,
                        source_id=self.source_id,
                        rank=rank,
                        metric_value=_rank_exposure(rank) * 0.75,
                        evidence=Evidence(
                            source_id=self.source_id,
                            title=f"{brand} / {item_name}",
                            url=ZOZO_URL,
                            metric=f"rank:{rank}",
                        ),
                        extraction_confidence=ExtractionConfidence.MEDIUM,
                        domain_class=classify_domain(
                            CandidateType.BRAND, self.source_id, text=brand
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
