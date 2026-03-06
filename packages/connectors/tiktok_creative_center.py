"""TikTok Creative Center trend discovery connector."""

from __future__ import annotations

import math
import re
from typing import Any

import requests

from packages.connectors.base import BaseConnector, FetchResult, SignalResult
from packages.core.domain_classifier import classify_domain
from packages.core.models import CandidateType, Evidence, ExtractionConfidence, RawCandidate

TIKTOK_CREATIVE_CENTER_URL = (
    "https://ads.tiktok.com/business/creativecenter/inspiration/popular/hashtag/pc/en"
)
ITEM_RE = re.compile(
    r"(?:data-hashtag|data-keyword)=[\"']([^\"']+)[\"']|<span[^>]*class=[\"'][^\"']*(?:hashtag|keyword)[^\"']*[\"'][^>]*>([^<]+)</span>",
    re.IGNORECASE,
)


class TikTokCreativeCenterConnector(BaseConnector):
    def __init__(self, **kwargs: Any) -> None:
        super().__init__(source_id="TIKTOK_CREATIVE_CENTER", stability="A", **kwargs)

    def fetch(self) -> FetchResult:
        try:
            response = requests.get(TIKTOK_CREATIVE_CENTER_URL, timeout=30)
            response.raise_for_status()
        except requests.RequestException as exc:
            return FetchResult(error=str(exc))
        items = self.parse_items(response.text)
        return FetchResult(items=items, item_count=len(items))

    def parse_items(self, html: str) -> list[dict[str, Any]]:
        items: list[dict[str, Any]] = []
        seen: set[str] = set()
        for rank, match in enumerate(ITEM_RE.findall(html), start=1):
            keyword = (match[0] or match[1]).strip()
            if not keyword or keyword in seen:
                continue
            seen.add(keyword)
            items.append({"keyword": keyword, "rank": rank})
        return items

    def extract_candidates(self, items: list[dict[str, Any]]) -> list[RawCandidate]:
        candidates: list[RawCandidate] = []
        for item in items:
            keyword = str(item.get("keyword", "")).strip()
            rank = int(item.get("rank", 0) or 0)
            if not keyword:
                continue
            candidate_type = (
                CandidateType.HASHTAG if keyword.startswith("#") else CandidateType.PHRASE
            )
            candidates.append(
                RawCandidate(
                    name=keyword,
                    type=candidate_type,
                    source_id=self.source_id,
                    rank=rank,
                    metric_value=_rank_exposure(rank),
                    evidence=Evidence(
                        source_id=self.source_id,
                        title=keyword,
                        url=TIKTOK_CREATIVE_CENTER_URL,
                        metric=f"rank:{rank}",
                    ),
                    extraction_confidence=ExtractionConfidence.HIGH,
                    domain_class=classify_domain(candidate_type, self.source_id, text=keyword),
                )
            )
        return candidates

    def compute_signals(
        self, items: list[dict[str, Any]], candidates: list[RawCandidate]
    ) -> list[SignalResult]:
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
