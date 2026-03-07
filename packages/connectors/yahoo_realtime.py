"""Yahoo! Real-time Search rising words connector."""

from __future__ import annotations

import math
import re
from typing import Any

import requests

from packages.connectors.base import BaseConnector, FetchResult, SignalResult
from packages.core.models import CandidateType, Evidence, ExtractionConfidence, RawCandidate
from packages.core.topic_extract import extract_topic_candidates
from packages.core.topic_normalize import should_keep_topic

YAHOO_REALTIME_URL = "https://search.yahoo.co.jp/realtime"
YAHOO_REALTIME_THEME_URL = "https://search.yahoo.co.jp/realtime/search"
REQUEST_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36"
    ),
    "Accept-Language": "ja,en-US;q=0.9,en;q=0.8",
}
WORD_RE = re.compile(
    r"(?:data-keyword|data-word|data-term|data-title)=[\"']([^\"']+)[\"']"
    r"|<span[^>]*class=[\"'][^\"']*(?:word|keyword|trend|title)[^\"']*[\"'][^>]*>([^<]+)</span>"
    r"|<a[^>]*href=[\"'][^\"']*realtime/search[^\"']*[\"'][^>]*>([^<]+)</a>",
    re.IGNORECASE,
)


class YahooRealtimeConnector(BaseConnector):
    def __init__(self, **kwargs: Any) -> None:
        super().__init__(source_id="YAHOO_REALTIME", stability="A", **kwargs)

    def fetch(self) -> FetchResult:
        errors: list[str] = []
        for url, fallback_name in (
            (YAHOO_REALTIME_URL, ""),
            (
                f"{YAHOO_REALTIME_THEME_URL}"
                "?ei=UTF-8&p=%23%E6%80%A5%E4%B8%8A%E6%98%87%E3%83%AF%E3%83%BC%E3%83%89",
                "theme_page",
            ),
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
                    metadata={"url": url},
                )
            errors.append(f"{url}: zero_items")
        return FetchResult(error=" | ".join(errors[-2:]) if errors else "no yahoo realtime data")

    def parse_items(self, html: str) -> list[dict[str, Any]]:
        items: list[dict[str, Any]] = []
        seen: set[str] = set()
        for rank, match in enumerate(WORD_RE.findall(html), start=1):
            keyword = next((group.strip() for group in match if group and group.strip()), "")
            if not keyword or keyword in seen or not should_keep_topic(keyword):
                continue
            seen.add(keyword)
            items.append({"keyword": keyword, "rank": rank})
        return items

    def extract_candidates(self, items: list[dict[str, Any]]) -> list[RawCandidate]:
        candidates: list[RawCandidate] = []
        for item in items:
            keyword = str(item.get("keyword", "")).strip()
            rank = int(item.get("rank", 0) or 0)
            if not keyword or not should_keep_topic(keyword):
                continue
            evidence = Evidence(
                source_id=self.source_id,
                title=keyword,
                url=YAHOO_REALTIME_URL,
                metric=f"rank:{rank}",
            )
            topic_candidates = extract_topic_candidates(
                keyword,
                self.source_id,
                {"surfaceType": "realtime_word"},
                metric_value=_rank_exposure(rank),
                evidence=evidence,
                max_candidates=4,
            )
            if topic_candidates:
                for candidate in topic_candidates:
                    candidate.rank = rank
                    if candidate.extraction_confidence == ExtractionConfidence.LOW:
                        candidate.extraction_confidence = ExtractionConfidence.MEDIUM
                    candidates.append(candidate)
                continue
            candidate_type = _candidate_type(keyword)
            candidates.append(
                RawCandidate(
                    name=keyword,
                    type=candidate_type,
                    source_id=self.source_id,
                    rank=rank,
                    metric_value=_rank_exposure(rank),
                    evidence=evidence,
                    extraction_confidence=ExtractionConfidence.MEDIUM,
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

def _candidate_type(keyword: str) -> CandidateType:
    if keyword.startswith("#"):
        return CandidateType.HASHTAG
    if any(token in keyword for token in ("界隈", "活", "チャレンジ", "交換")):
        return CandidateType.BEHAVIOR
    return CandidateType.PHRASE


def _rank_exposure(rank: int) -> float:
    return 1.0 / math.log2(max(rank, 1) + 1)
