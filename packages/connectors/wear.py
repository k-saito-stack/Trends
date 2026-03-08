"""WEAR trending words connector."""

from __future__ import annotations

import html
import math
import re
from typing import Any

import requests

from packages.connectors.base import BaseConnector, FetchResult, SignalResult
from packages.connectors.fetch_common import build_fetch_metadata, mark_parse_counts, mark_soft_fail
from packages.core.models import CandidateType, Evidence, ExtractionConfidence, RawCandidate
from packages.core.topic_extract import extract_topic_candidates
from packages.core.topic_normalize import should_keep_topic

WEAR_URL = "https://wear.jp/keyword/"
WEAR_ARTICLE_URL = "https://wear.jp/topics/"
REQUEST_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36"
    ),
    "Accept-Language": "ja,en-US;q=0.9,en;q=0.8",
}
KEYWORD_RE = re.compile(
    r"(?:data-keyword|data-term|data-title)=[\"']([^\"']+)[\"']"
    r"|<a[^>]*class=[\"'][^\"']*(?:keyword|tag|link|title)[^\"']*[\"'][^>]*>([^<]+)</a>"
    r"|<span[^>]*class=[\"'][^\"']*(?:keyword|tag|title)[^\"']*[\"'][^>]*>([^<]+)</span>",
    re.IGNORECASE,
)


class WearConnector(BaseConnector):
    def __init__(self, **kwargs: Any) -> None:
        super().__init__(source_id="WEAR_WORDS", stability="A", **kwargs)

    def fetch(self) -> FetchResult:
        errors: list[str] = []
        last_success_metadata: dict[str, Any] | None = None
        for url, fallback_name in ((WEAR_URL, ""), (WEAR_ARTICLE_URL, "article_page")):
            try:
                response = requests.get(url, headers=REQUEST_HEADERS, timeout=30)
                response.raise_for_status()
            except requests.RequestException as exc:
                errors.append(f"{url}: {exc}")
                continue
            metadata = build_fetch_metadata(response, url=url, fallback_used=fallback_name)
            items = self.parse_items(response.text)
            metadata = mark_parse_counts(metadata, parse_raw_count=len(items))
            last_success_metadata = metadata
            if items:
                return FetchResult(
                    items=items,
                    item_count=len(items),
                    fallback_used=fallback_name,
                    metadata=metadata,
                )
            errors.append(f"{url}: zero_items")
        if last_success_metadata is not None:
            return FetchResult(
                items=[],
                item_count=0,
                fallback_used=str(last_success_metadata.get("fallbackUsed", "")),
                metadata=mark_soft_fail(last_success_metadata, error_type="zero_items"),
            )
        return FetchResult(error=" | ".join(errors[-2:]) if errors else "no wear data")

    def parse_items(self, html: str) -> list[dict[str, Any]]:
        items: list[dict[str, Any]] = []
        seen: set[str] = set()
        for rank, match in enumerate(KEYWORD_RE.findall(html), start=1):
            keyword = _clean_keyword(match[0] or match[1])
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
                url=WEAR_URL,
                metric=f"rank:{rank}",
            )
            topic_candidates = extract_topic_candidates(
                keyword,
                self.source_id,
                {"surfaceType": "wear_keyword"},
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
    if "コーデ" in keyword or "メイク" in keyword:
        return CandidateType.STYLE
    if any(token in keyword for token in ("サングラス", "バッグ", "スカート", "スニーカー")):
        return CandidateType.PRODUCT
    return CandidateType.PHRASE


def _rank_exposure(rank: int) -> float:
    return 1.0 / math.log2(max(rank, 1) + 1)


def _clean_keyword(keyword: str) -> str:
    normalized = html.unescape(keyword or "")
    normalized = re.sub(r"\s+", " ", normalized).strip()
    return normalized
