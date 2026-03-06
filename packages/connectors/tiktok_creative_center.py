"""TikTok Creative Center trend discovery connector."""

from __future__ import annotations

import html
import math
import re
from typing import Any
from urllib.parse import unquote

import requests

from packages.connectors.base import BaseConnector, FetchResult, SignalResult
from packages.core.domain_classifier import classify_domain
from packages.core.models import CandidateType, Evidence, ExtractionConfidence, RawCandidate
from packages.core.topic_normalize import should_keep_topic

TIKTOK_CREATIVE_CENTER_URL = (
    "https://ads.tiktok.com/business/creativecenter/inspiration/popular/hashtag/pc/en"
)
ROW_RE = re.compile(
    r'<a[^>]*data-testid=["\']cc_commonCom-trend_hashtag_item-\d+["\'][^>]*href=["\']([^"\']+)["\'][^>]*>(.*?)</a>\s*</a>',
    re.IGNORECASE | re.DOTALL,
)
TITLE_RE = re.compile(
    r'<span[^>]*class=["\'][^"\']*CardPc_titleText[^"\']*["\'][^>]*>(.*?)</span>',
    re.IGNORECASE | re.DOTALL,
)
ITEM_RE = re.compile(
    r"(?:data-hashtag|data-keyword)=[\"']([^\"']+)[\"']|<span[^>]*class=[\"'][^\"']*(?:hashtag|keyword)[^\"']*[\"'][^>]*>([^<]+)</span>",
    re.IGNORECASE,
)
COMMENT_RE = re.compile(r"<!--.*?-->", re.DOTALL)
TAG_RE = re.compile(r"<[^>]+>")
HASHTAG_PATH_RE = re.compile(r"/business/creativecenter/hashtag/([^/?\"']+)", re.IGNORECASE)
HASHTAG_SPACE_RE = re.compile(r"^#\s+")


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
        row_matches = ROW_RE.findall(html)
        if row_matches:
            for rank, (href, body) in enumerate(row_matches, start=1):
                keyword = _extract_ranked_keyword(href, body)
                if not keyword or keyword in seen or not should_keep_topic(keyword):
                    continue
                seen.add(keyword)
                items.append({"keyword": keyword, "rank": rank})
            if items:
                return items

        for rank, match in enumerate(ITEM_RE.findall(html), start=1):
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


def _extract_ranked_keyword(href: str, body: str) -> str:
    title_match = TITLE_RE.search(body)
    if title_match:
        return _clean_keyword(title_match.group(1))

    href_match = HASHTAG_PATH_RE.search(href)
    if not href_match:
        return ""
    return _clean_keyword(f"#{unquote(href_match.group(1))}")


def _clean_keyword(text: str) -> str:
    normalized = html.unescape(text or "")
    normalized = COMMENT_RE.sub("", normalized)
    normalized = TAG_RE.sub(" ", normalized)
    normalized = re.sub(r"\s+", " ", normalized).strip()
    return HASHTAG_SPACE_RE.sub("#", normalized)
