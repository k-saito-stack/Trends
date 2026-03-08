"""FASHIONSNAP editorial connector using top100 and sitemap fallbacks."""

from __future__ import annotations

import math
import re
from typing import Any
from xml.etree import ElementTree

import requests

from packages.connectors.base import BaseConnector, FetchResult, SignalResult
from packages.connectors.fetch_common import build_fetch_metadata, mark_parse_counts, mark_soft_fail
from packages.core.models import Evidence, RawCandidate
from packages.core.topic_extract import extract_topic_candidates

URLS = (
    ("https://www.fashionsnap.com/top100/", "top100"),
    ("https://www.fashionsnap.com/sitemap.xml", "sitemap"),
    ("https://www.fashionsnap.com/", "homepage"),
)
REQUEST_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36"
    ),
    "Accept-Language": "ja,en-US;q=0.9,en;q=0.8",
}
TITLE_RE = re.compile(
    r"<title>([^<]+)</title>"
    r"|<a[^>]*class=[\"'][^\"']*(?:title|headline|ttl)[^\"']*[\"'][^>]*>([^<]+)</a>"
    r"|<loc>https://www\\.fashionsnap\\.com/article/([^<]+)</loc>",
    re.IGNORECASE,
)


class EditorialFashionsnapConnector(BaseConnector):
    def __init__(self, **kwargs: Any) -> None:
        super().__init__(source_id="EDITORIAL_FASHIONSNAP", stability="B", **kwargs)

    def fetch(self) -> FetchResult:
        errors: list[str] = []
        last_success_metadata: dict[str, Any] | None = None
        for url, fallback_name in URLS:
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
        return FetchResult(error=" | ".join(errors[-3:]) if errors else "no fashionsnap data")

    def parse_items(self, html: str) -> list[dict[str, Any]]:
        if "<urlset" in html and "<loc>" in html:
            sitemap_items = _parse_sitemap_items(html)
            if sitemap_items:
                return sitemap_items
        items: list[dict[str, Any]] = []
        seen: set[str] = set()
        for idx, groups in enumerate(TITLE_RE.findall(html), start=1):
            title = next((group.strip() for group in groups if group and group.strip()), "")
            if not title or title in seen:
                continue
            if "/" in title and "article" in title:
                title = _slug_to_title(title.rsplit("/", 1)[-1])
            if title in {"FASHIONSNAP", "FashionSnap"}:
                continue
            seen.add(title)
            items.append({"title": title, "rank": idx})
        return items

    def extract_candidates(self, items: list[dict[str, Any]]) -> list[RawCandidate]:
        candidates: list[RawCandidate] = []
        for item in items:
            title = str(item.get("title", "")).strip()
            rank = int(item.get("rank", 0) or 0)
            evidence = Evidence(
                source_id=self.source_id,
                title=title,
                url=str(item.get("url", "")),
                metric=f"rank:{rank}",
            )
            candidates.extend(
                extract_topic_candidates(
                    title,
                    self.source_id,
                    {"surfaceType": "fashion_editorial"},
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


def _parse_sitemap_items(xml_text: str) -> list[dict[str, Any]]:
    try:
        root = ElementTree.fromstring(xml_text)
    except ElementTree.ParseError:
        return []

    items: list[dict[str, Any]] = []
    seen: set[str] = set()
    index = 1
    for loc in root.iter():
        if not loc.tag.endswith("loc") or not loc.text:
            continue
        url = loc.text.strip()
        if "/article/" not in url:
            continue
        title = _slug_to_title(url.rstrip("/").rsplit("/", 1)[-1])
        if not title or title in seen:
            continue
        seen.add(title)
        items.append({"title": title, "rank": index, "url": url})
        index += 1
    return items


def _slug_to_title(slug: str) -> str:
    normalized = slug.replace("-", " ").replace("_", " ").strip()
    return re.sub(r"\s+", " ", normalized)
