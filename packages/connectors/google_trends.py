"""Google Trends constrained Trending Now connector."""

from __future__ import annotations

import logging
import math
import re
from typing import Any

import requests

from packages.connectors.base import BaseConnector, FetchResult, SignalResult
from packages.core.models import (
    CandidateType,
    DomainClass,
    Evidence,
    ExtractionConfidence,
    RawCandidate,
)
from packages.core.topic_extract import extract_topic_candidates

logger = logging.getLogger(__name__)

GOOGLE_TRENDING_NOW_URL = "https://trends.google.com/trending"
GOOGLE_TRENDS_RSS_URL = "https://trends.google.com/trending/rss?geo=JP"
REQUEST_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36"
    ),
    "Accept-Language": "ja,en-US;q=0.9,en;q=0.8",
}
ROW_PATTERNS = (
    re.compile(
        r'data-term=["\'](?P<title>[^"\']+)["\'][^>]*'
        r'data-search-volume=["\'](?P<volume>[^"\']*)["\'][^>]*'
        r'data-started=["\'](?P<started>[^"\']*)["\']',
        re.IGNORECASE,
    ),
    re.compile(
        r"<tr[^>]*>\s*<td[^>]*>(?P<title>[^<]+)</td>\s*<td[^>]*>(?P<volume>[^<]*)</td>"
        r"\s*<td[^>]*>(?P<started>[^<]*)</td>",
        re.IGNORECASE,
    ),
)


class GoogleTrendingNowConnector(BaseConnector):
    """Constrained Google Trends connector for JP 24h categories."""

    def __init__(
        self,
        *,
        source_id: str = "TRENDS",
        category: str = "GENERAL",
        geo: str = "JP",
        time_window_hours: int = 24,
        active_only: bool = True,
        max_results: int = 50,
        **kwargs: Any,
    ) -> None:
        super().__init__(source_id=source_id, stability="A", max_consecutive_failures=5, **kwargs)
        self.category = category
        self.geo = geo
        self.time_window_hours = time_window_hours
        self.active_only = active_only
        self.max_results = max_results

    def fetch(self) -> FetchResult:
        errors: list[str] = []
        try:
            rows = self._fetch_trending_now_rows()
            if rows:
                return FetchResult(
                    items=rows[: self.max_results],
                    item_count=min(len(rows), self.max_results),
                    metadata={
                        "sourceSurface": "google_trending_now",
                        "trendCategory": self.category,
                        "trendGeo": self.geo,
                        "timeWindowHours": self.time_window_hours,
                        "activeTrend": self.active_only,
                        "sortMode": "active_only" if self.active_only else "default",
                    },
                )
            errors.append("trending_now: zero_items")
        except requests.RequestException as exc:
            errors.append(f"trending_now: {exc}")

        try:
            response = requests.get(GOOGLE_TRENDS_RSS_URL, timeout=30, headers=REQUEST_HEADERS)
            response.raise_for_status()
            items = _parse_trends_rss(response.text)
            return FetchResult(
                items=items[: self.max_results],
                item_count=min(len(items), self.max_results),
                metadata={
                    "sourceSurface": "google_trending_rss",
                    "trendCategory": self.category,
                    "trendGeo": self.geo,
                    "timeWindowHours": self.time_window_hours,
                    "activeTrend": self.active_only,
                    "sortMode": "rss_fallback",
                },
                fallback_used="rss",
            )
        except requests.RequestException as exc:
            errors.append(f"rss: {exc}")
            return FetchResult(error=" | ".join(errors[-2:]) if errors else str(exc))

    def _fetch_trending_now_rows(self) -> list[dict[str, Any]]:
        export_rows = self._fetch_trending_now_export()
        if export_rows:
            return self._parse_trend_rows(export_rows)

        params: dict[str, str | int] = {
            "geo": self.geo,
            "hours": self.time_window_hours,
            "category": self.category.lower(),
        }
        response = requests.get(
            GOOGLE_TRENDING_NOW_URL,
            params=params,
            headers=REQUEST_HEADERS,
            timeout=30,
        )
        response.raise_for_status()
        return self._parse_trend_rows(response.text)

    def _fetch_trending_now_export(self) -> str:
        params: dict[str, str | int] = {
            "geo": self.geo,
            "hours": self.time_window_hours,
            "category": self.category.lower(),
            "export": "1",
        }
        response = requests.get(
            GOOGLE_TRENDING_NOW_URL,
            params=params,
            headers=REQUEST_HEADERS,
            timeout=30,
        )
        if response.status_code >= 400:
            return ""
        return response.text

    def _parse_trend_rows(self, payload: str) -> list[dict[str, Any]]:
        rows: list[dict[str, Any]] = []
        seen: set[str] = set()
        for pattern in ROW_PATTERNS:
            for rank, match in enumerate(pattern.finditer(payload), start=1):
                title = match.group("title").strip()
                if not title or title in seen:
                    continue
                seen.add(title)
                variants = self._extract_query_breakdown(title)
                rows.append(
                    {
                        "title": title,
                        "url": GOOGLE_TRENDING_NOW_URL,
                        "searchVolumeText": match.groupdict().get("volume", "").strip(),
                        "startedText": match.groupdict().get("started", "").strip(),
                        "queryVariants": variants,
                        "trendBreakdownCount": len(variants),
                        "rank": rank,
                        "category": self.category,
                    }
                )
        return rows

    def _extract_query_breakdown(self, text: str) -> list[str]:
        parts = re.split(r"[／/・,，、|｜]", text or "")
        variants = [part.strip() for part in parts if part.strip()]
        if len(variants) <= 1:
            return []
        return variants[:5]

    def extract_candidates(self, items: list[dict[str, Any]]) -> list[RawCandidate]:
        if self.category == "BEAUTY_FASHION":
            return self._extract_candidates_for_beauty_fashion(items)
        return self._extract_candidates_for_entertainment(items)

    def _extract_candidates_for_entertainment(
        self,
        items: list[dict[str, Any]],
    ) -> list[RawCandidate]:
        from packages.core.ner import extract_entities

        candidates: list[RawCandidate] = []
        for item in items:
            rank = int(item.get("rank", 0) or 0)
            title = str(item.get("title", "")).strip()
            if not title:
                continue
            evidence = _build_evidence(self.source_id, title, item, rank)
            extra = _build_extra(item)
            entities = extract_entities(title, max_entities=5)
            if entities:
                for ent_text, ent_type in entities:
                    try:
                        candidate_type = CandidateType(ent_type)
                    except ValueError:
                        candidate_type = CandidateType.KEYWORD
                    candidates.append(
                        RawCandidate(
                            name=ent_text,
                            type=candidate_type,
                            source_id=self.source_id,
                            rank=rank,
                            metric_value=_rank_exposure(rank),
                            evidence=evidence,
                            extraction_confidence=ExtractionConfidence.MEDIUM,
                            domain_class=DomainClass.ENTERTAINMENT,
                            extra=extra,
                        )
                    )
            topic_candidates = extract_topic_candidates(
                title,
                self.source_id,
                extra,
                metric_value=_rank_exposure(rank),
                evidence=evidence,
                max_candidates=3,
            )
            for candidate in topic_candidates:
                if candidate.type in {
                    CandidateType.PHRASE,
                    CandidateType.HASHTAG,
                    CandidateType.BEHAVIOR,
                    CandidateType.KEYWORD,
                }:
                    candidate.rank = rank
                    candidate.domain_class = DomainClass.ENTERTAINMENT
                    candidates.append(candidate)
        return candidates

    def _extract_candidates_for_beauty_fashion(
        self,
        items: list[dict[str, Any]],
    ) -> list[RawCandidate]:
        candidates: list[RawCandidate] = []
        allowed = {
            CandidateType.STYLE,
            CandidateType.PRODUCT,
            CandidateType.BRAND,
            CandidateType.BEHAVIOR,
            CandidateType.PHRASE,
        }
        for item in items:
            rank = int(item.get("rank", 0) or 0)
            title = str(item.get("title", "")).strip()
            if not title:
                continue
            evidence = _build_evidence(self.source_id, title, item, rank)
            extra = _build_extra(item)
            topic_candidates = extract_topic_candidates(
                title,
                self.source_id,
                extra,
                metric_value=_rank_exposure(rank),
                evidence=evidence,
                max_candidates=5,
            )
            for candidate in topic_candidates:
                if candidate.type not in allowed:
                    continue
                candidate.rank = rank
                candidate.domain_class = DomainClass.FASHION_BEAUTY
                candidate.extraction_confidence = max(
                    candidate.extraction_confidence,
                    ExtractionConfidence.MEDIUM,
                    key=lambda confidence: confidence.weight,
                )
                candidates.append(candidate)
        return candidates

    def compute_signals(
        self,
        items: list[dict[str, Any]],
        candidates: list[RawCandidate],
    ) -> list[SignalResult]:
        signals: dict[str, SignalResult] = {}
        for candidate in candidates:
            key = candidate.name
            if key in signals:
                signals[key].signal_value += candidate.metric_value
            else:
                signals[key] = SignalResult(
                    candidate_name=key,
                    signal_value=candidate.metric_value,
                    evidence=candidate.evidence,
                )
        return list(signals.values())


GoogleTrendsConnector = GoogleTrendingNowConnector


def _build_evidence(source_id: str, title: str, item: dict[str, Any], rank: int) -> Evidence:
    search_volume = str(item.get("searchVolumeText", "")).strip()
    started = str(item.get("startedText", "")).strip()
    metric_parts = [f"rank:{rank}"]
    if search_volume:
        metric_parts.append(f"volume:{search_volume}")
    if started:
        metric_parts.append(f"started:{started}")
    return Evidence(
        source_id=source_id,
        title=title,
        url=str(item.get("url", GOOGLE_TRENDING_NOW_URL)),
        metric=",".join(metric_parts),
        snippet=" / ".join(str(value) for value in item.get("queryVariants", []) if value),
    )


def _build_extra(item: dict[str, Any]) -> dict[str, Any]:
    return {
        "trendCategory": str(item.get("category", "")),
        "trendGeo": "JP",
        "timeWindowHours": 24,
        "activeTrend": True,
        "searchVolumeText": str(item.get("searchVolumeText", "")),
        "startedText": str(item.get("startedText", "")),
        "queryVariants": [str(value) for value in item.get("queryVariants", []) if value],
        "trendBreakdownCount": int(item.get("trendBreakdownCount", 0) or 0),
        "sourceSurface": "google_trending_now",
        "countryCode": "JP",
        "region": "JP",
        "countries": ["JP"],
    }


def _parse_trends_rss(xml_content: str) -> list[dict[str, Any]]:
    import xml.etree.ElementTree as ET

    items: list[dict[str, Any]] = []
    try:
        root = ET.fromstring(xml_content)
    except ET.ParseError:
        logger.warning("Failed to parse Google Trends RSS XML")
        return items

    ns = {"ht": "https://trends.google.com/trending/rss"}
    for rank, item_el in enumerate(root.iter("item"), start=1):
        title_el = item_el.find("title")
        traffic_el = item_el.find("ht:approx_traffic", ns)
        title = title_el.text if title_el is not None and title_el.text else ""
        traffic = traffic_el.text if traffic_el is not None and traffic_el.text else ""
        url = ""
        news_item = item_el.find("ht:news_item", ns)
        if news_item is not None:
            url_el = news_item.find("ht:news_item_url", ns)
            url = url_el.text if url_el is not None and url_el.text else ""
        if not title:
            continue
        items.append(
            {
                "title": title,
                "url": url or GOOGLE_TRENDING_NOW_URL,
                "searchVolumeText": traffic,
                "startedText": "",
                "queryVariants": [],
                "trendBreakdownCount": 0,
                "rank": rank,
                "category": "GENERAL",
            }
        )
    return items


def _rank_exposure(rank: int) -> float:
    return 1.0 / math.log2(rank + 1)
