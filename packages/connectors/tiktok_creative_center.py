"""TikTok Creative Center trend discovery connector."""

from __future__ import annotations

import html
import math
import os
import re
from collections.abc import Sequence
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
TIKTOK_CREATIVE_CENTER_BROWSER_URL = (
    "https://ads.tiktok.com/business/creativecenter/inspiration/popular/hashtag/pad/en"
)
TIKTOK_CREATIVE_CENTER_API_URL = (
    "https://ads.tiktok.com/creative_radar_api/v1/popular_trend/hashtag/list"
)
DEFAULT_COUNTRY_CODES = ("JP", "KR", "TW", "HK", "TH", "VN", "ID", "PH", "MY", "SG")
PRIMARY_COUNTRY_CODE = "JP"
PRIMARY_COUNTRY_WEIGHT = 1.0
SECONDARY_COUNTRY_WEIGHT = 0.6
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
    def __init__(
        self,
        max_results: int = 20,
        country_codes: Sequence[str] | str | None = None,
        **kwargs: Any,
    ) -> None:
        super().__init__(source_id="TIKTOK_CREATIVE_CENTER", stability="A", **kwargs)
        self.max_results = max_results
        self.country_codes = _normalize_country_codes(country_codes)
        self.allow_global_fallback = (
            os.environ.get("TIKTOK_CREATIVE_CENTER_ALLOW_GLOBAL_FALLBACK", "").strip().lower()
            in {"1", "true", "yes", "on"}
        )

    def fetch(self) -> FetchResult:
        if self.country_codes:
            try:
                items = self.fetch_regional_items()
            except RuntimeError as exc:
                if not self.allow_global_fallback:
                    return FetchResult(error=str(exc))
            else:
                return FetchResult(items=items, item_count=len(items))

        try:
            response = requests.get(TIKTOK_CREATIVE_CENTER_URL, timeout=30)
            response.raise_for_status()
        except requests.RequestException as exc:
            return FetchResult(error=str(exc))
        items = self.parse_items(response.text)
        return FetchResult(items=items, item_count=len(items))

    def fetch_regional_items(self) -> list[dict[str, Any]]:
        if not self.country_codes:
            return []

        browser_headers = self.capture_browser_api_headers()
        session = requests.Session()
        try:
            session.get(TIKTOK_CREATIVE_CENTER_BROWSER_URL, timeout=30)
        except requests.RequestException as exc:
            raise RuntimeError(f"tiktok regional bootstrap failed: {exc}") from exc

        by_country: dict[str, list[dict[str, Any]]] = {}
        for country_code in self.country_codes:
            try:
                response = session.get(
                    _build_api_url(country_code, self.max_results),
                    headers=_build_browser_api_headers(browser_headers, country_code),
                    timeout=30,
                )
                response.raise_for_status()
            except requests.RequestException as exc:
                raise RuntimeError(
                    f"tiktok regional fetch failed for {country_code}: {exc}"
                ) from exc
            by_country[country_code] = self.parse_api_items(response.json(), country_code)

        return self.merge_regional_items(by_country)

    def capture_browser_api_headers(self) -> dict[str, str]:
        try:
            from playwright.sync_api import sync_playwright
        except ImportError as exc:
            raise RuntimeError(
                "playwright is required for regional TikTok fetches"
            ) from exc

        captured_headers: dict[str, str] = {}
        headless = (
            os.environ.get("TIKTOK_PLAYWRIGHT_HEADLESS", "").strip().lower()
            in {"1", "true", "yes", "on"}
        )
        with sync_playwright() as playwright:
            browser = playwright.chromium.launch(headless=headless)
            page = browser.new_page(locale="en-US")

            def handle_request(request: Any) -> None:
                nonlocal captured_headers
                if captured_headers:
                    return
                if TIKTOK_CREATIVE_CENTER_API_URL not in str(request.url):
                    return
                headers = getattr(request, "headers", None)
                if not isinstance(headers, dict):
                    return
                captured_headers = {
                    str(key).lower(): str(value)
                    for key, value in headers.items()
                    if value
                }

            page.on("request", handle_request)
            page.goto(
                TIKTOK_CREATIVE_CENTER_BROWSER_URL,
                wait_until="domcontentloaded",
                timeout=60_000,
            )
            for _ in range(20):
                if captured_headers:
                    break
                page.wait_for_timeout(500)
            browser.close()

        if not captured_headers:
            raise RuntimeError("tiktok regional fetch failed: browser headers not captured")
        return captured_headers

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

    def parse_api_items(
        self,
        payload: dict[str, Any],
        country_code: str,
    ) -> list[dict[str, Any]]:
        rows = payload.get("data", {}).get("list", [])
        if not isinstance(rows, list):
            return []

        items: list[dict[str, Any]] = []
        for row in rows:
            if not isinstance(row, dict):
                continue
            keyword = _clean_keyword(f"#{row.get('hashtag_name', '')}")
            rank = int(row.get("rank", 0) or 0)
            if not keyword or rank <= 0 or not should_keep_topic(keyword):
                continue
            items.append(
                {
                    "keyword": keyword,
                    "rank": rank,
                    "countryCode": country_code,
                    "publishCount": int(row.get("publish_cnt", 0) or 0),
                    "videoViews": int(row.get("video_views", 0) or 0),
                }
            )
        return items

    def merge_regional_items(
        self,
        items_by_country: dict[str, list[dict[str, Any]]],
    ) -> list[dict[str, Any]]:
        merged: dict[str, dict[str, Any]] = {}
        for country_code, items in items_by_country.items():
            for item in items:
                keyword = str(item.get("keyword", "")).strip()
                rank = int(item.get("rank", 0) or 0)
                if not keyword or rank <= 0:
                    continue
                entry = merged.setdefault(
                    keyword,
                    {
                        "keyword": keyword,
                        "countryRanks": {},
                        "countries": [],
                        "regionalScore": 0.0,
                        "bestRank": rank,
                    },
                )
                entry["countryRanks"][country_code] = rank
                if country_code not in entry["countries"]:
                    entry["countries"].append(country_code)
                entry["bestRank"] = min(int(entry["bestRank"]), rank)
                entry["regionalScore"] = float(entry["regionalScore"]) + (
                    _rank_exposure(rank) * _country_weight(country_code)
                )

        filtered = [
            entry
            for entry in merged.values()
            if PRIMARY_COUNTRY_CODE in entry["countryRanks"] or len(entry["countries"]) >= 2
        ]
        ranked = sorted(
            filtered,
            key=lambda item: (
                -float(item["regionalScore"]),
                -len(item["countries"]),
                int(item["bestRank"]),
                str(item["keyword"]),
            ),
        )
        for rank, item in enumerate(ranked[: self.max_results], start=1):
            item["rank"] = rank
            item["countries"] = sorted(
                [str(country) for country in item["countries"]],
                key=lambda country: (country != PRIMARY_COUNTRY_CODE, country),
            )
        return ranked[: self.max_results]

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
            countries = [
                str(country)
                for country in item.get("countries", [])
                if isinstance(country, str) and country
            ]
            country_ranks = {
                str(country): int(country_rank)
                for country, country_rank in item.get("countryRanks", {}).items()
                if country and country_rank
            }
            metric_value = float(item.get("regionalScore") or _rank_exposure(rank))
            extra: dict[str, Any] = {}
            if countries:
                extra["countries"] = countries
            if country_ranks:
                extra["countryRanks"] = country_ranks
            if item.get("countryCode"):
                extra["countryCode"] = str(item["countryCode"])
            if item.get("regionalScore") is not None:
                extra["regionalScore"] = metric_value
            candidates.append(
                RawCandidate(
                    name=keyword,
                    type=candidate_type,
                    source_id=self.source_id,
                    rank=rank,
                    metric_value=metric_value,
                    evidence=Evidence(
                        source_id=self.source_id,
                        title=keyword,
                        url=_evidence_url(countries),
                        metric=_evidence_metric(rank, country_ranks),
                    ),
                    extraction_confidence=ExtractionConfidence.HIGH,
                    domain_class=classify_domain(candidate_type, self.source_id, text=keyword),
                    extra=extra,
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


def _normalize_country_codes(country_codes: Sequence[str] | str | None) -> tuple[str, ...]:
    if country_codes is None:
        raw = os.environ.get("TIKTOK_CREATIVE_CENTER_COUNTRY_CODES", "")
        if raw.strip():
            country_codes = raw
        else:
            return DEFAULT_COUNTRY_CODES

    if isinstance(country_codes, str):
        values = [part.strip().upper() for part in country_codes.split(",")]
    else:
        values = [str(part).strip().upper() for part in country_codes]
    normalized = [value for value in values if value]
    return tuple(dict.fromkeys(normalized)) or DEFAULT_COUNTRY_CODES


def _build_api_url(country_code: str, limit: int) -> str:
    return (
        f"{TIKTOK_CREATIVE_CENTER_API_URL}?page=1&limit={limit}&period=7"
        f"&country_code={country_code}&sort_by=popular"
    )


def _build_browser_api_headers(
    captured_headers: dict[str, str],
    country_code: str,
) -> dict[str, str]:
    headers = dict(captured_headers)
    headers["referer"] = f"{TIKTOK_CREATIVE_CENTER_BROWSER_URL}?countryCode={country_code}&period=7"
    return headers


def _country_weight(country_code: str) -> float:
    if country_code == PRIMARY_COUNTRY_CODE:
        return PRIMARY_COUNTRY_WEIGHT
    return SECONDARY_COUNTRY_WEIGHT


def _evidence_metric(rank: int, country_ranks: dict[str, int]) -> str:
    if not country_ranks:
        return f"rank:{rank}"
    parts = [
        f"{country}:{country_ranks[country]}"
        for country in sorted(
            country_ranks,
            key=lambda country: (country != PRIMARY_COUNTRY_CODE, country_ranks[country], country),
        )
    ]
    return f"rank:{rank}|regions:{','.join(parts)}"


def _evidence_url(countries: Sequence[str]) -> str:
    country_code = PRIMARY_COUNTRY_CODE
    for country in countries:
        if country == PRIMARY_COUNTRY_CODE:
            country_code = country
            break
        if country:
            country_code = country
    return f"{TIKTOK_CREATIVE_CENTER_BROWSER_URL}?countryCode={country_code}&period=7"
