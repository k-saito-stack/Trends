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
from packages.core.topic_extract import extract_topic_candidates
from packages.core.topic_normalize import should_keep_topic

TIKTOK_SURFACE_PATHS = {
    "hashtag": "hashtag",
    "song": "song",
    "creator": "creator",
    "video": "video",
}
TIKTOK_SURFACE_SOURCE_IDS = {
    "hashtag": "TIKTOK_CREATIVE_CENTER_HASHTAGS",
    "song": "TIKTOK_CREATIVE_CENTER_SONGS",
    "creator": "TIKTOK_CREATIVE_CENTER_CREATORS",
    "video": "TIKTOK_CREATIVE_CENTER_VIDEOS",
}
LEGACY_TIKTOK_SOURCE_ID = "TIKTOK_CREATIVE_CENTER"
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
        surface: str = "hashtag",
        source_id: str | None = None,
        **kwargs: Any,
    ) -> None:
        normalized_surface = surface if surface in TIKTOK_SURFACE_PATHS else "hashtag"
        resolved_source_id = source_id or (
            LEGACY_TIKTOK_SOURCE_ID
            if normalized_surface == "hashtag"
            else TIKTOK_SURFACE_SOURCE_IDS[normalized_surface]
        )
        super().__init__(source_id=resolved_source_id, stability="A", **kwargs)
        self.max_results = max_results
        self.surface = normalized_surface
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
            response = requests.get(_public_page_url(self.surface), timeout=30)
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
            session.get(_browser_page_url(self.surface), timeout=30)
        except requests.RequestException as exc:
            raise RuntimeError(f"tiktok regional bootstrap failed: {exc}") from exc

        by_country: dict[str, list[dict[str, Any]]] = {}
        for country_code in self.country_codes:
            try:
                response = session.get(
                    _build_api_url(self.surface, country_code, self.max_results),
                    headers=_build_browser_api_headers(
                        self.surface,
                        browser_headers,
                        country_code,
                    ),
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
                if _api_url_prefix(self.surface) not in str(request.url):
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
                _browser_page_url(self.surface),
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
        if self.surface == "hashtag":
            return self._parse_hashtag_items(html)
        return self._parse_generic_surface_items(html)

    def _parse_hashtag_items(self, html: str) -> list[dict[str, Any]]:
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

    def _parse_generic_surface_items(self, html_text: str) -> list[dict[str, Any]]:
        row_matches = ROW_RE.findall(html_text)
        items: list[dict[str, Any]] = []
        seen: set[str] = set()
        for rank, (href, body) in enumerate(row_matches, start=1):
            title = _extract_ranked_keyword(href, body)
            if not title:
                title = _extract_generic_title(body)
            if not title or title in seen:
                continue
            seen.add(title)
            items.append({"name": title, "rank": rank})
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
            rank = int(row.get("rank", 0) or 0)
            if rank <= 0:
                continue
            parsed = _parse_surface_row(self.surface, row)
            if parsed is None:
                continue
            parsed["rank"] = rank
            parsed["countryCode"] = country_code
            parsed["publishCount"] = int(row.get("publish_cnt", 0) or 0)
            parsed["videoViews"] = int(row.get("video_views", 0) or 0)
            items.append(parsed)
        return items

    def merge_regional_items(
        self,
        items_by_country: dict[str, list[dict[str, Any]]],
    ) -> list[dict[str, Any]]:
        merged: dict[str, dict[str, Any]] = {}
        for country_code, items in items_by_country.items():
            for item in items:
                keyword = _surface_item_key(item)
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
                for key, value in item.items():
                    if key not in {"rank", "countryCode"} and key not in entry:
                        entry[key] = value
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
        if self.surface == "song":
            return self._extract_song_candidates(items)
        if self.surface == "creator":
            return self._extract_creator_candidates(items)
        if self.surface == "video":
            return self._extract_video_candidates(items)

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
            extra["surface"] = self.surface
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
                        url=_evidence_url(self.surface, countries),
                        metric=_evidence_metric(rank, country_ranks),
                    ),
                    extraction_confidence=ExtractionConfidence.HIGH,
                    domain_class=classify_domain(candidate_type, self.source_id, text=keyword),
                    extra=extra,
                )
            )
        return candidates

    def _extract_song_candidates(self, items: list[dict[str, Any]]) -> list[RawCandidate]:
        candidates: list[RawCandidate] = []
        for item in items:
            rank = int(item.get("rank", 0) or 0)
            track = str(item.get("name", "")).strip()
            artist = str(item.get("artist", "")).strip()
            metric_value = float(item.get("regionalScore") or _rank_exposure(rank))
            extra = _surface_extra(item, self.surface, metric_value)
            if track:
                candidates.append(
                    RawCandidate(
                        name=track,
                        type=CandidateType.MUSIC_TRACK,
                        source_id=self.source_id,
                        rank=rank,
                        metric_value=metric_value,
                        evidence=Evidence(
                            source_id=self.source_id,
                            title=f"{track} - {artist}",
                            url=_evidence_url(self.surface, extra.get("countries", [])),
                            metric=_evidence_metric(rank, extra.get("countryRanks", {})),
                        ),
                        extraction_confidence=ExtractionConfidence.HIGH,
                        domain_class=classify_domain(
                            CandidateType.MUSIC_TRACK,
                            self.source_id,
                            text=track,
                        ),
                        extra=dict(extra, artist=artist),
                    )
                )
            if artist:
                candidates.append(
                    RawCandidate(
                        name=artist,
                        type=CandidateType.MUSIC_ARTIST,
                        source_id=self.source_id,
                        rank=rank,
                        metric_value=metric_value * 0.75,
                        evidence=Evidence(
                            source_id=self.source_id,
                            title=f"{artist} / {track}",
                            url=_evidence_url(self.surface, extra.get("countries", [])),
                            metric=_evidence_metric(rank, extra.get("countryRanks", {})),
                        ),
                        extraction_confidence=ExtractionConfidence.MEDIUM,
                        domain_class=classify_domain(
                            CandidateType.MUSIC_ARTIST,
                            self.source_id,
                            text=artist,
                        ),
                        extra=dict(extra, track=track),
                    )
                )
        return candidates

    def _extract_creator_candidates(self, items: list[dict[str, Any]]) -> list[RawCandidate]:
        candidates: list[RawCandidate] = []
        for item in items:
            rank = int(item.get("rank", 0) or 0)
            name = str(item.get("name", "")).strip()
            if not name:
                continue
            metric_value = float(item.get("regionalScore") or _rank_exposure(rank))
            extra = _surface_extra(item, self.surface, metric_value)
            candidates.append(
                RawCandidate(
                    name=name,
                    type=CandidateType.PERSON,
                    source_id=self.source_id,
                    rank=rank,
                    metric_value=metric_value,
                    evidence=Evidence(
                        source_id=self.source_id,
                        title=name,
                        url=_evidence_url(self.surface, extra.get("countries", [])),
                        metric=_evidence_metric(rank, extra.get("countryRanks", {})),
                    ),
                    extraction_confidence=ExtractionConfidence.MEDIUM,
                    domain_class=classify_domain(CandidateType.PERSON, self.source_id, text=name),
                    extra=extra,
                )
            )
        return candidates

    def _extract_video_candidates(self, items: list[dict[str, Any]]) -> list[RawCandidate]:
        candidates: list[RawCandidate] = []
        for item in items:
            rank = int(item.get("rank", 0) or 0)
            name = str(item.get("name", "")).strip()
            metric_value = float(item.get("regionalScore") or _rank_exposure(rank))
            extra = _surface_extra(item, self.surface, metric_value)
            hashtag_text = " ".join(
                str(tag) for tag in item.get("hashtags", []) if isinstance(tag, str)
            )
            text = " ".join([name, hashtag_text]).strip()
            evidence = Evidence(
                source_id=self.source_id,
                title=name[:120] if name else "TikTok video theme",
                url=_evidence_url(self.surface, extra.get("countries", [])),
                metric=_evidence_metric(rank, extra.get("countryRanks", {})),
            )
            extracted = extract_topic_candidates(
                text,
                self.source_id,
                dict(extra),
                metric_value=metric_value,
                evidence=evidence,
                max_candidates=5,
            )
            for candidate in extracted:
                candidate.rank = rank
                candidates.append(candidate)
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


def _extract_generic_title(body: str) -> str:
    title_match = TITLE_RE.search(body)
    if not title_match:
        return ""
    return _clean_keyword(title_match.group(1))


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


def _build_api_url(surface: str, country_code: str, limit: int) -> str:
    return (
        f"{_api_url_prefix(surface)}?page=1&limit={limit}&period=7"
        f"&country_code={country_code}&sort_by=popular"
    )


def _build_browser_api_headers(
    surface: str,
    captured_headers: dict[str, str],
    country_code: str,
) -> dict[str, str]:
    headers = dict(captured_headers)
    headers["referer"] = f"{_browser_page_url(surface)}?countryCode={country_code}&period=7"
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


def _evidence_url(surface: str, countries: Sequence[str]) -> str:
    country_code = PRIMARY_COUNTRY_CODE
    for country in countries:
        if country == PRIMARY_COUNTRY_CODE:
            country_code = country
            break
        if country:
            country_code = country
    return f"{_browser_page_url(surface)}?countryCode={country_code}&period=7"


def _surface_extra(item: dict[str, Any], surface: str, metric_value: float) -> dict[str, Any]:
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
    extra: dict[str, Any] = {"surface": surface}
    if countries:
        extra["countries"] = countries
    if country_ranks:
        extra["countryRanks"] = country_ranks
    if item.get("countryCode"):
        extra["countryCode"] = str(item["countryCode"])
    if item.get("regionalScore") is not None:
        extra["regionalScore"] = metric_value
    return extra


def _public_page_url(surface: str) -> str:
    path = TIKTOK_SURFACE_PATHS.get(surface, "hashtag")
    return f"https://ads.tiktok.com/business/creativecenter/inspiration/popular/{path}/pc/en"


def _browser_page_url(surface: str) -> str:
    path = TIKTOK_SURFACE_PATHS.get(surface, "hashtag")
    return f"https://ads.tiktok.com/business/creativecenter/inspiration/popular/{path}/pad/en"


def _api_url_prefix(surface: str) -> str:
    path = TIKTOK_SURFACE_PATHS.get(surface, "hashtag")
    return f"https://ads.tiktok.com/creative_radar_api/v1/popular_trend/{path}/list"


def _parse_surface_row(surface: str, row: dict[str, Any]) -> dict[str, Any] | None:
    if surface == "hashtag":
        keyword = _clean_keyword(f"#{row.get('hashtag_name', '')}")
        if not keyword or not should_keep_topic(keyword):
            return None
        return {"keyword": keyword}
    if surface == "song":
        track = _clean_keyword(
            str(
                row.get("song_name")
                or row.get("music_name")
                or row.get("title")
                or row.get("name")
                or ""
            )
        )
        artist = _clean_keyword(
            str(row.get("artist_name") or row.get("author_name") or row.get("creator_name") or "")
        )
        if not track:
            return None
        return {"name": track, "artist": artist}
    if surface == "creator":
        name = _clean_keyword(
            str(
                row.get("creator_name")
                or row.get("author_name")
                or row.get("nickname")
                or row.get("title")
                or row.get("name")
                or ""
            )
        )
        if not name:
            return None
        return {"name": name}

    title = _clean_keyword(
        str(
            row.get("video_desc")
            or row.get("video_name")
            or row.get("title")
            or row.get("name")
            or ""
        )
    )
    hashtags = row.get("hashtags") or row.get("hashtag_names") or []
    if isinstance(hashtags, str):
        hashtags = [hashtags]
    normalized_tags = [
        _clean_keyword(f"#{tag}" if not str(tag).strip().startswith("#") else str(tag))
        for tag in hashtags
        if str(tag).strip()
    ]
    if not title and not normalized_tags:
        return None
    return {"name": title, "hashtags": normalized_tags}


def _surface_item_key(item: dict[str, Any]) -> str:
    if item.get("keyword"):
        return str(item.get("keyword", "")).strip()
    return str(item.get("name", "")).strip()


class TikTokCreativeCenterHashtagConnector(TikTokCreativeCenterConnector):
    def __init__(self, **kwargs: Any) -> None:
        super().__init__(
            surface="hashtag",
            source_id=TIKTOK_SURFACE_SOURCE_IDS["hashtag"],
            **kwargs,
        )


class TikTokCreativeCenterSongsConnector(TikTokCreativeCenterConnector):
    def __init__(self, **kwargs: Any) -> None:
        super().__init__(
            surface="song",
            source_id=TIKTOK_SURFACE_SOURCE_IDS["song"],
            **kwargs,
        )


class TikTokCreativeCenterCreatorsConnector(TikTokCreativeCenterConnector):
    def __init__(self, **kwargs: Any) -> None:
        super().__init__(
            surface="creator",
            source_id=TIKTOK_SURFACE_SOURCE_IDS["creator"],
            **kwargs,
        )


class TikTokCreativeCenterVideosConnector(TikTokCreativeCenterConnector):
    def __init__(self, **kwargs: Any) -> None:
        super().__init__(
            surface="video",
            source_id=TIKTOK_SURFACE_SOURCE_IDS["video"],
            **kwargs,
        )
