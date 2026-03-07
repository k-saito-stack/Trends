"""Spotify Charts official site connector."""

from __future__ import annotations

import html
import math
import re
from hashlib import sha1
from typing import Any

import requests

from packages.connectors.base import BaseConnector, FetchResult, SignalResult
from packages.core.domain_classifier import classify_domain
from packages.core.models import CandidateType, Evidence, ExtractionConfidence, RawCandidate

SPOTIFY_CHARTS_JP_URL = "https://charts.spotify.com/charts/view/regional-jp-daily/latest"
REQUEST_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36"
    ),
    "Accept-Language": "ja,en-US;q=0.9,en;q=0.8",
}
ROW_RE = re.compile(r"<tr[^>]*>(.*?)</tr>", re.IGNORECASE | re.DOTALL)
RANK_RE = re.compile(r"(?:data-rank=[\"']|<td[^>]*>)(\d{1,3})(?:[\"']|</td>)", re.IGNORECASE)
TRACK_RE = re.compile(
    r"(?:data-testid=[\"']track-name[\"'][^>]*>|class=[\"'][^\"']*track-name[^\"']*[\"'][^>]*>)"
    r"\s*([^<]+?)\s*</",
    re.IGNORECASE | re.DOTALL,
)
ARTIST_RE = re.compile(
    r"(?:data-testid=[\"']artist-name[\"'][^>]*>|class=[\"'][^\"']*artist-name[^\"']*[\"'][^>]*>)"
    r"\s*([^<]+?)\s*</",
    re.IGNORECASE | re.DOTALL,
)
SCRIPT_ENTRY_RE = re.compile(
    r'"rank"\s*:\s*(\d{1,3}).*?"trackName"\s*:\s*"([^"]+)".*?"artistName"\s*:\s*"([^"]+)"',
    re.IGNORECASE | re.DOTALL,
)


class SpotifyChartsConnector(BaseConnector):
    def __init__(self, region: str = "jp", **kwargs: Any) -> None:
        source_id = kwargs.pop("source_id", f"SPOTIFY_CHARTS_{region.upper()}")
        super().__init__(source_id=source_id, stability="B", **kwargs)
        self.region = region.lower()

    @property
    def chart_url(self) -> str:
        if self.region == "kr":
            return "https://charts.spotify.com/charts/view/regional-kr-daily/latest"
        return SPOTIFY_CHARTS_JP_URL

    def fetch(self) -> FetchResult:
        try:
            response = requests.get(self.chart_url, headers=REQUEST_HEADERS, timeout=30)
            response.raise_for_status()
        except requests.RequestException as exc:
            return FetchResult(error=str(exc))
        items = self.parse_items(response.text)
        return FetchResult(items=items, item_count=len(items))

    def parse_items(self, html_text: str) -> list[dict[str, Any]]:
        items: list[dict[str, Any]] = []
        seen: set[tuple[int, str, str]] = set()

        for row_html in ROW_RE.findall(html_text):
            rank_match = RANK_RE.search(row_html)
            track_match = TRACK_RE.search(row_html)
            artist_match = ARTIST_RE.search(row_html)
            if not rank_match or not track_match:
                continue
            rank = int(rank_match.group(1))
            track = _clean_text(track_match.group(1))
            artist = _clean_text(artist_match.group(1) if artist_match else "")
            key = (rank, track, artist)
            if not track or key in seen:
                continue
            seen.add(key)
            items.append({"track": track, "artist": artist, "rank": rank})

        if items:
            return items

        for rank_raw, track_raw, artist_raw in SCRIPT_ENTRY_RE.findall(html_text):
            rank = int(rank_raw)
            track = _clean_text(track_raw)
            artist = _clean_text(artist_raw)
            key = (rank, track, artist)
            if not track or key in seen:
                continue
            seen.add(key)
            items.append({"track": track, "artist": artist, "rank": rank})

        return sorted(items, key=lambda item: int(item.get("rank", 0) or 0))

    def extract_candidates(self, items: list[dict[str, Any]]) -> list[RawCandidate]:
        candidates: list[RawCandidate] = []
        for item in items:
            rank = int(item.get("rank", 0) or 0)
            track = str(item.get("track", "")).strip()
            artist = str(item.get("artist", "")).strip()
            source_item_id = _source_item_id(self.source_id, rank, track, artist)
            if track:
                candidates.append(
                    RawCandidate(
                        name=track,
                        type=CandidateType.MUSIC_TRACK,
                        source_id=self.source_id,
                        source_item_id=source_item_id,
                        rank=rank,
                        metric_value=_rank_exposure(rank),
                        evidence=Evidence(
                            source_id=self.source_id,
                            title=f"{track} - {artist}",
                            url=self.chart_url,
                            metric=f"rank:{rank}",
                        ),
                        extraction_confidence=ExtractionConfidence.HIGH,
                        domain_class=classify_domain(
                            CandidateType.MUSIC_TRACK,
                            self.source_id,
                            text=track,
                        ),
                        extra={"artist": artist, "region": self.region.upper()},
                    )
                )
            if artist:
                candidates.append(
                    RawCandidate(
                        name=artist,
                        type=CandidateType.MUSIC_ARTIST,
                        source_id=self.source_id,
                        source_item_id=source_item_id,
                        rank=rank,
                        metric_value=_rank_exposure(rank) * 0.75,
                        evidence=Evidence(
                            source_id=self.source_id,
                            title=f"{artist} / {track}",
                            url=self.chart_url,
                            metric=f"rank:{rank}",
                        ),
                        extraction_confidence=ExtractionConfidence.HIGH,
                        domain_class=classify_domain(
                            CandidateType.MUSIC_ARTIST,
                            self.source_id,
                            text=artist,
                        ),
                        extra={"track": track, "region": self.region.upper()},
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


def _clean_text(value: str) -> str:
    return re.sub(r"\s+", " ", html.unescape(value or "")).strip()


def _rank_exposure(rank: int) -> float:
    return 1.0 / math.log2(max(rank, 1) + 1)


def _source_item_id(source_id: str, rank: int, track: str, artist: str) -> str:
    raw = f"{source_id}|{rank}|{track}|{artist}"
    return sha1(raw.encode("utf-8")).hexdigest()[:16]
