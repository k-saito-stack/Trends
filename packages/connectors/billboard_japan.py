"""Billboard Japan rise-chart connector."""

from __future__ import annotations

import math
import re
from typing import Any

import requests

from packages.connectors.base import BaseConnector, FetchResult, SignalResult
from packages.core.domain_classifier import classify_domain
from packages.core.models import CandidateType, Evidence, ExtractionConfidence, RawCandidate

BILLBOARD_JAPAN_URL = "https://www.billboard-japan.com/charts/detail?a=hot100"
BILLBOARD_JAPAN_ARTIST_URL = "https://www.billboard-japan.com/charts/detail?a=artist"
REQUEST_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36"
    ),
    "Accept-Language": "ja,en-US;q=0.9,en;q=0.8",
}
TRACK_RE = re.compile(
    r"<li[^>]*data-track=[\"']([^\"']+)[\"'][^>]*data-artist=[\"']([^\"']+)[\"'][^>]*>"
    r"|<p[^>]*class=[\"'][^\"']*(?:music-title|title)[^\"']*[\"'][^>]*>([^<]+)</p>\s*"
    r"(?:.*?<p[^>]*class=[\"'][^\"']*(?:artist-name|artist)[^\"']*[\"'][^>]*>([^<]+)</p>)?",
    re.IGNORECASE | re.DOTALL,
)


class BillboardJapanConnector(BaseConnector):
    def __init__(self, **kwargs: Any) -> None:
        super().__init__(source_id="BILLBOARD_JAPAN", stability="A", **kwargs)

    def fetch(self) -> FetchResult:
        errors: list[str] = []
        merged_items: list[dict[str, Any]] = []
        seen: set[tuple[str, str]] = set()
        for url, chart_type in (
            (BILLBOARD_JAPAN_URL, "hot100"),
            (BILLBOARD_JAPAN_ARTIST_URL, "artist"),
        ):
            try:
                response = requests.get(url, headers=REQUEST_HEADERS, timeout=30)
                response.raise_for_status()
            except requests.RequestException as exc:
                errors.append(f"{url}: {exc}")
                continue
            items = self.parse_items(response.text)
            for item in items:
                key = (str(item.get("track", "")), str(item.get("artist", "")))
                if key in seen:
                    continue
                seen.add(key)
                item["chartType"] = chart_type
                merged_items.append(item)
        if merged_items:
            return FetchResult(items=merged_items, item_count=len(merged_items))
        return FetchResult(error=" | ".join(errors[-2:]) if errors else "no billboard data")

    def parse_items(self, html: str) -> list[dict[str, Any]]:
        items: list[dict[str, Any]] = []
        for rank, match in enumerate(TRACK_RE.findall(html), start=1):
            track = next((group.strip() for group in match[:3] if group and group.strip()), "")
            artist = next((group.strip() for group in match[1:] if group and group.strip()), "")
            if not track:
                continue
            items.append({"track": track, "artist": artist, "rank": rank})
        return items

    def extract_candidates(self, items: list[dict[str, Any]]) -> list[RawCandidate]:
        candidates: list[RawCandidate] = []
        for item in items:
            rank = int(item.get("rank", 0) or 0)
            track = str(item.get("track", "")).strip()
            artist = str(item.get("artist", "")).strip()
            if track:
                candidates.append(
                    RawCandidate(
                        name=track,
                        type=CandidateType.MUSIC_TRACK,
                        source_id=self.source_id,
                        rank=rank,
                        metric_value=_rank_exposure(rank),
                        evidence=Evidence(
                            source_id=self.source_id,
                            title=f"{track} - {artist}",
                            url=BILLBOARD_JAPAN_URL,
                            metric=f"rank:{rank}",
                        ),
                        extraction_confidence=ExtractionConfidence.HIGH,
                        domain_class=classify_domain(
                            CandidateType.MUSIC_TRACK, self.source_id, text=track
                        ),
                        extra={"chartType": item.get("chartType", "hot100")},
                    )
                )
            if artist:
                candidates.append(
                    RawCandidate(
                        name=artist,
                        type=CandidateType.MUSIC_ARTIST,
                        source_id=self.source_id,
                        rank=rank,
                        metric_value=_rank_exposure(rank) * 0.8,
                        evidence=Evidence(
                            source_id=self.source_id,
                            title=f"{artist} / {track}",
                            url=BILLBOARD_JAPAN_URL,
                            metric=f"rank:{rank}",
                        ),
                        extraction_confidence=ExtractionConfidence.HIGH,
                        domain_class=classify_domain(
                            CandidateType.MUSIC_ARTIST, self.source_id, text=artist
                        ),
                        extra={"chartType": item.get("chartType", "hot100")},
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
