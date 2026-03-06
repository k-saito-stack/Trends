"""Billboard Japan rise-chart connector."""

from __future__ import annotations

import math
import re
from typing import Any

import requests

from packages.connectors.base import BaseConnector, FetchResult, SignalResult
from packages.core.domain_classifier import classify_domain
from packages.core.models import CandidateType, Evidence, ExtractionConfidence, RawCandidate

BILLBOARD_JAPAN_URL = "https://www.billboard-japan.com/charts/"
TRACK_RE = re.compile(
    r"<li[^>]*data-track=[\"']([^\"']+)[\"'][^>]*data-artist=[\"']([^\"']+)[\"'][^>]*>",
    re.IGNORECASE,
)


class BillboardJapanConnector(BaseConnector):
    def __init__(self, **kwargs: Any) -> None:
        super().__init__(source_id="BILLBOARD_JAPAN", stability="A", **kwargs)

    def fetch(self) -> FetchResult:
        try:
            response = requests.get(BILLBOARD_JAPAN_URL, timeout=30)
            response.raise_for_status()
        except requests.RequestException as exc:
            return FetchResult(error=str(exc))
        items = self.parse_items(response.text)
        return FetchResult(items=items, item_count=len(items))

    def parse_items(self, html: str) -> list[dict[str, Any]]:
        items: list[dict[str, Any]] = []
        for rank, match in enumerate(TRACK_RE.findall(html), start=1):
            items.append({"track": match[0].strip(), "artist": match[1].strip(), "rank": rank})
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
                        domain_class=classify_domain(CandidateType.MUSIC_TRACK, self.source_id, text=track),
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
                        domain_class=classify_domain(CandidateType.MUSIC_ARTIST, self.source_id, text=artist),
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
