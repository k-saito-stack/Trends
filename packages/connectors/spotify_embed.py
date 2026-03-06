"""Spotify public embed connector."""

from __future__ import annotations

import math
import re
from typing import Any

import requests

from packages.connectors.base import BaseConnector, FetchResult, SignalResult
from packages.core.models import CandidateType, Evidence, RawCandidate

SPOTIFY_EMBED_URL = "https://open.spotify.com/embed/playlist/37i9dQZEVXbKXQ4mDTEBXq"
TRACK_RE = re.compile(r"data-track=[\"']([^\"']+)[\"'][^>]*data-artist=[\"']([^\"']+)[\"']", re.IGNORECASE)


class SpotifyEmbedConnector(BaseConnector):
    def __init__(self, **kwargs: Any) -> None:
        super().__init__(source_id="SPOTIFY_EMBED", stability="B", **kwargs)

    def fetch(self) -> FetchResult:
        try:
            response = requests.get(SPOTIFY_EMBED_URL, timeout=30)
            response.raise_for_status()
        except requests.RequestException as exc:
            return FetchResult(error=str(exc))
        items = [{"track": track, "artist": artist, "rank": idx + 1} for idx, (track, artist) in enumerate(TRACK_RE.findall(response.text))]
        return FetchResult(items=items, item_count=len(items))

    def extract_candidates(self, items: list[dict[str, Any]]) -> list[RawCandidate]:
        candidates: list[RawCandidate] = []
        for item in items:
            rank = int(item.get("rank", 0) or 0)
            track = str(item.get("track", "")).strip()
            artist = str(item.get("artist", "")).strip()
            if track:
                candidates.append(RawCandidate(name=track, type=CandidateType.MUSIC_TRACK, source_id=self.source_id, rank=rank, metric_value=_rank_exposure(rank), evidence=Evidence(source_id=self.source_id, title=f"{track} - {artist}", url=SPOTIFY_EMBED_URL, metric=f"rank:{rank}")))
            if artist:
                candidates.append(RawCandidate(name=artist, type=CandidateType.MUSIC_ARTIST, source_id=self.source_id, rank=rank, metric_value=_rank_exposure(rank) * 0.75, evidence=Evidence(source_id=self.source_id, title=f"{artist} / {track}", url=SPOTIFY_EMBED_URL, metric=f"rank:{rank}")))
        return candidates

    def compute_signals(self, items: list[dict[str, Any]], candidates: list[RawCandidate]) -> list[SignalResult]:
        return [SignalResult(candidate_name=candidate.name, signal_value=candidate.metric_value, evidence=candidate.evidence) for candidate in candidates]


def _rank_exposure(rank: int) -> float:
    return 1.0 / math.log2(max(rank, 1) + 1)
