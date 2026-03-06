"""Rakuten Ichiba ranking API connector."""

from __future__ import annotations

import math
import os
from typing import Any

import requests

from packages.connectors.base import BaseConnector, FetchResult, SignalResult
from packages.core.models import CandidateType, Evidence, RawCandidate

API_URL = "https://app.rakuten.co.jp/services/api/IchibaItem/Ranking/20220601"


class RakutenIchibaRankingConnector(BaseConnector):
    def __init__(self, genre_id: str = "100371", **kwargs: Any) -> None:
        super().__init__(source_id="RAKUTEN_ICHIBA_RANKING", stability="B", **kwargs)
        self.genre_id = genre_id
        self.application_id = os.environ.get("RAKUTEN_APP_ID", "")

    def fetch(self) -> FetchResult:
        if not self.application_id:
            return FetchResult(error="RAKUTEN_APP_ID not set")
        params = {"applicationId": self.application_id, "genreId": self.genre_id}
        try:
            response = requests.get(API_URL, params=params, timeout=30)
            response.raise_for_status()
        except requests.RequestException as exc:
            return FetchResult(error=str(exc))
        payload = response.json()
        items = [entry.get("Item", {}) for entry in payload.get("Items", [])]
        return FetchResult(items=items, item_count=len(items))

    def extract_candidates(self, items: list[dict[str, Any]]) -> list[RawCandidate]:
        candidates: list[RawCandidate] = []
        for rank, item in enumerate(items, start=1):
            item_name = str(item.get("itemName", "")).strip()
            shop_name = str(item.get("shopName", "")).strip()
            if item_name:
                candidates.append(
                    RawCandidate(
                        name=item_name,
                        type=CandidateType.PRODUCT,
                        source_id=self.source_id,
                        rank=rank,
                        metric_value=_rank_exposure(rank),
                        evidence=Evidence(
                            source_id=self.source_id,
                            title=item_name,
                            url=str(item.get("itemUrl", "")),
                            metric=f"rank:{rank}",
                        ),
                    )
                )
            if shop_name:
                candidates.append(
                    RawCandidate(
                        name=shop_name,
                        type=CandidateType.BRAND,
                        source_id=self.source_id,
                        rank=rank,
                        metric_value=_rank_exposure(rank) * 0.7,
                        evidence=Evidence(
                            source_id=self.source_id,
                            title=f"{shop_name} / {item_name}",
                            url=str(item.get("itemUrl", "")),
                            metric=f"rank:{rank}",
                        ),
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
