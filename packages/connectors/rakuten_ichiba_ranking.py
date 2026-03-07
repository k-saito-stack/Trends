"""Rakuten Ichiba ranking API connector."""

from __future__ import annotations

import math
import os
from collections.abc import Sequence
from typing import Any

import requests

from packages.connectors.base import BaseConnector, FetchResult, SignalResult
from packages.core.models import CandidateType, Evidence, RawCandidate

API_URL = "https://app.rakuten.co.jp/services/api/IchibaItem/Ranking/20220601"
DEFAULT_GENRE_IDS = ("100371", "551177", "216131", "558885")


class RakutenIchibaRankingConnector(BaseConnector):
    def __init__(
        self,
        genre_id: str = "100371",
        genre_ids: Sequence[str] | None = None,
        **kwargs: Any,
    ) -> None:
        super().__init__(source_id="RAKUTEN_ICHIBA_RANKING", stability="B", **kwargs)
        self.genre_id = genre_id
        self.genre_ids = tuple(genre_ids or DEFAULT_GENRE_IDS)
        self.application_id = os.environ.get("RAKUTEN_APP_ID", "")
        self.access_key = os.environ.get("RAKUTEN_ACCESS_KEY", "")

    def fetch(self) -> FetchResult:
        if not self.application_id:
            return FetchResult(error="RAKUTEN_APP_ID not set")
        genre_ids = self.genre_ids or (self.genre_id,)
        headers = {"Origin": "https://k-saito-stack.github.io"}
        errors: list[str] = []
        items: list[dict[str, Any]] = []
        for genre_id in genre_ids:
            params: dict[str, Any] = {"applicationId": self.application_id, "genreId": genre_id}
            if self.access_key:
                params["accessKey"] = self.access_key
            try:
                response = requests.get(API_URL, params=params, headers=headers, timeout=30)
                response.raise_for_status()
            except requests.RequestException as exc:
                errors.append(f"{genre_id}: {exc}")
                continue
            payload = response.json()
            for entry in payload.get("Items", []):
                item = entry.get("Item", {})
                if not isinstance(item, dict):
                    continue
                item["genreId"] = genre_id
                items.append(item)
        if items:
            return FetchResult(items=items, item_count=len(items))
        return FetchResult(error=" | ".join(errors[-4:]) if errors else "no rakuten ichiba data")

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
                            metric=f"rank:{rank},genre:{item.get('genreId', '')}",
                        ),
                        extra={"genreId": str(item.get("genreId", ""))},
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
                            metric=f"rank:{rank},genre:{item.get('genreId', '')}",
                        ),
                        extra={"genreId": str(item.get("genreId", ""))},
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
