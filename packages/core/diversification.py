"""Lane assignment and soft-quota interleaving."""

from __future__ import annotations

import json
from collections import defaultdict
from pathlib import Path
from typing import Any, cast

from packages.core.models import CandidateType, RankingLane

CONFIG_PATH = Path(__file__).resolve().parents[2] / "configs" / "type_diversification.yaml"


def _load_config() -> dict[str, Any]:
    return cast(dict[str, Any], json.loads(CONFIG_PATH.read_text(encoding="utf-8")))


def infer_lane(candidate_type: CandidateType) -> RankingLane:
    lane_name = _load_config()["lane_by_type"].get(candidate_type.value, RankingLane.SHADOW.value)
    return RankingLane(lane_name)


def interleave_ranked_items(items: list[dict[str, Any]], top_k: int = 20) -> list[dict[str, Any]]:
    if not items:
        return []

    config = _load_config()["soft_quota"]
    grouped: dict[RankingLane, list[dict[str, Any]]] = defaultdict(list)
    for item in sorted(
        items, key=lambda entry: -entry.get("primary_score", entry.get("trend_score", 0.0))
    ):
        grouped[RankingLane(item["lane"])].append(item)

    counts = {lane: 0 for lane in RankingLane}
    people_music_max = max(1, int(top_k * config["people_music_max_ratio"]))
    words_behaviors_min = int(top_k * config["words_behaviors_min_ratio"] + 0.999)
    style_products_min = int(top_k * config["style_products_min_ratio"] + 0.999)

    selected: list[dict[str, Any]] = []
    while len(selected) < top_k:
        remaining_slots = top_k - len(selected)
        unmet_words = max(0, words_behaviors_min - counts[RankingLane.WORDS_BEHAVIORS])
        unmet_style = max(0, style_products_min - counts[RankingLane.STYLE_PRODUCTS])

        forced_lanes: set[RankingLane] = set()
        if remaining_slots <= unmet_words + unmet_style:
            if unmet_words and grouped[RankingLane.WORDS_BEHAVIORS]:
                forced_lanes.add(RankingLane.WORDS_BEHAVIORS)
            if unmet_style and grouped[RankingLane.STYLE_PRODUCTS]:
                forced_lanes.add(RankingLane.STYLE_PRODUCTS)

        best_lane: RankingLane | None = None
        best_item: dict[str, Any] | None = None

        for lane, queue in grouped.items():
            if not queue:
                continue
            if forced_lanes and lane not in forced_lanes:
                continue
            if lane == RankingLane.PEOPLE_MUSIC and counts[lane] >= people_music_max:
                other_available = any(grouped[other] for other in grouped if other != lane)
                if other_available:
                    continue
            candidate = queue[0]
            candidate_score = float(
                candidate.get("primary_score", candidate.get("trend_score", 0.0))
            )
            candidate_score += _lane_boost(lane, counts, config)
            if best_item is None or candidate_score > float(
                best_item.get("_selectionScore", float("-inf"))
            ):
                candidate["_selectionScore"] = candidate_score
                best_item = candidate
                best_lane = lane

        if best_item is None or best_lane is None:
            break

        grouped[best_lane].pop(0)
        best_item.pop("_selectionScore", None)
        selected.append(best_item)
        counts[best_lane] += 1

    return selected


def _lane_boost(
    lane: RankingLane, counts: dict[RankingLane, int], config: dict[str, float]
) -> float:
    if lane == RankingLane.WORDS_BEHAVIORS and counts[lane] == 0:
        return 0.08
    if lane == RankingLane.STYLE_PRODUCTS and counts[lane] == 0:
        return 0.06
    if lane == RankingLane.SHOWS_FORMATS and counts[lane] < max(
        1, int(20 * config["shows_formats_target_ratio"])
    ):
        return 0.03
    return 0.0
