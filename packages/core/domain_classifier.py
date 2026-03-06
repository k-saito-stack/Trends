"""Rule-based domain classification for v2 ranking gates."""

from __future__ import annotations

import json
from functools import lru_cache
from pathlib import Path
from typing import Any, cast

from packages.core.models import CandidateType, DomainClass, SourceFamily
from packages.core.source_catalog import get_source_entry

CONFIG_PATH = Path(__file__).resolve().parents[2] / "configs" / "domain_filters.yaml"


@lru_cache(maxsize=1)
def _load_filters() -> dict[str, list[str]]:
    return cast(dict[str, list[str]], json.loads(CONFIG_PATH.read_text(encoding="utf-8")))


def classify_domain(
    candidate_type: CandidateType,
    source_id: str,
    text: str = "",
    metadata: dict[str, Any] | None = None,
) -> DomainClass:
    metadata = metadata or {}
    lowered = f"{text} {metadata.get('title', '')} {metadata.get('publisherName', '')}".lower()
    filters = _load_filters()

    source_entry = get_source_entry(source_id)
    if source_entry is not None:
        if source_entry.family_primary == SourceFamily.MUSIC_CHART:
            return DomainClass.ENTERTAINMENT
        if source_entry.family_primary == SourceFamily.SHOW_CHART:
            return DomainClass.ENTERTAINMENT
        if source_entry.family_primary == SourceFamily.FASHION_STYLE:
            return DomainClass.FASHION_BEAUTY
        if source_entry.family_primary == SourceFamily.COMMERCE and candidate_type in {
            CandidateType.STYLE,
            CandidateType.PRODUCT,
            CandidateType.BRAND,
        }:
            return DomainClass.CONSUMER_CULTURE

    if any(keyword.lower() in lowered for keyword in filters["business_keywords"]):
        return DomainClass.BUSINESS_PROFESSIONAL

    publisher = str(metadata.get("publisherName", "")).lower()
    if any(keyword.lower() in publisher for keyword in filters["business_publishers"]):
        return DomainClass.BUSINESS_PROFESSIONAL

    if candidate_type in {
        CandidateType.PERSON,
        CandidateType.GROUP,
        CandidateType.MUSIC_ARTIST,
        CandidateType.MUSIC_TRACK,
        CandidateType.WORK,
        CandidateType.SHOW,
        CandidateType.REALITY_SHOW,
    }:
        return DomainClass.ENTERTAINMENT

    if any(keyword.lower() in lowered for keyword in filters["fashion_keywords"]):
        return DomainClass.FASHION_BEAUTY

    if any(keyword.lower() in lowered for keyword in filters["consumer_keywords"]):
        return DomainClass.CONSUMER_CULTURE

    if any(keyword.lower() in lowered for keyword in filters["entertainment_keywords"]):
        return DomainClass.ENTERTAINMENT

    if candidate_type in {CandidateType.STYLE, CandidateType.PRODUCT, CandidateType.BRAND}:
        return DomainClass.FASHION_BEAUTY

    if candidate_type in {
        CandidateType.PHRASE,
        CandidateType.HASHTAG,
        CandidateType.BEHAVIOR,
        CandidateType.KEYWORD,
    }:
        return DomainClass.CONSUMER_CULTURE

    return DomainClass.OTHER


def is_main_ranking_domain(domain_class: DomainClass) -> bool:
    return domain_class in {
        DomainClass.ENTERTAINMENT,
        DomainClass.FASHION_BEAUTY,
        DomainClass.CONSUMER_CULTURE,
    }
