"""Japan-market realism features layered on top of v2 fusion."""

from __future__ import annotations

from packages.core.family_features import FamilyAggregateMetrics
from packages.core.models import Candidate, DailySourceFeature, DomainClass

CONSTRAINED_TRENDS_ENT = "TRENDS_JP_24H_ENT"
CONSTRAINED_TRENDS_BEAUTY = "TRENDS_JP_24H_BEAUTY_FASHION"
YAHOO_REALTIME = "YAHOO_REALTIME"
JP_SOURCE_IDS = {
    CONSTRAINED_TRENDS_ENT,
    CONSTRAINED_TRENDS_BEAUTY,
    YAHOO_REALTIME,
    "APPLE_MUSIC_JP",
    "NETFLIX_TV_JP",
    "NETFLIX_FILMS_JP",
    "TVER_RANKING_JP",
    "YOUTUBE_TREND_JP",
}
ASIA_COUNTRIES = {"JP", "KR", "TW", "HK", "TH", "VN", "ID", "PH", "MY", "SG"}


def compute_jp_relevance(feature_list: list[DailySourceFeature], candidate: Candidate) -> float:
    jp_sources = 0
    asia_overlap = 0
    for feature in feature_list:
        countries = {
            str(country).upper()
            for country in feature.metadata.get("countries", [])
            if isinstance(country, str) and country
        }
        if feature.source_id in JP_SOURCE_IDS or "JP" in countries or feature.metadata.get(
            "countryCode"
        ) == "JP":
            jp_sources += 1
        if countries & ASIA_COUNTRIES:
            asia_overlap += len(countries & ASIA_COUNTRIES)
    base = min(1.0, jp_sources * 0.24 + min(asia_overlap, 4) * 0.08)
    if candidate.domain_class in {DomainClass.ENTERTAINMENT, DomainClass.FASHION_BEAUTY}:
        base += 0.08
    return round(max(0.0, min(1.0, base)), 4)


def compute_constrained_trends_support(
    feature_list: list[DailySourceFeature],
    domain_class: DomainClass,
) -> tuple[float, float]:
    ent = sum(
        feature.surprise01
        for feature in feature_list
        if feature.source_id == CONSTRAINED_TRENDS_ENT
    )
    beauty = sum(
        feature.surprise01
        for feature in feature_list
        if feature.source_id == CONSTRAINED_TRENDS_BEAUTY
    )
    if domain_class == DomainClass.ENTERTAINMENT:
        ent = min(1.0, ent * 1.1)
    if domain_class == DomainClass.FASHION_BEAUTY:
        beauty = min(1.0, beauty * 1.1)
    return round(min(1.0, ent), 4), round(min(1.0, beauty), 4)


def compute_yahoo_realtime_support(feature_list: list[DailySourceFeature]) -> float:
    total = sum(
        feature.surprise01
        for feature in feature_list
        if feature.source_id == YAHOO_REALTIME
    )
    return round(
        min(1.0, total),
        4,
    )


def compute_mature_mass_only_penalty(
    candidate: Candidate,
    aggregate: FamilyAggregateMetrics,
) -> float:
    if candidate.maturity < 0.8:
        return 0.0
    if float(aggregate.get("has_discovery", 0.0)) > 0:
        return 0.0
    mass_confirmation = float(aggregate.get("music_confirmation", 0.0)) + float(
        aggregate.get("show_confirmation", 0.0)
    )
    if mass_confirmation <= 0:
        return 0.0
    return round(min(1.0, 0.28 + candidate.maturity * 0.35 + mass_confirmation * 0.18), 4)
