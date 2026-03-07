"""Connector registry wired to the v2 source catalog."""

from __future__ import annotations

import logging
from collections.abc import Callable
from typing import Any

from packages.connectors.apple_music import AppleMusicConnector
from packages.connectors.base import BaseConnector
from packages.connectors.billboard_japan import BillboardJapanConnector
from packages.connectors.editorial_fashionsnap import EditorialFashionsnapConnector
from packages.connectors.editorial_magazine import EditorialMagazineConnector
from packages.connectors.editorial_modelpress import EditorialModelpressConnector
from packages.connectors.google_trends import GoogleTrendsConnector
from packages.connectors.netflix import NetflixTop10Connector
from packages.connectors.rakuten_fashion import RakutenFashionConnector
from packages.connectors.rakuten_ichiba_ranking import RakutenIchibaRankingConnector
from packages.connectors.spotify_charts import SpotifyChartsConnector
from packages.connectors.spotify_embed import SpotifyEmbedConnector
from packages.connectors.tiktok_creative_center import (
    TikTokCreativeCenterConnector,
    TikTokCreativeCenterCreatorsConnector,
    TikTokCreativeCenterHashtagConnector,
    TikTokCreativeCenterSongsConnector,
    TikTokCreativeCenterVideosConnector,
)
from packages.connectors.tver import TVerRankingConnector
from packages.connectors.wear import WearConnector
from packages.connectors.yahoo_realtime import YahooRealtimeConnector
from packages.connectors.youtube import YouTubeConnector
from packages.connectors.zozo import ZozoConnector
from packages.core.models import SourceRole, SourceStatus
from packages.core.source_catalog import get_source_entry, iter_active_catalog

logger = logging.getLogger(__name__)

FACTORIES: dict[str, Callable[[dict[str, Any]], BaseConnector]] = {
    "TRENDS": lambda cfg: GoogleTrendsConnector(enabled=cfg.get("enabled", True)),
    "YAHOO_REALTIME": lambda cfg: YahooRealtimeConnector(enabled=cfg.get("enabled", True)),
    "WEAR_WORDS": lambda cfg: WearConnector(enabled=cfg.get("enabled", True)),
    "ZOZO_RANKING": lambda cfg: ZozoConnector(enabled=cfg.get("enabled", True)),
    "RAKUTEN_FASHION": lambda cfg: RakutenFashionConnector(enabled=cfg.get("enabled", True)),
    "RAKUTEN_ICHIBA_RANKING": lambda cfg: RakutenIchibaRankingConnector(
        enabled=cfg.get("enabled", True)
    ),
    "BILLBOARD_JAPAN": lambda cfg: BillboardJapanConnector(enabled=cfg.get("enabled", True)),
    "APPLE_MUSIC_JP": lambda cfg: AppleMusicConnector(
        region="JP", max_results=cfg.get("fetchLimit", 50), enabled=cfg.get("enabled", True)
    ),
    "APPLE_MUSIC_KR": lambda cfg: AppleMusicConnector(
        region="KR", max_results=cfg.get("fetchLimit", 50), enabled=cfg.get("enabled", True)
    ),
    "APPLE_MUSIC_GLOBAL": lambda cfg: AppleMusicConnector(
        region="GLOBAL", max_results=cfg.get("fetchLimit", 50), enabled=cfg.get("enabled", True)
    ),
    "SPOTIFY_EMBED": lambda cfg: SpotifyEmbedConnector(enabled=cfg.get("enabled", True)),
    "SPOTIFY_CHARTS_JP": lambda cfg: SpotifyChartsConnector(
        region="jp",
        max_consecutive_failures=cfg.get("maxConsecutiveFailures", 3),
        enabled=cfg.get("enabled", True),
    ),
    "NETFLIX_TV_JP": lambda cfg: NetflixTop10Connector(
        category="tv", max_results=cfg.get("fetchLimit", 10), enabled=cfg.get("enabled", True)
    ),
    "NETFLIX_FILMS_JP": lambda cfg: NetflixTop10Connector(
        category="films", max_results=cfg.get("fetchLimit", 10), enabled=cfg.get("enabled", True)
    ),
    "TVER_RANKING_JP": lambda cfg: TVerRankingConnector(
        max_results=cfg.get("fetchLimit", 20), enabled=cfg.get("enabled", True)
    ),
    "TIKTOK_CREATIVE_CENTER": lambda cfg: TikTokCreativeCenterConnector(
        max_results=cfg.get("fetchLimit", 20),
        country_codes=cfg.get("countryCodes"),
        enabled=cfg.get("enabled", True),
    ),
    "TIKTOK_CREATIVE_CENTER_HASHTAGS": lambda cfg: TikTokCreativeCenterHashtagConnector(
        max_results=cfg.get("fetchLimit", 20),
        country_codes=cfg.get("countryCodes"),
        enabled=cfg.get("enabled", True),
    ),
    "TIKTOK_CREATIVE_CENTER_SONGS": lambda cfg: TikTokCreativeCenterSongsConnector(
        max_results=cfg.get("fetchLimit", 20),
        country_codes=cfg.get("countryCodes"),
        enabled=cfg.get("enabled", True),
    ),
    "TIKTOK_CREATIVE_CENTER_CREATORS": lambda cfg: TikTokCreativeCenterCreatorsConnector(
        max_results=cfg.get("fetchLimit", 20),
        country_codes=cfg.get("countryCodes"),
        enabled=cfg.get("enabled", True),
    ),
    "TIKTOK_CREATIVE_CENTER_VIDEOS": lambda cfg: TikTokCreativeCenterVideosConnector(
        max_results=cfg.get("fetchLimit", 20),
        country_codes=cfg.get("countryCodes"),
        enabled=cfg.get("enabled", True),
    ),
    "EDITORIAL_MODELPRESS": lambda cfg: EditorialModelpressConnector(
        enabled=cfg.get("enabled", True)
    ),
    "EDITORIAL_FASHIONSNAP": lambda cfg: EditorialFashionsnapConnector(
        enabled=cfg.get("enabled", True)
    ),
    "EDITORIAL_MAGAZINE": lambda cfg: EditorialMagazineConnector(enabled=cfg.get("enabled", True)),
    "YOUTUBE_TREND_JP": lambda cfg: YouTubeConnector(
        max_results=cfg.get("fetchLimit", 50), enabled=cfg.get("enabled", True)
    ),
}


def build_source_plan_from_catalog(source_cfgs: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Build the effective runtime source plan from catalog semantics + config overrides."""
    cfg_map = {str(cfg.get("sourceId")): dict(cfg) for cfg in source_cfgs if cfg.get("sourceId")}
    plan: list[dict[str, Any]] = []
    for entry in iter_active_catalog():
        if entry.role == SourceRole.EVIDENCE_ONLY:
            continue
        cfg = dict(cfg_map.get(entry.source_id, {}))
        plan.append(
            {
                "sourceId": entry.source_id,
                "displayName": entry.display_name,
                "enabled": bool(cfg.get("enabled", entry.status != SourceStatus.DISABLED)),
                "role": entry.role.value,
                "familyPrimary": entry.family_primary.value,
                "familySecondary": entry.family_secondary.value if entry.family_secondary else "",
                "status": entry.status.value,
                "accessMode": entry.access_mode.value,
                "availabilityTier": entry.availability_tier.value,
                "regionGroup": entry.region_group,
                "fallbackChain": list(entry.fallback_chain),
                "fetchLimit": cfg.get("fetchLimit"),
                "countryCodes": cfg.get("countryCodes"),
                "requiresCredentials": entry.requires_credentials,
                "supportsPhraseCandidates": entry.supports_phrase_candidates,
                "supportsEntityCandidates": entry.supports_entity_candidates,
            }
        )
    return plan


def validate_runtime_source_cfgs(source_cfgs: list[dict[str, Any]]) -> dict[str, list[str]]:
    """Detect config drift between Firestore runtime configs and the v2 source catalog."""
    known_ids = {entry.source_id for entry in iter_active_catalog(statuses=tuple(SourceStatus))}
    registry_ids = set(FACTORIES)
    unknown_cfgs = sorted(
        str(cfg.get("sourceId"))
        for cfg in source_cfgs
        if cfg.get("sourceId") and str(cfg.get("sourceId")) not in known_ids
    )
    stale_cfgs = sorted(
        str(cfg.get("sourceId"))
        for cfg in source_cfgs
        if cfg.get("sourceId")
        and str(cfg.get("sourceId")) in known_ids
        and str(cfg.get("sourceId")) not in registry_ids
    )
    missing_active_factories = sorted(
        entry_id for entry_id in known_ids if entry_id not in registry_ids
    )
    return {
        "unknownConfigSourceIds": unknown_cfgs,
        "staleConfigSourceIds": stale_cfgs,
        "missingFactorySourceIds": missing_active_factories,
    }


def build_connectors(
    source_cfgs: list[dict[str, Any]],
    source_plan: list[dict[str, Any]] | None = None,
) -> list[BaseConnector]:
    cfg_map = {str(cfg.get("sourceId")): dict(cfg) for cfg in source_cfgs if cfg.get("sourceId")}
    connectors: list[BaseConnector] = []
    planned_ids = {
        str(entry.get("sourceId"))
        for entry in (source_plan or [])
        if entry.get("sourceId") and entry.get("enabled", True)
    }

    for entry in iter_active_catalog():
        if source_plan is not None and entry.source_id not in planned_ids:
            continue
        cfg = dict(cfg_map.get(entry.source_id, {}))
        cfg.setdefault("enabled", entry.status != SourceStatus.DISABLED)

        if entry.role == SourceRole.EVIDENCE_ONLY:
            continue

        factory = FACTORIES.get(entry.source_id)
        if factory is None:
            logger.error("Active source %s has no registered connector factory", entry.source_id)
            continue

        try:
            connectors.append(factory(cfg))
        except Exception as exc:
            logger.warning("Failed to create connector %s: %s", entry.source_id, exc)

    return connectors


def get_connector_factory(source_id: str) -> Callable[[dict[str, Any]], BaseConnector] | None:
    entry = get_source_entry(source_id)
    if (
        entry is None
        or entry.status == SourceStatus.DISABLED
        or entry.role == SourceRole.EVIDENCE_ONLY
    ):
        return None
    return FACTORIES.get(source_id)
