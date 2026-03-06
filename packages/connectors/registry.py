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
from packages.connectors.spotify_embed import SpotifyEmbedConnector
from packages.connectors.tiktok_creative_center import TikTokCreativeCenterConnector
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
    "RAKUTEN_ICHIBA_RANKING": lambda cfg: RakutenIchibaRankingConnector(enabled=cfg.get("enabled", True)),
    "BILLBOARD_JAPAN": lambda cfg: BillboardJapanConnector(enabled=cfg.get("enabled", True)),
    "APPLE_MUSIC_JP": lambda cfg: AppleMusicConnector(region="JP", max_results=cfg.get("fetchLimit", 50), enabled=cfg.get("enabled", True)),
    "APPLE_MUSIC_GLOBAL": lambda cfg: AppleMusicConnector(region="GLOBAL", max_results=cfg.get("fetchLimit", 50), enabled=cfg.get("enabled", True)),
    "SPOTIFY_EMBED": lambda cfg: SpotifyEmbedConnector(enabled=cfg.get("enabled", True)),
    "NETFLIX_TV_JP": lambda cfg: NetflixTop10Connector(category="tv", max_results=cfg.get("fetchLimit", 10), enabled=cfg.get("enabled", True)),
    "NETFLIX_FILMS_JP": lambda cfg: NetflixTop10Connector(category="films", max_results=cfg.get("fetchLimit", 10), enabled=cfg.get("enabled", True)),
    "TVER_RANKING_JP": lambda cfg: TVerRankingConnector(max_results=cfg.get("fetchLimit", 20), enabled=cfg.get("enabled", True)),
    "TIKTOK_CREATIVE_CENTER": lambda cfg: TikTokCreativeCenterConnector(enabled=cfg.get("enabled", True)),
    "EDITORIAL_MODELPRESS": lambda cfg: EditorialModelpressConnector(enabled=cfg.get("enabled", True)),
    "EDITORIAL_FASHIONSNAP": lambda cfg: EditorialFashionsnapConnector(enabled=cfg.get("enabled", True)),
    "EDITORIAL_MAGAZINE": lambda cfg: EditorialMagazineConnector(enabled=cfg.get("enabled", True)),
    "YOUTUBE_TREND_JP": lambda cfg: YouTubeConnector(max_results=cfg.get("fetchLimit", 50), enabled=cfg.get("enabled", True)),
}


def build_connectors(source_cfgs: list[dict[str, Any]]) -> list[BaseConnector]:
    cfg_map = {str(cfg.get("sourceId")): dict(cfg) for cfg in source_cfgs if cfg.get("sourceId")}
    connectors: list[BaseConnector] = []

    for entry in iter_active_catalog():
        cfg = dict(cfg_map.get(entry.source_id, {}))
        cfg.setdefault("enabled", entry.status != SourceStatus.DISABLED)

        if entry.role == SourceRole.EVIDENCE_ONLY:
            continue

        factory = FACTORIES.get(entry.source_id)
        if factory is None:
            continue

        try:
            connectors.append(factory(cfg))
        except Exception as exc:
            logger.warning("Failed to create connector %s: %s", entry.source_id, exc)

    return connectors


def get_connector_factory(source_id: str) -> Callable[[dict[str, Any]], BaseConnector] | None:
    entry = get_source_entry(source_id)
    if entry is None or entry.status == SourceStatus.DISABLED or entry.role == SourceRole.EVIDENCE_ONLY:
        return None
    return FACTORIES.get(source_id)
