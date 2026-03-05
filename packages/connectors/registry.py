"""Connector registry: sourceId -> connector factory.

Maps Firestore source configs to connector instances,
applying enabled/fetchLimit/killSwitch settings from config.
"""

from __future__ import annotations

import logging
from typing import Any, Callable

from packages.connectors.apple_music import AppleMusicConnector
from packages.connectors.base import BaseConnector
from packages.connectors.google_trends import GoogleTrendsConnector
from packages.connectors.rakuten_magazine import RakutenMagazineConnector
from packages.connectors.rss_feeds import RSSFeedConnector
from packages.connectors.x_search import XTrendingConnector
from packages.connectors.youtube import YouTubeConnector

logger = logging.getLogger(__name__)

# sourceId -> factory(cfg) -> connector instance
FACTORIES: dict[str, Callable[[dict[str, Any]], BaseConnector]] = {
    "YOUTUBE_TREND_JP": lambda cfg: YouTubeConnector(
        max_results=cfg.get("fetchLimit", 50),
        max_consecutive_failures=cfg.get("killSwitch", {}).get("maxConsecutiveFailures", 3),
        enabled=cfg.get("enabled", True),
    ),
    "APPLE_MUSIC_JP": lambda cfg: AppleMusicConnector(
        region="JP",
        max_results=cfg.get("fetchLimit", 50),
        max_consecutive_failures=cfg.get("killSwitch", {}).get("maxConsecutiveFailures", 3),
        enabled=cfg.get("enabled", True),
    ),
    "APPLE_MUSIC_GLOBAL": lambda cfg: AppleMusicConnector(
        region="GLOBAL",
        max_results=cfg.get("fetchLimit", 50),
        max_consecutive_failures=cfg.get("killSwitch", {}).get("maxConsecutiveFailures", 3),
        enabled=cfg.get("enabled", True),
    ),
    "TRENDS": lambda cfg: GoogleTrendsConnector(
        enabled=cfg.get("enabled", True),
    ),
    "NEWS_RSS": lambda cfg: RSSFeedConnector(
        max_items_per_feed=cfg.get("fetchLimit", 30),
        enabled=cfg.get("enabled", True),
    ),
    "RAKUTEN_MAG": lambda cfg: RakutenMagazineConnector(
        enabled=cfg.get("enabled", True),
    ),
    "X_TRENDING": lambda cfg: XTrendingConnector(
        enabled=cfg.get("enabled", True),
    ),
}


def build_connectors(source_cfgs: list[dict[str, Any]]) -> list[BaseConnector]:
    """Build connector instances from Firestore source configs.

    Only builds connectors that have a registered factory.
    Sources without a factory (e.g. WIKI_PAGEVIEWS, X_SEARCH, IG_BOOST)
    are handled separately in the pipeline.
    """
    connectors: list[BaseConnector] = []
    for cfg in source_cfgs:
        sid = cfg.get("sourceId")
        if not sid:
            continue
        factory = FACTORIES.get(sid)
        if factory is None:
            continue
        try:
            connectors.append(factory(cfg))
        except Exception as e:
            logger.warning("Failed to create connector %s: %s", sid, e)
    return connectors
