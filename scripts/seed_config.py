"""Seed initial config documents into Firestore.

Run this once after Firebase project setup to populate /config/* with
the default values from the spec (Appendix C).

Usage:
    python scripts/seed_config.py

Requires FIREBASE_SA_JSON or GOOGLE_APPLICATION_CREDENTIALS env var.
"""

from __future__ import annotations

import logging
import os
import sys

# Add project root to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from packages.core import firestore_client
from packages.core.models import AlgorithmConfig, AppConfig, MusicConfig, NerConfig

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


# Source configs (spec Appendix E)
SOURCE_CONFIGS = [
    {
        "sourceId": "YOUTUBE_TREND_JP",
        "enabled": True,
        "stability": "A",
        "fetchLimit": 50,
        "killSwitch": {"maxConsecutiveFailures": 3},
        "description": "YouTube mostPopular JP (official API)",
    },
    {
        "sourceId": "APPLE_MUSIC_JP",
        "enabled": True,
        "stability": "A",
        "fetchLimit": 50,
        "killSwitch": {"maxConsecutiveFailures": 3},
        "description": "Apple Music RSS Japan",
    },
    {
        "sourceId": "APPLE_MUSIC_GLOBAL",
        "enabled": True,
        "stability": "A",
        "fetchLimit": 50,
        "killSwitch": {"maxConsecutiveFailures": 3},
        "description": "Apple Music RSS Global",
    },
    {
        "sourceId": "TRENDS",
        "enabled": True,
        "stability": "B",
        "fetchLimit": 20,
        "killSwitch": {"maxConsecutiveFailures": 5},
        "description": "Google Trends (alpha API preferred, fallback to public)",
    },
    {
        "sourceId": "NEWS_RSS",
        "enabled": True,
        "stability": "B",
        "fetchLimit": 100,
        "killSwitch": {"maxConsecutiveFailures": 3},
        "description": "News RSS feeds",
    },
    {
        "sourceId": "RAKUTEN_MAG",
        "enabled": True,
        "stability": "B",
        "fetchLimit": 30,
        "killSwitch": {"maxConsecutiveFailures": 3},
        "description": "Rakuten Books magazine search API",
    },
    {
        "sourceId": "WIKI_PAGEVIEWS",
        "enabled": True,
        "stability": "A",
        "fetchLimit": 0,
        "killSwitch": {"maxConsecutiveFailures": 3},
        "description": "Wikipedia Pageviews (power score, display only)",
    },
    {
        "sourceId": "X_SEARCH",
        "enabled": True,
        "stability": "B",
        "fetchLimit": 30,
        "killSwitch": {"budgetDegradeTarget": True},
        "description": "xAI X Search (evidence enrichment for top candidates)",
    },
    {
        "sourceId": "NETFLIX_TV_JP",
        "enabled": True,
        "stability": "B",
        "fetchLimit": 10,
        "killSwitch": {"maxConsecutiveFailures": 3},
        "description": "Netflix Top 10 Japan TV Shows (weekly, HTML scraping)",
    },
    {
        "sourceId": "NETFLIX_FILMS_JP",
        "enabled": True,
        "stability": "B",
        "fetchLimit": 10,
        "killSwitch": {"maxConsecutiveFailures": 3},
        "description": "Netflix Top 10 Japan Films (weekly, HTML scraping)",
    },
    {
        "sourceId": "IG_BOOST",
        "enabled": False,
        "stability": "C",
        "fetchLimit": 0,
        "killSwitch": {"maxConsecutiveFailures": 3},
        "description": "Instagram Business Discovery (off until PCA approved)",
    },
]


def seed() -> None:
    """Write initial config to Firestore."""

    # /config/app
    app_config = AppConfig()
    logger.info("Writing /config/app ...")
    firestore_client.set_document("config", "app", app_config.to_dict())

    # /config/algorithm
    algo_config = AlgorithmConfig()
    logger.info("Writing /config/algorithm ...")
    firestore_client.set_document("config", "algorithm", algo_config.to_dict())

    # /config/music
    music_config = MusicConfig()
    logger.info("Writing /config/music ...")
    firestore_client.set_document("config", "music", music_config.to_dict())

    # /config/ner
    ner_config = NerConfig()
    logger.info("Writing /config/ner ...")
    firestore_client.set_document("config", "ner", ner_config.to_dict())

    # /config_sources/{sourceId}
    for source in SOURCE_CONFIGS:
        source_id = source["sourceId"]
        logger.info("Writing /config_sources/%s ...", source_id)
        firestore_client.set_document("config_sources", source_id, source)

    logger.info("Seed completed successfully!")


if __name__ == "__main__":
    seed()
