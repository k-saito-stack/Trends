"""Seed initial config using Firestore REST API.

Alternative to seed_config.py for environments where gRPC is blocked
(e.g., corporate proxies that don't support HTTP/2).

Usage:
    python scripts/seed_config_rest.py
"""

from __future__ import annotations

import json
import logging
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from google.auth.transport.requests import Request
from google.oauth2 import service_account

from packages.core.models import AlgorithmConfig, AppConfig, MusicConfig, NerConfig

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Firestore REST API base URL
FIRESTORE_BASE = "https://firestore.googleapis.com/v1"


def get_credentials() -> service_account.Credentials:
    """Get authenticated credentials."""
    sa_path = os.environ.get("GOOGLE_APPLICATION_CREDENTIALS", "")
    if not sa_path or not os.path.exists(sa_path):
        raise RuntimeError("GOOGLE_APPLICATION_CREDENTIALS not set or file not found")

    creds = service_account.Credentials.from_service_account_file(
        sa_path, scopes=["https://www.googleapis.com/auth/datastore"]
    )
    creds.refresh(Request())
    return creds


def get_project_id() -> str:
    """Read project ID from service account JSON."""
    sa_path = os.environ["GOOGLE_APPLICATION_CREDENTIALS"]
    with open(sa_path) as f:
        return json.load(f)["project_id"]


def python_to_firestore_value(value: object) -> dict:
    """Convert a Python value to Firestore REST API value format."""
    if value is None:
        return {"nullValue": None}
    if isinstance(value, bool):
        return {"booleanValue": value}
    if isinstance(value, int):
        return {"integerValue": str(value)}
    if isinstance(value, float):
        return {"doubleValue": value}
    if isinstance(value, str):
        return {"stringValue": value}
    if isinstance(value, list):
        return {"arrayValue": {"values": [python_to_firestore_value(v) for v in value]}}
    if isinstance(value, dict):
        return {"mapValue": {"fields": {
            k: python_to_firestore_value(v) for k, v in value.items()
        }}}
    return {"stringValue": str(value)}


def dict_to_firestore_fields(data: dict) -> dict:
    """Convert a Python dict to Firestore fields format."""
    return {k: python_to_firestore_value(v) for k, v in data.items()}


def write_document(
    session: object,
    project_id: str,
    collection: str,
    doc_id: str,
    data: dict,
    headers: dict,
) -> None:
    """Write a document to Firestore via REST API."""
    import requests

    url = (
        f"{FIRESTORE_BASE}/projects/{project_id}/databases/(default)"
        f"/documents/{collection}/{doc_id}"
    )
    body = {"fields": dict_to_firestore_fields(data)}

    resp = requests.patch(url, json=body, headers=headers, timeout=30)
    resp.raise_for_status()
    logger.info("  Written: %s/%s", collection, doc_id)


# Source configs (same as seed_config.py)
SOURCE_CONFIGS = [
    {"sourceId": "YOUTUBE_TREND_JP", "enabled": True, "stability": "A",
     "fetchLimit": 50, "killSwitch": {"maxConsecutiveFailures": 3},
     "description": "YouTube mostPopular JP (official API)"},
    {"sourceId": "APPLE_MUSIC_JP", "enabled": True, "stability": "A",
     "fetchLimit": 50, "killSwitch": {"maxConsecutiveFailures": 3},
     "description": "Apple Music RSS Japan"},
    {"sourceId": "APPLE_MUSIC_GLOBAL", "enabled": True, "stability": "A",
     "fetchLimit": 50, "killSwitch": {"maxConsecutiveFailures": 3},
     "description": "Apple Music RSS Global"},
    {"sourceId": "TRENDS", "enabled": True, "stability": "B",
     "fetchLimit": 20, "killSwitch": {"maxConsecutiveFailures": 5},
     "description": "Google Trends (alpha API preferred, fallback to public)"},
    {"sourceId": "NEWS_RSS", "enabled": True, "stability": "B",
     "fetchLimit": 100, "killSwitch": {"maxConsecutiveFailures": 3},
     "description": "News RSS feeds"},
    {"sourceId": "RAKUTEN_MAG", "enabled": True, "stability": "B",
     "fetchLimit": 30, "killSwitch": {"maxConsecutiveFailures": 3},
     "description": "Rakuten Books magazine search API"},
    {"sourceId": "WIKI_PAGEVIEWS", "enabled": True, "stability": "A",
     "fetchLimit": 0, "killSwitch": {"maxConsecutiveFailures": 3},
     "description": "Wikipedia Pageviews (power score, display only)"},
    {"sourceId": "X_SEARCH", "enabled": True, "stability": "B",
     "fetchLimit": 30, "killSwitch": {"budgetDegradeTarget": True},
     "description": "xAI X Search (evidence enrichment for top candidates)"},
    {"sourceId": "NETFLIX_TV_JP", "enabled": True, "stability": "B",
     "fetchLimit": 10, "killSwitch": {"maxConsecutiveFailures": 3},
     "description": "Netflix Top 10 Japan TV Shows (weekly, HTML scraping)"},
    {"sourceId": "NETFLIX_FILMS_JP", "enabled": True, "stability": "B",
     "fetchLimit": 10, "killSwitch": {"maxConsecutiveFailures": 3},
     "description": "Netflix Top 10 Japan Films (weekly, HTML scraping)"},
    {"sourceId": "IG_BOOST", "enabled": False, "stability": "C",
     "fetchLimit": 0, "killSwitch": {"maxConsecutiveFailures": 3},
     "description": "Instagram Business Discovery (off until PCA approved)"},
]


def seed() -> None:
    """Write initial config to Firestore via REST API."""
    import requests

    creds = get_credentials()
    project_id = get_project_id()
    headers = {
        "Authorization": f"Bearer {creds.token}",
        "Content-Type": "application/json",
    }

    logger.info("Project: %s", project_id)

    # /config/app
    logger.info("Writing /config/app ...")
    app_config = AppConfig()
    write_document(requests, project_id, "config", "app", app_config.to_dict(), headers)

    # /config/algorithm
    logger.info("Writing /config/algorithm ...")
    algo_config = AlgorithmConfig()
    write_document(requests, project_id, "config", "algorithm", algo_config.to_dict(), headers)

    # /config/music
    logger.info("Writing /config/music ...")
    music_config = MusicConfig()
    write_document(requests, project_id, "config", "music", music_config.to_dict(), headers)

    # /config/ner
    logger.info("Writing /config/ner ...")
    ner_config = NerConfig()
    write_document(requests, project_id, "config", "ner", ner_config.to_dict(), headers)

    # /config_sources/{sourceId}
    for source in SOURCE_CONFIGS:
        source_id = source["sourceId"]
        logger.info("Writing /config_sources/%s ...", source_id)
        write_document(requests, project_id, "config_sources", source_id, source, headers)

    logger.info("Seed completed successfully!")


if __name__ == "__main__":
    seed()
