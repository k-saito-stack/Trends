"""Configuration loader from Firestore.

All runtime config is stored in Firestore /config/* documents,
NOT in code (public repo risk).
"""

from __future__ import annotations

from typing import Any

from packages.core import firestore_client
from packages.core.models import (
    AlgorithmConfig,
    AppConfig,
    MusicConfig,
    NerConfig,
    SourceWeightingConfig,
)


def load_app_config() -> AppConfig:
    """Load /config/app from Firestore."""
    data = firestore_client.get_document("config", "app")
    if data is None:
        return AppConfig()
    return AppConfig.from_dict(data)


def load_algorithm_config() -> AlgorithmConfig:
    """Load /config/algorithm from Firestore."""
    data = firestore_client.get_document("config", "algorithm")
    if data is None:
        return AlgorithmConfig()
    return AlgorithmConfig.from_dict(data)


def load_music_config() -> MusicConfig:
    """Load /config/music from Firestore."""
    data = firestore_client.get_document("config", "music")
    if data is None:
        return MusicConfig()
    return MusicConfig.from_dict(data)


def load_ner_config() -> NerConfig:
    """Load /config/ner from Firestore."""
    data = firestore_client.get_document("config", "ner")
    if data is None:
        return NerConfig()
    return NerConfig.from_dict(data)


def load_source_weighting_config() -> SourceWeightingConfig:
    """Load /config/source_weighting from Firestore."""
    data = firestore_client.get_document("config", "source_weighting")
    if data is None:
        return SourceWeightingConfig()
    return SourceWeightingConfig.from_dict(data)


def load_source_config(source_id: str) -> dict[str, Any] | None:
    """Load /config_sources/{sourceId} from Firestore."""
    return firestore_client.get_document("config_sources", source_id)


def load_all_source_configs() -> list[dict[str, Any]]:
    """Load all source configs."""
    return firestore_client.get_collection("config_sources")
