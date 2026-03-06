from __future__ import annotations

from batch.degrade import DegradeState
from batch.run import _apply_runtime_feature_flags, _create_connectors


class TestApplyRuntimeFeatureFlags:
    def test_x_search_disabled_when_config_missing(self) -> None:
        degrade = DegradeState(x_search_enabled=True)

        result = _apply_runtime_feature_flags(degrade, [])

        assert result.x_search_enabled is False

    def test_x_search_respects_config_enabled(self) -> None:
        degrade = DegradeState(x_search_enabled=True)

        result = _apply_runtime_feature_flags(degrade, [
            {"sourceId": "X_SEARCH", "enabled": True},
        ])

        assert result.x_search_enabled is True


class TestCreateConnectorsFallback:
    def test_defaults_include_netflix_and_tver(self) -> None:
        connectors = _create_connectors([])
        source_ids = {connector.source_id for connector in connectors}

        assert "NETFLIX_TV_JP" in source_ids
        assert "NETFLIX_FILMS_JP" in source_ids
        assert "TVER_RANKING_JP" in source_ids
