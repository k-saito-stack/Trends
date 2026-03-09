"""Tests for rerun publish mode selection."""

from __future__ import annotations

from batch import run as run_module
from packages.core.models import DailyRankingMeta


class TestLoadExistingPublishedMeta:
    def test_prefers_day_document_when_publish_fields_exist(self, monkeypatch) -> None:
        monkeypatch.setattr(
            "packages.core.firestore_client.get_document",
            lambda *args, **kwargs: {
                "date": "2026-03-07",
                "generatedAt": "2026-03-07T02:09:35+09:00",
                "runId": "run_day",
                "topK": 50,
                "status": "PUBLISHED",
                "publishedAt": "2026-03-07T02:09:44+09:00",
                "latestPublishedRunId": "run_day",
            },
        )
        monkeypatch.setattr(
            "packages.core.firestore_client.get_collection",
            lambda *args, **kwargs: [],
        )

        meta = run_module._load_existing_published_meta("2026-03-07")

        assert isinstance(meta, DailyRankingMeta)
        assert meta is not None
        assert meta.run_id == "run_day"

    def test_falls_back_to_latest_published_run_when_day_doc_was_overwritten(
        self, monkeypatch
    ) -> None:
        monkeypatch.setattr(
            "packages.core.firestore_client.get_document",
            lambda *args, **kwargs: {
                "date": "2026-03-07",
                "generatedAt": "2026-03-07T07:36:56+09:00",
                "runId": "failed_run",
                "topK": 50,
                "status": "BUILDING",
                "publishedAt": "",
                "latestPublishedRunId": "",
            },
        )
        monkeypatch.setattr(
            "packages.core.firestore_client.get_collection",
            lambda *args, **kwargs: [
                {
                    "date": "2026-03-07",
                    "generatedAt": "2026-03-07T02:02:35+09:00",
                    "runId": "run_old",
                    "topK": 50,
                    "status": "PUBLISHED",
                    "publishedAt": "2026-03-07T02:02:45+09:00",
                    "latestPublishedRunId": "run_old",
                },
                {
                    "date": "2026-03-07",
                    "generatedAt": "2026-03-07T02:09:35+09:00",
                    "runId": "run_new",
                    "topK": 50,
                    "status": "PUBLISHED",
                    "publishedAt": "2026-03-07T02:09:44+09:00",
                    "latestPublishedRunId": "run_new",
                },
            ],
        )

        meta = run_module._load_existing_published_meta("2026-03-07")

        assert isinstance(meta, DailyRankingMeta)
        assert meta is not None
        assert meta.run_id == "run_new"


class TestLightPublishMode:
    def test_uses_light_publish_when_existing_snapshot_exists(self, monkeypatch) -> None:
        monkeypatch.delenv("BATCH_FORCE_FULL_PERSIST", raising=False)
        monkeypatch.delenv("BATCH_LIGHT_PUBLISH_ONLY", raising=False)

        existing = DailyRankingMeta(
            date="2026-03-07",
            generated_at="2026-03-07T02:09:35+09:00",
            run_id="run_new",
            top_k=50,
            status="PUBLISHED",
            published_at="2026-03-07T02:09:44+09:00",
            latest_published_run_id="run_new",
        )

        assert run_module._should_use_light_publish(existing) is True

    def test_force_full_persist_disables_light_publish(self, monkeypatch) -> None:
        monkeypatch.setenv("BATCH_FORCE_FULL_PERSIST", "1")
        monkeypatch.delenv("BATCH_LIGHT_PUBLISH_ONLY", raising=False)

        existing = DailyRankingMeta(
            date="2026-03-07",
            generated_at="2026-03-07T02:09:35+09:00",
            run_id="run_new",
            top_k=50,
            status="PUBLISHED",
            published_at="2026-03-07T02:09:44+09:00",
            latest_published_run_id="run_new",
        )

        assert run_module._should_use_light_publish(existing) is False


class TestPublishPathPlanning:
    def test_light_publish_writes_only_versioned_run_items(self) -> None:
        paths = run_module._build_item_collection_paths(
            "2026-03-07",
            "run_new",
            light_publish=True,
            shadow_only=False,
        )

        assert paths == ("daily_rankings/2026-03-07/runs/run_new/items",)

    def test_full_publish_keeps_legacy_and_shadow_paths(self) -> None:
        paths = run_module._build_item_collection_paths(
            "2026-03-07",
            "run_new",
            light_publish=False,
            shadow_only=False,
        )

        assert paths == (
            "daily_rankings/2026-03-07/items",
            "daily_rankings_v2_shadow/2026-03-07/items",
            "daily_rankings/2026-03-07/runs/run_new/items",
        )

    def test_shadow_only_publish_writes_shadow_and_versioned_items(self) -> None:
        paths = run_module._build_item_collection_paths(
            "2026-03-07",
            "run_new",
            light_publish=False,
            shadow_only=True,
        )

        assert paths == (
            "daily_rankings/2026-03-07/items",
            "daily_rankings_v2_shadow/2026-03-07/items",
            "daily_rankings/2026-03-07/runs/run_new/items",
        )

    def test_shadow_only_publish_resets_shadow_path_only(self) -> None:
        paths = run_module._build_reset_collection_paths(
            "2026-03-07",
            light_publish=False,
            shadow_only=True,
        )

        assert paths == (
            "daily_rankings/2026-03-07/items",
            "daily_rankings_v2_shadow/2026-03-07/items",
        )

    def test_collection_publish_status_keeps_shadow_marker_only_for_shadow_collection(self) -> None:
        assert (
            run_module._collection_publish_status("daily_rankings", shadow_only=True)
            == "PUBLISHED"
        )
        assert (
            run_module._collection_publish_status("daily_rankings_v2_shadow", shadow_only=True)
            == "SHADOW_ONLY"
        )

    def test_public_collections_use_public_meta_only(self) -> None:
        meta = DailyRankingMeta(
            date="2026-03-07",
            generated_at="2026-03-07T02:09:35+09:00",
            run_id="run_new",
            top_k=50,
            degrade_state={"summaryMode": "LLM"},
            status="PUBLISHED",
            published_at="2026-03-07T02:09:44+09:00",
            latest_published_run_id="run_new",
            publish_health={"publicEligible": True},
            source_availability_snapshot={"healthyCoreAvailabilityRatio": 0.8},
        )

        payload = run_module._serialize_collection_meta("daily_rankings", meta)

        assert payload["latestPublishedRunId"] == "run_new"
        assert "publishHealth" not in payload
        assert "sourceAvailabilitySnapshot" not in payload
        assert "degradeState" not in payload

    def test_shadow_collection_keeps_full_meta(self) -> None:
        meta = DailyRankingMeta(
            date="2026-03-07",
            generated_at="2026-03-07T02:09:35+09:00",
            run_id="run_new",
            top_k=50,
            degrade_state={"summaryMode": "LLM"},
            status="SHADOW_ONLY",
            published_at="2026-03-07T02:09:44+09:00",
            latest_published_run_id="run_new",
            publish_health={"publicEligible": False},
            source_availability_snapshot={"healthyCoreAvailabilityRatio": 0.5},
        )

        payload = run_module._serialize_collection_meta("daily_rankings_v2_shadow", meta)

        assert payload["publishHealth"] == {"publicEligible": False}
        assert payload["sourceAvailabilitySnapshot"] == {"healthyCoreAvailabilityRatio": 0.5}
        assert payload["degradeState"] == {"summaryMode": "LLM"}
