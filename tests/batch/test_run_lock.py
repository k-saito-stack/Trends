"""Tests for daily batch per-date lock behavior."""

from __future__ import annotations

from batch import run as run_module


class TestAcquireLock:
    def test_completed_lock_skips_without_rerun_override(self, monkeypatch) -> None:
        set_calls: list[tuple[str, str, dict[str, object]]] = []

        monkeypatch.setattr(
            "packages.core.firestore_client.create_document",
            lambda *args, **kwargs: False,
        )
        monkeypatch.setattr(
            "packages.core.firestore_client.get_document",
            lambda *args, **kwargs: {"status": "COMPLETED", "runId": "old_run"},
        )
        monkeypatch.setattr(
            "packages.core.firestore_client.set_document",
            lambda collection, document_id, data: set_calls.append(
                (collection, document_id, data)
            ),
        )

        acquired = run_module.acquire_lock(
            "2026-03-06",
            "new_run",
            allow_completed_rerun=False,
        )

        assert acquired is False
        assert set_calls == []

    def test_completed_lock_is_reacquired_for_manual_rerun(self, monkeypatch) -> None:
        set_calls: list[tuple[str, str, dict[str, object]]] = []

        monkeypatch.setattr(
            "packages.core.firestore_client.create_document",
            lambda *args, **kwargs: False,
        )
        monkeypatch.setattr(
            "packages.core.firestore_client.get_document",
            lambda *args, **kwargs: {"status": "COMPLETED", "runId": "old_run"},
        )
        monkeypatch.setattr(
            "packages.core.firestore_client.set_document",
            lambda collection, document_id, data: set_calls.append(
                (collection, document_id, data)
            ),
        )

        acquired = run_module.acquire_lock(
            "2026-03-06",
            "new_run",
            allow_completed_rerun=True,
        )

        assert acquired is True
        assert len(set_calls) == 1
        collection, document_id, data = set_calls[0]
        assert collection == "runs"
        assert document_id == "_lock_2026-03-06"
        assert data["status"] == "RUNNING"
        assert data["runId"] == "new_run"
        assert data["targetDate"] == "2026-03-06"
        assert isinstance(data["startedAt"], str)
