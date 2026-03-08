from __future__ import annotations

from packages.core import candidate_store


def test_load_daily_source_features_queries_by_date(monkeypatch) -> None:
    calls: list[tuple[str, list[tuple[str, str, object]] | None]] = []

    def fake_get_collection(collection: str, order_by=None, limit=None, filters=None):  # type: ignore[no-untyped-def]
        calls.append((collection, filters))
        return [
            {
                "date": "2026-03-08",
                "sourceId": "A",
                "candidateId": "cand-a",
                "candidateType": "KEYWORD",
                "candidateKind": "TOPIC",
                "sourceRole": "DISCOVERY",
                "familyPrimary": "SEARCH",
                "signalValue": 1.0,
                "sourceWeight": 1.0,
                "evidence": [],
                "domainClass": "OTHER",
                "extractionConfidence": "HIGH",
                "metadata": {},
            }
        ]

    monkeypatch.setattr(candidate_store.firestore_client, "get_collection", fake_get_collection)

    features = candidate_store.load_daily_source_features_by_dates(["2026-03-08"])

    assert len(features) == 1
    assert calls == [("daily_source_features", [("date", "==", "2026-03-08")])]


def test_load_all_candidates_prefers_active_filter(monkeypatch) -> None:
    calls: list[tuple[str, list[tuple[str, str, object]] | None]] = []

    def fake_get_collection(collection: str, order_by=None, limit=None, filters=None):  # type: ignore[no-untyped-def]
        calls.append((collection, filters))
        return [
            {
                "candidateId": "cand-1",
                "type": "KEYWORD",
                "kind": "TOPIC",
                "canonicalName": "test",
                "displayName": "test",
                "status": "ACTIVE",
            }
        ]

    monkeypatch.setattr(candidate_store.firestore_client, "get_collection", fake_get_collection)

    candidates = candidate_store.load_all_candidates()

    assert list(candidates) == ["cand-1"]
    assert calls[0] == ("candidates", [("status", "==", "ACTIVE")])
