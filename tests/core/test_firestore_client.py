"""Tests for Firestore client helpers."""

from __future__ import annotations

from packages.core import firestore_client


class FakeDocumentReference:
    def __init__(self, collection_path: str, document_id: str) -> None:
        self.collection_path = collection_path
        self.document_id = document_id


class FakeCollectionReference:
    def __init__(self, path: str) -> None:
        self.path = path

    def document(self, document_id: str) -> FakeDocumentReference:
        return FakeDocumentReference(self.path, document_id)


class FakeBatch:
    def __init__(self, db: FakeDB) -> None:
        self._db = db
        self._ops: list[tuple[str, str]] = []

    def set(self, doc_ref: FakeDocumentReference, data: dict[str, object]) -> None:
        self._ops.append((doc_ref.collection_path, doc_ref.document_id))

    def commit(self) -> None:
        self._db.commit_sizes.append(len(self._ops))


class FakeDB:
    def __init__(self) -> None:
        self.commit_sizes: list[int] = []

    def collection(self, path: str) -> FakeCollectionReference:
        return FakeCollectionReference(path)

    def batch(self) -> FakeBatch:
        return FakeBatch(self)


class TestBatchWrite:
    def test_batch_write_chunks_at_firestore_limit(self, monkeypatch) -> None:
        fake_db = FakeDB()
        monkeypatch.setattr(firestore_client, "get_db", lambda: fake_db)

        operations = [
            ("daily_rankings/2026-03-06/items", f"cand_{i}", {"rank": i})
            for i in range(501)
        ]

        firestore_client.batch_write(operations)

        assert fake_db.commit_sizes == [500, 1]
