"""Tests for Firestore client helpers."""

from __future__ import annotations

from packages.core import firestore_client


class FakeDocumentReference:
    def __init__(self, collection_path: str, document_id: str) -> None:
        self.collection_path = collection_path
        self.document_id = document_id


class FakeStreamDocument:
    def __init__(self, data: dict[str, object]) -> None:
        self._data = data

    def to_dict(self) -> dict[str, object]:
        return dict(self._data)


class FakeQuery:
    def __init__(self, docs: list[dict[str, object]]) -> None:
        self._docs = docs

    def where(self, field_path: str, operator: str, value: object) -> FakeQuery:
        if operator == "==":
            docs = [doc for doc in self._docs if doc.get(field_path) == value]
        elif operator == "in":
            allowed = set(value) if isinstance(value, list) else {value}
            docs = [doc for doc in self._docs if doc.get(field_path) in allowed]
        else:
            raise AssertionError(f"Unexpected operator: {operator}")
        return FakeQuery(docs)

    def order_by(self, field_path: str) -> FakeQuery:
        return FakeQuery(sorted(self._docs, key=lambda doc: doc.get(field_path)))

    def limit(self, count: int) -> FakeQuery:
        return FakeQuery(self._docs[:count])

    def stream(self) -> list[FakeStreamDocument]:
        return [FakeStreamDocument(doc) for doc in self._docs]


class FakeCollectionReference:
    def __init__(self, path: str, docs: list[dict[str, object]] | None = None) -> None:
        self.path = path
        self._docs = docs or []

    def document(self, document_id: str) -> FakeDocumentReference:
        return FakeDocumentReference(self.path, document_id)

    def where(self, field_path: str, operator: str, value: object) -> FakeQuery:
        return FakeQuery(self._docs).where(field_path, operator, value)

    def order_by(self, field_path: str) -> FakeQuery:
        return FakeQuery(self._docs).order_by(field_path)

    def limit(self, count: int) -> FakeQuery:
        return FakeQuery(self._docs).limit(count)

    def stream(self) -> list[FakeStreamDocument]:
        return FakeQuery(self._docs).stream()


class FakeBatch:
    def __init__(self, db: FakeDB) -> None:
        self._db = db
        self._ops: list[tuple[str, str]] = []

    def set(
        self,
        doc_ref: FakeDocumentReference,
        data: dict[str, object],
        merge: bool | None = None,
    ) -> None:
        self._ops.append((doc_ref.collection_path, doc_ref.document_id))

    def commit(self) -> None:
        self._db.commit_attempts += 1
        if self._db.fail_commit_attempts > 0:
            self._db.fail_commit_attempts -= 1
            raise RuntimeError("429 Quota exceeded.")
        self._db.commit_sizes.append(len(self._ops))


class FakeDB:
    def __init__(self) -> None:
        self.commit_sizes: list[int] = []
        self.commit_attempts = 0
        self.fail_commit_attempts = 0
        self.collections: dict[str, list[dict[str, object]]] = {}

    def collection(self, path: str) -> FakeCollectionReference:
        return FakeCollectionReference(path, docs=self.collections.get(path, []))

    def batch(self) -> FakeBatch:
        return FakeBatch(self)


class TestBatchWrite:
    def test_batch_write_chunks_at_firestore_limit(self, monkeypatch) -> None:
        fake_db = FakeDB()
        monkeypatch.setattr(firestore_client, "get_db", lambda: fake_db)

        operations = [
            ("daily_rankings/2026-03-06/items", f"cand_{i}", {"rank": i}) for i in range(501)
        ]

        firestore_client.batch_write(operations)

        assert fake_db.commit_sizes == [500, 1]

    def test_batch_write_with_custom_chunk_size(self, monkeypatch) -> None:
        fake_db = FakeDB()
        monkeypatch.setattr(firestore_client, "get_db", lambda: fake_db)

        operations = [
            ("daily_rankings/2026-03-07/items", f"cand_{i}", {"rank": i}) for i in range(5)
        ]

        firestore_client.batch_write_with_chunk_size(operations, chunk_size=2)

        assert fake_db.commit_sizes == [2, 2, 1]

    def test_batch_write_retries_on_quota_errors(self, monkeypatch) -> None:
        fake_db = FakeDB()
        fake_db.fail_commit_attempts = 1
        monkeypatch.setattr(firestore_client, "get_db", lambda: fake_db)
        monkeypatch.setattr(firestore_client.time, "sleep", lambda _: None)

        firestore_client.batch_write(
            [("daily_rankings/2026-03-07/items", "cand_1", {"rank": 1})]
        )

        assert fake_db.commit_attempts == 2
        assert fake_db.commit_sizes == [1]


class TestGetCollection:
    def test_get_collection_applies_filters(self, monkeypatch) -> None:
        fake_db = FakeDB()
        fake_db.collections["daily_source_features"] = [
            {"date": "2026-03-08", "sourceId": "A", "score": 2},
            {"date": "2026-03-09", "sourceId": "B", "score": 1},
        ]
        monkeypatch.setattr(firestore_client, "get_db", lambda: fake_db)

        docs = firestore_client.get_collection(
            "daily_source_features",
            filters=[("date", "==", "2026-03-08")],
        )

        assert docs == [{"date": "2026-03-08", "sourceId": "A", "score": 2}]

    def test_get_collection_supports_in_filters_and_order(self, monkeypatch) -> None:
        fake_db = FakeDB()
        fake_db.collections["shadow_evaluations"] = [
            {"date": "2026-03-09", "rank": 2},
            {"date": "2026-03-07", "rank": 1},
            {"date": "2026-03-08", "rank": 3},
        ]
        monkeypatch.setattr(firestore_client, "get_db", lambda: fake_db)

        docs = firestore_client.get_collection(
            "shadow_evaluations",
            filters=[("date", "in", ["2026-03-08", "2026-03-07"])],
            order_by="rank",
            limit=1,
        )

        assert docs == [{"date": "2026-03-07", "rank": 1}]
