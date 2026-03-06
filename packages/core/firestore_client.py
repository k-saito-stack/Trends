"""Firestore client for Trends platform.

Handles Firebase Admin SDK initialization and basic CRUD operations.
Supports two authentication modes:
  1. Service account JSON (via env FIREBASE_SA_JSON or GOOGLE_APPLICATION_CREDENTIALS)
  2. Application Default Credentials (for local dev with gcloud auth)
"""

from __future__ import annotations

import json
import os
from typing import Any, cast

import firebase_admin
from firebase_admin import credentials, firestore
from google.cloud.firestore_v1 import Client as FirestoreClient

_app: firebase_admin.App | None = None
_db: FirestoreClient | None = None
MAX_BATCH_WRITE_OPERATIONS = 500


def _initialize() -> None:
    """Initialize Firebase Admin SDK (called once)."""
    global _app, _db

    if _app is not None:
        return

    # Priority 1: FIREBASE_SA_JSON env var (JSON string, for GitHub Actions)
    sa_json = os.environ.get("FIREBASE_SA_JSON")
    if sa_json:
        cred_dict = json.loads(sa_json)
        cred = credentials.Certificate(cred_dict)
        _app = firebase_admin.initialize_app(cred)
        _db = firestore.client(_app)
        return

    # Priority 2: GOOGLE_APPLICATION_CREDENTIALS file path
    sa_path = os.environ.get("GOOGLE_APPLICATION_CREDENTIALS")
    if sa_path and os.path.exists(sa_path):
        cred = credentials.Certificate(sa_path)
        _app = firebase_admin.initialize_app(cred)
        _db = firestore.client(_app)
        return

    # Priority 3: Application Default Credentials
    cred = credentials.ApplicationDefault()
    project_id = os.environ.get("FIREBASE_PROJECT_ID")
    options = {"projectId": project_id} if project_id else {}
    _app = firebase_admin.initialize_app(cred, options)
    _db = firestore.client(_app)


def get_db() -> FirestoreClient:
    """Get initialized Firestore client."""
    _initialize()
    assert _db is not None, "Firestore client not initialized"
    return _db


def get_document(collection: str, document_id: str) -> dict[str, Any] | None:
    """Read a single document from Firestore.

    Returns None if the document doesn't exist.
    """
    db = get_db()
    doc_ref = db.collection(collection).document(document_id)
    doc: Any = doc_ref.get()
    if doc.exists:
        return cast(dict[str, Any], doc.to_dict())
    return None


def create_document(collection: str, document_id: str, data: dict[str, Any]) -> bool:
    """Create a document only if it does not exist.

    Returns True if created, False if already exists.
    Uses Firestore's create() which is atomic.
    """
    db = get_db()
    doc_ref = db.collection(collection).document(document_id)
    try:
        doc_ref.create(data)
        return True
    except Exception as e:
        # google.cloud.exceptions.Conflict (409) when doc already exists
        if "already exists" in str(e).lower() or "conflict" in str(e).lower():
            return False
        raise


def set_document(collection: str, document_id: str, data: dict[str, Any]) -> None:
    """Write a document to Firestore (overwrite)."""
    db = get_db()
    doc_ref = db.collection(collection).document(document_id)
    doc_ref.set(data)


def update_document(collection: str, document_id: str, data: dict[str, Any]) -> None:
    """Update specific fields of a document."""
    db = get_db()
    doc_ref = db.collection(collection).document(document_id)
    doc_ref.update(data)


def get_collection(
    collection: str,
    order_by: str | None = None,
    limit: int | None = None,
) -> list[dict[str, Any]]:
    """Read all documents from a collection.

    Args:
        collection: Collection path (can include subcollections,
            e.g. "daily_rankings/2026-03-03/items")
        order_by: Field to order by
        limit: Max number of documents to return
    """
    db = get_db()
    query: Any = db.collection(collection)
    if order_by:
        query = query.order_by(order_by)
    if limit:
        query = query.limit(limit)
    docs = query.stream()
    return [doc.to_dict() for doc in docs]


def set_subcollection_document(
    parent_collection: str,
    parent_id: str,
    sub_collection: str,
    document_id: str,
    data: dict[str, Any],
) -> None:
    """Write a document in a subcollection."""
    db = get_db()
    doc_ref = (
        db.collection(parent_collection)
        .document(parent_id)
        .collection(sub_collection)
        .document(document_id)
    )
    doc_ref.set(data)


def get_subcollection(
    parent_collection: str,
    parent_id: str,
    sub_collection: str,
    order_by: str | None = None,
    limit: int | None = None,
) -> list[dict[str, Any]]:
    """Read documents from a subcollection."""
    db = get_db()
    query: Any = (
        db.collection(parent_collection)
        .document(parent_id)
        .collection(sub_collection)
    )
    if order_by:
        query = query.order_by(order_by)
    if limit:
        query = query.limit(limit)
    docs = query.stream()
    return [doc.to_dict() for doc in docs]


def batch_write(operations: list[tuple[str, str, dict[str, Any]]]) -> None:
    """Execute multiple writes in a single batch.

    Args:
        operations: List of (collection_path, document_id, data) tuples.
            collection_path can be nested like "daily_rankings/2026-03-03/items"
    """
    db = get_db()
    for i in range(0, len(operations), MAX_BATCH_WRITE_OPERATIONS):
        chunk = operations[i:i + MAX_BATCH_WRITE_OPERATIONS]
        batch = db.batch()
        for collection_path, doc_id, data in chunk:
            doc_ref = db.collection(collection_path).document(doc_id)
            batch.set(doc_ref, data)
        batch.commit()


def delete_collection_documents(collection_path: str) -> int:
    """Delete all documents in a collection path, including nested paths.

    Returns the number of deleted documents.
    """
    db = get_db()
    docs = list(db.collection(collection_path).stream())
    deleted = 0

    for i in range(0, len(docs), MAX_BATCH_WRITE_OPERATIONS):
        chunk = docs[i:i + MAX_BATCH_WRITE_OPERATIONS]
        batch = db.batch()
        for doc in chunk:
            batch.delete(doc.reference)
        batch.commit()
        deleted += len(chunk)

    return deleted


def reset_client() -> None:
    """Reset the client (for testing)."""
    global _app, _db
    if _app is not None:
        firebase_admin.delete_app(_app)
    _app = None
    _db = None
