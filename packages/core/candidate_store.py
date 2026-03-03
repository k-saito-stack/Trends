"""Candidate store: CRUD operations for /candidates collection.

Manages candidate master records in Firestore.

Spec reference: Section 12 (Candidate Collection)
"""

from __future__ import annotations

import logging
from typing import Any

from packages.core import firestore_client
from packages.core.models import Candidate

logger = logging.getLogger(__name__)


def load_all_candidates() -> dict[str, Candidate]:
    """Load all active candidates from Firestore.

    Returns:
        Dict of candidateId -> Candidate
    """
    docs = firestore_client.get_collection("candidates")
    candidates: dict[str, Candidate] = {}
    for doc in docs:
        cand = Candidate.from_dict(doc)
        candidates[cand.candidate_id] = cand
    return candidates


def save_candidate(candidate: Candidate) -> None:
    """Save or update a candidate in Firestore."""
    firestore_client.set_document(
        "candidates", candidate.candidate_id, candidate.to_dict()
    )


def save_candidates_batch(candidates: dict[str, Candidate]) -> None:
    """Save multiple candidates in a batch write."""
    operations: list[tuple[str, str, dict[str, Any]]] = [
        ("candidates", cand_id, cand.to_dict())
        for cand_id, cand in candidates.items()
    ]
    if operations:
        firestore_client.batch_write(operations)
        logger.info("Saved %d candidates", len(operations))
