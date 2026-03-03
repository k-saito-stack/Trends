"""Run logging: record batch run metadata to Firestore.

Each run creates a document in /runs/{runId} with status, timing,
errors, and summary information.

Spec reference: Section 13 (Batch Run Logging)
"""

from __future__ import annotations

import logging
from datetime import datetime, timedelta, timezone
from typing import Any

from packages.core import firestore_client

logger = logging.getLogger(__name__)

JST = timezone(timedelta(hours=9))


def start_run(
    run_id: str,
    target_date: str,
    degrade_state: dict[str, object] | None = None,
) -> None:
    """Record the start of a batch run."""
    now = datetime.now(JST).isoformat()
    data: dict[str, Any] = {
        "runId": run_id,
        "targetDate": target_date,
        "status": "RUNNING",
        "startedAt": now,
        "endedAt": None,
        "degradeState": degrade_state or {},
        "errors": [],
        "sourceResults": {},
        "candidateCount": 0,
        "topK": 0,
    }
    firestore_client.set_document("runs", run_id, data)
    logger.info("Run started: %s (target: %s)", run_id, target_date)


def update_run_source(
    run_id: str,
    source_id: str,
    item_count: int,
    error: str | None = None,
) -> None:
    """Update run log with source fetch results."""
    data: dict[str, Any] = {
        f"sourceResults.{source_id}": {
            "itemCount": item_count,
            "error": error,
        }
    }
    firestore_client.update_document("runs", run_id, data)


def end_run(
    run_id: str,
    status: str = "SUCCESS",
    candidate_count: int = 0,
    top_k: int = 0,
    errors: list[str] | None = None,
    cost_jpy: float = 0.0,
) -> None:
    """Record the end of a batch run."""
    now = datetime.now(JST).isoformat()
    data: dict[str, Any] = {
        "status": status,
        "endedAt": now,
        "candidateCount": candidate_count,
        "topK": top_k,
        "costJPY": cost_jpy,
    }
    if errors:
        data["errors"] = errors
    firestore_client.update_document("runs", run_id, data)
    logger.info("Run ended: %s (status: %s)", run_id, status)
