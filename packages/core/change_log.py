"""Change log: record config/candidate changes to Firestore.

Every config or candidate modification is logged for audit trail
and rollback capability.

Spec reference: Section 12 (Change Logs)
"""

from __future__ import annotations

import logging
from datetime import datetime, timedelta, timezone
from typing import Any

from packages.core import firestore_client
from packages.core.models import ChangeLog

logger = logging.getLogger(__name__)

JST = timezone(timedelta(hours=9))


def record_change(
    log_id: str,
    collection: str,
    document_path: str,
    changed_by: str,
    before: dict[str, Any],
    after: dict[str, Any],
) -> None:
    """Record a change to Firestore /change_logs/{logId}."""
    now = datetime.now(JST).isoformat()
    log = ChangeLog(
        log_id=log_id,
        collection=collection,
        document_path=document_path,
        changed_by=changed_by,
        changed_at=now,
        before=before,
        after=after,
    )
    firestore_client.set_document("change_logs", log_id, log.to_dict())
    logger.info("Change logged: %s -> %s by %s", document_path, log_id, changed_by)


def get_recent_changes(limit: int = 20) -> list[dict[str, Any]]:
    """Get recent change logs, newest first."""
    return firestore_client.get_collection(
        "change_logs", order_by="changedAt", limit=limit
    )
