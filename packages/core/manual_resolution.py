"""Manual application helpers for unresolved merge/link/separate decisions."""

from __future__ import annotations

from copy import deepcopy
from datetime import datetime, timedelta, timezone
from typing import Any

from packages.core.alias_registry import build_alias_records, delete_alias_records_for_candidate
from packages.core.change_log import record_change
from packages.core.models import Candidate, CandidateRelation, CandidateStatus
from packages.core.normalize import normalize_name

JST = timezone(timedelta(hours=9))


def apply_manual_resolution(
    queue_item: dict[str, Any],
    candidates_by_id: dict[str, Candidate],
    *,
    action: str,
    changed_by: str,
    winner_candidate_id: str | None = None,
    changed_at: str | None = None,
) -> dict[str, Any]:
    normalized_action = action.strip().lower()
    if normalized_action not in {"merge", "link", "separate"}:
        raise ValueError(f"Unsupported action: {action}")

    left_id = str(queue_item.get("leftCandidateId", ""))
    right_id = str(queue_item.get("rightCandidateId", ""))
    left = candidates_by_id.get(left_id)
    right = candidates_by_id.get(right_id)
    if left is None or right is None:
        raise ValueError("Queue item references missing candidates")

    timestamp = changed_at or datetime.now(JST).isoformat()
    left_before = deepcopy(left)
    right_before = deepcopy(right)
    if normalized_action == "merge":
        winner_id = winner_candidate_id or left_id
        if winner_id not in {left_id, right_id}:
            raise ValueError("winner_candidate_id must be one of the queued candidates")
        loser_id = right_id if winner_id == left_id else left_id
        winner = candidates_by_id[winner_id]
        loser = candidates_by_id[loser_id]
        _apply_merge(winner, loser, changed_by=changed_by, changed_at=timestamp)
        queue_item.update(
            {
                "status": "APPLIED",
                "appliedAction": "merge",
                "winnerCandidateId": winner_id,
                "loserCandidateId": loser_id,
                "appliedAt": timestamp,
                "appliedBy": changed_by,
            }
        )
        return {
            "updatedCandidates": [winner, loser],
            "aliasRecords": build_alias_records([winner]),
            "deleteAliasCandidateIds": [loser_id],
            "relations": [],
            "queueItem": queue_item,
            "changeLogs": _build_change_logs(
                left_before=left_before,
                right_before=right_before,
                left_after=winner if winner_id == left_id else loser,
                right_after=loser if winner_id == left_id else winner,
                changed_by=changed_by,
            ),
        }

    relations = _manual_relations(left_id, right_id, normalized_action, timestamp)
    _apply_non_merge_action(left, right, normalized_action, changed_by, timestamp)
    queue_item.update(
        {
            "status": "APPLIED",
            "appliedAction": normalized_action,
            "appliedAt": timestamp,
            "appliedBy": changed_by,
        }
    )
    return {
        "updatedCandidates": [left, right],
        "aliasRecords": build_alias_records([left, right]),
        "deleteAliasCandidateIds": [],
        "relations": relations,
        "queueItem": queue_item,
        "changeLogs": _build_change_logs(
            left_before=left_before,
            right_before=right_before,
            left_after=left,
            right_after=right,
            changed_by=changed_by,
        ),
    }


def persist_manual_resolution(result: dict[str, Any]) -> None:
    from packages.core import firestore_client

    updated_candidates = result.get("updatedCandidates", [])
    for candidate in updated_candidates:
        firestore_client.set_document("candidates", candidate.candidate_id, candidate.to_dict())

    for candidate_id in result.get("deleteAliasCandidateIds", []):
        delete_alias_records_for_candidate(str(candidate_id))

    alias_records = result.get("aliasRecords", [])
    if alias_records:
        from packages.core.alias_registry import save_alias_records

        save_alias_records(alias_records)

    for relation in result.get("relations", []):
        firestore_client.upsert_document(
            "candidate_relations",
            relation.document_id,
            relation.to_dict(),
        )

    queue_item = result.get("queueItem")
    if queue_item:
        pair_id = str(queue_item.get("pairId", ""))
        target_date = str(queue_item.get("date", ""))
        if pair_id and target_date:
            firestore_client.upsert_document(
                f"unresolved_resolution_queue/{target_date}/items",
                pair_id,
                queue_item,
            )

    for change_log in result.get("changeLogs", []):
        record_change(**change_log)


def _apply_merge(
    winner: Candidate,
    loser: Candidate,
    *,
    changed_by: str,
    changed_at: str,
) -> None:
    merged_aliases = [winner.canonical_name, winner.display_name, *winner.aliases]
    merged_aliases.extend([loser.canonical_name, loser.display_name, *loser.aliases])
    deduped_aliases = []
    seen = set()
    for alias in merged_aliases:
        normalized = normalize_name(alias)
        if not normalized:
            continue
        key = normalized.casefold()
        if key in seen or key == normalize_name(winner.canonical_name).casefold():
            continue
        seen.add(key)
        deduped_aliases.append(normalized)

    winner.aliases = deduped_aliases
    winner.related_candidate_ids = sorted(
        {
            related_id
            for related_id in [*winner.related_candidate_ids, *loser.related_candidate_ids]
            if related_id and related_id not in {winner.candidate_id, loser.candidate_id}
        }
    )
    merged_from = set(winner.metadata.get("mergedFromCandidateIds", []))
    merged_from.add(loser.candidate_id)
    winner.metadata["mergedFromCandidateIds"] = sorted(merged_from)
    winner.metadata["lastManualResolutionAt"] = changed_at
    winner.metadata["lastManualResolutionBy"] = changed_by
    winner.manual_lock = True

    for provider, external_id in loser.external_ids.items():
        winner.external_ids.setdefault(provider, external_id)

    loser.status = CandidateStatus.MERGED
    loser.manual_lock = True
    loser.metadata["mergedIntoCandidateId"] = winner.candidate_id
    loser.metadata["mergedAt"] = changed_at
    loser.metadata["mergedBy"] = changed_by


def _apply_non_merge_action(
    left: Candidate,
    right: Candidate,
    action: str,
    changed_by: str,
    changed_at: str,
) -> None:
    if action == "link":
        left.related_candidate_ids = sorted(set(left.related_candidate_ids) | {right.candidate_id})
        right.related_candidate_ids = sorted(set(right.related_candidate_ids) | {left.candidate_id})
    if action == "separate":
        _append_resolution_exclusion(left, right.candidate_id)
        _append_resolution_exclusion(right, left.candidate_id)
    for candidate in (left, right):
        candidate.metadata["lastManualResolutionAt"] = changed_at
        candidate.metadata["lastManualResolutionBy"] = changed_by


def _append_resolution_exclusion(candidate: Candidate, other_candidate_id: str) -> None:
    current = candidate.metadata.get("resolutionExcludeIds", [])
    values = {str(value) for value in current if value} if isinstance(current, list) else set()
    values.add(other_candidate_id)
    candidate.metadata["resolutionExcludeIds"] = sorted(values)


def _manual_relations(
    left_candidate_id: str,
    right_candidate_id: str,
    action: str,
    created_at: str,
) -> list[CandidateRelation]:
    if action != "link":
        return []
    return [
        CandidateRelation(
            src_candidate_id=left_candidate_id,
            relation_type="manual_related",
            dst_candidate_id=right_candidate_id,
            confidence=1.0,
            source="manual_resolution",
            created_at=created_at,
        ),
        CandidateRelation(
            src_candidate_id=right_candidate_id,
            relation_type="manual_related",
            dst_candidate_id=left_candidate_id,
            confidence=1.0,
            source="manual_resolution",
            created_at=created_at,
        ),
    ]


def _build_change_logs(
    *,
    left_before: Candidate,
    right_before: Candidate,
    left_after: Candidate,
    right_after: Candidate,
    changed_by: str,
) -> list[dict[str, Any]]:
    return [
        {
            "log_id": f"candidate:{left_after.candidate_id}:{datetime.now(JST).timestamp()}",
            "collection": "candidates",
            "document_path": f"candidates/{left_after.candidate_id}",
            "changed_by": changed_by,
            "before": left_before.to_dict(),
            "after": left_after.to_dict(),
        },
        {
            "log_id": f"candidate:{right_after.candidate_id}:{datetime.now(JST).timestamp()}",
            "collection": "candidates",
            "document_path": f"candidates/{right_after.candidate_id}",
            "changed_by": changed_by,
            "before": right_before.to_dict(),
            "after": right_after.to_dict(),
        },
    ]
