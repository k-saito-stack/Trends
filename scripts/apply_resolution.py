"""Apply a manual unresolved-resolution action to Firestore.

Usage:
    python scripts/apply_resolution.py \
        --date 2026-03-07 \
        --pair-id <pairId> \
        --action merge \
        --winner-candidate-id cand_1 \
        --changed-by you@example.com
    python scripts/apply_resolution.py \
        --date 2026-03-07 \
        --pair-id <pairId> \
        --action link \
        --changed-by you@example.com
    python scripts/apply_resolution.py \
        --date 2026-03-07 \
        --pair-id <pairId> \
        --action separate \
        --changed-by you@example.com
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from packages.core import firestore_client  # noqa: E402
from packages.core.candidate_store import load_candidates_by_ids  # noqa: E402
from packages.core.manual_resolution import (  # noqa: E402
    apply_manual_resolution,
    persist_manual_resolution,
)


def main() -> None:
    parser = argparse.ArgumentParser(description="Apply manual resolution action")
    parser.add_argument("--date", required=True, help="Queue date (YYYY-MM-DD)")
    parser.add_argument("--pair-id", required=True, help="Queue pair ID")
    parser.add_argument(
        "--action",
        required=True,
        choices=["merge", "link", "separate"],
        help="Manual action to apply",
    )
    parser.add_argument(
        "--winner-candidate-id",
        help="Required when action=merge and you want the right candidate to win",
    )
    parser.add_argument("--changed-by", required=True, help="Email or operator identifier")
    args = parser.parse_args()

    queue_item = firestore_client.get_document(
        f"unresolved_resolution_queue/{args.date}/items",
        args.pair_id,
    )
    if queue_item is None:
        raise SystemExit(f"Queue item not found: {args.date}/{args.pair_id}")
    queue_item.setdefault("date", args.date)

    candidate_ids = [
        str(queue_item.get("leftCandidateId", "")),
        str(queue_item.get("rightCandidateId", "")),
    ]
    candidates = load_candidates_by_ids(candidate_ids)
    result = apply_manual_resolution(
        queue_item,
        candidates,
        action=args.action,
        winner_candidate_id=args.winner_candidate_id,
        changed_by=args.changed_by,
    )
    persist_manual_resolution(result)


if __name__ == "__main__":
    main()
