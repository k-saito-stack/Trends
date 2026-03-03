"""Daily batch entry point.

Orchestrates the 12-step pipeline:
  0) Load config, cost_logs, candidates
  1) Start run (ULID, targetDate, degradeState)
  2) Ingest (fetch from each source)
  3) Extract raw candidates
  4) Normalize -> Resolve (assign candidate IDs)
  5) Compute daily signals x(s,q,t)
  6) EWMA/EWMVar update -> sig -> momentum
  7) Aggregate to buckets + multiBonus
  8) Select Top15
  9) Select EvidenceTop3
  10) Generate summary
  11) Write to Firestore
  12) End logging + cost_logs

Spec reference: Section 13 (Daily Batch Runbook)
"""

from __future__ import annotations

import argparse
import logging
from datetime import datetime, timedelta, timezone

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
)
logger = logging.getLogger(__name__)

JST = timezone(timedelta(hours=9))


def get_target_date(date_arg: str) -> str:
    """Parse the --date argument and return YYYY-MM-DD in JST."""
    if date_arg == "today":
        return datetime.now(JST).strftime("%Y-%m-%d")
    return date_arg


def main(date_arg: str = "today") -> None:
    """Run the daily batch pipeline."""
    target_date = get_target_date(date_arg)
    logger.info("=== Trends Daily Batch ===")
    logger.info("Target date: %s", target_date)

    # TODO: Implement steps 0-12 (Phase 4)
    logger.info("Batch pipeline not yet implemented. Exiting.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Trends daily batch")
    parser.add_argument(
        "--date",
        default="today",
        help="Target date (YYYY-MM-DD or 'today')",
    )
    args = parser.parse_args()
    main(date_arg=args.date)
