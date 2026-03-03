"""Manual batch run script for local development.

Usage:
    python scripts/manual_run.py
    python scripts/manual_run.py --date 2026-03-01

Requires:
    - GOOGLE_APPLICATION_CREDENTIALS or FIREBASE_SA_JSON env var
    - Or: gcloud auth application-default login
"""

from __future__ import annotations

import sys
from pathlib import Path

# Add project root to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from batch.run import main  # noqa: E402

if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Manual Trends batch run")
    parser.add_argument(
        "--date",
        default="today",
        help="Target date (YYYY-MM-DD or 'today')",
    )
    args = parser.parse_args()
    main(date_arg=args.date)
