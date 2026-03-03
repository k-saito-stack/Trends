"""API cost tracking and budget management.

Tracks estimated API costs per run and monthly totals.
Used by the degrade module to decide cost-saving measures.

Spec reference: Section 7 (Cost Management)
"""

from __future__ import annotations

import logging
from datetime import datetime, timedelta, timezone
from typing import Any

from packages.core import firestore_client

logger = logging.getLogger(__name__)

JST = timezone(timedelta(hours=9))

# Estimated costs per API call (JPY)
COST_TABLE: dict[str, float] = {
    "YOUTUBE_TREND_JP": 0.0,       # YouTube Data API free tier
    "APPLE_MUSIC_JP": 0.0,         # Apple RSS is free
    "APPLE_MUSIC_GLOBAL": 0.0,     # Apple RSS is free
    "TRENDS": 0.0,                 # Google Trends RSS is free
    "NEWS_RSS": 0.0,               # RSS is free
    "RAKUTEN_MAG": 0.0,            # Rakuten API free tier
    "WIKIPEDIA": 0.0,              # Wikipedia API is free
    "X_SEARCH": 5.0,               # xAI API per call estimate
    "LLM_SUMMARY": 3.0,            # xAI API per summary estimate
}


def estimate_run_cost(
    sources_used: list[str],
    x_search_calls: int = 0,
    llm_summary_calls: int = 0,
) -> float:
    """Estimate the cost of a single run in JPY."""
    total = 0.0
    for source in sources_used:
        total += COST_TABLE.get(source, 0.0)
    total += x_search_calls * COST_TABLE.get("X_SEARCH", 0.0)
    total += llm_summary_calls * COST_TABLE.get("LLM_SUMMARY", 0.0)
    return total


def get_monthly_cost(year_month: str | None = None) -> float:
    """Get total cost for a given month from Firestore.

    Args:
        year_month: "YYYY-MM" format. Defaults to current month.

    Returns:
        Total cost in JPY for the month.
    """
    if year_month is None:
        year_month = datetime.now(JST).strftime("%Y-%m")

    data = firestore_client.get_document("cost_logs", year_month)
    if data is None:
        return 0.0
    return float(data.get("totalJPY", 0.0))


def record_run_cost(
    run_id: str,
    target_date: str,
    cost_jpy: float,
    details: dict[str, Any] | None = None,
) -> None:
    """Record a run's cost to Firestore.

    Updates both the per-run record and the monthly total.
    """
    now = datetime.now(JST)
    year_month = target_date[:7]  # "YYYY-MM"

    # Record per-run cost
    run_cost_data: dict[str, Any] = {
        "runId": run_id,
        "targetDate": target_date,
        "costJPY": cost_jpy,
        "recordedAt": now.isoformat(),
    }
    if details:
        run_cost_data["details"] = details

    firestore_client.set_subcollection_document(
        "cost_logs", year_month, "runs", run_id, run_cost_data
    )

    # Update monthly total
    current_total = get_monthly_cost(year_month)
    new_total = current_total + cost_jpy
    firestore_client.set_document("cost_logs", year_month, {
        "yearMonth": year_month,
        "totalJPY": new_total,
        "lastUpdated": now.isoformat(),
    })

    logger.info(
        "Cost recorded: run=%s, cost=%.1f JPY, monthly_total=%.1f JPY",
        run_id, cost_jpy, new_total,
    )


def get_budget_ratio(monthly_budget_jpy: int = 5000) -> float:
    """Get current month's cost as ratio of budget.

    Returns:
        Ratio (0.0 to 1.0+). e.g. 0.6 means 60% of budget used.
    """
    current_cost = get_monthly_cost()
    if monthly_budget_jpy <= 0:
        return 0.0
    return current_cost / monthly_budget_jpy
