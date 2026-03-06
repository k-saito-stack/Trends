"""Cost degradation logic.

When monthly budget usage exceeds thresholds, automatically reduce
expensive operations to stay within budget.

Spec reference: Section 7 (Cost Degradation Tiers)
"""

from __future__ import annotations

import logging
from dataclasses import dataclass

from packages.core.models import AppConfig

logger = logging.getLogger(__name__)


@dataclass
class DegradeState:
    """Current degradation state for this run."""
    summary_mode: str = "LLM"        # LLM / TEMPLATE / OFF
    x_search_enabled: bool = True
    x_search_max: int = 20           # Max candidates to search (bounded by top_k)
    reason: str = ""                  # Why degraded

    def to_dict(self) -> dict[str, object]:
        return {
            "summaryMode": self.summary_mode,
            "xSearchEnabled": self.x_search_enabled,
            "xSearchMax": self.x_search_max,
            "reason": self.reason,
        }


def compute_degrade_state(
    budget_ratio: float,
    app_config: AppConfig,
) -> DegradeState:
    """Determine degradation level based on budget usage.

    Thresholds (from spec):
    - < 60%: Full mode (LLM summaries, full X search)
    - 60-80%: Switch summaries to TEMPLATE (no LLM cost)
    - 80-100%: Reduce X search to top 5 only
    - >= 100%: Disable X search entirely, summaries OFF

    Args:
        budget_ratio: Current month cost / monthly budget (0.0 to 1.0+)
        app_config: App config with threshold settings

    Returns:
        DegradeState for this run
    """
    template_at = app_config.template_at_ratio
    x_reduce_at = app_config.x_search_reduce_at_ratio
    full_x_search_max = app_config.top_k
    reduced_x_search_max = min(5, app_config.top_k)

    if budget_ratio >= 1.0:
        logger.warning("Budget exceeded (%.0f%%), disabling all paid APIs", budget_ratio * 100)
        return DegradeState(
            summary_mode="OFF",
            x_search_enabled=False,
            x_search_max=0,
            reason=f"Budget exceeded ({budget_ratio:.0%})",
        )

    if budget_ratio >= x_reduce_at:
        logger.info("Budget at %.0f%%, reducing X search to top 5", budget_ratio * 100)
        return DegradeState(
            summary_mode="TEMPLATE",
            x_search_enabled=True,
            x_search_max=reduced_x_search_max,
            reason=f"Budget at {budget_ratio:.0%}, reducing X search",
        )

    if budget_ratio >= template_at:
        logger.info("Budget at %.0f%%, switching to template summaries", budget_ratio * 100)
        return DegradeState(
            summary_mode="TEMPLATE",
            x_search_enabled=True,
            x_search_max=full_x_search_max,
            reason=f"Budget at {budget_ratio:.0%}, template summaries",
        )

    # Full mode
    return DegradeState(
        summary_mode="LLM",
        x_search_enabled=True,
        x_search_max=full_x_search_max,
        reason="",
    )
