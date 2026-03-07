"""Cost degradation logic.

When monthly budget usage exceeds thresholds, automatically reduce
expensive operations to stay within budget.

Current policy:
- xAI summaries stay on even above budget thresholds.
- Only X search is degraded by budget ratio.
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

    Thresholds:
    - < template_at: LLM summaries, full X search
    - template_at to x_reduce_at: LLM summaries, full X search
    - x_reduce_at to 100%: LLM summaries, reduced X search
    - >= 100%: LLM summaries, X search disabled

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
        logger.warning(
            "Budget exceeded (%.0f%%), disabling X search but keeping LLM summaries on",
            budget_ratio * 100,
        )
        return DegradeState(
            summary_mode="LLM",
            x_search_enabled=False,
            x_search_max=0,
            reason=f"Budget exceeded ({budget_ratio:.0%}), X search disabled",
        )

    if budget_ratio >= x_reduce_at:
        logger.info(
            "Budget at %.0f%%, reducing X search to top 5 while keeping LLM summaries on",
            budget_ratio * 100,
        )
        return DegradeState(
            summary_mode="LLM",
            x_search_enabled=True,
            x_search_max=reduced_x_search_max,
            reason=f"Budget at {budget_ratio:.0%}, reducing X search",
        )

    if budget_ratio >= template_at:
        logger.info(
            "Budget at %.0f%%, keeping LLM summaries on and leaving X search unchanged",
            budget_ratio * 100,
        )
        return DegradeState(
            summary_mode="LLM",
            x_search_enabled=True,
            x_search_max=full_x_search_max,
            reason=f"Budget at {budget_ratio:.0%}, summaries stay LLM",
        )

    # Full mode
    return DegradeState(
        summary_mode="LLM",
        x_search_enabled=True,
        x_search_max=full_x_search_max,
        reason="",
    )
