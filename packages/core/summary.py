"""Summary generation for candidate cards.

Three modes (controlled by config):
- LLM: Call xAI API to generate natural language summary
- TEMPLATE: Fill in a simple template string (no API cost)
- OFF: No summary (empty string)

Spec reference: Section 10.9 (Summary)
"""

from __future__ import annotations

import logging
from typing import Any

from packages.core.models import BucketScore, Evidence

logger = logging.getLogger(__name__)

# Summary modes
MODE_LLM = "LLM"
MODE_TEMPLATE = "TEMPLATE"
MODE_OFF = "OFF"


def generate_summary(
    candidate_name: str,
    trend_score: float,
    breakdown: list[BucketScore],
    evidence: list[Evidence],
    mode: str = MODE_TEMPLATE,
    llm_client: Any = None,
) -> str:
    """Generate a summary string for a candidate card.

    Args:
        candidate_name: Display name of the candidate
        trend_score: Computed TrendScore
        breakdown: Bucket score breakdown
        evidence: Evidence items
        mode: One of "LLM", "TEMPLATE", "OFF"
        llm_client: Optional LLM client for LLM mode (Phase 6)

    Returns:
        Summary string
    """
    if mode == MODE_OFF:
        return ""

    if mode == MODE_LLM:
        return _generate_llm_summary(
            candidate_name, trend_score, breakdown, evidence, llm_client
        )

    # Default: TEMPLATE mode
    return _generate_template_summary(
        candidate_name, trend_score, breakdown, evidence
    )


def _generate_template_summary(
    candidate_name: str,
    trend_score: float,
    breakdown: list[BucketScore],
    evidence: list[Evidence],
) -> str:
    """Generate a template-based summary (no API cost).

    Format: "{name}が{top_sources}で注目されています。(スコア: {score:.1f})"
    """
    # Get top contributing sources
    source_names = [b.bucket for b in breakdown[:3] if b.score > 0]

    if not source_names:
        return f"{candidate_name}がトレンドに浮上しています。(スコア: {trend_score:.1f})"

    sources_str = "・".join(source_names)
    return f"{candidate_name}が{sources_str}で注目されています。(スコア: {trend_score:.1f})"


def _generate_llm_summary(
    candidate_name: str,
    trend_score: float,
    breakdown: list[BucketScore],
    evidence: list[Evidence],
    llm_client: Any = None,
) -> str:
    """Generate an LLM-based summary using xAI API.

    Phase 6 implementation. For now, falls back to template.
    """
    if llm_client is None:
        logger.info("LLM client not available, falling back to TEMPLATE mode")
        return _generate_template_summary(
            candidate_name, trend_score, breakdown, evidence
        )

    # Full LLM implementation will be added in Phase 6
    # For now, return template as fallback
    return _generate_template_summary(
        candidate_name, trend_score, breakdown, evidence
    )
