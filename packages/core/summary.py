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
        llm_client: LLMClient instance (required for LLM mode)

    Returns:
        Summary string
    """
    if mode == MODE_OFF:
        return ""

    if mode == MODE_LLM:
        return _generate_llm_summary(candidate_name, trend_score, breakdown, evidence, llm_client)

    # Default: TEMPLATE mode
    return _generate_template_summary(candidate_name, trend_score, breakdown, evidence)


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

    Produces a 1-2 sentence summary explaining why this candidate
    is trending, based on the evidence and score breakdown.
    Falls back to template on failure.
    """
    if llm_client is None or not getattr(llm_client, "available", False):
        logger.info("LLM client not available, falling back to TEMPLATE mode")
        return _generate_template_summary(candidate_name, trend_score, breakdown, evidence)

    # Build context for the LLM
    bucket_info = ", ".join(f"{b.bucket}({b.score:.1f})" for b in breakdown[:5] if b.score > 0)
    evidence_info = "\n".join(f"- [{e.source_id}] {e.title}" for e in evidence[:3])

    prompt = (
        f"以下のトレンド候補について、なぜ今注目されているか1〜2文で簡潔に要約してください。\n\n"
        f"候補名: {candidate_name}\n"
        f"トレンドスコア: {trend_score:.1f}\n"
        f"スコア内訳: {bucket_info}\n"
    )
    if evidence_info:
        prompt += f"エビデンス:\n{evidence_info}\n"
    prompt += "\n要約（日本語、1〜2文）:"

    system_msg = "あなたはトレンド分析の専門家です。簡潔に日本語で回答してください。"
    messages = [
        {"role": "system", "content": system_msg},
        {"role": "user", "content": prompt},
    ]

    try:
        result = llm_client.chat(messages, temperature=0.3, max_tokens=200)
        if result:
            # Clean up the response
            summary = result.strip().strip('"').strip("'")
            # Limit length
            if len(summary) > 200:
                summary = summary[:197] + "..."
            return str(summary)
    except Exception as e:
        logger.warning("LLM summary failed for %s: %s", candidate_name, e)

    # Fallback to template
    return _generate_template_summary(candidate_name, trend_score, breakdown, evidence)
