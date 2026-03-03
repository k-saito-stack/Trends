"""Evidence selection: pick top-3 evidence items per candidate.

Each candidate card shows up to 3 evidence items from different sources,
prioritized by source diversity and signal strength.

Spec reference: Section 10.8 (EvidenceTop3)
"""

from __future__ import annotations

from typing import Any

from packages.core.models import Evidence


def select_evidence_top3(
    evidence_pool: list[Evidence],
    max_items: int = 3,
) -> list[Evidence]:
    """Select top evidence items for a candidate card.

    Selection rules (from spec):
    1. At most 1 item per source_id (source diversity)
    2. Prioritize sources with highest signal contribution
    3. Cap at max_items (default 3)

    Args:
        evidence_pool: All evidence items collected for this candidate
        max_items: Maximum number of evidence items to return

    Returns:
        List of up to max_items Evidence objects, source-diverse
    """
    if not evidence_pool:
        return []

    seen_sources: set[str] = set()
    selected: list[Evidence] = []

    # Evidence pool is assumed to be pre-sorted by relevance/signal strength
    for ev in evidence_pool:
        if ev.source_id in seen_sources:
            continue
        seen_sources.add(ev.source_id)
        selected.append(ev)
        if len(selected) >= max_items:
            break

    return selected


def build_evidence_pool(
    raw_evidence: list[dict[str, Any]],
) -> list[Evidence]:
    """Convert raw evidence dicts to Evidence objects.

    Each raw evidence dict should have:
    - source_id: str
    - title: str
    - url: str
    - published_at: str (optional)
    - metric: str (optional, e.g. "rank:3", "viewCount:120000")
    - snippet: str (optional)
    - signal_value: float (used for sorting, not stored in Evidence)
    """
    evidence_list: list[Evidence] = []

    for raw in raw_evidence:
        ev = Evidence(
            source_id=raw.get("source_id", ""),
            title=raw.get("title", ""),
            url=raw.get("url", ""),
            published_at=raw.get("published_at", ""),
            metric=raw.get("metric", ""),
            snippet=raw.get("snippet", ""),
        )
        evidence_list.append(ev)

    # Sort by signal_value (higher = more important)
    # Pair with original raw to access signal_value
    paired = list(zip(raw_evidence, evidence_list, strict=True))
    paired.sort(key=lambda p: -p[0].get("signal_value", 0.0))

    return [ev for _, ev in paired]
