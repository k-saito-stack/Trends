"""Base connector for all data sources.

Each source connector inherits from BaseConnector and implements:
  - fetch(): Get raw data from external API/feed
  - extract_candidates(): Extract candidate names from raw data
  - compute_signals(): Calculate daily signal x(s,q,t) for each candidate

Spec reference: Section 8 (Discover rules)
"""

from __future__ import annotations

import logging
import time
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import Any

from packages.core.models import Evidence, RawCandidate

logger = logging.getLogger(__name__)


@dataclass
class FetchResult:
    """Result from a connector's fetch operation."""

    items: list[dict[str, Any]] = field(default_factory=list)
    error: str | None = None
    item_count: int = 0
    metadata: dict[str, Any] = field(default_factory=dict)
    fallback_used: str = ""
    response_ms: int | None = None


@dataclass
class SignalResult:
    """Daily signal for a single candidate from a source."""

    candidate_name: str
    signal_value: float
    evidence: Evidence | None = None


@dataclass
class ConnectorRunResult:
    """Result from a connector's full run() pipeline."""

    source_id: str
    ok: bool
    item_count: int
    candidates: list[RawCandidate] = field(default_factory=list)
    signals: list[SignalResult] = field(default_factory=list)
    error: str | None = None
    kept_item_count: int = 0
    metadata: dict[str, Any] = field(default_factory=dict)
    fallback_used: str = ""
    response_ms: int | None = None


class BaseConnector(ABC):
    """Abstract base connector for data sources.

    Attributes:
        source_id: Unique identifier (e.g. "YOUTUBE_TREND_JP")
        enabled: Whether this source is active
        stability: A (core), B (supplementary), C (optional)
        consecutive_failures: Tracks failures for kill switch
        max_consecutive_failures: Kill switch threshold
    """

    def __init__(
        self,
        source_id: str,
        enabled: bool = True,
        stability: str = "A",
        max_consecutive_failures: int = 3,
    ) -> None:
        self.source_id = source_id
        self.enabled = enabled
        self.stability = stability
        self.consecutive_failures = 0
        self.max_consecutive_failures = max_consecutive_failures

    @abstractmethod
    def fetch(self) -> FetchResult:
        """Fetch raw data from the external source.

        Returns FetchResult with items on success, or error message on failure.
        Must NOT raise exceptions - catch and return error in FetchResult.
        """

    @abstractmethod
    def extract_candidates(self, items: list[dict[str, Any]]) -> list[RawCandidate]:
        """Extract candidate names and types from raw items."""

    @abstractmethod
    def compute_signals(
        self, items: list[dict[str, Any]], candidates: list[RawCandidate]
    ) -> list[SignalResult]:
        """Compute daily signal x(s,q,t) for each candidate."""

    def run(self) -> ConnectorRunResult:
        """Execute full pipeline: fetch -> extract -> signal.

        Returns ConnectorRunResult with ok=True on success (including 0 items),
        ok=False on fetch/extract/signal failure. This distinction allows
        batch/run.py to correctly handle 0-observation (x=0) vs missing data
        (x=None).
        """
        if not self.enabled:
            logger.info("[%s] Skipped (disabled)", self.source_id)
            return ConnectorRunResult(
                source_id=self.source_id,
                ok=False,
                item_count=0,
                error="disabled",
                kept_item_count=0,
            )

        # Kill switch check
        if self.consecutive_failures >= self.max_consecutive_failures:
            logger.warning(
                "[%s] Kill switch active (%d consecutive failures)",
                self.source_id,
                self.consecutive_failures,
            )
            return ConnectorRunResult(
                source_id=self.source_id,
                ok=False,
                item_count=0,
                error="kill_switch",
                kept_item_count=0,
            )

        # Fetch
        started_at = time.monotonic()
        try:
            result = self.fetch()
        except Exception as e:
            self.consecutive_failures += 1
            logger.error("[%s] Fetch error: %s", self.source_id, e)
            return ConnectorRunResult(
                source_id=self.source_id,
                ok=False,
                item_count=0,
                error=str(e),
                kept_item_count=0,
            )

        if result.error:
            response_ms = result.response_ms or int((time.monotonic() - started_at) * 1000)
            self.consecutive_failures += 1
            logger.error("[%s] Fetch error: %s", self.source_id, result.error)
            return ConnectorRunResult(
                source_id=self.source_id,
                ok=False,
                item_count=0,
                error=result.error,
                kept_item_count=0,
                metadata=result.metadata,
                fallback_used=result.fallback_used,
                response_ms=response_ms,
            )

        if not result.items:
            response_ms = result.response_ms or int((time.monotonic() - started_at) * 1000)
            logger.info("[%s] No items returned", self.source_id)
            # ok=True: fetch succeeded but 0 items (valid 0-observation)
            return ConnectorRunResult(
                source_id=self.source_id,
                ok=True,
                item_count=0,
                kept_item_count=0,
                metadata=result.metadata,
                fallback_used=result.fallback_used,
                response_ms=response_ms,
            )

        # Reset failure counter on success
        self.consecutive_failures = 0

        # Extract candidates
        try:
            candidates = self.extract_candidates(result.items)
        except Exception as e:
            response_ms = result.response_ms or int((time.monotonic() - started_at) * 1000)
            logger.error("[%s] Extract error: %s", self.source_id, e)
            return ConnectorRunResult(
                source_id=self.source_id,
                ok=False,
                item_count=len(result.items),
                error=f"extract: {e}",
                kept_item_count=0,
                metadata=result.metadata,
                fallback_used=result.fallback_used,
                response_ms=response_ms,
            )

        # Compute signals
        try:
            signals = self.compute_signals(result.items, candidates)
        except Exception as e:
            response_ms = result.response_ms or int((time.monotonic() - started_at) * 1000)
            logger.error("[%s] Signal error: %s", self.source_id, e)
            signal_metadata = dict(result.metadata)
            signal_metadata["failureStage"] = "signal"
            return ConnectorRunResult(
                source_id=self.source_id,
                ok=False,
                item_count=len(result.items),
                candidates=candidates,
                error=f"signal: {e}",
                kept_item_count=len(candidates),
                metadata=signal_metadata,
                fallback_used=result.fallback_used,
                response_ms=response_ms,
            )

        response_ms = result.response_ms or int((time.monotonic() - started_at) * 1000)
        logger.info(
            "[%s] OK: %d items -> %d candidates, %d signals",
            self.source_id,
            len(result.items),
            len(candidates),
            len(signals),
        )
        return ConnectorRunResult(
            source_id=self.source_id,
            ok=True,
            item_count=len(result.items),
            candidates=candidates,
            signals=signals,
            kept_item_count=len(candidates),
            metadata=result.metadata,
            fallback_used=result.fallback_used,
            response_ms=response_ms,
        )
