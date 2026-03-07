"""Operational health records separate from semantic source weighting."""

from __future__ import annotations

from dataclasses import dataclass, field


@dataclass
class SourceHealthRecord:
    date: str
    source_id: str
    ok: bool
    raw_item_count: int
    kept_item_count: int
    error: str = ""
    failure_class: str = ""
    availability_tier: str = ""
    fallback_used: str = ""
    response_ms: int | None = None
    metadata: dict[str, object] = field(default_factory=dict)

    @property
    def document_id(self) -> str:
        return f"{self.date}_{self.source_id}"

    def to_dict(self) -> dict[str, object]:
        payload: dict[str, object] = {
            "date": self.date,
            "sourceId": self.source_id,
            "ok": self.ok,
            "itemCount": self.raw_item_count,
            "rawItemCount": self.raw_item_count,
            "keptItemCount": self.kept_item_count,
            "error": self.error,
            "failureClass": self.failure_class,
            "availabilityTier": self.availability_tier,
            "fallbackUsed": self.fallback_used,
            "metadata": self.metadata,
        }
        if self.response_ms is not None:
            payload["responseMs"] = self.response_ms
        return payload


def build_source_health_records(
    date: str,
    source_ok: dict[str, bool],
    source_item_count: dict[str, int],
    source_kept_count: dict[str, int] | None = None,
    errors: dict[str, str] | None = None,
    availability_tiers: dict[str, str] | None = None,
    fallback_used: dict[str, str] | None = None,
    response_ms: dict[str, int] | None = None,
) -> list[SourceHealthRecord]:
    errors = errors or {}
    source_kept_count = source_kept_count or {}
    availability_tiers = availability_tiers or {}
    fallback_used = fallback_used or {}
    response_ms = response_ms or {}
    return [
        SourceHealthRecord(
            date=date,
            source_id=source_id,
            ok=ok,
            raw_item_count=source_item_count.get(source_id, 0),
            kept_item_count=source_kept_count.get(source_id, 0),
            error=errors.get(source_id, ""),
            failure_class=classify_source_failure(
                ok=ok,
                raw_item_count=source_item_count.get(source_id, 0),
                kept_item_count=source_kept_count.get(source_id, 0),
                error=errors.get(source_id, ""),
            ),
            availability_tier=availability_tiers.get(source_id, ""),
            fallback_used=fallback_used.get(source_id, ""),
            response_ms=response_ms.get(source_id),
        )
        for source_id, ok in source_ok.items()
    ]


def classify_source_failure(
    *,
    ok: bool,
    raw_item_count: int,
    kept_item_count: int,
    error: str,
) -> str:
    if error:
        lowered = error.lower()
        if "403" in lowered or "forbidden" in lowered:
            return "blocked"
        if "404" in lowered or "not found" in lowered:
            return "endpoint_missing"
        if "timeout" in lowered or "timed out" in lowered:
            return "timeout"
        if "400" in lowered:
            return "bad_request"
        if "disabled" in lowered:
            return "disabled"
        if "kill_switch" in lowered:
            return "kill_switch"
        if "extract:" in lowered:
            return "extract_failed"
        if "signal:" in lowered:
            return "signal_failed"
        return "fetch_failed"
    if not ok:
        return "unavailable"
    if raw_item_count <= 0:
        return "empty_source"
    if kept_item_count <= 0:
        return "zero_kept"
    return "healthy"
