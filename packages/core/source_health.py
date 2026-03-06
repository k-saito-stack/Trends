"""Operational health records separate from semantic source weighting."""

from __future__ import annotations

from dataclasses import dataclass, field


@dataclass
class SourceHealthRecord:
    date: str
    source_id: str
    ok: bool
    item_count: int
    error: str = ""
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
            "itemCount": self.item_count,
            "error": self.error,
            "metadata": self.metadata,
        }
        if self.response_ms is not None:
            payload["responseMs"] = self.response_ms
        return payload


def build_source_health_records(
    date: str,
    source_ok: dict[str, bool],
    source_item_count: dict[str, int],
    errors: dict[str, str] | None = None,
) -> list[SourceHealthRecord]:
    errors = errors or {}
    return [
        SourceHealthRecord(
            date=date,
            source_id=source_id,
            ok=ok,
            item_count=source_item_count.get(source_id, 0),
            error=errors.get(source_id, ""),
        )
        for source_id, ok in source_ok.items()
    ]
