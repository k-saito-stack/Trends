"""Source catalog loader for the v2 taxonomy."""

from __future__ import annotations

import json
from dataclasses import dataclass
from functools import lru_cache
from pathlib import Path
from typing import Any

from packages.core.models import AccessMode, DomainClass, SourceFamily, SourceRole, SourceStatus

CONFIG_DIR = Path(__file__).resolve().parents[2] / "configs"
SOURCE_CATALOG_PATH = CONFIG_DIR / "source_catalog.yaml"


@dataclass(frozen=True)
class SourceCatalogEntry:
    source_id: str
    display_name: str
    role: SourceRole
    family_primary: SourceFamily
    family_secondary: SourceFamily | None
    status: SourceStatus
    access_mode: AccessMode
    requires_credentials: bool
    requires_manual_approval: bool
    supports_phrase_candidates: bool
    supports_entity_candidates: bool
    target_domains: tuple[DomainClass, ...]
    max_weight_cap: float

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> SourceCatalogEntry:
        secondary = data.get("family_secondary") or data.get("familySecondary")
        return cls(
            source_id=str(data["source_id"]),
            display_name=str(data["display_name"]),
            role=SourceRole(str(data["role"])),
            family_primary=SourceFamily(str(data["family_primary"])),
            family_secondary=SourceFamily(str(secondary)) if secondary else None,
            status=SourceStatus(str(data["status"])),
            access_mode=AccessMode(str(data["access_mode"])),
            requires_credentials=bool(data.get("requires_credentials", False)),
            requires_manual_approval=bool(data.get("requires_manual_approval", False)),
            supports_phrase_candidates=bool(data.get("supports_phrase_candidates", False)),
            supports_entity_candidates=bool(data.get("supports_entity_candidates", True)),
            target_domains=tuple(
                DomainClass(value) for value in data.get("target_domains", [DomainClass.OTHER.value])
            ),
            max_weight_cap=float(data.get("max_weight_cap", 1.0)),
        )

    def to_dict(self) -> dict[str, Any]:
        return {
            "source_id": self.source_id,
            "display_name": self.display_name,
            "role": self.role.value,
            "family_primary": self.family_primary.value,
            "family_secondary": self.family_secondary.value if self.family_secondary else "",
            "status": self.status.value,
            "access_mode": self.access_mode.value,
            "requires_credentials": self.requires_credentials,
            "requires_manual_approval": self.requires_manual_approval,
            "supports_phrase_candidates": self.supports_phrase_candidates,
            "supports_entity_candidates": self.supports_entity_candidates,
            "target_domains": [domain.value for domain in self.target_domains],
            "max_weight_cap": self.max_weight_cap,
        }


def _load_json_like(path: Path) -> Any:
    return json.loads(path.read_text(encoding="utf-8"))


@lru_cache(maxsize=1)
def load_source_catalog(path: str | Path | None = None) -> tuple[SourceCatalogEntry, ...]:
    catalog_path = Path(path) if path else SOURCE_CATALOG_PATH
    raw = _load_json_like(catalog_path)
    return tuple(SourceCatalogEntry.from_dict(item) for item in raw)


@lru_cache(maxsize=1)
def load_source_catalog_map(path: str | Path | None = None) -> dict[str, SourceCatalogEntry]:
    return {entry.source_id: entry for entry in load_source_catalog(path)}


def get_source_entry(source_id: str) -> SourceCatalogEntry | None:
    return load_source_catalog_map().get(source_id)


def iter_active_catalog(statuses: tuple[SourceStatus, ...] | None = None) -> tuple[SourceCatalogEntry, ...]:
    allowed = statuses or (SourceStatus.CORE, SourceStatus.EXPERIMENTAL)
    return tuple(entry for entry in load_source_catalog() if entry.status in allowed)
