"""Config loader for relation propagation weights."""

from __future__ import annotations

import json
from functools import lru_cache
from pathlib import Path

CONFIG_PATH = Path(__file__).resolve().parents[2] / "configs" / "relation_weights.yaml"


@lru_cache(maxsize=1)
def load_relation_weights() -> dict[str, float]:
    raw = json.loads(CONFIG_PATH.read_text(encoding="utf-8"))
    return {str(key): float(value) for key, value in raw.items()}
