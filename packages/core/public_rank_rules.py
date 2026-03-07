"""Config loader for public ranking guardrails."""

from __future__ import annotations

import json
from functools import lru_cache
from pathlib import Path
from typing import Any, cast

CONFIG_PATH = Path(__file__).resolve().parents[2] / "configs" / "public_rank_rules.yaml"


@lru_cache(maxsize=1)
def load_public_rank_rules() -> dict[str, Any]:
    return cast(dict[str, Any], json.loads(CONFIG_PATH.read_text(encoding="utf-8")))
