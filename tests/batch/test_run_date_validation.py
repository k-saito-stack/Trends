from __future__ import annotations

import pytest

from batch.run import get_target_date


def test_get_target_date_accepts_iso_date() -> None:
    assert get_target_date("2026-03-09") == "2026-03-09"


def test_get_target_date_rejects_invalid_format() -> None:
    with pytest.raises(ValueError):
        get_target_date("2026/03/09")
