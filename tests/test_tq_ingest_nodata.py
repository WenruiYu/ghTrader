from __future__ import annotations

from datetime import date
from pathlib import Path

import pytest

from ghtrader.tq_ingest import _load_no_data_dates, _mark_no_data_dates


def test_no_data_dates_roundtrip(tmp_path: Path):
    symbol = "SHFE.cu2602"
    d1 = date(2025, 2, 17)
    d2 = date(2025, 2, 18)

    assert _load_no_data_dates(tmp_path, symbol) == set()
    _mark_no_data_dates(tmp_path, symbol, {d1, d2})
    loaded = _load_no_data_dates(tmp_path, symbol)
    assert loaded == {d1, d2}


def test_no_data_dates_rejects_v1(tmp_path: Path):
    symbol = "SHFE.cu2602"
    d1 = date(2025, 2, 17)

    with pytest.raises(Exception):
        _load_no_data_dates(tmp_path, symbol, lake_version="v1")  # type: ignore[arg-type]

    with pytest.raises(Exception):
        _mark_no_data_dates(tmp_path, symbol, {d1}, lake_version="v1")  # type: ignore[arg-type]

