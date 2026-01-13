from __future__ import annotations

from datetime import date
from pathlib import Path

import pandas as pd
import pytest

from ghtrader.symbol_resolver import resolve_trading_symbol


def _write_schedule(path: Path, rows: list[dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    pd.DataFrame(rows).to_parquet(path, index=False)


def test_resolve_trading_symbol_falls_back_to_lake_v2_schedule_copy(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("GHTRADER_LAKE_VERSION", "v2")

    data_dir = tmp_path / "data"
    alias = "KQ.m@SHFE.cu"
    p_v2 = data_dir / "lake_v2" / "main_l5" / "ticks" / f"symbol={alias}" / "schedule.parquet"

    _write_schedule(
        p_v2,
        rows=[
            {"date": date(2025, 1, 1), "main_contract": "SHFE.cu2501"},
            {"date": date(2025, 1, 2), "main_contract": "SHFE.cu2502"},
        ],
    )

    resolved = resolve_trading_symbol(symbol=alias, data_dir=data_dir, trading_day=date(2025, 1, 2))
    assert resolved == "SHFE.cu2502"


def test_resolve_trading_symbol_prefers_selected_lake_version_when_both_exist(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    data_dir = tmp_path / "data"
    alias = "KQ.m@SHFE.cu"
    p_v1 = data_dir / "lake" / "main_l5" / "ticks" / f"symbol={alias}" / "schedule.parquet"
    p_v2 = data_dir / "lake_v2" / "main_l5" / "ticks" / f"symbol={alias}" / "schedule.parquet"

    _write_schedule(p_v1, rows=[{"date": date(2025, 1, 1), "main_contract": "SHFE.cu2501"}])
    _write_schedule(p_v2, rows=[{"date": date(2025, 1, 1), "main_contract": "SHFE.cu9999"}])

    monkeypatch.setenv("GHTRADER_LAKE_VERSION", "v2")
    assert resolve_trading_symbol(symbol=alias, data_dir=data_dir, trading_day=date(2025, 1, 1)) == "SHFE.cu9999"

    monkeypatch.setenv("GHTRADER_LAKE_VERSION", "v1")
    assert resolve_trading_symbol(symbol=alias, data_dir=data_dir, trading_day=date(2025, 1, 1)) == "SHFE.cu2501"

