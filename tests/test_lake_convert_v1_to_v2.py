from __future__ import annotations

from datetime import date, datetime, timezone
from pathlib import Path

import pandas as pd


def _ns(dt: datetime) -> int:
    # Treat naive datetimes as UTC so the test is stable across host timezones.
    return int(dt.replace(tzinfo=timezone.utc).timestamp() * 1_000_000_000)

def _tick_df(symbol: str, times: list[int]) -> pd.DataFrame:
    from ghtrader.lake import TICK_COLUMN_NAMES

    n = len(times)
    out: dict[str, object] = {}
    for c in TICK_COLUMN_NAMES:
        if c == "symbol":
            out[c] = [symbol] * n
        elif c == "datetime":
            out[c] = times
        else:
            out[c] = [1.0] * n
    return pd.DataFrame(out)


def test_convert_lake_v1_to_v2_splits_and_merges_into_trading_days(tmp_path: Path) -> None:
    from ghtrader.lake import read_ticks_for_symbol_date, write_ticks_partition
    from ghtrader.lake_convert import convert_lake_v1_to_v2

    data_dir = tmp_path / "data"
    symbol = "SHFE.cu2501"

    # v1 date=2025-01-01 contains a night-session tick (>=18:00) that should map to v2 date=2025-01-02.
    t1 = _ns(datetime(2025, 1, 1, 19, 0, 0))
    df1 = _tick_df(symbol, [t1])
    write_ticks_partition(df1, data_dir=data_dir, symbol=symbol, dt=date(2025, 1, 1), part_id="v1d1", lake_version="v1")

    # v1 date=2025-01-02 contains one day-session tick (stays 2025-01-02) and one night-session tick (moves to 2025-01-03).
    t2 = _ns(datetime(2025, 1, 2, 9, 0, 0))
    t3 = _ns(datetime(2025, 1, 2, 19, 0, 0))
    df2 = _tick_df(symbol, [t2, t3])
    write_ticks_partition(df2, data_dir=data_dir, symbol=symbol, dt=date(2025, 1, 2), part_id="v1d2", lake_version="v1")

    stats1 = convert_lake_v1_to_v2(data_dir=data_dir, symbols=[symbol])
    assert stats1.written_files > 0

    v2_d2 = read_ticks_for_symbol_date(data_dir, symbol, date(2025, 1, 2), lake_version="v2")
    assert set(v2_d2["datetime"].tolist()) == {t1, t2}

    v2_d3 = read_ticks_for_symbol_date(data_dir, symbol, date(2025, 1, 3), lake_version="v2")
    assert v2_d3["datetime"].tolist() == [t3]

    # Idempotent: rerun should not write additional files/rows.
    stats2 = convert_lake_v1_to_v2(data_dir=data_dir, symbols=[symbol])
    assert stats2.written_files == 0

    v2_d2b = read_ticks_for_symbol_date(data_dir, symbol, date(2025, 1, 2), lake_version="v2")
    v2_d3b = read_ticks_for_symbol_date(data_dir, symbol, date(2025, 1, 3), lake_version="v2")
    assert set(v2_d2b["datetime"].tolist()) == {t1, t2}
    assert v2_d3b["datetime"].tolist() == [t3]

