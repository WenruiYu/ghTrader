from __future__ import annotations

from datetime import date

import pandas as pd

from ghtrader.lake import (
    TICK_COLUMN_NAMES,
    list_available_dates,
    read_ticks_for_symbol,
    write_ticks_partition,
)


def test_write_read_roundtrip(synthetic_data_dir, small_synthetic_tick_df):
    symbol = "SHFE.cu2501"
    dt = date(2025, 1, 1)
    out_path = write_ticks_partition(
        small_synthetic_tick_df.copy(), data_dir=synthetic_data_dir, symbol=symbol, dt=dt, part_id="x"
    )
    assert out_path.exists()

    df = read_ticks_for_symbol(synthetic_data_dir, symbol)
    assert isinstance(df, pd.DataFrame)
    assert len(df) == len(small_synthetic_tick_df)
    assert df.columns.tolist() == TICK_COLUMN_NAMES


def test_read_with_date_range(synthetic_lake):
    data_dir, symbol, dates = synthetic_lake

    df_all = read_ticks_for_symbol(data_dir, symbol)
    assert len(df_all) > 0

    df_d0 = read_ticks_for_symbol(data_dir, symbol, start_date=dates[0], end_date=dates[0])
    df_d1 = read_ticks_for_symbol(data_dir, symbol, start_date=dates[1], end_date=dates[1])
    assert len(df_d0) > 0 and len(df_d1) > 0
    assert len(df_all) == len(df_d0) + len(df_d1)

    got_dates = list_available_dates(data_dir, symbol)
    assert got_dates == dates


def test_read_range_uses_arrow_dataset_path(synthetic_lake):
    """
    Ensure the Arrow-first range scan helper returns the same rows as the pandas reader.

    This locks in the new scalable code path used for large ranges.
    """
    import pandas as pd

    from ghtrader.lake import read_ticks_for_symbol, read_ticks_for_symbol_arrow

    data_dir, symbol, dates = synthetic_lake

    df = read_ticks_for_symbol(data_dir, symbol, start_date=dates[0], end_date=dates[-1])
    table = read_ticks_for_symbol_arrow(data_dir, symbol, start_date=dates[0], end_date=dates[-1])
    df2 = table.to_pandas()

    assert isinstance(df, pd.DataFrame)
    assert len(df) == len(df2)
    assert df.sort_values("datetime").reset_index(drop=True)["datetime"].tolist() == df2.sort_values("datetime").reset_index(drop=True)["datetime"].tolist()

