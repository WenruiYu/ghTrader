from __future__ import annotations

from datetime import date

import pandas as pd


def test_questdb_df_for_ticks_shape_and_row_hash_changes(small_synthetic_tick_df: pd.DataFrame) -> None:
    from ghtrader.tq.ingest import _questdb_df_for_main_l5

    df = small_synthetic_tick_df.head(5).copy()
    td = date(2025, 1, 1)

    out1 = _questdb_df_for_main_l5(
        df=df,
        derived_symbol="KQ.m@SHFE.cu",
        trading_day=td,
        underlying_contract="SHFE.cu2501",
        segment_id=0,
        schedule_hash="test",
        dataset_version="v2",
    )
    assert len(out1) == len(df)

    # Required columns for ILP ingestion
    for c in [
        "symbol",
        "datetime_ns",
        "ts",
        "trading_day",
        "row_hash",
        "dataset_version",
        "ticks_kind",
        "last_price",
        "underlying_contract",
        "segment_id",
        "schedule_hash",
    ]:
        assert c in out1.columns

    assert out1["trading_day"].nunique() == 1
    assert out1["trading_day"].iloc[0] == td.isoformat()
    assert out1["dataset_version"].nunique() == 1 and out1["dataset_version"].iloc[0] == "v2"
    assert out1["ticks_kind"].nunique() == 1 and out1["ticks_kind"].iloc[0] == "main_l5"

    assert out1["datetime_ns"].astype("int64").tolist() == df["datetime"].astype("int64").tolist()

    # Deterministic hashing for stable inputs
    out2 = _questdb_df_for_main_l5(
        df=df,
        derived_symbol="KQ.m@SHFE.cu",
        trading_day=td,
        underlying_contract="SHFE.cu2501",
        segment_id=0,
        schedule_hash="test",
        dataset_version="v2",
    )
    assert out1["row_hash"].tolist() == out2["row_hash"].tolist()

    # Changing any canonical numeric column should change the row_hash for that row
    df2 = df.copy()
    df2.loc[df2.index[0], "last_price"] = float(df2.loc[df2.index[0], "last_price"]) + 1.0
    out3 = _questdb_df_for_main_l5(
        df=df2,
        derived_symbol="KQ.m@SHFE.cu",
        trading_day=td,
        underlying_contract="SHFE.cu2501",
        segment_id=0,
        schedule_hash="test",
        dataset_version="v2",
    )
    assert int(out3["row_hash"].iloc[0]) != int(out1["row_hash"].iloc[0])

