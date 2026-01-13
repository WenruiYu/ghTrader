from __future__ import annotations

from pathlib import Path

import pandas as pd
import pyarrow.parquet as pq

from ghtrader.main_depth import materialize_main_with_depth
from ghtrader.features import FactorEngine, read_features_for_symbol
from ghtrader.labels import build_labels_for_symbol, read_labels_for_symbol
from ghtrader.lake import write_ticks_partition


def test_materialize_main_with_depth_writes_partitions(synthetic_lake, tmp_path: Path):
    data_dir, symbol, dates = synthetic_lake
    derived_symbol = "KQ.m@SHFE.cu"

    # Minimal schedule: use the same underlying for both dates.
    schedule = pd.DataFrame(
        [
            {"date": dates[0], "main_contract": symbol},
            {"date": dates[1], "main_contract": symbol},
        ]
    )
    schedule_path = tmp_path / "schedule.parquet"
    schedule.to_parquet(schedule_path, index=False)

    res = materialize_main_with_depth(
        derived_symbol=derived_symbol,
        schedule_path=schedule_path,
        data_dir=data_dir,
        overwrite=True,
    )
    assert res.rows_total > 0

    # Verify output layout exists
    out0 = data_dir / "lake" / "main_l5" / "ticks" / f"symbol={derived_symbol}" / f"date={dates[0].isoformat()}"
    out1 = data_dir / "lake" / "main_l5" / "ticks" / f"symbol={derived_symbol}" / f"date={dates[1].isoformat()}"
    assert any(out0.glob("*.parquet"))
    assert any(out1.glob("*.parquet"))


def test_no_roll_leakage_in_labels_and_features(synthetic_lake_two_symbols, tmp_path: Path):
    data_dir, (sym_a, sym_b), dates = synthetic_lake_two_symbols
    d0, d1 = dates

    # Derived symbol with a roll (A -> B)
    derived_roll = "KQ.m@SHFE.cu_roll"
    schedule_roll = pd.DataFrame([{"date": d0, "main_contract": sym_a}, {"date": d1, "main_contract": sym_b}])
    p_roll = tmp_path / "schedule_roll.parquet"
    schedule_roll.to_parquet(p_roll, index=False)
    materialize_main_with_depth(derived_symbol=derived_roll, schedule_path=p_roll, data_dir=data_dir, overwrite=True)

    # Derived symbol without a roll (A -> A)
    derived_noroll = "KQ.m@SHFE.cu_noroll"
    schedule_noroll = pd.DataFrame([{"date": d0, "main_contract": sym_a}, {"date": d1, "main_contract": sym_a}])
    p_noroll = tmp_path / "schedule_noroll.parquet"
    schedule_noroll.to_parquet(p_noroll, index=False)
    materialize_main_with_depth(derived_symbol=derived_noroll, schedule_path=p_noroll, data_dir=data_dir, overwrite=True)

    # Labels: horizon=10 should leak across day boundary only when underlying stays the same.
    build_labels_for_symbol(symbol=derived_roll, data_dir=data_dir, horizons=[10], ticks_lake="main_l5")
    build_labels_for_symbol(symbol=derived_noroll, data_dir=data_dir, horizons=[10], ticks_lake="main_l5")

    labels_roll = read_labels_for_symbol(data_dir, derived_roll)
    labels_noroll = read_labels_for_symbol(data_dir, derived_noroll)
    assert "underlying_contract" in labels_roll.columns
    assert "segment_id" in labels_roll.columns

    # Day0 has 50 ticks in synthetic fixture; index 45 is within the last 10 ticks of the day.
    # Labels are written in datetime order; first 50 rows correspond to day0 in our synthetic fixture.
    day0_roll = labels_roll.iloc[:50]
    day0_noroll = labels_noroll.iloc[:50]

    assert len(day0_roll) > 45
    assert len(day0_noroll) > 45

    assert pd.isna(day0_roll["label_10"].iloc[45])
    assert not pd.isna(day0_noroll["label_10"].iloc[45])

    # Features: return_10 should carry continuity only when underlying stays the same.
    engine = FactorEngine(enabled_factors=["return_10"])
    engine.build_features_for_symbol(symbol=derived_roll, data_dir=data_dir, ticks_lake="main_l5")
    engine.build_features_for_symbol(symbol=derived_noroll, data_dir=data_dir, ticks_lake="main_l5")

    feat_roll = read_features_for_symbol(data_dir, derived_roll)
    feat_noroll = read_features_for_symbol(data_dir, derived_noroll)
    assert "underlying_contract" in feat_roll.columns
    assert "segment_id" in feat_roll.columns

    # First row of day1 features for roll should be NaN (no tail carried).
    # Features are written in datetime order; last 50 rows correspond to day1 in our synthetic fixture.
    day1_roll = feat_roll.iloc[-50:]
    day1_noroll = feat_noroll.iloc[-50:]

    assert len(day1_roll) > 0
    assert len(day1_noroll) > 0

    assert pd.isna(day1_roll["return_10"].iloc[0])
    assert not pd.isna(day1_noroll["return_10"].iloc[0])


def test_materialize_main_with_depth_v2_writes_segment_metadata(synthetic_data_dir: Path, small_synthetic_tick_df: pd.DataFrame, tmp_path: Path):
    from datetime import date

    data_dir = synthetic_data_dir
    d0 = date(2025, 1, 1)
    d1 = date(2025, 1, 2)

    sym_a = "SHFE.cu2501"
    sym_b = "SHFE.cu2502"
    derived = "KQ.m@SHFE.cu_roll_v2"

    # Raw ticks in lake_v2
    df0a = small_synthetic_tick_df.copy()
    df0a["symbol"] = sym_a
    write_ticks_partition(df0a, data_dir=data_dir, symbol=sym_a, dt=d0, part_id="a-d0", lake_version="v2")

    df1b = small_synthetic_tick_df.copy()
    df1b["symbol"] = sym_b
    df1b["datetime"] = df1b["datetime"].astype("int64") + 10_000_000_000  # shift 10s
    write_ticks_partition(df1b, data_dir=data_dir, symbol=sym_b, dt=d1, part_id="b-d1", lake_version="v2")

    schedule = pd.DataFrame([{"date": d0, "main_contract": sym_a}, {"date": d1, "main_contract": sym_b}])
    schedule_path = tmp_path / "schedule_v2.parquet"
    schedule.to_parquet(schedule_path, index=False)

    materialize_main_with_depth(
        derived_symbol=derived,
        schedule_path=schedule_path,
        data_dir=data_dir,
        overwrite=True,
        lake_version="v2",
    )

    out0 = data_dir / "lake_v2" / "main_l5" / "ticks" / f"symbol={derived}" / f"date={d0.isoformat()}"
    out1 = data_dir / "lake_v2" / "main_l5" / "ticks" / f"symbol={derived}" / f"date={d1.isoformat()}"
    p0 = sorted(out0.glob("*.parquet"))[0]
    p1 = sorted(out1.glob("*.parquet"))[0]

    t0 = pq.ParquetFile(p0).read(columns=["underlying_contract", "segment_id"])
    assert set(t0.column("underlying_contract").to_pylist()) == {sym_a}
    assert set(t0.column("segment_id").to_pylist()) == {0}

    t1 = pq.ParquetFile(p1).read(columns=["underlying_contract", "segment_id"])
    assert set(t1.column("underlying_contract").to_pylist()) == {sym_b}
    assert set(t1.column("segment_id").to_pylist()) == {1}

