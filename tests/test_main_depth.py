from __future__ import annotations

from pathlib import Path

import pandas as pd

from ghtrader.main_depth import materialize_main_with_depth


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

