from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from pathlib import Path
from typing import Any

import pandas as pd

from ghtrader.lake import write_ticks_partition
from ghtrader.main_depth import materialize_main_with_depth
from ghtrader.serving_db import ServingDBBackend, ServingDBConfig, make_serving_backend, sync_ticks_to_serving_db


@dataclass
class _Call:
    table: str
    cols: list[str]
    n_rows: int


class _FakeBackend(ServingDBBackend):
    def __init__(self):
        super().__init__(config=ServingDBConfig(backend="questdb"))
        self.ensure_calls: list[tuple[str, bool]] = []
        self.ingest_calls: list[_Call] = []

    def ensure_table(self, *, table: str, include_segment_metadata: bool) -> None:
        self.ensure_calls.append((table, bool(include_segment_metadata)))

    def ingest_df(self, *, table: str, df: pd.DataFrame) -> None:
        self.ingest_calls.append(_Call(table=table, cols=list(df.columns), n_rows=int(len(df))))


def test_serving_db_sync_incremental_records_state_and_includes_segment_metadata_v2(
    synthetic_data_dir: Path, small_synthetic_tick_df: pd.DataFrame, tmp_path: Path
) -> None:
    """
    Ensure the serving DB sync path:
    - reads main_l5 v2 partitions
    - includes underlying_contract + segment_id in the outgoing batch
    - writes an incremental state file keyed by date
    """
    data_dir = synthetic_data_dir
    d0 = date(2025, 1, 1)
    d1 = date(2025, 1, 2)

    sym_a = "SHFE.cu2501"
    sym_b = "SHFE.cu2502"
    derived = "KQ.m@SHFE.cu_sync_v2"

    # Raw ticks in lake_v2
    df0a = small_synthetic_tick_df.copy()
    df0a["symbol"] = sym_a
    write_ticks_partition(df0a, data_dir=data_dir, symbol=sym_a, dt=d0, part_id="a-d0", lake_version="v2")

    df1b = small_synthetic_tick_df.copy()
    df1b["symbol"] = sym_b
    df1b["datetime"] = df1b["datetime"].astype("int64") + 10_000_000_000
    write_ticks_partition(df1b, data_dir=data_dir, symbol=sym_b, dt=d1, part_id="b-d1", lake_version="v2")

    schedule = pd.DataFrame([{"date": d0, "main_contract": sym_a}, {"date": d1, "main_contract": sym_b}])
    schedule_path = tmp_path / "schedule_sync_v2.parquet"
    schedule.to_parquet(schedule_path, index=False)

    materialize_main_with_depth(
        derived_symbol=derived,
        schedule_path=schedule_path,
        data_dir=data_dir,
        overwrite=True,
        lake_version="v2",
    )

    backend = _FakeBackend()
    state_dir = tmp_path / "state"

    out = sync_ticks_to_serving_db(
        backend=backend,
        backend_type="questdb",
        table="ticks_test",
        data_dir=data_dir,
        symbol=derived,
        ticks_lake="main_l5",
        lake_version="v2",
        mode="incremental",
        start_date=None,
        end_date=None,
        state_dir=state_dir,
    )

    assert int(out["ingested"]) == 2
    assert backend.ensure_calls and backend.ensure_calls[0][0] == "ticks_test"
    assert backend.ensure_calls[0][1] is True  # include_segment_metadata

    # Each call should include the metadata columns for v2 main_l5.
    assert backend.ingest_calls
    for c in backend.ingest_calls:
        assert "underlying_contract" in c.cols
        assert "segment_id" in c.cols
        assert c.n_rows > 0

    assert Path(out["state_path"]).exists()

