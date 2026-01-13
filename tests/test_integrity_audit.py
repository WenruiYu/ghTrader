from __future__ import annotations

from datetime import date
from pathlib import Path

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

from ghtrader.audit import run_audit
from ghtrader.features import FactorEngine
from ghtrader.integrity import read_sha256_sidecar, write_sha256_sidecar
from ghtrader.labels import build_labels_for_symbol
from ghtrader.lake import TICK_ARROW_SCHEMA
from ghtrader.main_depth import materialize_main_with_depth


def test_ticks_write_has_sha_and_manifest(synthetic_lake):
    data_dir, symbol, dates = synthetic_lake
    d0 = dates[0]
    part_dir = data_dir / "lake" / "ticks" / f"symbol={symbol}" / f"date={d0.isoformat()}"
    parquet_files = list(part_dir.glob("*.parquet"))
    assert parquet_files, "expected parquet partition file"
    assert (part_dir / "_manifest.json").exists()
    assert read_sha256_sidecar(parquet_files[0]) is not None


def test_audit_passes_on_clean_synthetic_lake(synthetic_lake, tmp_path: Path):
    data_dir, symbol, dates = synthetic_lake

    # Build features + labels so audit can cover all scopes
    build_labels_for_symbol(symbol=symbol, data_dir=data_dir, horizons=[10], ticks_lake="raw")
    FactorEngine().build_features_for_symbol(symbol=symbol, data_dir=data_dir, ticks_lake="raw")

    report_path, report = run_audit(data_dir=data_dir, runs_dir=tmp_path / "runs", scopes=["all"])
    assert report_path.exists()
    assert report["summary"]["errors"] == 0


def test_audit_fails_on_tampered_checksum(synthetic_lake, tmp_path: Path):
    data_dir, symbol, dates = synthetic_lake
    d0 = dates[0]
    part_dir = data_dir / "lake" / "ticks" / f"symbol={symbol}" / f"date={d0.isoformat()}"
    p = sorted(part_dir.glob("*.parquet"))[0]
    sidecar = p.with_suffix(p.suffix + ".sha256")
    sidecar.write_text("deadbeef\n", encoding="utf-8")

    _, report = run_audit(data_dir=data_dir, runs_dir=tmp_path / "runs", scopes=["ticks"])
    assert report["summary"]["errors"] > 0
    codes = {f["code"] for f in report["findings"]}
    assert "checksum_mismatch" in codes or "missing_checksum" in codes


def test_audit_detects_derived_vs_raw_mismatch(synthetic_lake_two_symbols, tmp_path: Path):
    data_dir, (sym_a, sym_b), dates = synthetic_lake_two_symbols
    d0, d1 = dates

    derived = "KQ.m@SHFE.cu_audit"
    schedule = pd.DataFrame([{"date": d0, "main_contract": sym_a}, {"date": d1, "main_contract": sym_b}])
    schedule_path = tmp_path / "schedule.parquet"
    schedule.to_parquet(schedule_path, index=False)
    materialize_main_with_depth(derived_symbol=derived, schedule_path=schedule_path, data_dir=data_dir, overwrite=True)

    # Tamper one derived parquet value but keep checksum consistent
    der_dir = data_dir / "lake" / "main_l5" / "ticks" / f"symbol={derived}" / f"date={d0.isoformat()}"
    p_der = sorted(der_dir.glob("*.parquet"))[0]
    t = pq.read_table(p_der, schema=TICK_ARROW_SCHEMA)
    df = t.to_pandas()
    df.loc[df.index[0], "last_price"] = float(df.loc[df.index[0], "last_price"]) + 1.0
    t2 = pa.Table.from_pandas(df[TICK_ARROW_SCHEMA.names], schema=TICK_ARROW_SCHEMA, preserve_index=False)
    pq.write_table(t2, p_der, compression="zstd")
    write_sha256_sidecar(p_der)

    _, report = run_audit(data_dir=data_dir, runs_dir=tmp_path / "runs", scopes=["main_l5"])
    assert report["summary"]["errors"] > 0
    codes = {f["code"] for f in report["findings"]}
    assert "derived_mismatch" in codes


def test_audit_passes_on_v2_main_l5_with_metadata(synthetic_data_dir: Path, small_synthetic_tick_df: pd.DataFrame, tmp_path: Path):
    from datetime import date

    from ghtrader.lake import write_ticks_partition

    data_dir = synthetic_data_dir
    d0 = date(2025, 1, 1)
    d1 = date(2025, 1, 2)

    sym_a = "SHFE.cu2501"
    sym_b = "SHFE.cu2502"
    derived = "KQ.m@SHFE.cu_audit_v2"

    df0a = small_synthetic_tick_df.copy()
    df0a["symbol"] = sym_a
    write_ticks_partition(df0a, data_dir=data_dir, symbol=sym_a, dt=d0, part_id="a-d0", lake_version="v2")

    df1b = small_synthetic_tick_df.copy()
    df1b["symbol"] = sym_b
    df1b["datetime"] = df1b["datetime"].astype("int64") + 10_000_000_000
    write_ticks_partition(df1b, data_dir=data_dir, symbol=sym_b, dt=d1, part_id="b-d1", lake_version="v2")

    schedule = pd.DataFrame([{"date": d0, "main_contract": sym_a}, {"date": d1, "main_contract": sym_b}])
    schedule_path = tmp_path / "schedule_audit_v2.parquet"
    schedule.to_parquet(schedule_path, index=False)
    materialize_main_with_depth(
        derived_symbol=derived,
        schedule_path=schedule_path,
        data_dir=data_dir,
        overwrite=True,
        lake_version="v2",
    )

    _, report = run_audit(data_dir=data_dir, runs_dir=tmp_path / "runs", scopes=["main_l5"], lake_version="v2")
    assert report["summary"]["errors"] == 0

