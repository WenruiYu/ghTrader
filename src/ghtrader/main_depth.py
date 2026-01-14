"""
Materialize a derived "main-with-depth" dataset.

Given:
- A roll schedule: date -> underlying specific contract (TqSdk symbol)
- Raw L5 ticks for underlying contracts in the canonical lake

We build:
- A derived lake under data/lake_v2/main_l5/ticks/ with symbol=KQ.m@... and per-date partitions.

This materialization is optional but convenient for downstream training/eval.
"""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from datetime import date, datetime
from pathlib import Path
from typing import Any

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import structlog

from ghtrader.config import get_data_dir
from ghtrader.lake import LakeVersion, TICK_ARROW_SCHEMA, TICK_COLUMN_NAMES, ticks_date_dir, ticks_symbol_dir

log = structlog.get_logger()


def _derived_symbol_root(data_dir: Path, derived_symbol: str, *, lake_version: LakeVersion) -> Path:
    return ticks_symbol_dir(data_dir, derived_symbol, ticks_lake="main_l5", lake_version=lake_version)


def _derived_partition_dir(data_dir: Path, derived_symbol: str, dt: date, *, lake_version: LakeVersion) -> Path:
    return ticks_date_dir(data_dir, derived_symbol, dt, ticks_lake="main_l5", lake_version=lake_version)


def _stable_hash_df(df: pd.DataFrame) -> str:
    payload = df.to_csv(index=False).encode()
    return hashlib.sha256(payload).hexdigest()[:16]


def _write_tick_partition(df: pd.DataFrame, out_path: Path) -> None:
    if df.empty:
        return

    missing = set(TICK_COLUMN_NAMES) - set(df.columns)
    if missing:
        raise ValueError(f"Missing tick columns for derived write: {missing}")

    df = df[TICK_COLUMN_NAMES].copy()
    df["symbol"] = df["symbol"].astype(str)
    df["datetime"] = df["datetime"].astype("int64")
    for col in TICK_COLUMN_NAMES[2:]:
        df[col] = df[col].astype("float64")

    table = pa.Table.from_pandas(df, schema=TICK_ARROW_SCHEMA, preserve_index=False)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    pq.write_table(table, out_path, compression="zstd")


def _raw_partition_dir(data_dir: Path, symbol: str, dt: date, *, lake_version: LakeVersion) -> Path:
    return ticks_date_dir(data_dir, symbol, dt, ticks_lake="raw", lake_version=lake_version)


def _iter_raw_partition_files(data_dir: Path, symbol: str, dt: date, *, lake_version: LakeVersion) -> list[Path]:
    d = _raw_partition_dir(data_dir, symbol, dt, lake_version=lake_version)
    if not d.exists():
        return []
    return sorted(d.glob("*.parquet"))


def _replace_symbol_in_table(table: pa.Table, derived_symbol: str) -> pa.Table:
    if table.num_rows == 0:
        return table
    sym_col_idx = table.schema.get_field_index("symbol")
    if sym_col_idx < 0:
        raise ValueError("Missing 'symbol' column in tick table")
    sym_arr = pa.array([derived_symbol] * table.num_rows, type=pa.string())
    return table.set_column(sym_col_idx, "symbol", sym_arr)


@dataclass(frozen=True)
class MainDepthMaterializationResult:
    derived_root: Path
    manifest_path: Path
    schedule_hash: str
    rows_total: int


def materialize_main_with_depth(
    *,
    derived_symbol: str,
    schedule_path: Path,
    data_dir: Path | None = None,
    overwrite: bool = False,
    lake_version: LakeVersion = "v2",
) -> MainDepthMaterializationResult:
    """
    Materialize a derived main-with-depth lake from a schedule.

    Args:
        derived_symbol: continuous-style symbol name (e.g. 'KQ.m@SHFE.cu')
        schedule_path: path to schedule.parquet (must include columns: date, main_contract)
        data_dir: ghTrader data dir (defaults to config)
        overwrite: if True, delete existing derived symbol directory before writing
    """
    if data_dir is None:
        data_dir = get_data_dir()

    if not schedule_path.exists():
        raise FileNotFoundError(str(schedule_path))

    schedule = pd.read_parquet(schedule_path)
    if schedule.empty:
        raise ValueError("Schedule is empty")

    if "date" not in schedule.columns or "main_contract" not in schedule.columns:
        raise ValueError(f"Schedule missing required columns: {list(schedule.columns)}")

    schedule = schedule.copy()
    schedule["date"] = pd.to_datetime(schedule["date"], errors="coerce").dt.date
    schedule = schedule.dropna(subset=["date", "main_contract"])
    schedule = schedule.sort_values("date").reset_index(drop=True)

    # Ensure segment_id exists (older schedules may not include it).
    if "segment_id" not in schedule.columns:
        seg_ids: list[int] = []
        seg = 0
        prev: str | None = None
        for mc in schedule["main_contract"].astype(str).tolist():
            if prev is None:
                seg = 0
            elif mc != prev:
                seg += 1
            seg_ids.append(int(seg))
            prev = mc
        schedule["segment_id"] = seg_ids

    schedule_hash = _stable_hash_df(schedule[["date", "main_contract", "segment_id"]])

    root = _derived_symbol_root(data_dir, derived_symbol, lake_version=lake_version)
    if overwrite and root.exists():
        # Dangerous but explicit; caller must opt in.
        import shutil

        shutil.rmtree(root)

    root.mkdir(parents=True, exist_ok=True)

    # Persist schedule provenance inside the derived root for reproducibility.
    schedule_copy_path = root / "schedule.parquet"
    schedule.to_parquet(schedule_copy_path, index=False)

    row_counts: dict[str, int] = {}
    used_underlyings: set[str] = set()
    total_rows = 0

    for _, row in schedule.iterrows():
        dt: date = row["date"]
        underlying: str = str(row["main_contract"])
        seg_id: int = int(row.get("segment_id", 0) or 0)
        used_underlyings.add(underlying)

        out_dir = _derived_partition_dir(data_dir, derived_symbol, dt, lake_version=lake_version)
        existing_files = list(out_dir.glob("*.parquet")) if out_dir.exists() else []
        if existing_files:
            continue

        raw_files = _iter_raw_partition_files(data_dir, underlying, dt, lake_version=lake_version)
        if not raw_files:
            log.warning("main_depth.missing_ticks", underlying=underlying, date=dt.isoformat())
            continue

        out_dir.mkdir(parents=True, exist_ok=True)
        rows_day = 0
        for rf in raw_files:
            out_path = out_dir / rf.name
            if out_path.exists():
                # Idempotent: keep existing derived partition.
                # Count will be filled from newly written files only.
                continue

            table = pq.read_table(rf, schema=TICK_ARROW_SCHEMA)
            table = _replace_symbol_in_table(table, derived_symbol)
            # v2-only: Segment metadata (used to prevent cross-roll leakage downstream).
            u = pa.array([underlying] * table.num_rows, type=pa.string())
            seg = pa.array([int(seg_id)] * table.num_rows, type=pa.int64())
            table = table.append_column("underlying_contract", u)
            table = table.append_column("segment_id", seg)
            pq.write_table(table, out_path, compression="zstd")
            try:
                from ghtrader.integrity import write_sha256_sidecar

                write_sha256_sidecar(out_path)
            except Exception as e:
                log.warning("main_depth.checksum_failed", path=str(out_path), error=str(e))
            rows_day += int(table.num_rows)

        if rows_day == 0:
            # All partitions already existed; approximate row count from first file.
            try:
                table0 = pq.read_table(raw_files[0], schema=TICK_ARROW_SCHEMA)
                rows_day = int(table0.num_rows)
            except Exception:
                rows_day = 0

        row_counts[str(dt)] = int(rows_day)
        total_rows += int(rows_day)

        # Per-date manifest for derived ticks
        try:
            from ghtrader.integrity import build_partition_manifest, write_json_atomic

            m = build_partition_manifest(
                partition_dir=out_dir,
                dataset="ticks_main_l5",
                symbol=derived_symbol,
                dt=dt.isoformat(),
            )
            write_json_atomic(out_dir / "_manifest.json", m)
        except Exception as e:
            log.warning("main_depth.manifest_failed", date=dt.isoformat(), error=str(e))

    manifest: dict[str, Any] = {
        "created_at": datetime.now().isoformat(),
        "derived_symbol": derived_symbol,
        "derived_root": str(root),
        "schedule_path": str(schedule_path),
        "schedule_copy_path": str(schedule_copy_path),
        "schedule_hash": schedule_hash,
        "rows_total": int(total_rows),
        "row_counts": row_counts,
        "underlyings_used": sorted(used_underlyings),
    }
    manifest_path = root / "manifest.json"
    manifest_path.parent.mkdir(parents=True, exist_ok=True)
    with open(manifest_path, "w") as f:
        json.dump(manifest, f, indent=2, default=str)

    log.info(
        "main_depth.materialized",
        derived_symbol=derived_symbol,
        schedule_hash=schedule_hash,
        rows_total=total_rows,
        manifest_path=str(manifest_path),
    )

    return MainDepthMaterializationResult(
        derived_root=root,
        manifest_path=manifest_path,
        schedule_hash=schedule_hash,
        rows_total=total_rows,
    )

