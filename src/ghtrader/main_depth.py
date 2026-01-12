"""
Materialize a derived "main-with-depth" dataset.

Given:
- A roll schedule: date -> underlying specific contract (TqSdk symbol)
- Raw L5 ticks for underlying contracts in the canonical lake

We build:
- A derived lake under data/lake/main_l5/ticks/ with symbol=KQ.m@... and per-date partitions.

This materialization is optional but convenient for downstream training/eval.
"""

from __future__ import annotations

import hashlib
import json
import uuid
from dataclasses import dataclass
from datetime import date, datetime
from pathlib import Path
from typing import Any

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import structlog

from ghtrader.config import get_data_dir
from ghtrader.lake import TICK_ARROW_SCHEMA, TICK_COLUMN_NAMES, read_ticks_for_symbol

log = structlog.get_logger()


def _main_l5_ticks_root(data_dir: Path) -> Path:
    return data_dir / "lake" / "main_l5" / "ticks"


def _derived_symbol_root(data_dir: Path, derived_symbol: str) -> Path:
    return _main_l5_ticks_root(data_dir) / f"symbol={derived_symbol}"


def _derived_partition_dir(data_dir: Path, derived_symbol: str, dt: date) -> Path:
    return _derived_symbol_root(data_dir, derived_symbol) / f"date={dt.isoformat()}"


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
    schedule_hash = _stable_hash_df(schedule[["date", "main_contract"]])

    root = _derived_symbol_root(data_dir, derived_symbol)
    if overwrite and root.exists():
        # Dangerous but explicit; caller must opt in.
        import shutil

        shutil.rmtree(root)

    row_counts: dict[str, int] = {}
    used_underlyings: set[str] = set()
    total_rows = 0

    for _, row in schedule.iterrows():
        dt: date = row["date"]
        underlying: str = str(row["main_contract"])
        used_underlyings.add(underlying)

        out_dir = _derived_partition_dir(data_dir, derived_symbol, dt)
        existing_files = list(out_dir.glob("*.parquet")) if out_dir.exists() else []
        if existing_files:
            continue

        df_day = read_ticks_for_symbol(data_dir, underlying, start_date=dt, end_date=dt)
        if df_day.empty:
            log.warning("main_depth.missing_ticks", underlying=underlying, date=dt.isoformat())
            continue

        df_day = df_day.copy()
        df_day["symbol"] = derived_symbol

        out_path = out_dir / f"part-{uuid.uuid4().hex[:8]}.parquet"
        _write_tick_partition(df_day, out_path)

        row_counts[str(dt)] = int(len(df_day))
        total_rows += int(len(df_day))

    manifest: dict[str, Any] = {
        "created_at": datetime.now().isoformat(),
        "derived_symbol": derived_symbol,
        "derived_root": str(root),
        "schedule_path": str(schedule_path),
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

