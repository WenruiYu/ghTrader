"""
Event-time multi-horizon labels and walk-forward split builder.

Labels are based on mid-price movement over N ticks:
- mid = (bid_price1 + ask_price1) / 2
- label = {DOWN, FLAT, UP} based on mid[t+N] - mid[t] relative to k * price_tick
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from pathlib import Path
from typing import Literal
import shutil
import uuid

import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import structlog

from ghtrader.lake import LakeVersion, TicksLake, list_available_dates_in_lake, read_ticks_for_symbol, ticks_symbol_dir

log = structlog.get_logger()

# Label classes
LabelClass = Literal["DOWN", "FLAT", "UP"]
LABEL_MAP = {"DOWN": 0, "FLAT": 1, "UP": 2}
LABEL_MAP_INV = {v: k for k, v in LABEL_MAP.items()}


# ---------------------------------------------------------------------------
# Price tick lookup (simplified; in production, fetch from TqSdk quote)
# ---------------------------------------------------------------------------

# Approximate price ticks for SHFE metals (adjust based on contract specs)
PRICE_TICKS = {
    "cu": 10.0,   # Copper: 10 yuan/ton
    "au": 0.02,   # Gold: 0.02 yuan/gram
    "ag": 1.0,    # Silver: 1 yuan/kg
}


def _get_price_tick(symbol: str) -> float:
    """Get price tick for a symbol (simplified lookup)."""
    # Extract product code from symbol like "SHFE.cu2502"
    parts = symbol.split(".")
    if len(parts) >= 2:
        code = parts[1]
        for product, tick in PRICE_TICKS.items():
            if code.startswith(product):
                return tick
    # Default fallback
    return 1.0


# ---------------------------------------------------------------------------
# Label computation
# ---------------------------------------------------------------------------

def compute_mid_price(df: pd.DataFrame) -> pd.Series:
    """Compute mid price from bid/ask."""
    return (df["bid_price1"] + df["ask_price1"]) / 2


def compute_labels_for_horizon(
    mid: pd.Series,
    horizon_n: int,
    threshold_k: int,
    price_tick: float,
) -> pd.Series:
    """
    Compute 3-class labels for a single horizon.
    
    Args:
        mid: Mid price series
        horizon_n: Number of ticks to look ahead
        threshold_k: Threshold in price ticks
        price_tick: Price tick size for the instrument
    
    Returns:
        Series of labels (0=DOWN, 1=FLAT, 2=UP), NaN for last horizon_n rows
    """
    # Future mid price
    future_mid = mid.shift(-horizon_n)
    
    # Price change
    delta = future_mid - mid
    
    # Threshold in price units
    threshold = threshold_k * price_tick
    
    # Classify
    labels = pd.Series(index=mid.index, dtype=float)
    labels[:] = LABEL_MAP["FLAT"]  # Default
    labels[delta >= threshold] = LABEL_MAP["UP"]
    labels[delta <= -threshold] = LABEL_MAP["DOWN"]
    
    # Last horizon_n rows have no label (can't see future)
    labels.iloc[-horizon_n:] = np.nan
    
    return labels


def compute_multi_horizon_labels(
    df: pd.DataFrame,
    horizons: list[int],
    threshold_k: int,
    price_tick: float,
) -> pd.DataFrame:
    """
    Compute labels for multiple horizons.
    
    Args:
        df: Tick DataFrame with bid_price1, ask_price1
        horizons: List of horizon values (N ticks)
        threshold_k: Threshold in price ticks
        price_tick: Price tick size
    
    Returns:
        DataFrame with columns: label_N for each horizon
    """
    mid = compute_mid_price(df)
    
    result = pd.DataFrame(index=df.index)
    result["mid"] = mid
    
    for n in horizons:
        col_name = f"label_{n}"
        result[col_name] = compute_labels_for_horizon(mid, n, threshold_k, price_tick)
    
    return result


# ---------------------------------------------------------------------------
# Build labels for symbol
# ---------------------------------------------------------------------------

def labels_dir(data_dir: Path) -> Path:
    """Return the labels directory."""
    return data_dir / "labels"


def build_labels_for_symbol(
    symbol: str,
    data_dir: Path,
    horizons: list[int],
    threshold_k: int = 1,
    ticks_lake: TicksLake = "raw",
    overwrite: bool = False,
    *,
    lake_version: LakeVersion = "v1",
) -> Path:
    """
    Build multi-horizon labels for a symbol from Parquet lake.
    
    Args:
        symbol: Instrument code
        data_dir: Data directory root
        horizons: List of horizon values (N ticks)
        threshold_k: Threshold in price ticks
    
    Returns:
        Path to output Parquet file
    """
    log.info("labels.build_start", symbol=symbol, horizons=horizons, threshold_k=threshold_k)

    dates = list_available_dates_in_lake(data_dir, symbol, ticks_lake=ticks_lake, lake_version=lake_version)
    if not dates:
        raise ValueError(f"No tick data found for {symbol}")

    # Output root
    out_root = labels_dir(data_dir) / f"symbol={symbol}"
    if overwrite and out_root.exists():
        shutil.rmtree(out_root)
    out_root.mkdir(parents=True, exist_ok=True)

    # Get price tick
    price_tick = _get_price_tick(symbol)
    log.debug("labels.price_tick", symbol=symbol, price_tick=price_tick)

    max_h = max(horizons) if horizons else 0

    # For derived ticks, avoid cross-day lookahead on roll boundaries.
    underlying_by_date: dict[date, str] = {}
    segment_id_by_date: dict[date, int] = {}
    if ticks_lake == "main_l5":
        try:
            schedule_path = ticks_symbol_dir(
                data_dir,
                symbol,
                ticks_lake="main_l5",
                lake_version=lake_version,
            ) / "schedule.parquet"
            if schedule_path.exists():
                sched = pd.read_parquet(schedule_path)
                if "date" in sched.columns and "main_contract" in sched.columns:
                    sched = sched.copy()
                    sched["date"] = pd.to_datetime(sched["date"], errors="coerce").dt.date
                    sched = sched.dropna(subset=["date", "main_contract"])
                    # Ensure segment_id exists for older schedule copies.
                    if "segment_id" not in sched.columns:
                        seg_ids: list[int] = []
                        seg = 0
                        prev: str | None = None
                        for mc in sched["main_contract"].astype(str).tolist():
                            if prev is None:
                                seg = 0
                            elif mc != prev:
                                seg += 1
                            seg_ids.append(int(seg))
                            prev = mc
                        sched["segment_id"] = seg_ids

                    for _, r in sched.iterrows():
                        d = r["date"]
                        underlying_by_date[d] = str(r["main_contract"])
                        try:
                            segment_id_by_date[d] = int(r.get("segment_id", 0) or 0)
                        except Exception:
                            segment_id_by_date[d] = 0
        except Exception as e:
            log.warning("labels.schedule_load_failed", symbol=symbol, error=str(e))

    # Canonical schema for partitions
    schema_fields: list[pa.Field] = [pa.field("symbol", pa.string()), pa.field("datetime", pa.int64())]
    if ticks_lake == "main_l5":
        schema_fields += [pa.field("underlying_contract", pa.string()), pa.field("segment_id", pa.int64())]
    schema_fields += [pa.field("mid", pa.float64())] + [pa.field(f"label_{n}", pa.float64()) for n in horizons]
    schema = pa.schema(schema_fields)

    # Manifest safety gate: do not mix outputs across config/schema changes unless overwrite is explicit.
    if not overwrite:
        manifest_path = out_root / "manifest.json"
        if manifest_path.exists():
            try:
                import hashlib
                import json

                cols_str = ",".join(f"{f.name}:{f.type}" for f in schema)
                expected_schema_hash = hashlib.sha256(cols_str.encode()).hexdigest()[:16]
                existing = json.loads(manifest_path.read_text())
                if str(existing.get("dataset")) != "labels":
                    raise ValueError("dataset mismatch")
                if str(existing.get("symbol")) != str(symbol):
                    raise ValueError("symbol mismatch")
                if str(existing.get("ticks_lake")) != str(ticks_lake):
                    raise ValueError("ticks_lake mismatch")
                if str(existing.get("lake_version") or "v1") != str(lake_version):
                    raise ValueError("lake_version mismatch")
                if list(existing.get("horizons") or []) != list(horizons):
                    raise ValueError("horizons mismatch")
                if int(existing.get("threshold_k") or 0) != int(threshold_k):
                    raise ValueError("threshold_k mismatch")
                if str(existing.get("schema_hash") or "") != expected_schema_hash:
                    raise ValueError("schema_hash mismatch")
            except Exception as e:
                raise ValueError(
                    f"Existing labels manifest does not match requested build ({e}). "
                    f"Refusing to mix outputs. Re-run with overwrite=True to rebuild."
                )

    # Determine which dates need work (incremental default).
    existing_dates: set[date] = set()
    for ddir in out_root.iterdir():
        if not ddir.is_dir() or not ddir.name.startswith("date="):
            continue
        try:
            dt = date.fromisoformat(ddir.name.split("=", 1)[1])
        except Exception:
            continue
        if any(ddir.glob("*.parquet")):
            existing_dates.add(dt)

    missing_dates = [d for d in dates if d not in existing_dates]
    dates_to_process: set[date] = set()
    if overwrite:
        dates_to_process = set(dates)
    else:
        # Backfill/appends guard: if a day is missing, also recompute the previous available tick day.
        idx_map = {d: i for i, d in enumerate(dates)}
        for d in missing_dates:
            dates_to_process.add(d)
            j = idx_map.get(d)
            if j is not None and j - 1 >= 0:
                dates_to_process.add(dates[j - 1])

    total_rows = 0
    # Process each day; include up to max_h future ticks from next day for cross-boundary labels
    for dt in sorted(dates_to_process):
        idx = {d: i for i, d in enumerate(dates)}.get(dt, None)
        df_day = read_ticks_for_symbol(
            data_dir,
            symbol,
            start_date=dt,
            end_date=dt,
            ticks_lake=ticks_lake,
            lake_version=lake_version,
        )
        if df_day.empty:
            continue

        df_future = pd.DataFrame()
        if max_h > 0 and idx is not None and idx + 1 < len(dates):
            next_dt = dates[idx + 1]
            allow_cross = True
            if ticks_lake == "main_l5":
                seg0 = segment_id_by_date.get(dt)
                seg1 = segment_id_by_date.get(next_dt)
                allow_cross = bool(seg0 is not None and seg1 is not None and seg0 == seg1)
                if not allow_cross:
                    u0 = underlying_by_date.get(dt)
                    u1 = underlying_by_date.get(next_dt)
                    allow_cross = bool(u0 and u1 and u0 == u1)

            if allow_cross:
                df_next = read_ticks_for_symbol(
                    data_dir,
                    symbol,
                    start_date=next_dt,
                    end_date=next_dt,
                    ticks_lake=ticks_lake,
                    lake_version=lake_version,
                )
            else:
                df_next = pd.DataFrame()
            if not df_next.empty:
                df_future = df_next.head(max_h)

        if not df_future.empty:
            df_in = pd.concat([df_day, df_future], ignore_index=True)
        else:
            df_in = df_day

        labels_in = compute_multi_horizon_labels(df_in, horizons, threshold_k, price_tick)
        labels_day = labels_in.iloc[: len(df_day)].reset_index(drop=True)

        labels_day["datetime"] = df_day["datetime"].values
        labels_day["symbol"] = symbol
        if ticks_lake == "main_l5":
            labels_day["underlying_contract"] = underlying_by_date.get(dt) or ""
            labels_day["segment_id"] = int(segment_id_by_date.get(dt, 0) or 0)

        cols = ["symbol", "datetime"]
        if ticks_lake == "main_l5":
            cols += ["underlying_contract", "segment_id"]
        cols += ["mid"] + [f"label_{n}" for n in horizons]
        labels_day = labels_day[cols]

        out_dir = out_root / f"date={dt.isoformat()}"
        out_dir.mkdir(parents=True, exist_ok=True)
        # Deterministic per-date output name; remove any previous parquet(s) for this date.
        for p in out_dir.glob("*.parquet*"):
            try:
                p.unlink()
            except Exception:
                pass

        out_path = out_dir / "part.parquet"
        tmp_path = out_dir / "part.parquet.tmp"
        table = pa.Table.from_pandas(labels_day, schema=schema, preserve_index=False)
        pq.write_table(table, tmp_path, compression="zstd")
        tmp_path.replace(out_path)
        try:
            from ghtrader.integrity import write_sha256_sidecar

            write_sha256_sidecar(out_path)
        except Exception as e:
            log.warning("labels.checksum_failed", path=str(out_path), error=str(e))

        total_rows += len(labels_day)

    log.info("labels.build_done", symbol=symbol, rows=total_rows, root=str(out_root))

    # Rebuild symbol-level manifest from on-disk partitions (covers incremental runs).
    row_counts: dict[str, int] = {}
    files: list[dict] = []
    try:
        import hashlib

        from ghtrader.integrity import now_utc_iso, parquet_num_rows, read_sha256_sidecar, write_json_atomic

        total_rows2 = 0
        for ddir in sorted([p for p in out_root.iterdir() if p.is_dir() and p.name.startswith("date=")], key=lambda p: p.name):
            dt_s = ddir.name.split("=", 1)[1]
            pq_files = sorted(ddir.glob("*.parquet"))
            if not pq_files:
                continue
            p = pq_files[0]
            rows = parquet_num_rows(p)
            sha = read_sha256_sidecar(p) or ""
            row_counts[dt_s] = int(rows)
            total_rows2 += int(rows)
            files.append({"date": dt_s, "file": f"{ddir.name}/{p.name}", "rows": int(rows), "sha256": sha})

        cols_str = ",".join(f"{f.name}:{f.type}" for f in schema)
        schema_hash = hashlib.sha256(cols_str.encode()).hexdigest()[:16]
        manifest = {
            "created_at": now_utc_iso(),
            "dataset": "labels",
            "symbol": symbol,
            "ticks_lake": ticks_lake,
            "lake_version": lake_version,
            "horizons": list(horizons),
            "threshold_k": int(threshold_k),
            "schema_hash": schema_hash,
            "rows_total": int(total_rows2),
            "row_counts": row_counts,
            "files": files,
        }
        write_json_atomic(out_root / "manifest.json", manifest)
    except Exception as e:
        log.warning("labels.manifest_failed", symbol=symbol, error=str(e))

    return out_root


def read_labels_for_symbol(data_dir: Path, symbol: str) -> pd.DataFrame:
    """Read labels for a symbol."""
    base_dir = labels_dir(data_dir) / f"symbol={symbol}"
    legacy_path = base_dir / "labels.parquet"
    if legacy_path.exists():
        legacy_schema = pq.read_schema(legacy_path)
        fields = []
        for f in legacy_schema:
            if f.name == "symbol":
                fields.append(pa.field("symbol", pa.string()))
            else:
                fields.append(f)
        schema = pa.schema(fields)
        return pq.read_table(legacy_path, schema=schema).to_pandas()

    if not base_dir.exists():
        raise FileNotFoundError(f"Labels not found for {symbol}: {base_dir}")

    partitions: list[Path] = []
    for date_dir in sorted(base_dir.iterdir()):
        if not date_dir.is_dir() or not date_dir.name.startswith("date="):
            continue
        partitions.extend(sorted(date_dir.glob("*.parquet")))

    if not partitions:
        raise FileNotFoundError(f"No label partitions found for {symbol}: {base_dir}")

    schema = pq.read_schema(partitions[0])
    tables = [pq.read_table(p, schema=schema) for p in partitions]
    combined = pa.concat_tables(tables)
    df = combined.to_pandas()
    return df.sort_values("datetime").reset_index(drop=True)


# ---------------------------------------------------------------------------
# Walk-forward splits
# ---------------------------------------------------------------------------

@dataclass
class WalkForwardSplit:
    """A single train/val/test split for walk-forward evaluation."""
    
    split_id: int
    train_start_idx: int
    train_end_idx: int
    val_start_idx: int
    val_end_idx: int
    test_start_idx: int
    test_end_idx: int


def create_walk_forward_splits(
    n_samples: int,
    n_splits: int = 5,
    train_ratio: float = 0.6,
    val_ratio: float = 0.2,
    min_train_samples: int = 10000,
) -> list[WalkForwardSplit]:
    """
    Create walk-forward (expanding or rolling) splits.
    
    Each split uses earlier data for training and later data for validation/test.
    No shuffling across time (preserves temporal ordering).
    
    Args:
        n_samples: Total number of samples
        n_splits: Number of splits to create
        train_ratio: Fraction of available data for training in each split
        val_ratio: Fraction for validation
        min_train_samples: Minimum samples required for training
    
    Returns:
        List of WalkForwardSplit objects
    """
    test_ratio = 1.0 - train_ratio - val_ratio
    
    splits: list[WalkForwardSplit] = []
    
    # Calculate step size
    step_size = n_samples // (n_splits + 2)  # Reserve space for expanding window
    
    for i in range(n_splits):
        # Expanding window: each split uses more history
        split_end = min(n_samples, (i + 3) * step_size)
        window_size = split_end
        
        train_size = int(window_size * train_ratio)
        val_size = int(window_size * val_ratio)
        test_size = window_size - train_size - val_size
        
        if train_size < min_train_samples:
            continue
        
        train_start = 0
        train_end = train_size
        val_start = train_end
        val_end = val_start + val_size
        test_start = val_end
        test_end = min(split_end, test_start + test_size)
        
        if test_end <= test_start:
            continue
        
        splits.append(WalkForwardSplit(
            split_id=i,
            train_start_idx=train_start,
            train_end_idx=train_end,
            val_start_idx=val_start,
            val_end_idx=val_end,
            test_start_idx=test_start,
            test_end_idx=test_end,
        ))
    
    return splits


def apply_split(df: pd.DataFrame, split: WalkForwardSplit) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """Apply a walk-forward split to a DataFrame."""
    train = df.iloc[split.train_start_idx:split.train_end_idx].copy()
    val = df.iloc[split.val_start_idx:split.val_end_idx].copy()
    test = df.iloc[split.test_start_idx:split.test_end_idx].copy()
    return train, val, test
