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

from ghtrader.lake import list_available_dates, read_ticks_for_symbol

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

    dates = list_available_dates(data_dir, symbol)
    if not dates:
        raise ValueError(f"No tick data found for {symbol}")

    # Overwrite derived labels for idempotency
    out_root = labels_dir(data_dir) / f"symbol={symbol}"
    if out_root.exists():
        shutil.rmtree(out_root)
    out_root.mkdir(parents=True, exist_ok=True)

    # Get price tick
    price_tick = _get_price_tick(symbol)
    log.debug("labels.price_tick", symbol=symbol, price_tick=price_tick)

    max_h = max(horizons) if horizons else 0

    # Canonical schema for partitions
    schema = pa.schema(
        [("symbol", pa.string()), ("datetime", pa.int64()), ("mid", pa.float64())]
        + [(f"label_{n}", pa.float64()) for n in horizons]
    )

    total_rows = 0
    # Process each day; include up to max_h future ticks from next day for cross-boundary labels
    for idx, dt in enumerate(dates):
        df_day = read_ticks_for_symbol(data_dir, symbol, start_date=dt, end_date=dt)
        if df_day.empty:
            continue

        df_future = pd.DataFrame()
        if max_h > 0 and idx + 1 < len(dates):
            next_dt = dates[idx + 1]
            df_next = read_ticks_for_symbol(data_dir, symbol, start_date=next_dt, end_date=next_dt)
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

        cols = ["symbol", "datetime", "mid"] + [f"label_{n}" for n in horizons]
        labels_day = labels_day[cols]

        part_id = uuid.uuid4().hex[:8]
        out_dir = out_root / f"date={dt.isoformat()}"
        out_dir.mkdir(parents=True, exist_ok=True)
        out_path = out_dir / f"part-{part_id}.parquet"

        table = pa.Table.from_pandas(labels_day, schema=schema, preserve_index=False)
        pq.write_table(table, out_path, compression="zstd")

        total_rows += len(labels_day)

    log.info("labels.build_done", symbol=symbol, rows=total_rows, root=str(out_root))
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
