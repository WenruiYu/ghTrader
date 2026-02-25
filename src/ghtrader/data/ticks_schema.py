from __future__ import annotations

import hashlib
from typing import Literal

import numpy as np
import pandas as pd

# ---------------------------------------------------------------------------
# Canonical tick column names (QuestDB-first)
# ---------------------------------------------------------------------------

# L5 order book columns (SHFE provides 5 levels)
L5_BOOK_COLS: list[str] = []
for i in range(1, 6):
    L5_BOOK_COLS.extend([f"bid_price{i}", f"bid_volume{i}", f"ask_price{i}", f"ask_volume{i}"])

# Canonical tick column names used throughout ghTrader (dataframes, feature builders, etc.)
# Note: QuestDB stores `datetime_ns`; callers typically alias to `datetime` when loading into pandas.
TICK_COLUMN_NAMES: list[str] = [
    "symbol",
    "datetime",
    "last_price",
    "average",
    "highest",
    "lowest",
    "volume",
    "amount",
    "open_interest",
] + list(L5_BOOK_COLS)

TICK_NUMERIC_COLUMNS: list[str] = [c for c in TICK_COLUMN_NAMES if c not in {"symbol", "datetime"}]

DatasetVersion = Literal["v2"]
TicksKind = Literal["raw", "main_l5"]
ROW_HASH_ALGO_VERSION = "fnv1a64_v1"


def ticks_schema_hash() -> str:
    """
    Stable schema hash for canonical tick shape.

    This replaces the old Parquet/Arrow schema hash; ghTrader is QuestDB-only.
    """
    cols = []
    cols.append("symbol:string")
    cols.append("datetime:int64")
    for c in TICK_NUMERIC_COLUMNS:
        cols.append(f"{c}:float64")
    s = ",".join(cols)
    return hashlib.sha256(s.encode("utf-8")).hexdigest()[:16]


def null_rate(series: pd.Series, total: int, *, default_on_error: float = 0.0) -> float:
    """Fraction of null/NaN values in *series* relative to *total* rows."""
    if total <= 0:
        return 0.0
    try:
        return float(series.isna().sum()) / float(total)
    except Exception:
        return float(default_on_error)


def row_hash_from_ticks_df(df: pd.DataFrame) -> pd.Series:
    """
    Deterministic row identity hash used by QuestDB ingest (LONG).

    The hash is derived from `datetime` and all canonical numeric tick columns.
    """
    if "datetime" in df.columns:
        dt_ns = pd.to_numeric(df["datetime"], errors="coerce").fillna(0).astype("int64")
    else:
        dt_ns = pd.Series([0] * int(len(df)), index=df.index, dtype="int64")
    prime = np.uint64(1099511628211)
    h = np.full(len(dt_ns), np.uint64(1469598103934665603))

    # Mix datetime_ns (int64 bits)
    h ^= dt_ns.to_numpy(dtype="int64", copy=False).view(np.uint64)
    h *= prime

    # Mix all canonical numeric tick columns (float64 bits)
    for col in TICK_NUMERIC_COLUMNS:
        if col in df.columns:
            a = pd.to_numeric(df[col], errors="coerce").fillna(0).to_numpy(dtype="float64", copy=False)
        else:
            a = np.zeros(len(dt_ns), dtype="float64")
        h ^= a.view(np.uint64)
        h *= prime

    return pd.Series(h.view(np.int64), index=df.index, dtype="int64")


def row_hash_algorithm_version() -> str:
    """Stable identifier for row-hash algorithm compatibility checks."""
    return str(ROW_HASH_ALGO_VERSION)


def row_hash_aggregates(row_hash: pd.Series) -> dict[str, int | None]:
    """
    Compute simple per-partition checksum aggregates over `row_hash`.

    We keep aggregates in signed int64 space (QuestDB LONG). Sums use int64
    wrap-around semantics to stay within range.
    """
    try:
        rh = pd.to_numeric(row_hash, errors="coerce").dropna().astype("int64")
    except Exception:
        rh = pd.Series([], dtype="int64")
    if rh.empty:
        return {"row_hash_min": None, "row_hash_max": None, "row_hash_sum": None, "row_hash_sum_abs": None}

    a = rh.to_numpy(dtype="int64", copy=False)
    rh_min = int(a.min())
    rh_max = int(a.max())
    rh_sum = int(np.sum(a, dtype=np.int64))
    rh_sum_abs = int(np.sum(np.abs(a), dtype=np.int64))
    return {"row_hash_min": rh_min, "row_hash_max": rh_max, "row_hash_sum": rh_sum, "row_hash_sum_abs": rh_sum_abs}

