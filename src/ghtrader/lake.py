"""
Parquet data lake: schema, partitioning, manifest writing/reading.

The canonical storage for raw L5 ticks uses:
- Partitioning: data/lake/ticks/symbol=.../date=YYYY-MM-DD/part-....parquet
- Compression: ZSTD
- Schema: locked Arrow schema for reproducibility
"""

from __future__ import annotations

import hashlib
import json
import subprocess
import uuid
from dataclasses import dataclass, field
from datetime import date, datetime
from pathlib import Path
from typing import Any

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import structlog

log = structlog.get_logger()

# ---------------------------------------------------------------------------
# L5 Tick Schema (canonical for SHFE)
# ---------------------------------------------------------------------------

# Column names for L5 order book (SHFE provides 5 levels)
L5_BOOK_COLS: list[str] = []
for i in range(1, 6):
    L5_BOOK_COLS.extend([f"bid_price{i}", f"bid_volume{i}", f"ask_price{i}", f"ask_volume{i}"])

TICK_SCHEMA_COLS: list[tuple[str, pa.DataType]] = [
    ("symbol", pa.string()),
    ("datetime", pa.int64()),  # epoch-nanoseconds (Beijing time)
    ("last_price", pa.float64()),
    ("average", pa.float64()),
    ("highest", pa.float64()),
    ("lowest", pa.float64()),
    ("volume", pa.float64()),
    ("amount", pa.float64()),
    ("open_interest", pa.float64()),
]
# Add L5 book columns
for col in L5_BOOK_COLS:
    TICK_SCHEMA_COLS.append((col, pa.float64()))

TICK_ARROW_SCHEMA = pa.schema(TICK_SCHEMA_COLS)

TICK_COLUMN_NAMES = [c[0] for c in TICK_SCHEMA_COLS]


def get_tick_schema() -> pa.Schema:
    """Return the canonical Arrow schema for L5 ticks."""
    return TICK_ARROW_SCHEMA


def schema_hash() -> str:
    """Return a stable hash of the tick schema for manifest tracking."""
    cols_str = ",".join(f"{name}:{dtype}" for name, dtype in TICK_SCHEMA_COLS)
    return hashlib.sha256(cols_str.encode()).hexdigest()[:16]


# ---------------------------------------------------------------------------
# Lake paths
# ---------------------------------------------------------------------------

def lake_ticks_dir(data_dir: Path) -> Path:
    """Return the base path for tick Parquet partitions."""
    return data_dir / "lake" / "ticks"


def partition_path(data_dir: Path, symbol: str, dt: date) -> Path:
    """Return the partition directory for a symbol and date."""
    return lake_ticks_dir(data_dir) / f"symbol={symbol}" / f"date={dt.isoformat()}"


def partition_file(data_dir: Path, symbol: str, dt: date, part_id: str | None = None) -> Path:
    """Return the Parquet file path for a partition."""
    if part_id is None:
        part_id = uuid.uuid4().hex[:8]
    return partition_path(data_dir, symbol, dt) / f"part-{part_id}.parquet"


# ---------------------------------------------------------------------------
# Writing ticks
# ---------------------------------------------------------------------------

def write_ticks_partition(
    df: pd.DataFrame,
    data_dir: Path,
    symbol: str,
    dt: date,
    part_id: str | None = None,
) -> Path:
    """
    Write a DataFrame of ticks to a Parquet partition.
    
    The DataFrame must contain all columns in TICK_COLUMN_NAMES.
    Returns the path to the written file.
    """
    # Ensure symbol column is set
    if "symbol" not in df.columns:
        df = df.copy()
        df["symbol"] = symbol
    
    # Validate columns
    missing = set(TICK_COLUMN_NAMES) - set(df.columns)
    if missing:
        raise ValueError(f"Missing columns in tick DataFrame: {missing}")
    
    # Reorder to canonical schema
    df = df[TICK_COLUMN_NAMES].copy()
    
    # Ensure correct types
    df["symbol"] = df["symbol"].astype(str)
    df["datetime"] = df["datetime"].astype("int64")
    for col in TICK_COLUMN_NAMES[2:]:  # All numeric columns after symbol and datetime
        df[col] = df[col].astype("float64")
    
    # Convert to Arrow table with schema
    table = pa.Table.from_pandas(df, schema=TICK_ARROW_SCHEMA, preserve_index=False)
    
    # Write
    out_path = partition_file(data_dir, symbol, dt, part_id)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    pq.write_table(table, out_path, compression="zstd")
    
    log.debug("lake.write_partition", path=str(out_path), rows=len(df))
    return out_path


# ---------------------------------------------------------------------------
# Reading ticks
# ---------------------------------------------------------------------------

def read_ticks_for_symbol(
    data_dir: Path,
    symbol: str,
    start_date: date | None = None,
    end_date: date | None = None,
) -> pd.DataFrame:
    """
    Read all tick partitions for a symbol within a date range.
    
    Returns a concatenated DataFrame sorted by datetime.
    """
    base_dir = lake_ticks_dir(data_dir) / f"symbol={symbol}"
    if not base_dir.exists():
        log.warning("lake.no_data", symbol=symbol, base_dir=str(base_dir))
        return pd.DataFrame(columns=TICK_COLUMN_NAMES)
    
    # Find all date partitions
    partitions: list[Path] = []
    for date_dir in sorted(base_dir.iterdir()):
        if not date_dir.is_dir() or not date_dir.name.startswith("date="):
            continue
        dt_str = date_dir.name.split("=")[1]
        dt = date.fromisoformat(dt_str)
        if start_date and dt < start_date:
            continue
        if end_date and dt > end_date:
            continue
        for pq_file in date_dir.glob("*.parquet"):
            partitions.append(pq_file)
    
    if not partitions:
        log.warning("lake.no_partitions", symbol=symbol)
        return pd.DataFrame(columns=TICK_COLUMN_NAMES)
    
    # Read and concatenate (read with schema to avoid type mismatches)
    tables = [pq.read_table(p, schema=TICK_ARROW_SCHEMA) for p in partitions]
    combined = pa.concat_tables(tables)
    df = combined.to_pandas()
    df = df.sort_values("datetime").reset_index(drop=True)
    
    log.debug("lake.read_ticks", symbol=symbol, rows=len(df), partitions=len(partitions))
    return df


def list_available_dates(data_dir: Path, symbol: str) -> list[date]:
    """Return sorted list of dates that have data for a symbol."""
    base_dir = lake_ticks_dir(data_dir) / f"symbol={symbol}"
    if not base_dir.exists():
        return []
    
    dates: list[date] = []
    for date_dir in base_dir.iterdir():
        if date_dir.is_dir() and date_dir.name.startswith("date="):
            dt_str = date_dir.name.split("=")[1]
            dates.append(date.fromisoformat(dt_str))
    return sorted(dates)


# ---------------------------------------------------------------------------
# Manifests
# ---------------------------------------------------------------------------

@dataclass
class IngestManifest:
    """Manifest for a data ingest run."""
    
    run_id: str
    created_at: str
    symbols: list[str]
    start_date: str
    end_date: str
    source: str  # "tq_dl" or "live"
    row_counts: dict[str, int] = field(default_factory=dict)
    schema_hash: str = ""
    code_version: str = ""
    
    def to_dict(self) -> dict[str, Any]:
        return {
            "run_id": self.run_id,
            "created_at": self.created_at,
            "symbols": self.symbols,
            "start_date": self.start_date,
            "end_date": self.end_date,
            "source": self.source,
            "row_counts": self.row_counts,
            "schema_hash": self.schema_hash,
            "code_version": self.code_version,
        }
    
    @classmethod
    def from_dict(cls, d: dict[str, Any]) -> IngestManifest:
        return cls(**d)


def manifests_dir(data_dir: Path) -> Path:
    """Return the manifests directory."""
    return data_dir / "manifests"


def _get_git_hash() -> str:
    """Get current git commit hash, or 'unknown' if not in a git repo."""
    try:
        result = subprocess.run(
            ["git", "rev-parse", "HEAD"],
            capture_output=True,
            text=True,
            timeout=5,
        )
        if result.returncode == 0:
            return result.stdout.strip()[:12]
    except Exception:
        pass
    return "unknown"


def write_manifest(
    data_dir: Path,
    symbols: list[str],
    start_date: date,
    end_date: date,
    source: str,
    row_counts: dict[str, int],
) -> Path:
    """Write an ingest manifest and return its path."""
    run_id = uuid.uuid4().hex[:12]
    manifest = IngestManifest(
        run_id=run_id,
        created_at=datetime.now().isoformat() + "Z",
        symbols=symbols,
        start_date=start_date.isoformat(),
        end_date=end_date.isoformat(),
        source=source,
        row_counts=row_counts,
        schema_hash=schema_hash(),
        code_version=_get_git_hash(),
    )
    
    out_dir = manifests_dir(data_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / f"{run_id}.json"
    
    with open(out_path, "w") as f:
        json.dump(manifest.to_dict(), f, indent=2)
    
    log.info("lake.manifest_written", run_id=run_id, path=str(out_path))
    return out_path


def read_manifest(path: Path) -> IngestManifest:
    """Read a manifest from disk."""
    with open(path) as f:
        return IngestManifest.from_dict(json.load(f))


def list_manifests(data_dir: Path) -> list[Path]:
    """Return list of all manifest files."""
    m_dir = manifests_dir(data_dir)
    if not m_dir.exists():
        return []
    return sorted(m_dir.glob("*.json"))
