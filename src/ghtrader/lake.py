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
from typing import Any, Literal

import pandas as pd
import pyarrow as pa
import pyarrow.dataset as ds
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

LakeVersion = Literal["v1", "v2"]


def lake_root_dir(data_dir: Path, lake_version: LakeVersion = "v1") -> Path:
    """
    Return the base lake directory.

    - v1: data/lake/ (legacy)
    - v2: data/lake_v2/ (trading-day partitioning; preferred)
    """
    if lake_version == "v1":
        return data_dir / "lake"
    if lake_version == "v2":
        return data_dir / "lake_v2"
    raise ValueError(f"Unknown lake_version: {lake_version!r}")


def lake_ticks_dir(data_dir: Path, lake_version: LakeVersion = "v1") -> Path:
    """Return the base path for *raw* tick Parquet partitions."""
    return lake_root_dir(data_dir, lake_version) / "ticks"


TicksLake = Literal["raw", "main_l5"]


def ticks_root_dir(
    data_dir: Path,
    ticks_lake: TicksLake = "raw",
    *,
    lake_version: LakeVersion = "v1",
) -> Path:
    """
    Return the base path for tick partitions for a given lake.

    - raw:     data/lake{,_v2}/ticks/
    - main_l5: data/lake{,_v2}/main_l5/ticks/
    """
    if ticks_lake == "raw":
        return lake_root_dir(data_dir, lake_version) / "ticks"
    if ticks_lake == "main_l5":
        return lake_root_dir(data_dir, lake_version) / "main_l5" / "ticks"
    raise ValueError(f"Unknown ticks_lake: {ticks_lake!r}")


def ticks_symbol_dir(
    data_dir: Path,
    symbol: str,
    ticks_lake: TicksLake = "raw",
    *,
    lake_version: LakeVersion = "v1",
) -> Path:
    """Return the root directory for a symbol within a ticks lake."""
    return ticks_root_dir(data_dir, ticks_lake, lake_version=lake_version) / f"symbol={symbol}"


def ticks_date_dir(
    data_dir: Path,
    symbol: str,
    dt: date,
    ticks_lake: TicksLake = "raw",
    *,
    lake_version: LakeVersion = "v1",
) -> Path:
    """Return the directory for a symbol+date partition within a ticks lake."""
    return ticks_symbol_dir(data_dir, symbol, ticks_lake, lake_version=lake_version) / f"date={dt.isoformat()}"


def read_ticks_for_symbol_date(
    data_dir: Path,
    symbol: str,
    dt: date,
    *,
    ticks_lake: TicksLake = "raw",
    lake_version: LakeVersion = "v1",
) -> pd.DataFrame:
    """
    Read tick partitions for a single symbol+date (fast path).
    """
    date_dir = ticks_date_dir(data_dir, symbol, dt, ticks_lake, lake_version=lake_version)
    if not date_dir.exists():
        return pd.DataFrame(columns=TICK_COLUMN_NAMES)

    parts = sorted(date_dir.glob("*.parquet"))
    if not parts:
        return pd.DataFrame(columns=TICK_COLUMN_NAMES)

    tables = [pq.read_table(p, schema=TICK_ARROW_SCHEMA) for p in parts]
    combined = pa.concat_tables(tables) if len(tables) > 1 else tables[0]
    df = combined.to_pandas()
    return df.sort_values("datetime").reset_index(drop=True)


def read_ticks_for_symbol_arrow(
    data_dir: Path,
    symbol: str,
    *,
    start_date: date | None = None,
    end_date: date | None = None,
    ticks_lake: TicksLake = "raw",
    lake_version: LakeVersion = "v1",
) -> pa.Table:
    """
    Arrow-first tick reader using pyarrow.dataset scanning.

    This avoids manual directory walks and is the scalable path for large ranges.
    """
    base_dir = ticks_symbol_dir(data_dir, symbol, ticks_lake, lake_version=lake_version)
    if not base_dir.exists():
        return pa.Table.from_arrays([], schema=TICK_ARROW_SCHEMA)

    # Note: our lake writes checksum sidecars like `part-xxxx.parquet.sha256` alongside parquet files.
    # Arrow Dataset directory discovery can pick those up; exclude invalid files so only real parquet
    # inputs are scanned.
    dataset = ds.dataset(str(base_dir), format="parquet", partitioning="hive", exclude_invalid_files=True)
    filt = None
    if start_date is not None or end_date is not None:
        # Hive partition column will be 'date' as string-like; compare as ISO strings.
        if start_date is None:
            start_s = "0000-01-01"
        else:
            start_s = start_date.isoformat()
        if end_date is None:
            end_s = "9999-12-31"
        else:
            end_s = end_date.isoformat()
        filt = (ds.field("date") >= ds.scalar(start_s)) & (ds.field("date") <= ds.scalar(end_s))

    # Read only canonical columns; 'date' partition column is used only for filtering.
    table = dataset.to_table(filter=filt, columns=TICK_COLUMN_NAMES)

    # Normalize types to the canonical tick schema (e.g. decode dictionary-encoded strings).
    try:
        table = table.cast(TICK_ARROW_SCHEMA, safe=False)
    except Exception:
        # Best-effort: keep whatever was read if casting fails.
        pass

    # Ensure canonical column ordering.
    cols = [c for c in TICK_COLUMN_NAMES if c in table.column_names]
    table = table.select(cols)
    return table


def partition_path(data_dir: Path, symbol: str, dt: date) -> Path:
    """Return the partition directory for a symbol and date."""
    return lake_ticks_dir(data_dir, lake_version="v1") / f"symbol={symbol}" / f"date={dt.isoformat()}"


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
    *,
    lake_version: LakeVersion = "v1",
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
    
    # Write (atomic): write to temp file in same directory, then rename.
    pid = part_id or uuid.uuid4().hex[:8]
    out_path = ticks_date_dir(data_dir, symbol, dt, ticks_lake="raw", lake_version=lake_version) / f"part-{pid}.parquet"
    out_path.parent.mkdir(parents=True, exist_ok=True)

    if out_path.exists():
        raise FileExistsError(str(out_path))

    tmp_path = out_path.with_suffix(".tmp")
    try:
        pq.write_table(table, tmp_path, compression="zstd")
        tmp_path.replace(out_path)
    finally:
        # Best-effort cleanup if something failed before the replace.
        try:
            if tmp_path.exists():
                tmp_path.unlink()
        except Exception:
            pass

    # Integrity: checksum sidecar + per-date manifest
    try:
        from ghtrader.integrity import build_partition_manifest, write_json_atomic, write_sha256_sidecar

        write_sha256_sidecar(out_path)
        manifest = build_partition_manifest(
            partition_dir=out_path.parent,
            dataset="ticks_raw",
            symbol=symbol,
            dt=dt.isoformat(),
        )
        write_json_atomic(out_path.parent / "_manifest.json", manifest)
    except Exception as e:
        log.warning("lake.integrity_write_failed", path=str(out_path), error=str(e))
    
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
    ticks_lake: TicksLake = "raw",
    *,
    lake_version: LakeVersion = "v1",
) -> pd.DataFrame:
    """
    Read all tick partitions for a symbol within a date range.
    
    Returns a concatenated DataFrame sorted by datetime.
    """
    base_dir = ticks_root_dir(data_dir, ticks_lake, lake_version=lake_version) / f"symbol={symbol}"
    if not base_dir.exists():
        log.warning("lake.no_data", symbol=symbol, base_dir=str(base_dir))
        return pd.DataFrame(columns=TICK_COLUMN_NAMES)

    # Fast path: one-day read should not scan all partitions.
    if start_date is not None and end_date is not None and start_date == end_date:
        return read_ticks_for_symbol_date(
            data_dir,
            symbol,
            start_date,
            ticks_lake=ticks_lake,
            lake_version=lake_version,
        )
    
    # Scalable range path: Arrow Dataset scan.
    table = read_ticks_for_symbol_arrow(
        data_dir,
        symbol,
        start_date=start_date,
        end_date=end_date,
        ticks_lake=ticks_lake,
        lake_version=lake_version,
    )
    if table.num_rows == 0:
        log.warning("lake.no_partitions", symbol=symbol)
        return pd.DataFrame(columns=TICK_COLUMN_NAMES)

    df = table.to_pandas()
    df = df.sort_values("datetime").reset_index(drop=True)
    log.debug("lake.read_ticks", symbol=symbol, rows=len(df))
    return df


def list_available_dates(data_dir: Path, symbol: str, *, lake_version: LakeVersion = "v1") -> list[date]:
    """Return sorted list of dates that have data for a symbol."""
    base_dir = ticks_root_dir(data_dir, "raw", lake_version=lake_version) / f"symbol={symbol}"
    if not base_dir.exists():
        return []
    
    dates: list[date] = []
    for date_dir in base_dir.iterdir():
        if date_dir.is_dir() and date_dir.name.startswith("date="):
            dt_str = date_dir.name.split("=")[1]
            dates.append(date.fromisoformat(dt_str))
    return sorted(dates)


def list_available_dates_in_lake(
    data_dir: Path,
    symbol: str,
    ticks_lake: TicksLake = "raw",
    *,
    lake_version: LakeVersion = "v1",
) -> list[date]:
    """Return sorted list of dates that have data for a symbol in a specific ticks lake."""
    base_dir = ticks_root_dir(data_dir, ticks_lake, lake_version=lake_version) / f"symbol={symbol}"
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
