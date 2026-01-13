from __future__ import annotations

import hashlib
import os
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pyarrow.parquet as pq
import structlog

log = structlog.get_logger()


def sha256_file(path: Path, *, chunk_size: int = 8 * 1024 * 1024) -> str:
    """
    Compute SHA256 of a file by streaming bytes from disk.
    """
    h = hashlib.sha256()
    with open(path, "rb") as f:
        while True:
            b = f.read(chunk_size)
            if not b:
                break
            h.update(b)
    return h.hexdigest()


def sha256_sidecar_path(parquet_path: Path) -> Path:
    return parquet_path.with_suffix(parquet_path.suffix + ".sha256")


def write_sha256_sidecar(parquet_path: Path) -> Path:
    """
    Write a sibling *.parquet.sha256 file containing the hex digest.
    """
    digest = sha256_file(parquet_path)
    sidecar = sha256_sidecar_path(parquet_path)
    tmp = sidecar.with_suffix(sidecar.suffix + ".tmp")
    tmp.parent.mkdir(parents=True, exist_ok=True)
    tmp.write_text(digest + "\n", encoding="utf-8")
    os.replace(tmp, sidecar)
    return sidecar


def read_sha256_sidecar(parquet_path: Path) -> str | None:
    sidecar = sha256_sidecar_path(parquet_path)
    if not sidecar.exists():
        return None
    return sidecar.read_text(encoding="utf-8").strip() or None


def verify_sha256_sidecar(parquet_path: Path) -> tuple[bool, str | None, str]:
    expected = read_sha256_sidecar(parquet_path)
    actual = sha256_file(parquet_path)
    if expected is None:
        return False, None, actual
    return expected == actual, expected, actual


def parquet_num_rows(path: Path) -> int:
    pf = pq.ParquetFile(path)
    return int(pf.metadata.num_rows) if pf.metadata is not None else 0


def now_utc_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def schema_hash_from_parquet(path: Path) -> str:
    schema = pq.read_schema(path)
    cols_str = ",".join(f"{f.name}:{f.type}" for f in schema)
    return hashlib.sha256(cols_str.encode()).hexdigest()[:16]


@dataclass(frozen=True)
class FileEntry:
    file: str
    rows: int
    sha256: str


def build_partition_manifest(
    *,
    partition_dir: Path,
    dataset: str,
    symbol: str,
    dt: str,
) -> dict[str, Any]:
    """
    Build a per-date partition manifest from on-disk parquet + sha256 sidecars.
    """
    files: list[FileEntry] = []
    rows_total = 0
    for p in sorted(partition_dir.glob("*.parquet")):
        rows = parquet_num_rows(p)
        sha = read_sha256_sidecar(p) or ""
        files.append(FileEntry(file=p.name, rows=rows, sha256=sha))
        rows_total += rows

    return {
        "created_at": now_utc_iso(),
        "dataset": dataset,
        "symbol": symbol,
        "date": dt,
        "rows_total": int(rows_total),
        "files": [f.__dict__ for f in files],
    }


def write_json_atomic(path: Path, payload: dict[str, Any]) -> None:
    import json

    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    with open(tmp, "w") as f:
        json.dump(payload, f, indent=2, default=str)
    os.replace(tmp, path)

