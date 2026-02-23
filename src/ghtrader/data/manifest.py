from __future__ import annotations

import subprocess
import uuid
from dataclasses import dataclass
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any

import structlog

from ghtrader.util.json_io import read_json, write_json_atomic
from .ticks_schema import row_hash_algorithm_version, ticks_schema_hash

log = structlog.get_logger()


@dataclass(frozen=True)
class IngestManifest:
    run_id: str
    created_at: str
    symbols: list[str]
    start_date: str
    end_date: str
    source: str
    row_counts: dict[str, int]
    schema_hash: str
    code_version: str
    row_hash_algo: str = "fnv1a64_v1"

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
            "row_hash_algo": self.row_hash_algo,
        }

    @classmethod
    def from_dict(cls, d: dict[str, Any]) -> "IngestManifest":
        payload = dict(d or {})
        payload.setdefault("row_hash_algo", row_hash_algorithm_version())
        return cls(**payload)


def manifests_dir(data_dir: Path) -> Path:
    return data_dir / "manifests"


def _now_utc_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _get_git_hash() -> str:
    try:
        result = subprocess.run(
            ["git", "rev-parse", "HEAD"],
            capture_output=True,
            text=True,
            timeout=5,
            check=False,
        )
        if result.returncode == 0:
            return result.stdout.strip()[:12]
    except Exception:
        pass
    return "unknown"


def write_manifest(
    *,
    data_dir: Path,
    symbols: list[str],
    start_date: date,
    end_date: date,
    source: str,
    row_counts: dict[str, int],
) -> Path:
    run_id = uuid.uuid4().hex[:12]
    manifest = IngestManifest(
        run_id=run_id,
        created_at=_now_utc_iso(),
        symbols=symbols,
        start_date=start_date.isoformat(),
        end_date=end_date.isoformat(),
        source=source,
        row_counts=row_counts,
        schema_hash=ticks_schema_hash(),
        code_version=_get_git_hash(),
        row_hash_algo=row_hash_algorithm_version(),
    )

    out_dir = manifests_dir(data_dir)
    out_path = out_dir / f"{run_id}.json"
    write_json_atomic(out_path, manifest.to_dict())
    log.info("ingest_manifest.written", run_id=run_id, path=str(out_path))
    return out_path


def read_manifest(path: Path) -> IngestManifest:
    obj = read_json(path)
    if not isinstance(obj, dict):
        raise ValueError(f"Invalid manifest JSON: {path}")
    return IngestManifest.from_dict(obj)


def list_manifests(*, data_dir: Path) -> list[Path]:
    m_dir = manifests_dir(data_dir)
    if not m_dir.exists():
        return []
    return sorted(m_dir.glob("*.json"))

