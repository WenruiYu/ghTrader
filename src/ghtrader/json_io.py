"""Shared JSON I/O helpers for atomic read/write operations.

Centralized to avoid duplication across Data Hub modules (PRD redundancy cleanup).
"""

from __future__ import annotations

import json
import uuid
from pathlib import Path
from typing import Any


def read_json(path: Path) -> dict[str, Any] | None:
    """Read a JSON file, returning None if it doesn't exist or is invalid."""
    try:
        if not path.exists():
            return None
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return None


def write_json_atomic(path: Path, obj: dict[str, Any]) -> None:
    """Atomically write a JSON file (creates parent dirs if needed)."""
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(f".tmp-{uuid.uuid4().hex}")
    tmp.write_text(
        json.dumps(obj, ensure_ascii=False, indent=2, sort_keys=True, default=str),
        encoding="utf-8",
    )
    tmp.replace(path)
