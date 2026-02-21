from __future__ import annotations

import json
import time
from pathlib import Path
from typing import Any

from ghtrader.util.json_io import read_json


def read_json_file(path: Path) -> dict[str, Any] | None:
    return read_json(path)


def read_redis_json(key: str) -> dict[str, Any] | None:
    """
    Best-effort sync Redis JSON read for hot state/desired keys.
    Falls back to file-based mirrors when Redis is unavailable.
    """
    try:
        import redis as redis_sync

        client = redis_sync.Redis(
            host="localhost",
            port=6379,
            db=0,
            decode_responses=True,
            socket_connect_timeout=0.2,
            socket_timeout=0.2,
        )
        raw = client.get(str(key))
        if not raw:
            return None
        obj = json.loads(raw)
        return obj if isinstance(obj, dict) else None
    except Exception:
        return None


def read_jsonl_tail(path: Path, *, max_lines: int, max_bytes: int = 256 * 1024) -> list[dict[str, Any]]:
    """
    Best-effort tail reader for JSONL artifacts (snapshots/events).
    Returns parsed objects in chronological order (oldest -> newest).
    """
    try:
        if not path.exists() or int(max_lines) <= 0:
            return []
        with open(path, "rb") as f:
            f.seek(0, 2)
            size = f.tell()
            offset = max(0, size - int(max_bytes))
            f.seek(offset)
            chunk = f.read().decode("utf-8", errors="ignore")
        lines = [ln for ln in chunk.splitlines() if ln.strip()]
        out: list[dict[str, Any]] = []
        for ln in lines[-int(max_lines) :]:
            try:
                obj = json.loads(ln)
                if isinstance(obj, dict):
                    out.append(obj)
            except Exception:
                continue
        return out
    except Exception:
        return []


def read_last_jsonl_obj(path: Path) -> dict[str, Any] | None:
    out = read_jsonl_tail(path, max_lines=1)
    return out[-1] if out else None


def artifact_age_sec(path: Path) -> float | None:
    try:
        return float(time.time() - float(path.stat().st_mtime))
    except Exception:
        return None


def health_error(state: Any) -> str:
    try:
        if isinstance(state, dict) and isinstance(state.get("health"), dict):
            return str(state["health"].get("error") or "")
    except Exception:
        return ""
    return ""


def status_from_desired_and_state(*, root_exists: bool, desired_mode: str, state_age: float | None) -> str:
    """
    Shared status mapping used by the Trading Console.

    Statuses: not_initialized | desired_idle | starting | running | degraded
    """
    if not bool(root_exists):
        return "not_initialized"
    dm = str(desired_mode or "").strip().lower()
    if dm in {"", "idle"}:
        if state_age is not None and float(state_age) < 15.0:
            return "running"
        return "desired_idle"
    # desired is non-idle
    if state_age is None:
        return "starting"
    return "running" if float(state_age) < 15.0 else "degraded"


__all__ = [
    "read_json_file",
    "read_redis_json",
    "read_jsonl_tail",
    "read_last_jsonl_obj",
    "artifact_age_sec",
    "health_error",
    "status_from_desired_and_state",
]

