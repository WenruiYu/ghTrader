from __future__ import annotations

import json
import time
from datetime import datetime
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


def state_revision(state: Any) -> int | None:
    try:
        if isinstance(state, dict):
            v = int(state.get("state_revision"))
            return int(v) if int(v) >= 0 else None
    except Exception:
        return None
    return None


def _updated_at_ts(state: Any) -> float | None:
    try:
        if not isinstance(state, dict):
            return None
        s = str(state.get("updated_at") or "").strip()
        if not s:
            return None
        if s.endswith("Z"):
            s = s[:-1] + "+00:00"
        return float(datetime.fromisoformat(s).timestamp())
    except Exception:
        return None


def choose_latest_state(redis_state: Any, file_state: Any) -> tuple[dict[str, Any] | None, str]:
    """
    Choose the freshest state snapshot using revision first, then updated_at.
    """
    rs = redis_state if isinstance(redis_state, dict) else None
    fs = file_state if isinstance(file_state, dict) else None
    if rs is None and fs is None:
        return None, "none"
    if rs is None:
        return fs, "file"
    if fs is None:
        return rs, "redis"

    rr = state_revision(rs)
    fr = state_revision(fs)
    if rr is not None or fr is not None:
        if rr is None:
            return fs, "file"
        if fr is None:
            return rs, "redis"
        if int(rr) > int(fr):
            return rs, "redis"
        if int(fr) > int(rr):
            return fs, "file"

    rt = _updated_at_ts(rs)
    ft = _updated_at_ts(fs)
    if rt is not None or ft is not None:
        if rt is None:
            return fs, "file"
        if ft is None:
            return rs, "redis"
        if float(rt) >= float(ft):
            return rs, "redis"
        return fs, "file"

    return rs, "redis"


def read_state_with_revision(*, redis_key: str, file_path: Path) -> tuple[dict[str, Any] | None, str]:
    """
    Read state from Redis and file mirror, then choose freshest snapshot.
    """
    rs = read_redis_json(redis_key)
    fs = read_json(file_path) if file_path.exists() else None
    return choose_latest_state(rs, fs)


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
    "state_revision",
    "choose_latest_state",
    "read_state_with_revision",
    "read_jsonl_tail",
    "read_last_jsonl_obj",
    "artifact_age_sec",
    "health_error",
    "status_from_desired_and_state",
]

