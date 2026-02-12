from __future__ import annotations

import ast
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import structlog

from ghtrader.util.json_io import read_json, write_json_atomic

log = structlog.get_logger()


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def trading_sessions_cache_path(*, data_dir: Path, exchange: str, variety: str) -> Path:
    ex = str(exchange).upper().strip()
    v = str(variety).lower().strip()
    return data_dir / "trading_sessions" / f"sessions_exchange={ex}_var={v}.json"


def _parse_trading_time(raw: Any) -> dict[str, Any]:
    if isinstance(raw, dict):
        return raw
    try:
        if isinstance(raw, str):
            s = raw.strip()
        else:
            s = str(raw).strip()
    except Exception:
        return {}
    if not s:
        return {}
    try:
        # TqSdk uses single quotes; ast.literal_eval is tolerant.
        obj = ast.literal_eval(s)
        return obj if isinstance(obj, dict) else {}
    except Exception:
        try:
            obj = json.loads(s)
            return obj if isinstance(obj, dict) else {}
        except Exception:
            return {}


def _time_to_minutes(t: str) -> int | None:
    try:
        parts = t.split(":")
        if len(parts) < 2:
            return None
        h = int(parts[0])
        m = int(parts[1])
        return int(h * 60 + m)
    except Exception:
        return None


def normalize_trading_sessions(*, trading_time: Any, exchange: str, variety: str) -> dict[str, Any]:
    parsed = _parse_trading_time(trading_time)
    sessions: list[dict[str, Any]] = []
    for key in ("day", "night"):
        windows = parsed.get(key) if isinstance(parsed, dict) else None
        if not isinstance(windows, list):
            continue
        for w in windows:
            if not isinstance(w, (list, tuple)) or len(w) < 2:
                continue
            start = str(w[0]).strip()
            end = str(w[1]).strip()
            if not start or not end:
                continue
            end_min = _time_to_minutes(end)
            sessions.append(
                {
                    "session": key,
                    "start": start,
                    "end": end,
                    "cross_midnight": bool(end_min is not None and end_min >= 24 * 60),
                }
            )

    try:
        raw_str = json.dumps(parsed, ensure_ascii=True, sort_keys=True)
    except Exception:
        raw_str = str(trading_time or "")

    return {
        "exchange": str(exchange).upper().strip(),
        "variety": str(variety).lower().strip(),
        "raw_trading_time": raw_str,
        "sessions": sessions,
        "updated_at": _now_iso(),
    }


def write_trading_sessions_cache(*, data_dir: Path, exchange: str, variety: str, payload: dict[str, Any]) -> Path:
    path = trading_sessions_cache_path(data_dir=data_dir, exchange=exchange, variety=variety)
    path.parent.mkdir(parents=True, exist_ok=True)
    write_json_atomic(path, payload)
    return path


def read_trading_sessions_cache(*, data_dir: Path, exchange: str, variety: str) -> dict[str, Any] | None:
    path = trading_sessions_cache_path(data_dir=data_dir, exchange=exchange, variety=variety)
    if not path.exists():
        return None
    try:
        obj = read_json(path)
        return obj if isinstance(obj, dict) else None
    except Exception as e:
        log.warning("trading_sessions.cache_read_failed", path=str(path), error=str(e))
        return None
