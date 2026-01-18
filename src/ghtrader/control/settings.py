from __future__ import annotations

import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from ghtrader.util.json_io import read_json, write_json_atomic


from ghtrader.util.time import now_iso as _now_iso


def tqsdk_scheduler_settings_path(*, runs_dir: Path) -> Path:
    """
    Persisted dashboard settings for the TqSdk scheduler.

    Stored under runs/ (gitignored) so operators can change via UI and have it survive restarts.
    """
    return runs_dir / "control" / "cache" / "tqsdk_scheduler" / "settings.json"


def _parse_positive_int(v: Any) -> int | None:
    try:
        n = int(v)
    except Exception:
        return None
    return n if n > 0 else None


def read_tqsdk_scheduler_settings(*, runs_dir: Path) -> dict[str, Any] | None:
    p = tqsdk_scheduler_settings_path(runs_dir=runs_dir)
    obj = read_json(p)
    if not isinstance(obj, dict):
        return None
    return obj


def get_tqsdk_scheduler_state(*, runs_dir: Path, default_max_parallel: int = 4) -> dict[str, Any]:
    """
    Return the effective scheduler max_parallel and its source.

    Precedence:
    - env var GHTRADER_MAX_PARALLEL_TQSDK_JOBS (runtime, can be changed by UI)
    - persisted settings under runs/control/cache/
    - default
    """
    env_raw = os.environ.get("GHTRADER_MAX_PARALLEL_TQSDK_JOBS")
    env_val = _parse_positive_int(env_raw)

    persisted_obj = read_tqsdk_scheduler_settings(runs_dir=runs_dir) or {}
    persisted_val = _parse_positive_int(persisted_obj.get("max_parallel"))

    if env_val is not None:
        max_parallel = int(env_val)
        source = "env"
    elif persisted_val is not None:
        max_parallel = int(persisted_val)
        source = "persisted"
    else:
        max_parallel = int(default_max_parallel)
        source = "default"

    max_parallel = max(1, int(max_parallel))

    return {
        "ok": True,
        "max_parallel": int(max_parallel),
        "source": str(source),
        "env_value": str(env_raw) if env_raw is not None else "",
        "persisted_value": int(persisted_val) if persisted_val is not None else None,
        "settings_path": str(tqsdk_scheduler_settings_path(runs_dir=runs_dir)),
    }


def apply_tqsdk_scheduler_settings_from_disk(*, runs_dir: Path, default_max_parallel: int = 4) -> dict[str, Any]:
    """
    Best-effort: load persisted setting and populate env var on startup.

    We only apply the persisted value if the env var is not already set.
    """
    env_raw = os.environ.get("GHTRADER_MAX_PARALLEL_TQSDK_JOBS")
    if _parse_positive_int(env_raw) is not None:
        return get_tqsdk_scheduler_state(runs_dir=runs_dir, default_max_parallel=default_max_parallel)

    st = read_tqsdk_scheduler_settings(runs_dir=runs_dir) or {}
    v = _parse_positive_int(st.get("max_parallel"))
    if v is not None:
        os.environ["GHTRADER_MAX_PARALLEL_TQSDK_JOBS"] = str(int(v))

    return get_tqsdk_scheduler_state(runs_dir=runs_dir, default_max_parallel=default_max_parallel)


def set_tqsdk_scheduler_max_parallel(
    *,
    runs_dir: Path,
    max_parallel: Any,
    persist: bool = True,
    default_max_parallel: int = 4,
    min_v: int = 1,
    max_v: int = 64,
) -> dict[str, Any]:
    """
    Set the scheduler max-parallel for TqSdk-heavy jobs.

    - Applies immediately (updates process env var).
    - Optionally persists under runs/control/cache/ for restart survival.
    """
    v = _parse_positive_int(max_parallel)
    if v is None:
        raise ValueError("max_parallel must be a positive integer")
    v = max(int(min_v), min(int(max_v), int(v)))

    os.environ["GHTRADER_MAX_PARALLEL_TQSDK_JOBS"] = str(int(v))

    if persist:
        p = tqsdk_scheduler_settings_path(runs_dir=runs_dir)
        write_json_atomic(
            p,
            {
                "updated_at": _now_iso(),
                "max_parallel": int(v),
            },
        )

    return get_tqsdk_scheduler_state(runs_dir=runs_dir, default_max_parallel=default_max_parallel)

