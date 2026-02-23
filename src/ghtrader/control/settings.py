from __future__ import annotations

from pathlib import Path
from typing import Any

from ghtrader.config_service import get_config_resolver
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
    resolver = get_config_resolver(runs_dir=runs_dir)
    raw, src = resolver.get_raw_with_source("GHTRADER_MAX_PARALLEL_TQSDK_JOBS", None)
    parsed = _parse_positive_int(raw)
    if parsed is None:
        max_parallel = max(1, int(default_max_parallel))
        source = "default"
    else:
        max_parallel = int(parsed)
        source = str(src)

    return {
        "ok": True,
        "max_parallel": int(max_parallel),
        "source": str(source),
        "env_value": "",
        "persisted_value": int(parsed) if parsed is not None else None,
        "settings_path": str(tqsdk_scheduler_settings_path(runs_dir=runs_dir)),
        "config_revision": int(resolver.revision),
        "config_hash": str(resolver.snapshot_hash),
    }


def apply_tqsdk_scheduler_settings_from_disk(*, runs_dir: Path, default_max_parallel: int = 4) -> dict[str, Any]:
    """
    Best-effort: load persisted setting and populate env var on startup.

    We only apply the persisted value if the env var is not already set.
    """
    resolver = get_config_resolver(runs_dir=runs_dir)
    cur_raw, _src = resolver.get_raw_with_source("GHTRADER_MAX_PARALLEL_TQSDK_JOBS", None)
    if _parse_positive_int(cur_raw) is not None:
        return get_tqsdk_scheduler_state(runs_dir=runs_dir, default_max_parallel=default_max_parallel)

    st = read_tqsdk_scheduler_settings(runs_dir=runs_dir) or {}
    legacy_v = _parse_positive_int(st.get("max_parallel"))
    if legacy_v is not None:
        resolver.set_values(
            values={"GHTRADER_MAX_PARALLEL_TQSDK_JOBS": int(legacy_v)},
            actor="system",
            reason="migrate_legacy_scheduler_settings",
            action="migrate_settings",
        )
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

    resolver = get_config_resolver(runs_dir=runs_dir)
    rev = resolver.set_values(
        values={"GHTRADER_MAX_PARALLEL_TQSDK_JOBS": int(v)},
        actor="api",
        reason="update_tqsdk_scheduler_max_parallel",
        action="set",
    )

    if persist:
        # Keep a lightweight compatibility mirror for old operators/tools.
        p = tqsdk_scheduler_settings_path(runs_dir=runs_dir)
        write_json_atomic(
            p,
            {
                "updated_at": _now_iso(),
                "max_parallel": int(v),
                "source": "config_service",
                "revision": int(rev.revision),
            },
        )

    out = get_tqsdk_scheduler_state(runs_dir=runs_dir, default_max_parallel=default_max_parallel)
    out["revision"] = int(rev.revision)
    return out

