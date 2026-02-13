from __future__ import annotations

import os
from datetime import datetime, timezone
from typing import Any


def _env_int(key: str, default: int) -> int:
    try:
        return int(os.environ.get(key, default))
    except Exception:
        return int(default)


def _env_bool(key: str, default: bool) -> bool:
    raw = str(os.environ.get(key, "1" if default else "0") or "").strip().lower()
    return raw in {"1", "true", "yes", "on"}


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _gpu_count() -> int:
    try:
        import torch

        return max(0, int(torch.cuda.device_count()))
    except Exception:
        return 0


def _questdb_status() -> dict[str, Any]:
    try:
        from ghtrader.questdb.client import questdb_reachable_pg

        return dict(questdb_reachable_pg(connect_timeout_s=1, retries=1, backoff_s=0.2))
    except Exception as e:
        return {"ok": False, "error": str(e)}


def _redis_status() -> dict[str, Any]:
    enabled = _env_bool("GHTRADER_QDB_REDIS_CACHE_ENABLED", True)
    if not enabled:
        return {"enabled": False, "ok": True, "status": "disabled"}

    try:
        import redis
    except Exception as e:  # pragma: no cover - optional dependency
        return {"enabled": True, "ok": False, "error": f"redis_import_failed: {e}"}

    host = str(os.environ.get("GHTRADER_QDB_REDIS_HOST", "127.0.0.1"))
    port = max(1, _env_int("GHTRADER_QDB_REDIS_PORT", 6379))
    db = max(0, _env_int("GHTRADER_QDB_REDIS_DB", 0))
    try:
        client = redis.Redis(host=host, port=port, db=db, socket_connect_timeout=0.2, socket_timeout=0.2)
        client.ping()
        return {"enabled": True, "ok": True, "host": host, "port": port, "db": db}
    except Exception as e:
        return {"enabled": True, "ok": False, "host": host, "port": port, "db": db, "error": str(e)}


def collect_slo_snapshot(*, store: Any | None = None) -> dict[str, Any]:
    queue_warn = max(1, _env_int("GHTRADER_SLO_QUEUE_WARN", 64))
    queue_crit = max(queue_warn, _env_int("GHTRADER_SLO_QUEUE_CRIT", 128))
    gpu_min = max(0, _env_int("GHTRADER_SLO_GPU_MIN", 8))
    require_questdb = _env_bool("GHTRADER_SLO_REQUIRE_QUESTDB", True)
    require_redis = _env_bool("GHTRADER_SLO_REQUIRE_REDIS", False)

    running = 0
    queued = 0
    if store is not None:
        try:
            jobs = store.list_jobs(limit=2000)
            running = int(sum(1 for j in jobs if str(j.status or "").lower() == "running"))
            queued = int(sum(1 for j in jobs if str(j.status or "").lower() == "queued"))
        except Exception:
            running = 0
            queued = 0
    queue_depth = int(running + queued)
    if queue_depth >= queue_crit:
        queue_state = "error"
    elif queue_depth >= queue_warn:
        queue_state = "warn"
    else:
        queue_state = "ok"

    questdb = _questdb_status()
    redis_state = _redis_status()
    gpus = _gpu_count()

    data_state = "ok"
    if require_questdb and not bool(questdb.get("ok")):
        data_state = "error"
    elif require_redis and not bool(redis_state.get("ok")):
        data_state = "error"
    elif bool(redis_state.get("enabled")) and not bool(redis_state.get("ok")):
        data_state = "warn"

    train_state = "ok" if gpus >= gpu_min else ("warn" if gpus > 0 else "error")
    control_state = queue_state

    overall = "ok"
    if "error" in {data_state, train_state, control_state}:
        overall = "error"
    elif "warn" in {data_state, train_state, control_state}:
        overall = "warn"

    return {
        "ok": overall != "error",
        "overall": overall,
        "generated_at": _now_iso(),
        "thresholds": {
            "queue_warn": queue_warn,
            "queue_crit": queue_crit,
            "gpu_min": gpu_min,
            "require_questdb": require_questdb,
            "require_redis": require_redis,
        },
        "data_plane": {
            "state": data_state,
            "questdb": questdb,
            "redis": redis_state,
        },
        "training_plane": {
            "state": train_state,
            "gpu_count": int(gpus),
            "gpu_target": int(gpu_min),
        },
        "control_plane": {
            "state": control_state,
            "running_jobs": int(running),
            "queued_jobs": int(queued),
            "queue_depth": int(queue_depth),
        },
    }
