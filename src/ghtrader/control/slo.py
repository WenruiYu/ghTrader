from __future__ import annotations

from typing import Any

from ghtrader.config import env_bool, env_int, get_qdb_redis_config

def _env_int(key: str, default: int) -> int:
    return env_int(key, default)


def _env_bool(key: str, default: bool) -> bool:
    return env_bool(key, default)


from ghtrader.util.time import now_iso as _now_iso


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
    cfg = get_qdb_redis_config()
    if not bool(cfg.get("enabled")):
        return {"enabled": False, "ok": True, "status": "disabled"}

    try:
        import redis
    except Exception as e:  # pragma: no cover - optional dependency
        return {"enabled": True, "ok": False, "error": f"redis_import_failed: {e}"}

    host = str(cfg.get("host", "127.0.0.1"))
    port = max(1, int(cfg.get("port", 6379)))
    db = max(0, int(cfg.get("db", 0)))
    timeout_s = max(0.1, float(cfg.get("timeout_s", 0.2)))
    try:
        client = redis.Redis(host=host, port=port, db=db, socket_connect_timeout=timeout_s, socket_timeout=timeout_s)
        client.ping()
        return {"enabled": True, "ok": True, "host": host, "port": port, "db": db}
    except Exception as e:
        return {"enabled": True, "ok": False, "host": host, "port": port, "db": db, "error": str(e)}


def _supervisor_plane(telemetry: dict[str, Any] | None, *, tick_fail_crit: int) -> dict[str, Any]:
    root = telemetry if isinstance(telemetry, dict) else {}
    supervisors = root.get("supervisors") if isinstance(root.get("supervisors"), dict) else {}

    def _bucket(name: str) -> dict[str, Any]:
        raw = supervisors.get(name)
        if not isinstance(raw, dict):
            return {
                "ticks_total": 0,
                "tick_failures_total": 0,
                "started_total": 0,
                "stopped_total": 0,
                "last_tick_at": "",
                "last_tick_error": "",
                "restart_reasons_total": {},
                "stop_reasons_total": {},
            }
        return {
            "ticks_total": int(raw.get("ticks_total") or 0),
            "tick_failures_total": int(raw.get("tick_failures_total") or 0),
            "started_total": int(raw.get("started_total") or 0),
            "stopped_total": int(raw.get("stopped_total") or 0),
            "last_tick_at": str(raw.get("last_tick_at") or ""),
            "last_tick_error": str(raw.get("last_tick_error") or ""),
            "restart_reasons_total": dict(raw.get("restart_reasons_total") or {}),
            "stop_reasons_total": dict(raw.get("stop_reasons_total") or {}),
        }

    strategy = _bucket("strategy")
    gateway = _bucket("gateway")
    scheduler = _bucket("scheduler")
    tick_failures_total = (
        int(strategy.get("tick_failures_total") or 0)
        + int(gateway.get("tick_failures_total") or 0)
        + int(scheduler.get("tick_failures_total") or 0)
    )
    restarts_total = int(strategy.get("started_total") or 0) + int(gateway.get("started_total") or 0)
    if int(tick_failures_total) >= int(max(1, int(tick_fail_crit))):
        state = "error"
    elif int(tick_failures_total) > 0:
        state = "warn"
    else:
        state = "ok"
    return {
        "state": str(state),
        "tick_failures_total": int(tick_failures_total),
        "restarts_total": int(restarts_total),
        "generated_at": str(root.get("generated_at") or ""),
        "strategy": strategy,
        "gateway": gateway,
        "scheduler": scheduler,
    }


def collect_slo_snapshot(*, store: Any | None = None, supervisor_telemetry: dict[str, Any] | None = None) -> dict[str, Any]:
    queue_warn = max(1, _env_int("GHTRADER_SLO_QUEUE_WARN", 64))
    queue_crit = max(queue_warn, _env_int("GHTRADER_SLO_QUEUE_CRIT", 128))
    gpu_min = max(0, _env_int("GHTRADER_SLO_GPU_MIN", 8))
    supervisor_tick_fail_crit = max(1, _env_int("GHTRADER_SLO_SUPERVISOR_TICK_FAIL_CRIT", 10))
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
    supervisor_plane = _supervisor_plane(supervisor_telemetry, tick_fail_crit=supervisor_tick_fail_crit)

    data_state = "ok"
    if require_questdb and not bool(questdb.get("ok")):
        data_state = "error"
    elif require_redis and not bool(redis_state.get("ok")):
        data_state = "error"
    elif bool(redis_state.get("enabled")) and not bool(redis_state.get("ok")):
        data_state = "warn"

    train_state = "ok" if gpus >= gpu_min else ("warn" if gpus > 0 else "error")
    control_state = str(queue_state)
    if control_state != "error":
        if str(supervisor_plane.get("state") or "") == "error":
            control_state = "error"
        elif str(supervisor_plane.get("state") or "") == "warn" and control_state == "ok":
            control_state = "warn"

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
            "supervisor_tick_fail_crit": int(supervisor_tick_fail_crit),
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
            "supervisors": supervisor_plane,
        },
    }
