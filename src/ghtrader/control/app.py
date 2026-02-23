from __future__ import annotations

import os
import re
import time
import json
import uuid
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import structlog
import asyncio
from fastapi import FastAPI, HTTPException, Request, WebSocket, WebSocketDisconnect
from fastapi.responses import JSONResponse
from fastapi.staticfiles import StaticFiles
import redis.asyncio as redis

from ghtrader.config_service import enforce_no_legacy_env, get_config_resolver
from ghtrader.config import get_artifacts_dir, get_data_dir, get_runs_dir
from ghtrader.control import auth
from ghtrader.control.db import JobStore
from ghtrader.control.job_command import extract_cli_subcommand, extract_cli_subcommands
from ghtrader.control.jobs import JobManager, JobSpec, python_module_argv
from ghtrader.control.state_helpers import (
    artifact_age_sec as _artifact_age_sec,
    health_error as _health_error,
    read_json_file as _read_json_file,
    read_jsonl_tail as _read_jsonl_tail,
    read_last_jsonl_obj as _read_last_jsonl_obj,
    read_redis_json as _read_redis_json,
    status_from_desired_and_state as _status_from_desired_and_state,
)
from ghtrader.control.supervisor_helpers import (
    argv_opt as _argv_opt,
    is_gateway_job as _is_gateway_job,
    is_strategy_job as _is_strategy_job,
)
from ghtrader.control.supervisors import (
    gateway_supervisor_enabled as _gateway_supervisor_enabled_impl,
    gateway_supervisor_tick as _gateway_supervisor_tick_impl,
    scheduler_enabled as _scheduler_enabled_impl,
    start_gateway_supervisor as _start_gateway_supervisor_impl,
    start_strategy_supervisor as _start_strategy_supervisor_impl,
    start_tqsdk_scheduler as _start_tqsdk_scheduler_impl,
    strategy_supervisor_enabled as _strategy_supervisor_enabled_impl,
    strategy_supervisor_tick as _strategy_supervisor_tick_impl,
    tqsdk_scheduler_tick as _tqsdk_scheduler_tick_impl,
)
from ghtrader.control.routes.accounts import router as accounts_router
from ghtrader.control.routes.gateway import gateway_status_payload, router as gateway_router
from ghtrader.control.routes.models import router as models_router
from ghtrader.control.routes import build_api_router, build_root_router
from ghtrader.control.variety_context import allowed_varieties as _allowed_varieties
from ghtrader.control.variety_context import default_variety as _default_variety
from ghtrader.control.variety_context import derived_symbol_for_variety as _derived_symbol_for_variety
from ghtrader.control.variety_context import symbol_matches_variety as _symbol_matches_variety_impl
from ghtrader.control.cache import TTLCacheSlot
from ghtrader.control.views import _data_page_cache_clear, build_router
from ghtrader.util.json_io import read_json as _read_json
from ghtrader.util.observability import get_store

log = structlog.get_logger()

_TQSDK_HEAVY_SUBCOMMANDS = {"account"}
_TQSDK_HEAVY_DATA_SUBCOMMANDS = {"repair", "health", "main-l5-validate"}

_UI_STATUS_TTL_S = 5.0
_ui_status_cache = TTLCacheSlot()

_DASH_SUMMARY_TTL_S = 10.0
_dash_summary_cache = TTLCacheSlot()

_DATA_QUALITY_TTL_S = 60.0
_data_quality_cache = TTLCacheSlot()


from ghtrader.util.time import now_iso as _now_iso


def _iter_symbol_dirs(root: Path) -> set[str]:
    out: set[str] = set()
    try:
        if not root.exists():
            return out
        for p in root.iterdir():
            if not p.is_dir() or not p.name.startswith("symbol="):
                continue
            sym = p.name.split("=", 1)[-1]
            if sym:
                out.add(sym)
    except Exception:
        return out
    return out


def _clear_data_quality_cache() -> None:
    _data_quality_cache.clear()


def _scan_model_files(artifacts_dir: Path) -> list[dict[str, Any]]:
    """
    Return a list of discovered model artifact files.

    ghTrader canonical layout:
      artifacts/<symbol>/<model_type>/model_h<h>.(json|pt|pkl)
    Also allow namespaced layouts:
      artifacts/(production|candidates)/<symbol>/<model_type>/model_h<h>.*
    """
    out: list[dict[str, Any]] = []
    if not artifacts_dir.exists():
        return out
    try:
        import re

        pat = re.compile(r"^model_h(?P<h>\d+)\.[a-zA-Z0-9]+$")
        allowed_ext = {".json", ".pt", ".pkl"}
        for f in artifacts_dir.rglob("model_h*.*"):
            try:
                if not f.is_file():
                    continue
                if f.suffix.lower() not in allowed_ext:
                    continue
                m = pat.match(f.name)
                if not m:
                    continue
                rel = f.relative_to(artifacts_dir).parts
                if len(rel) < 3:
                    continue
                namespace = None
                sym = None
                model_type = None
                if rel[0] in {"production", "candidates", "temp"} and len(rel) >= 4:
                    namespace = rel[0]
                    sym = rel[1]
                    model_type = rel[2]
                else:
                    sym = rel[0]
                    model_type = rel[1]
                horizon = int(m.group("h"))
                st = f.stat()
                out.append(
                    {
                        "symbol": str(sym),
                        "model_type": str(model_type),
                        "horizon": int(horizon),
                        "namespace": str(namespace) if namespace else "",
                        "path": str(f),
                        "size_bytes": int(st.st_size),
                        "mtime": float(st.st_mtime),
                    }
                )
            except Exception:
                continue
    except Exception:
        return []
    return out


def _job_subcommand(argv: list[str]) -> str | None:
    return extract_cli_subcommand(argv)


def _job_subcommand2(argv: list[str]) -> tuple[str | None, str | None]:
    return extract_cli_subcommands(argv)


def _is_tqsdk_heavy_job(argv: list[str]) -> bool:
    sub1, sub2 = _job_subcommand2(argv)
    s1 = sub1 or ""
    s2 = sub2 or ""
    if s1 in _TQSDK_HEAVY_SUBCOMMANDS:
        return True
    # Nested CLI groups (e.g., `ghtrader data health`) may use TqSdk and must be throttled.
    if s1 == "data" and s2 in _TQSDK_HEAVY_DATA_SUBCOMMANDS:
        return True
    return False


def _normalize_variety_for_api(raw: str | None, *, allow_legacy_default: bool = False) -> str:
    v = str(raw or "").strip().lower()
    if not v:
        if allow_legacy_default:
            return _default_variety()
        raise HTTPException(status_code=400, detail="var is required")
    if v not in _allowed_varieties():
        allowed = ",".join(_allowed_varieties())
        raise HTTPException(status_code=400, detail=f"invalid var '{v}', allowed: {allowed}")
    return v


def _symbol_matches_variety(symbol: str | None, variety: str | None) -> bool:
    return _symbol_matches_variety_impl(symbol, variety)


def _job_matches_variety(job: Any, variety: str) -> bool:
    v = str(variety or "").strip().lower()
    if not v:
        return True
    meta = getattr(job, "metadata", None)
    if isinstance(meta, dict):
        mv = str(meta.get("variety") or "").strip().lower()
        if mv in set(_allowed_varieties()):
            return mv == v
        ms = str(meta.get("symbol") or "").strip()
        if ms:
            return _symbol_matches_variety(ms, v)
        mss = meta.get("symbols")
        if isinstance(mss, list) and mss:
            return any(_symbol_matches_variety(str(x or ""), v) for x in mss)
    cmd = [str(x or "").lower() for x in (getattr(job, "command", None) or [])]
    for i, tok in enumerate(cmd[:-1]):
        if tok == "--var" and cmd[i + 1] == v:
            return True
    if any(_symbol_matches_variety(tok, v) for tok in cmd):
        return True
    title = str(getattr(job, "title", "") or "").lower()
    if _symbol_matches_variety(title, v):
        return True
    return f" {v} " in f" {title} "


def _dashboard_guardrails_context() -> dict[str, Any]:
    resolver = get_config_resolver()
    enforce_health_global = resolver.get_bool("GHTRADER_PIPELINE_ENFORCE_HEALTH", True)
    enforce_health_schedule = resolver.get_bool("GHTRADER_MAIN_SCHEDULE_ENFORCE_HEALTH", enforce_health_global)
    enforce_health_main_l5 = resolver.get_bool("GHTRADER_MAIN_L5_ENFORCE_HEALTH", enforce_health_global)

    lock_wait_timeout_s = resolver.get_float("GHTRADER_LOCK_WAIT_TIMEOUT_S", 120.0, min_value=0.0)
    lock_poll_interval_s = resolver.get_float("GHTRADER_LOCK_POLL_INTERVAL_S", 1.0, min_value=0.1)
    lock_force_cancel = resolver.get_bool("GHTRADER_LOCK_FORCE_CANCEL_ON_TIMEOUT", True)
    lock_preempt_grace_s = resolver.get_float("GHTRADER_LOCK_PREEMPT_GRACE_S", 8.0, min_value=1.0)

    total_workers_raw, _src_total = resolver.get_raw_with_source("GHTRADER_MAIN_L5_TOTAL_WORKERS", None)
    segment_workers_raw, _src_seg = resolver.get_raw_with_source("GHTRADER_MAIN_L5_SEGMENT_WORKERS", None)
    total_workers_raw = str(total_workers_raw or "").strip() if total_workers_raw is not None else ""
    segment_workers_raw = str(segment_workers_raw or "").strip() if segment_workers_raw is not None else ""
    total_workers = resolver.get_int("GHTRADER_MAIN_L5_TOTAL_WORKERS", 0, min_value=0) if total_workers_raw else 0
    segment_workers = resolver.get_int("GHTRADER_MAIN_L5_SEGMENT_WORKERS", 0, min_value=0) if segment_workers_raw else 0
    health_gate_strict = bool(enforce_health_schedule and enforce_health_main_l5)

    return {
        "enforce_health_global": bool(enforce_health_global),
        "enforce_health_schedule": bool(enforce_health_schedule),
        "enforce_health_main_l5": bool(enforce_health_main_l5),
        "health_gate_state": ("ok" if health_gate_strict else "warn"),
        "health_gate_label": ("strict" if health_gate_strict else "partial"),
        "lock_wait_timeout_s": float(lock_wait_timeout_s),
        "lock_poll_interval_s": float(lock_poll_interval_s),
        "lock_force_cancel_on_timeout": bool(lock_force_cancel),
        "lock_preempt_grace_s": float(lock_preempt_grace_s),
        "lock_recovery_state": ("ok" if lock_force_cancel else "warn"),
        "main_l5_total_workers": int(total_workers),
        "main_l5_segment_workers": int(segment_workers),
        "main_l5_worker_mode": ("bounded" if (total_workers > 0 or segment_workers > 0) else "auto"),
    }


def _ui_state_payload(
    *,
    state: str,
    text: str,
    error: str = "",
    stale: bool = False,
    updated_at: str | None = None,
    **extra: Any,
) -> dict[str, Any]:
    payload: dict[str, Any] = {
        "state": str(state or "unknown"),
        "text": str(text or ""),
        "error": str(error or ""),
        "stale": bool(stale),
        "updated_at": str(updated_at or _now_iso()),
    }
    if extra:
        payload.update(extra)
    return payload


def _scheduler_enabled() -> bool:
    return _scheduler_enabled_impl()


def _start_tqsdk_scheduler(app: FastAPI, *, max_parallel_default: int = 4) -> None:
    _start_tqsdk_scheduler_impl(
        app=app,
        log=log,
        tick=lambda store, jm, max_parallel: _tqsdk_scheduler_tick(
            store=store,
            jm=jm,
            max_parallel=max_parallel,
        ),
        max_parallel_default=max_parallel_default,
    )


def _tqsdk_scheduler_tick(*, store: JobStore, jm: JobManager, max_parallel: int) -> int:
    """
    Run one scheduling tick: start up to `max_parallel` TqSdk-heavy queued jobs.

    Exposed for unit tests.
    """
    return _tqsdk_scheduler_tick_impl(
        store=store,
        jm=jm,
        max_parallel=max_parallel,
        is_tqsdk_heavy_job=_is_tqsdk_heavy_job,
    )


def _gateway_supervisor_enabled() -> bool:
    return _gateway_supervisor_enabled_impl()


def _strategy_supervisor_enabled() -> bool:
    return _strategy_supervisor_enabled_impl()


def _strategy_supervisor_tick(*, store: JobStore, jm: Any, runs_dir: Path) -> dict[str, int]:
    """
    Run one StrategySupervisor tick.

    Exposed for unit tests (pass fake `jm` to avoid launching subprocesses).
    """
    return _strategy_supervisor_tick_impl(store=store, jm=jm, runs_dir=runs_dir)


def _gateway_supervisor_tick(*, store: JobStore, jm: Any, runs_dir: Path) -> dict[str, int]:
    """
    Run one GatewaySupervisor tick.

    Exposed for unit tests (pass fake `jm` to avoid launching subprocesses).
    """
    return _gateway_supervisor_tick_impl(store=store, jm=jm, runs_dir=runs_dir)


def _start_strategy_supervisor(app: FastAPI) -> None:
    _start_strategy_supervisor_impl(
        app=app,
        log=log,
        tick=lambda store, jm, runs_dir: _strategy_supervisor_tick(
            store=store,
            jm=jm,
            runs_dir=runs_dir,
        ),
        get_runs_dir=get_runs_dir,
    )


def _start_gateway_supervisor(app: FastAPI) -> None:
    _start_gateway_supervisor_impl(
        app=app,
        log=log,
        tick=lambda store, jm, runs_dir: _gateway_supervisor_tick(
            store=store,
            jm=jm,
            runs_dir=runs_dir,
        ),
        get_runs_dir=get_runs_dir,
    )


def _control_root(runs_dir: Path) -> Path:
    return runs_dir / "control"


def _jobs_db_path(runs_dir: Path) -> Path:
    return _control_root(runs_dir) / "jobs.db"


def _logs_dir(runs_dir: Path) -> Path:
    return _control_root(runs_dir) / "logs"


class ConnectionManager:
    def __init__(self) -> None:
        self.active_connections: list[WebSocket] = []
        self.redis: redis.Redis | None = None
        self.pubsub: Any = None
        self.task: asyncio.Task | None = None

    async def connect(self, websocket: WebSocket) -> None:
        await websocket.accept()
        self.active_connections.append(websocket)
        if not self.redis:
            await self._start_redis()

    def disconnect(self, websocket: WebSocket) -> None:
        if websocket in self.active_connections:
            self.active_connections.remove(websocket)

    async def broadcast(self, message: str) -> None:
        for connection in self.active_connections:
            try:
                await connection.send_text(message)
            except Exception:
                pass

    async def _start_redis(self) -> None:
        try:
            self.redis = redis.Redis(host="localhost", port=6379, db=0, decode_responses=True)
            self.pubsub = self.redis.pubsub()
            await self.pubsub.psubscribe("ghtrader:*:updates:*")
            self.task = asyncio.create_task(self._redis_listener())
        except Exception as e:
            log.warning("websocket.redis_connect_failed", error=str(e))

    async def _redis_listener(self) -> None:
        if not self.pubsub:
            return
        try:
            async for message in self.pubsub.listen():
                if message["type"] == "pmessage":
                    channel = message["channel"]
                    data = message["data"]
                    try:
                        payload = json.loads(data)
                    except Exception:
                        payload = data
                    await self.broadcast(json.dumps({"channel": channel, "data": payload}, default=str))
        except Exception as e:
            log.warning("websocket.redis_listener_error", error=str(e))


def create_app() -> Any:
    runs_dir = get_runs_dir()
    enforce_no_legacy_env()
    store = JobStore(_jobs_db_path(runs_dir))
    jm = JobManager(store=store, logs_dir=_logs_dir(runs_dir))
    jm.reconcile()

    # Apply persisted dashboard settings before starting background schedulers.
    try:
        from ghtrader.control.settings import apply_tqsdk_scheduler_settings_from_disk

        apply_tqsdk_scheduler_settings_from_disk(runs_dir=runs_dir)
    except Exception:
        pass

    app = FastAPI(title="ghTrader Control", version="0.1.0")
    obs = get_store("control.api")

    @app.middleware("http")
    async def _observe_http_requests(request: Request, call_next: Any) -> Any:
        started = time.perf_counter()
        ok = False
        try:
            response = await call_next(request)
            status = int(getattr(response, "status_code", 500))
            ok = status < 500
            return response
        finally:
            route = request.scope.get("route")
            route_path = str(getattr(route, "path", "") or request.url.path or "/")
            obs.observe(
                metric=f"{request.method} {route_path}",
                latency_s=(time.perf_counter() - started),
                ok=bool(ok),
            )
    app.state.job_store = store
    app.state.job_manager = jm
    if _scheduler_enabled():
        _start_tqsdk_scheduler(app)
    if _gateway_supervisor_enabled():
        _start_gateway_supervisor(app)
    if _strategy_supervisor_enabled():
        _start_strategy_supervisor(app)

    # Static assets (CSS)
    static_dir = Path(__file__).parent / "static"
    app.mount("/static", StaticFiles(directory=str(static_dir)), name="static")

    # HTML pages
    app.include_router(build_router())
    app.include_router(build_api_router())
    app.include_router(build_root_router())

    # WebSocket Endpoint
    manager = ConnectionManager()

    @app.websocket("/ws/dashboard")
    async def websocket_endpoint(websocket: WebSocket) -> None:
        await manager.connect(websocket)
        try:
            while True:
                await websocket.receive_text()
        except WebSocketDisconnect:
            manager.disconnect(websocket)

    @app.get("/api/data/quality-summary", response_class=JSONResponse)
    def api_data_quality_summary(
        request: Request,
        exchange: str = "SHFE",
        var: str = "",
        limit: int = 300,
        search: str = "",
        issues_only: bool = False,
        refresh: bool = False,
    ) -> dict[str, Any]:
        """
        Return per-symbol data quality summary (QuestDB-first, cached).
        """
        if not auth.is_authorized(request):
            raise HTTPException(status_code=401, detail="Unauthorized")
        # Keep this as a best-effort read endpoint; unlike other deferred APIs it has
        # a complete implementation and does not enqueue removed CLI surfaces.

        ex = str(exchange).upper().strip() or "SHFE"
        v = _normalize_variety_for_api(var)
        lim = max(1, min(int(limit or 300), 500))
        q = str(search or "").strip().lower()
        only_issues = bool(issues_only)

        now = time.time()
        cache_key = f"{ex}|{v}|{lim}|{q}|{int(only_issues)}"
        cached = _data_quality_cache.get(ttl_s=_DATA_QUALITY_TTL_S, key=cache_key, now=now)
        if isinstance(cached, dict) and not refresh:
            return dict(cached)

        runs_dir = get_runs_dir()
        snap_path = runs_dir / "control" / "cache" / "contracts_snapshot" / f"contracts_exchange={ex}_var={v}.json"
        snap = _read_json(snap_path) if snap_path.exists() else None
        snap = dict(snap) if isinstance(snap, dict) else {}

        contracts = snap.get("contracts") if isinstance(snap.get("contracts"), list) else []
        comp_summary = None
        try:
            comp = snap.get("completeness") if isinstance(snap.get("completeness"), dict) else {}
            comp_summary = comp.get("summary") if isinstance(comp.get("summary"), dict) else None
        except Exception:
            comp_summary = None

        # Base rows (from cached snapshot; safe to load).
        base_rows: list[dict[str, Any]] = []
        freshness_max_age_sec: float | None = None
        for r in contracts:
            if not isinstance(r, dict):
                continue
            sym = str(r.get("symbol") or "").strip()
            if not sym:
                continue
            if q and q not in sym.lower():
                continue

            comp = r.get("completeness") if isinstance(r.get("completeness"), dict) else {}
            qc = r.get("questdb_coverage") if isinstance(r.get("questdb_coverage"), dict) else {}
            missing_days = comp.get("missing_days")
            expected_days = comp.get("expected_days")
            present_days = comp.get("present_days")
            status = str(r.get("status") or "").strip() or None
            last_tick_age_sec = r.get("last_tick_age_sec")
            try:
                if r.get("expired") is False and last_tick_age_sec is not None:
                    age = float(last_tick_age_sec)
                    if freshness_max_age_sec is None or age > freshness_max_age_sec:
                        freshness_max_age_sec = age
            except Exception:
                pass

            base_rows.append(
                {
                    "symbol": sym,
                    "status": status,
                    "last_tick_ts": str(qc.get("last_tick_ts") or "").strip() or None,
                    "last_tick_day": str(qc.get("last_tick_day") or "").strip() or None,
                    "missing_days": (int(missing_days) if missing_days is not None else None),
                    "expected_days": (int(expected_days) if expected_days is not None else None),
                    "present_days": (int(present_days) if present_days is not None else None),
                    "last_tick_age_sec": (float(last_tick_age_sec) if last_tick_age_sec is not None else None),
                }
            )
            if len(base_rows) >= lim:
                break

        # Join latest QuestDB quality ledgers when available.
        fq_by_sym: dict[str, dict[str, Any]] = {}
        gaps_by_sym: dict[str, dict[str, Any]] = {}
        fq_ok = False
        gaps_ok = False
        fq_error: str | None = None
        gaps_error: str | None = None
        questdb_error: str | None = None
        errors: list[str] = []
        try:
            from ghtrader.questdb.client import connect_pg as _connect, make_questdb_query_config_from_env

            cfg = make_questdb_query_config_from_env()
            syms = [r["symbol"] for r in base_rows if str(r.get("symbol") or "").strip()]
            if syms:
                placeholders = ", ".join(["%s"] * len(syms))
                fq_sql = (
                    "SELECT symbol, trading_day, rows_total, "
                    "bid_price1_null_rate, ask_price1_null_rate, last_price_null_rate, volume_null_rate, open_interest_null_rate, "
                    "l5_fields_null_rate, price_jump_outliers, volume_spike_outliers, spread_anomaly_outliers, updated_at "
                    "FROM ghtrader_field_quality_v2 "
                    f"WHERE symbol IN ({placeholders}) AND ticks_kind='main_l5' AND dataset_version='v2' "
                    "LATEST ON ts PARTITION BY symbol"
                )
                gaps_sql = (
                    "SELECT symbol, trading_day, tick_count_actual, tick_count_expected_min, tick_count_expected_max, "
                    "median_interval_ms, p95_interval_ms, p99_interval_ms, max_interval_ms, "
                    "abnormal_gaps_count, critical_gaps_count, largest_gap_duration_ms, updated_at "
                    "FROM ghtrader_tick_gaps_v2 "
                    f"WHERE symbol IN ({placeholders}) AND ticks_kind='main_l5' AND dataset_version='v2' "
                    "LATEST ON ts PARTITION BY symbol"
                )
                with _connect(cfg, connect_timeout_s=2) as conn:
                    with conn.cursor() as cur:
                        try:
                            cur.execute(fq_sql, syms)
                            for row in cur.fetchall() or []:
                                s = str(row[0] or "").strip()
                                if not s:
                                    continue
                                fq_by_sym[s] = {
                                    "trading_day": str(row[1] or "").strip() or None,
                                    "rows_total": (int(row[2]) if row[2] is not None else None),
                                    "bid_price1_null_rate": (float(row[3]) if row[3] is not None else None),
                                    "ask_price1_null_rate": (float(row[4]) if row[4] is not None else None),
                                    "last_price_null_rate": (float(row[5]) if row[5] is not None else None),
                                    "volume_null_rate": (float(row[6]) if row[6] is not None else None),
                                    "open_interest_null_rate": (float(row[7]) if row[7] is not None else None),
                                    "l5_fields_null_rate": (float(row[8]) if row[8] is not None else None),
                                    "price_jump_outliers": (int(row[9]) if row[9] is not None else 0),
                                    "volume_spike_outliers": (int(row[10]) if row[10] is not None else 0),
                                    "spread_anomaly_outliers": (int(row[11]) if row[11] is not None else 0),
                                    "updated_at": str(row[12] or "").strip() or None,
                                }
                            fq_ok = True
                        except Exception as e:
                            fq_error = str(e)
                            errors.append(f"field_quality: {fq_error}")
                            fq_ok = False
                        try:
                            cur.execute(gaps_sql, syms)
                            for row in cur.fetchall() or []:
                                s = str(row[0] or "").strip()
                                if not s:
                                    continue
                                gaps_by_sym[s] = {
                                    "trading_day": str(row[1] or "").strip() or None,
                                    "tick_count_actual": (int(row[2]) if row[2] is not None else None),
                                    "tick_count_expected_min": (int(row[3]) if row[3] is not None else None),
                                    "tick_count_expected_max": (int(row[4]) if row[4] is not None else None),
                                    "median_interval_ms": (int(row[5]) if row[5] is not None else None),
                                    "p95_interval_ms": (int(row[6]) if row[6] is not None else None),
                                    "p99_interval_ms": (int(row[7]) if row[7] is not None else None),
                                    "max_interval_ms": (int(row[8]) if row[8] is not None else None),
                                    "abnormal_gaps_count": (int(row[9]) if row[9] is not None else 0),
                                    "critical_gaps_count": (int(row[10]) if row[10] is not None else 0),
                                    "largest_gap_duration_ms": (int(row[11]) if row[11] is not None else None),
                                    "updated_at": str(row[12] or "").strip() or None,
                                }
                            gaps_ok = True
                        except Exception as e:
                            gaps_error = str(e)
                            errors.append(f"tick_gaps: {gaps_error}")
                            gaps_ok = False
        except Exception as e:
            questdb_error = str(e)
            errors.append(f"questdb: {questdb_error}")
            fq_ok = False
            gaps_ok = False

        # Compute derived metrics + per-symbol table.
        out_rows: list[dict[str, Any]] = []
        quality_scores: list[float] = []
        anomalies_total = 0
        for r in base_rows:
            sym = str(r.get("symbol") or "").strip()
            fq = fq_by_sym.get(sym) or {}
            gp = gaps_by_sym.get(sym) or {}

            rates = [
                fq.get("bid_price1_null_rate"),
                fq.get("ask_price1_null_rate"),
                fq.get("last_price_null_rate"),
                fq.get("volume_null_rate"),
                fq.get("open_interest_null_rate"),
            ]
            rates2 = [float(x) for x in rates if isinstance(x, (int, float))]
            score = None
            if rates2:
                m = float(sum(rates2)) / float(len(rates2))
                score = float(max(0.0, min(1.0, 1.0 - m)))
                quality_scores.append(score)

            outliers = int(fq.get("price_jump_outliers") or 0) + int(fq.get("volume_spike_outliers") or 0) + int(fq.get("spread_anomaly_outliers") or 0)
            gaps = int(gp.get("abnormal_gaps_count") or 0) + int(gp.get("critical_gaps_count") or 0)
            anomalies = int(outliers + gaps)
            anomalies_total += int(anomalies)

            # Simple status classification.
            st = "ok"
            try:
                if (fq.get("bid_price1_null_rate") is not None and float(fq.get("bid_price1_null_rate") or 0.0) > 0.01) or (
                    fq.get("ask_price1_null_rate") is not None and float(fq.get("ask_price1_null_rate") or 0.0) > 0.01
                ):
                    st = "error"
                elif int(gp.get("critical_gaps_count") or 0) > 0:
                    st = "warning"
                elif anomalies > 0:
                    st = "warning"
            except Exception:
                st = "unknown"

            if only_issues and st not in {"warning", "error"}:
                continue

            out_rows.append(
                {
                    "symbol": sym,
                    "status": st,
                    "last_tick_ts": r.get("last_tick_ts"),
                    "missing_days": r.get("missing_days"),
                    "expected_days": r.get("expected_days"),
                    "present_days": r.get("present_days"),
                    "field_quality_score": (round(score * 100.0, 2) if isinstance(score, (int, float)) else None),
                    "l1_null_rates": {k: fq.get(k) for k in ["bid_price1_null_rate", "ask_price1_null_rate", "last_price_null_rate", "volume_null_rate", "open_interest_null_rate"]},
                    "l5_fields_null_rate": fq.get("l5_fields_null_rate"),
                    "outliers": {
                        "price_jump": int(fq.get("price_jump_outliers") or 0),
                        "volume_spike": int(fq.get("volume_spike_outliers") or 0),
                        "spread_anomaly": int(fq.get("spread_anomaly_outliers") or 0),
                    },
                    "gaps": {
                        "abnormal": int(gp.get("abnormal_gaps_count") or 0),
                        "critical": int(gp.get("critical_gaps_count") or 0),
                        "largest_gap_duration_ms": gp.get("largest_gap_duration_ms"),
                        "p99_interval_ms": gp.get("p99_interval_ms"),
                        "max_interval_ms": gp.get("max_interval_ms"),
                    },
                    "ledger_days": {
                        "field_quality_day": fq.get("trading_day"),
                        "tick_gaps_day": gp.get("trading_day"),
                    },
                }
            )

        completeness_pct = None
        try:
            if isinstance(comp_summary, dict):
                total = float(comp_summary.get("symbols_total") or comp_summary.get("symbols") or 0.0)
                complete = float(comp_summary.get("symbols_complete") or 0.0)
                if total > 0:
                    completeness_pct = (complete / total) * 100.0
        except Exception:
            completeness_pct = None

        field_quality_pct = None
        try:
            if quality_scores:
                field_quality_pct = (float(sum(quality_scores)) / float(len(quality_scores))) * 100.0
        except Exception:
            field_quality_pct = None

        payload = {
            "ok": True,
            "exchange": ex,
            "var": v,
            "generated_at": _now_iso(),
            "kpis": {
                "completeness_pct": completeness_pct,
                "field_quality_pct": field_quality_pct,
                "anomalies_total": int(anomalies_total),
                "freshness_max_age_sec": freshness_max_age_sec,
            },
            "sources": {
                "contracts_snapshot": str(snap_path) if snap_path.exists() else None,
                "field_quality_ok": bool(fq_ok),
                "tick_gaps_ok": bool(gaps_ok),
                "field_quality_error": fq_error,
                "tick_gaps_error": gaps_error,
                "questdb_error": questdb_error,
            },
            "errors": errors,
            "count": int(len(out_rows)),
            "rows": out_rows,
        }
        if errors and not out_rows:
            payload["ok"] = False
            payload["error"] = errors[0]
        _data_quality_cache.set(dict(payload), key=cache_key, now=time.time())
        return payload

    @app.post("/api/data/enqueue-l5-start", response_class=JSONResponse)
    async def api_data_enqueue_l5_start(request: Request) -> dict[str, Any]:
        """
        Enqueue an L5-start computation job (runs `ghtrader data l5-start ...`).
        """
        if not auth.is_authorized(request):
            raise HTTPException(status_code=401, detail="Unauthorized")
        payload = await request.json()

        runs_dir_default = get_runs_dir()
        data_dir_default = get_data_dir()
        runs_dir = Path(str(payload.get("runs_dir") or str(runs_dir_default)).strip() or str(runs_dir_default))
        data_dir = Path(str(payload.get("data_dir") or str(data_dir_default)).strip() or str(data_dir_default))

        ex = str(payload.get("exchange") or "SHFE").upper().strip()
        v = _normalize_variety_for_api(payload.get("var"))
        refresh_catalog = bool(payload.get("refresh_catalog", False))

        symbols: list[str] = []
        raw_syms = payload.get("symbols")
        if isinstance(raw_syms, list):
            symbols = [str(s).strip() for s in raw_syms if str(s).strip()]

        argv = python_module_argv(
            "ghtrader.cli",
            "data",
            "l5-start",
            "--exchange",
            ex,
            "--var",
            v,
            "--refresh-catalog",
            ("1" if refresh_catalog else "0"),
            "--data-dir",
            str(data_dir),
            "--runs-dir",
            str(runs_dir),
        )
        for s in symbols:
            argv += ["--symbol", s]

        title = f"data-l5-start {ex}.{v}"
        store = request.app.state.job_store
        jm = request.app.state.job_manager

        # Dedupe: avoid duplicate l5-start jobs.
        try:
            active = store.list_active_jobs() + store.list_unstarted_queued_jobs(limit=2000)
            for j in active:
                if str(j.title or "").startswith(title):
                    if j.pid is None and str(j.status or "") == "queued":
                        started = jm.start_queued_job(j.id) or j
                        _data_page_cache_clear()
                        _clear_data_quality_cache()
                        return {"ok": True, "enqueued": [started.id], "count": 1, "deduped": True, "started": bool(started.pid)}
                    _data_page_cache_clear()
                    _clear_data_quality_cache()
                    return {"ok": True, "enqueued": [j.id], "count": 1, "deduped": True}
        except Exception:
            pass

        # L5-start is index-backed and light: start immediately.
        rec = jm.start_job(
            JobSpec(
                title=title,
                argv=argv,
                cwd=Path.cwd(),
                metadata={
                    "kind": "data_l5_start",
                    "exchange": ex,
                    "variety": v,
                    "symbol": _derived_symbol_for_variety(v, exchange=ex),
                    "symbols": symbols,
                },
            )
        )
        _data_page_cache_clear()
        _clear_data_quality_cache()
        return {"ok": True, "enqueued": [rec.id], "count": 1, "deduped": False}

    @app.post("/api/data/enqueue-main-l5-validate", response_class=JSONResponse)
    async def api_data_enqueue_main_l5_validate(request: Request) -> dict[str, Any]:
        """
        Enqueue a main_l5 validation job (runs `ghtrader data main-l5-validate ...`).
        """
        if not auth.is_authorized(request):
            raise HTTPException(status_code=401, detail="Unauthorized")
        payload = await request.json()

        runs_dir = get_runs_dir()
        data_dir = get_data_dir()

        ex = str(payload.get("exchange") or "SHFE").upper().strip()
        v = _normalize_variety_for_api(payload.get("var"))
        derived_symbol = str(payload.get("derived_symbol") or _derived_symbol_for_variety(v, exchange=ex)).strip()
        start = str(payload.get("start") or "").strip()
        end = str(payload.get("end") or "").strip()
        raw_check = payload.get("tqsdk_check", True)
        if isinstance(raw_check, str):
            tqsdk_check = raw_check.strip().lower() not in {"0", "false", "no", "off"}
        else:
            tqsdk_check = bool(raw_check)
        raw_incremental = payload.get("incremental", False)
        if isinstance(raw_incremental, str):
            incremental = raw_incremental.strip().lower() in {"1", "true", "yes", "on"}
        else:
            incremental = bool(raw_incremental)

        argv = python_module_argv(
            "ghtrader.cli",
            "data",
            "main-l5-validate",
            "--exchange",
            ex,
            "--var",
            v,
            "--symbol",
            derived_symbol,
            "--data-dir",
            str(data_dir),
            "--runs-dir",
            str(runs_dir),
        )
        if start:
            argv += ["--start", start]
        if end:
            argv += ["--end", end]
        if incremental:
            argv += ["--incremental"]
        argv += ["--tqsdk-check" if tqsdk_check else "--no-tqsdk-check"]

        for key, flag in [
            ("tqsdk_check_max_days", "--tqsdk-check-max-days"),
            ("tqsdk_check_max_segments", "--tqsdk-check-max-segments"),
            ("max_segments_per_day", "--max-segments-per-day"),
            ("gap_threshold_s", "--gap-threshold-s"),
            ("strict_ratio", "--strict-ratio"),
        ]:
            raw = payload.get(key)
            if raw not in (None, ""):
                argv += [flag, str(raw)]

        policy_preview: dict[str, Any] = {}
        try:
            from ghtrader.data.main_l5_validation import resolve_validation_policy_preview

            gp = payload.get("gap_threshold_s")
            sr = payload.get("strict_ratio")
            policy_preview = resolve_validation_policy_preview(
                variety=v,
                gap_threshold_s=(float(gp) if gp not in (None, "") else None),
                strict_ratio=(float(sr) if sr not in (None, "") else None),
            )
            preview_sources = dict(policy_preview.get("policy_sources") or {})
            if gp not in (None, ""):
                preview_sources["gap_threshold_s"] = "ui_payload"
            if sr not in (None, ""):
                preview_sources["strict_ratio"] = "ui_payload"
            policy_preview["policy_sources"] = preview_sources
        except Exception:
            policy_preview = {}

        title = f"main-l5-validate {ex}.{v} {derived_symbol}"
        store = request.app.state.job_store
        jm = request.app.state.job_manager

        try:
            active = store.list_active_jobs() + store.list_unstarted_queued_jobs(limit=2000)
            for j in active:
                if str(j.title or "").startswith(title):
                    if j.pid is None and str(j.status or "") == "queued":
                        started = jm.start_queued_job(j.id) or j
                        _data_page_cache_clear()
                        _clear_data_quality_cache()
                        return {
                            "ok": True,
                            "enqueued": [started.id],
                            "count": 1,
                            "deduped": True,
                            "started": bool(started.pid),
                        }
                    _data_page_cache_clear()
                    _clear_data_quality_cache()
                    return {"ok": True, "enqueued": [j.id], "count": 1, "deduped": True}
        except Exception:
            pass

        rec = jm.start_job(
            JobSpec(
                title=title,
                argv=argv,
                cwd=Path.cwd(),
                metadata={
                    "kind": "main_l5_validate",
                    "exchange": ex,
                    "variety": v,
                    "symbol": derived_symbol,
                    "incremental": bool(incremental),
                    "policy_preview": policy_preview,
                },
            )
        )
        _data_page_cache_clear()
        _clear_data_quality_cache()
        return {
            "ok": True,
            "enqueued": [rec.id],
            "count": 1,
            "deduped": False,
            "policy_preview": policy_preview,
        }

    @app.get("/api/data/reports", response_class=JSONResponse)
    def api_data_reports(
        request: Request,
        kind: str = "diagnose",
        exchange: str = "SHFE",
        var: str = "",
        limit: int = 5,
    ) -> dict[str, Any]:
        """
        Return recent data reports written under runs/control/reports/.

        kind:
        - diagnose: runs/control/reports/data_diagnose/diagnose_exchange=..._var=..._*.json
        - repair: runs/control/reports/data_repair/repair_*.plan.json (+ optional .result.json)
        """
        if not auth.is_authorized(request):
            raise HTTPException(status_code=401, detail="Unauthorized")
        k = str(kind or "").strip().lower()
        ex = str(exchange).upper().strip() or "SHFE"
        v = _normalize_variety_for_api(var)
        lim = max(1, min(int(limit or 5), 50))

        rd = get_runs_dir()

        if k == "diagnose":
            rep_dir = rd / "control" / "reports" / "data_diagnose"
            if not rep_dir.exists():
                return {"ok": False, "error": "no_reports", "kind": k, "exchange": ex, "var": v, "reports": []}
            pat = f"diagnose_exchange={ex}_var={v}_*.json"
            paths = sorted(rep_dir.glob(pat), key=lambda p: p.stat().st_mtime_ns, reverse=True)[:lim]
            reports: list[dict[str, Any]] = []
            for p in paths:
                obj = _read_json(p)
                if obj:
                    obj = dict(obj)
                    obj["_path"] = str(p)
                    reports.append(obj)
            return {"ok": True, "kind": k, "exchange": ex, "var": v, "count": int(len(reports)), "reports": reports}

        if k == "repair":
            rep_dir = rd / "control" / "reports" / "data_repair"
            if not rep_dir.exists():
                return {"ok": False, "error": "no_reports", "kind": k, "exchange": ex, "var": v, "reports": []}
            plan_paths = sorted(rep_dir.glob("repair_*.plan.json"), key=lambda p: p.stat().st_mtime_ns, reverse=True)
            reports2: list[dict[str, Any]] = []
            for p in plan_paths:
                plan = _read_json(p)
                if not isinstance(plan, dict):
                    continue
                if str(plan.get("exchange") or "").upper().strip() != ex:
                    continue
                if str(plan.get("var") or "").lower().strip() != v:
                    continue
                run_id = str(plan.get("run_id") or "").strip()
                res_path = rep_dir / f"repair_{run_id}.result.json" if run_id else None
                res_obj = _read_json(res_path) if (res_path and res_path.exists()) else None
                reports2.append(
                    {
                        "run_id": run_id or None,
                        "exchange": ex,
                        "var": v,
                        "plan": plan,
                        "result": res_obj,
                        "plan_path": str(p),
                        "result_path": (str(res_path) if res_path else None),
                    }
                )
                if len(reports2) >= lim:
                    break
            return {"ok": True, "kind": k, "exchange": ex, "var": v, "count": int(len(reports2)), "reports": reports2}

        raise HTTPException(status_code=400, detail="kind must be one of: diagnose, repair")

    @app.get("/api/data/l5-start", response_class=JSONResponse)
    def api_data_l5_start(request: Request, exchange: str = "SHFE", var: str = "", limit: int = 1) -> dict[str, Any]:
        """
        Return the most recent L5-start report(s) written under runs/control/reports/l5_start/.
        """
        if not auth.is_authorized(request):
            raise HTTPException(status_code=401, detail="Unauthorized")
        ex = str(exchange).upper().strip() or "SHFE"
        v = _normalize_variety_for_api(var)
        lim = max(1, min(int(limit or 1), 20))

        rd = get_runs_dir()
        rep_dir = rd / "control" / "reports" / "l5_start"
        if not rep_dir.exists():
            return {"ok": False, "error": "no_reports", "exchange": ex, "var": v, "reports": []}

        pat = f"l5_start_exchange={ex}_var={v}_*.json"
        paths = sorted(rep_dir.glob(pat), key=lambda p: p.stat().st_mtime_ns, reverse=True)[:lim]
        reports: list[dict[str, Any]] = []
        for p in paths:
            obj = _read_json(p)
            if obj:
                obj = dict(obj)
                obj["_path"] = str(p)
                reports.append(obj)
        return {"ok": True, "exchange": ex, "var": v, "count": int(len(reports)), "reports": reports}

    @app.get("/api/data/main-l5-validate", response_class=JSONResponse)
    def api_data_main_l5_validate(
        request: Request, exchange: str = "SHFE", var: str = "", symbol: str = "", limit: int = 1
    ) -> dict[str, Any]:
        """
        Return recent main_l5 validation report(s) written under runs/control/reports/main_l5_validate/.
        """
        if not auth.is_authorized(request):
            raise HTTPException(status_code=401, detail="Unauthorized")
        ex = str(exchange).upper().strip() or "SHFE"
        v = _normalize_variety_for_api(var)
        derived_symbol = str(symbol or _derived_symbol_for_variety(v, exchange=ex)).strip()
        lim = max(1, min(int(limit or 1), 20))

        rd = get_runs_dir()
        rep_dir = rd / "control" / "reports" / "main_l5_validate"
        if not rep_dir.exists():
            return {"ok": False, "error": "no_reports", "exchange": ex, "var": v, "reports": []}

        sym = re.sub(r"[^A-Za-z0-9]+", "_", derived_symbol).strip("_").lower()
        pat = f"main_l5_validate_exchange={ex}_var={v}_symbol={sym}_*.json"
        paths = sorted(rep_dir.glob(pat), key=lambda p: p.stat().st_mtime_ns, reverse=True)[:lim]
        reports: list[dict[str, Any]] = []
        for p in paths:
            obj = _read_json(p)
            if obj:
                obj = dict(obj)
                obj["_path"] = str(p)
                reports.append(obj)
        return {"ok": True, "exchange": ex, "var": v, "symbol": derived_symbol, "count": int(len(reports)), "reports": reports}

    @app.get("/api/data/main-l5-validate-summary", response_class=JSONResponse)
    def api_data_main_l5_validate_summary(
        request: Request, exchange: str = "SHFE", var: str = "", symbol: str = "", schedule_hash: str = "", limit: int = 30
    ) -> dict[str, Any]:
        """
        Return QuestDB-backed validation summary rows + overview.
        """
        if not auth.is_authorized(request):
            raise HTTPException(status_code=401, detail="Unauthorized")
        ex = str(exchange).upper().strip() or "SHFE"
        v = _normalize_variety_for_api(var)
        derived_symbol = str(symbol or _derived_symbol_for_variety(v, exchange=ex)).strip()
        sh = str(schedule_hash or "").strip()
        lim = max(1, min(int(limit or 30), 3650))
        try:
            from ghtrader.questdb.client import make_questdb_query_config_from_env
            from ghtrader.questdb.main_l5_validate import (
                fetch_latest_main_l5_validate_summary,
                fetch_main_l5_validate_overview,
            )

            cfg = make_questdb_query_config_from_env()
            overview = fetch_main_l5_validate_overview(
                cfg=cfg,
                symbol=derived_symbol,
                schedule_hash=(sh or None),
            )
            rows = fetch_latest_main_l5_validate_summary(
                cfg=cfg,
                symbol=derived_symbol,
                schedule_hash=(sh or None),
                limit=lim,
            )
            return {
                "ok": True,
                "exchange": ex,
                "var": v,
                "symbol": derived_symbol,
                "schedule_hash": (sh or None),
                "overview": overview,
                "count": int(len(rows)),
                "rows": rows,
            }
        except Exception as e:
            return {
                "ok": False,
                "error": str(e),
                "exchange": ex,
                "var": v,
                "symbol": derived_symbol,
                "schedule_hash": (sh or None),
                "rows": [],
            }

    @app.get("/api/data/main-l5-validate-gaps", response_class=JSONResponse)
    def api_data_main_l5_validate_gaps(
        request: Request,
        exchange: str = "SHFE",
        var: str = "",
        symbol: str = "",
        schedule_hash: str = "",
        trading_day: str = "",
        start: str = "",
        end: str = "",
        min_duration_s: int = 0,
        limit: int = 500,
    ) -> dict[str, Any]:
        """
        Return QuestDB-backed gap details for main_l5 validation.
        """
        if not auth.is_authorized(request):
            raise HTTPException(status_code=401, detail="Unauthorized")
        ex = str(exchange).upper().strip() or "SHFE"
        v = _normalize_variety_for_api(var)
        derived_symbol = str(symbol or _derived_symbol_for_variety(v, exchange=ex)).strip()
        sh = str(schedule_hash or "").strip()
        lim = max(1, min(int(limit or 500), 10000))
        day = None
        start_day = None
        end_day = None
        if trading_day:
            try:
                day = date.fromisoformat(str(trading_day)[:10])
            except Exception:
                day = None
        if start:
            try:
                start_day = date.fromisoformat(str(start)[:10])
            except Exception:
                start_day = None
        if end:
            try:
                end_day = date.fromisoformat(str(end)[:10])
            except Exception:
                end_day = None
        try:
            from ghtrader.questdb.client import make_questdb_query_config_from_env
            from ghtrader.questdb.main_l5_validate import list_main_l5_validate_gaps

            cfg = make_questdb_query_config_from_env()
            gaps = list_main_l5_validate_gaps(
                cfg=cfg,
                symbol=derived_symbol,
                schedule_hash=(sh or None),
                trading_day=day,
                start_day=start_day,
                end_day=end_day,
                min_duration_s=(int(min_duration_s) if int(min_duration_s or 0) > 0 else None),
                limit=lim,
            )
            return {
                "ok": True,
                "exchange": ex,
                "var": v,
                "symbol": derived_symbol,
                "schedule_hash": (sh or None),
                "count": int(len(gaps)),
                "gaps": gaps,
            }
        except Exception as e:
            return {
                "ok": False,
                "error": str(e),
                "exchange": ex,
                "var": v,
                "symbol": derived_symbol,
                "schedule_hash": (sh or None),
                "gaps": [],
            }

    @app.get("/api/data/quality/readiness", response_class=JSONResponse)
    def api_data_quality_readiness(
        request: Request,
        exchange: str = "SHFE",
        var: str = "",
        symbol: str = "",
    ) -> dict[str, Any]:
        """Return layered readiness summary for one variety/symbol."""
        if not auth.is_authorized(request):
            raise HTTPException(status_code=401, detail="Unauthorized")
        ex = str(exchange).upper().strip() or "SHFE"
        v = _normalize_variety_for_api(var)
        derived_symbol = str(symbol or _derived_symbol_for_variety(v, exchange=ex)).strip()
        report = None
        report_error = ""
        overview: dict[str, Any] = {}
        try:
            from ghtrader.data.main_l5_validation import read_latest_validation_report
            from ghtrader.questdb.client import make_questdb_query_config_from_env
            from ghtrader.questdb.main_l5_validate import fetch_main_l5_validate_overview

            report = read_latest_validation_report(
                runs_dir=get_runs_dir(),
                exchange=ex,
                variety=v,
                derived_symbol=derived_symbol,
            )
            schedule_hash_filter = str((report or {}).get("schedule_hash") or "").strip() or None
            cfg = make_questdb_query_config_from_env()
            overview = fetch_main_l5_validate_overview(
                cfg=cfg,
                symbol=derived_symbol,
                schedule_hash=schedule_hash_filter,
            )
        except Exception as e:
            report_error = str(e)

        rep = dict(report or {})
        engineering_state = str(rep.get("engineering_state") or "warn")
        source_state = str(rep.get("source_state") or "warn")
        policy_state = str(rep.get("policy_state") or "warn")
        overall_state = str(rep.get("overall_state") or rep.get("state") or "warn")
        checked_days = int(rep.get("checked_days") or 0)
        source_missing_days_count = int(rep.get("source_missing_days_count") or rep.get("missing_days") or 0)
        provider_missing_day_rate = (
            float(source_missing_days_count) / float(checked_days) if checked_days > 0 else 0.0
        )

        # SLO-style metrics for operations dashboarding.
        store = request.app.state.job_store
        main_l5_jobs = [j for j in store.list_jobs(limit=400) if str((j.metadata or {}).get("kind") or "") == "main_l5"]
        succ = len([j for j in main_l5_jobs if str(j.status or "") == "succeeded"])
        total = len(main_l5_jobs)
        main_l5_build_success_rate = (float(succ) / float(total) if total > 0 else None)

        return {
            "ok": bool(not report_error),
            "exchange": ex,
            "var": v,
            "symbol": derived_symbol,
            "overall_state": overall_state,
            "engineering_state": engineering_state,
            "source_state": source_state,
            "policy_state": policy_state,
            "reason_code": str(rep.get("reason_code") or ""),
            "action_hint": str(rep.get("action_hint") or ""),
            "updated_at": str(rep.get("created_at") or ""),
            "stale": False,
            "slo_metrics": {
                "provider_missing_day_rate": provider_missing_day_rate,
                "outside_session_seconds": int(rep.get("ticks_outside_sessions_seconds_total") or 0),
                "gap_count_gt_30": int(rep.get("gap_count_gt_30s") or 0),
                "validation_runtime_p95": None,
                "main_l5_build_success_rate": main_l5_build_success_rate,
            },
            "overview": overview,
            "report_error": report_error or None,
        }

    @app.get("/api/data/quality/anomalies", response_class=JSONResponse)
    def api_data_quality_anomalies(
        request: Request,
        exchange: str = "SHFE",
        var: str = "",
        symbol: str = "",
        limit: int = 50,
    ) -> dict[str, Any]:
        """Return compact anomaly list from latest validation report."""
        if not auth.is_authorized(request):
            raise HTTPException(status_code=401, detail="Unauthorized")
        ex = str(exchange).upper().strip() or "SHFE"
        v = _normalize_variety_for_api(var)
        derived_symbol = str(symbol or _derived_symbol_for_variety(v, exchange=ex)).strip()
        lim = max(1, min(int(limit or 50), 200))
        try:
            from ghtrader.data.main_l5_validation import read_latest_validation_report

            rep = read_latest_validation_report(
                runs_dir=get_runs_dir(),
                exchange=ex,
                variety=v,
                derived_symbol=derived_symbol,
            ) or {}
            issues = list(rep.get("issues") or [])[:lim]
            rows: list[dict[str, Any]] = []
            for it in issues:
                if not isinstance(it, dict):
                    continue
                rows.append(
                    {
                        "trading_day": it.get("trading_day"),
                        "missing_segments_total": int(it.get("missing_segments_total") or 0),
                        "missing_half_seconds": int(it.get("missing_half_seconds") or 0),
                        "ticks_outside_sessions_seconds": int(it.get("ticks_outside_sessions_seconds") or 0),
                        "max_gap_s": int(it.get("max_gap_s") or 0),
                    }
                )
            return {
                "ok": True,
                "exchange": ex,
                "var": v,
                "symbol": derived_symbol,
                "count": int(len(rows)),
                "rows": rows,
                "missing_days_sample": list(rep.get("missing_days_sample") or [])[:20],
                "source_missing_days_count": int(rep.get("source_missing_days_count") or rep.get("missing_days") or 0),
            }
        except Exception as e:
            return {"ok": False, "error": str(e), "exchange": ex, "var": v, "symbol": derived_symbol, "rows": []}

    @app.get("/api/data/quality/profiles", response_class=JSONResponse)
    def api_data_quality_profiles(request: Request, var: str = "") -> dict[str, Any]:
        """Return effective validation profile values and source mapping."""
        if not auth.is_authorized(request):
            raise HTTPException(status_code=401, detail="Unauthorized")
        v = _normalize_variety_for_api(var)
        try:
            from ghtrader.data.main_l5_validation import resolve_validation_policy_preview

            profile = resolve_validation_policy_preview(variety=v)
            return {"ok": True, "var": v, "profile": profile}
        except Exception as e:
            return {"ok": False, "var": v, "error": str(e), "profile": {}}

    @app.post("/api/questdb/query", response_class=JSONResponse)
    async def api_questdb_query(request: Request) -> dict[str, Any]:
        """
        Read-only QuestDB query endpoint (guarded).

        Intended for local SSH-forwarded dashboard use only.
        """
        if not auth.is_authorized(request):
            raise HTTPException(status_code=401, detail="Unauthorized")

        payload = await request.json()
        query = str(payload.get("query") or "").strip()
        try:
            limit = int(payload.get("limit") or 200)
        except Exception:
            limit = 200
        limit = max(1, min(limit, 500))

        if not query:
            raise HTTPException(status_code=400, detail="query is required")

        try:
            from ghtrader.questdb.client import make_questdb_query_config_from_env
            from ghtrader.questdb.queries import query_sql_read_only

            cfg = make_questdb_query_config_from_env()
            cols, rows = query_sql_read_only(cfg=cfg, query=query, limit=int(limit), connect_timeout_s=2)
            return {"ok": True, "columns": cols, "rows": rows}
        except ValueError as e:
            raise HTTPException(status_code=400, detail=str(e)) from e
        except RuntimeError as e:
            # Missing dependency / config issues.
            raise HTTPException(status_code=500, detail=str(e)) from e
        except Exception as e:
            raise HTTPException(status_code=400, detail=str(e)) from e

    @app.get("/api/dashboard/summary", response_class=JSONResponse)
    def api_dashboard_summary(request: Request, var: str = "") -> dict[str, Any]:
        """
        Aggregated KPIs for the dashboard home page.
        """
        if not auth.is_authorized(request):
            raise HTTPException(status_code=401, detail="Unauthorized")

        v = _normalize_variety_for_api(var, allow_legacy_default=True)
        now = time.time()
        cached = _dash_summary_cache.get(ttl_s=_DASH_SUMMARY_TTL_S, key=v, now=now)
        if isinstance(cached, dict):
            return dict(cached)

        data_dir = get_data_dir()
        runs_dir = get_runs_dir()
        artifacts_dir = get_artifacts_dir()

        # Job counts
        jobs = store.list_jobs(limit=200)
        jobs_filtered = [j for j in jobs if _job_matches_variety(j, v)]
        running_count = len([j for j in jobs_filtered if j.status == "running"])
        queued_count = len([j for j in jobs_filtered if j.status == "queued"])

        # QuestDB reachability
        questdb_ok = False
        try:
            from ghtrader.questdb.client import questdb_reachable_pg

            q = questdb_reachable_pg(connect_timeout_s=2, retries=1, backoff_s=0.2)
            questdb_ok = bool(q.get("ok"))
        except Exception:
            questdb_ok = False

        # Data symbols / status derived from canonical main_l5 table.
        data_symbols = 0
        data_status = "offline" if not questdb_ok else "unknown"

        # Model count (count actual model files, not arbitrary artifacts/ subdirs)
        model_files: list[dict[str, Any]] = []
        model_status = ""
        try:
            model_files_all = _scan_model_files(artifacts_dir)
            model_files = [m for m in model_files_all if _symbol_matches_variety(str(m.get("symbol") or ""), v)]
            if model_files:
                latest = sorted(model_files, key=lambda x: float(x.get("mtime") or 0.0), reverse=True)[0]
                model_status = f"{latest.get('model_type')} {latest.get('symbol')} h{latest.get('horizon')}"
        except Exception:
            model_files = []
            model_status = "error"
        model_count = int(len({(m.get("namespace"), m.get("symbol"), m.get("model_type"), m.get("horizon")) for m in model_files}))

        # Trading status
        trading_mode = "idle"
        trading_status = "No active runs"
        try:
            trading_dir = runs_dir / "trading"
            if trading_dir.exists():
                runs = sorted([d for d in trading_dir.iterdir() if d.is_dir()], key=lambda x: x.name, reverse=True)
                if runs:
                    latest = runs[0]
                    # Check if there's recent activity (snapshots.jsonl updated in last 5 minutes)
                    snapshots_file = latest / "snapshots.jsonl"
                    if snapshots_file.exists():
                        mtime = snapshots_file.stat().st_mtime
                        import time as t
                        if t.time() - mtime < 300:  # 5 minutes
                            trading_mode = "active"
                            trading_status = f"Run: {latest.name[:8]}"
        except Exception:
            pass

        summary_updated_at = _now_iso()
        validation_rows = 0

        # Pipeline status (simplified)
        pipeline = {
            "ingest": _ui_state_payload(
                state=("ok" if data_symbols > 0 else "warn"),
                text=f"{data_symbols} symbols",
                updated_at=summary_updated_at,
            ),
            "sync": _ui_state_payload(
                state=("ok" if questdb_ok else "error"),
                text=("connected" if questdb_ok else "offline"),
                updated_at=summary_updated_at,
            ),
            "schedule": _ui_state_payload(state="unknown", text="--", updated_at=summary_updated_at),
            "main_l5": _ui_state_payload(state="unknown", text="--", updated_at=summary_updated_at),
            "validation": _ui_state_payload(state="warn", text="not run", updated_at=summary_updated_at),
            "build": _ui_state_payload(state="unknown", text="--", updated_at=summary_updated_at),
            "train": _ui_state_payload(
                state=("ok" if model_count > 0 else "warn"),
                text=f"{model_count} models",
                updated_at=summary_updated_at,
            ),
        }

        # Check schedule + main_l5 availability (QuestDB canonical tables).
        try:
            if questdb_ok:
                from ghtrader.questdb.client import connect_pg, make_questdb_query_config_from_env
                from ghtrader.questdb.main_schedule import MAIN_SCHEDULE_TABLE_V2

                cfg = make_questdb_query_config_from_env()
                with connect_pg(cfg, connect_timeout_s=1) as conn:
                    with conn.cursor() as cur:
                        cur.execute(
                            f"SELECT count() FROM {MAIN_SCHEDULE_TABLE_V2} WHERE exchange=%s AND variety=%s",
                            ["SHFE", v],
                        )
                        n = int((cur.fetchone() or [0])[0] or 0)
                        cur.execute(
                            "SELECT count(DISTINCT symbol) "
                            "FROM ghtrader_ticks_main_l5_v2 "
                            "WHERE ticks_kind='main_l5' AND dataset_version='v2' "
                            "AND lower(symbol) LIKE %s",
                            [f"%shfe.{v}%"],
                        )
                        data_symbols = int((cur.fetchone() or [0])[0] or 0)
                        cur.execute(
                            "SELECT count() FROM ghtrader_main_l5_validate_summary_v2 "
                            "WHERE lower(symbol) LIKE %s",
                            [f"%shfe.{v}%"],
                        )
                        validation_rows = int((cur.fetchone() or [0])[0] or 0)
                if n > 0:
                    pipeline["schedule"] = _ui_state_payload(state="ok", text=f"{v} ready", updated_at=summary_updated_at)
                if data_symbols > 0:
                    pipeline["ingest"] = _ui_state_payload(
                        state="ok",
                        text=f"{data_symbols} symbols",
                        updated_at=summary_updated_at,
                    )
                    pipeline["main_l5"] = _ui_state_payload(
                        state="ok",
                        text=f"{data_symbols} symbols",
                        updated_at=summary_updated_at,
                    )
                    data_status = "ready"
                else:
                    pipeline["ingest"] = _ui_state_payload(state="warn", text="0 symbols", updated_at=summary_updated_at)
                    pipeline["main_l5"] = _ui_state_payload(state="warn", text="empty", updated_at=summary_updated_at)
                    data_status = "empty"
                validation_state = ("ok" if validation_rows > 0 else "warn")
                validation_text = (f"{validation_rows} rows" if validation_rows > 0 else "not run")
                try:
                    from ghtrader.data.main_l5_validation import read_latest_validation_report

                    rep = read_latest_validation_report(
                        runs_dir=runs_dir,
                        exchange="SHFE",
                        variety=v,
                        derived_symbol=_derived_symbol_for_variety(v, exchange="SHFE"),
                    )
                    if isinstance(rep, dict):
                        rep_state = str(rep.get("state") or "").strip().lower()
                        if rep_state == "error":
                            validation_state = "error"
                            validation_text = "error"
                        elif rep_state == "warn":
                            validation_state = "warn"
                            validation_text = "warn"
                        elif rep_state == "noop":
                            validation_state = "ok"
                            validation_text = "up-to-date"
                        elif rep_state == "ok":
                            validation_state = "ok"
                            validation_text = "ok"
                except Exception:
                    pass
                pipeline["validation"] = _ui_state_payload(
                    state=validation_state,
                    text=validation_text,
                    updated_at=summary_updated_at,
                )
        except Exception:
            if questdb_ok:
                pipeline["main_l5"] = _ui_state_payload(
                    state="error",
                    text="query error",
                    error="main_l5 query failed",
                    updated_at=summary_updated_at,
                )
                pipeline["validation"] = _ui_state_payload(
                    state="warn",
                    text="query unavailable",
                    error="validation summary unavailable",
                    updated_at=summary_updated_at,
                )
                data_status = "error"

        # Check features/labels builds exist (QuestDB build tables)
        try:
            if questdb_ok:
                from ghtrader.questdb.client import connect_pg, make_questdb_query_config_from_env
                from ghtrader.questdb.features_labels import FEATURE_BUILDS_TABLE_V2

                cfg = make_questdb_query_config_from_env()
                with connect_pg(cfg, connect_timeout_s=1) as conn:
                    with conn.cursor() as cur:
                        cur.execute(
                            f"SELECT count(DISTINCT symbol) FROM {FEATURE_BUILDS_TABLE_V2} "
                            "WHERE dataset_version='v2' AND lower(symbol) LIKE %s",
                            [f"%shfe.{v}%"],
                        )
                        n = int((cur.fetchone() or [0])[0] or 0)
                if n > 0:
                    pipeline["build"] = _ui_state_payload(state="ok", text=f"{n} symbols", updated_at=summary_updated_at)
        except Exception:
            pass

        guardrails = _dashboard_guardrails_context()

        payload = {
            "ok": True,
            "var": v,
            "state": "ok",
            "text": "summary ready",
            "error": "",
            "stale": False,
            "updated_at": summary_updated_at,
            "running_count": running_count,
            "queued_count": queued_count,
            "questdb_ok": questdb_ok,
            "data_symbols": data_symbols,
            "data_symbols_v2": int(data_symbols),
            "data_status": data_status,
            "model_count": model_count,
            "model_status": model_status,
            "trading_mode": trading_mode,
            "trading_status": trading_status,
            "pipeline": pipeline,
            "guardrails": guardrails,
        }
        _dash_summary_cache.set(dict(payload), key=v, now=time.time())
        return payload

    @app.get("/api/ui/status", response_class=JSONResponse)
    def api_ui_status(request: Request) -> dict[str, Any]:
        """
        Lightweight status endpoint for global UI chrome (topbar).

        Cached to avoid doing QuestDB connection checks too frequently.
        """
        if not auth.is_authorized(request):
            raise HTTPException(status_code=401, detail="Unauthorized")

        now = time.time()
        cached = _ui_status_cache.get(ttl_s=_UI_STATUS_TTL_S, now=now)
        if isinstance(cached, dict):
            return dict(cached)

        runs_dir = get_runs_dir()
        data_dir = get_data_dir()
        artifacts_dir = get_artifacts_dir()

        # Job counts
        jobs = store.list_jobs(limit=200)
        running_count = len([j for j in jobs if j.status == "running"])
        queued_count = len([j for j in jobs if j.status == "queued"])

        # GPU status (best-effort; comes from the same cached snapshot used by /api/system)
        gpu_status = "unknown"
        try:
            from ghtrader.control.system_info import system_snapshot

            snap = system_snapshot(
                data_dir=data_dir,
                runs_dir=runs_dir,
                artifacts_dir=artifacts_dir,
                include_dir_sizes=False,
                refresh="none",
            )
            gpu_status = str(((snap.get("gpu") or {}) if isinstance(snap.get("gpu"), dict) else {}).get("status") or "unknown")
        except Exception:
            gpu_status = "unknown"

        # QuestDB reachability (best-effort)
        questdb_ok = False
        try:
            from ghtrader.questdb.client import questdb_reachable_pg

            q = questdb_reachable_pg(connect_timeout_s=2, retries=1, backoff_s=0.2)
            questdb_ok = bool(q.get("ok"))
        except Exception:
            questdb_ok = False

        status_updated_at = _now_iso()
        payload = {
            "ok": True,
            "state": ("ok" if questdb_ok else "warn"),
            "text": ("QuestDB connected" if questdb_ok else "QuestDB offline"),
            "error": ("" if questdb_ok else "questdb_unreachable"),
            "stale": False,
            "updated_at": status_updated_at,
            "questdb_ok": bool(questdb_ok),
            "gpu_status": gpu_status,
            "running_count": int(running_count),
            "queued_count": int(queued_count),
        }
        _ui_status_cache.set(dict(payload), now=time.time())
        return payload

    # ---------------------------------------------------------------------
    # Trading artifacts helpers
    # ---------------------------------------------------------------------

    # ---------------------------------------------------------------------
    # Gateway (AccountGateway; OMS/EMS per account profile)
    # ---------------------------------------------------------------------

    # ---------------------------------------------------------------------
    # Strategy (AI StrategyRunner; consumes gateway state, writes targets)
    # ---------------------------------------------------------------------

    @app.get("/api/strategy/status", response_class=JSONResponse)
    def api_strategy_status(request: Request, account_profile: str = "default") -> dict[str, Any]:
        """
        Read StrategyRunner stable state for a profile (Redis hot state first, file mirror fallback).
        """
        if not auth.is_authorized(request):
            raise HTTPException(status_code=401, detail="Unauthorized")

        runs_dir = get_runs_dir()
        try:
            from ghtrader.tq.runtime import canonical_account_profile

            prof = canonical_account_profile(str(account_profile or "default"))
        except Exception:
            prof = str(account_profile or "default").strip() or "default"

        root = runs_dir / "strategy" / f"account={prof}"
        st_path = root / "state.json"
        desired_path = root / "desired.json"
        state = _read_redis_json(f"ghtrader:strategy:state:{prof}") or (_read_json_file(st_path) if st_path.exists() else None)
        desired = _read_redis_json(f"ghtrader:strategy:desired:{prof}") or (_read_json_file(desired_path) if desired_path.exists() else None)
        targets = _read_json_file((runs_dir / "gateway" / f"account={prof}" / "targets.json"))  # strategy output surface

        age = _artifact_age_sec(st_path) if st_path.exists() else None
        desired_cfg = desired.get("desired") if isinstance(desired, dict) and isinstance(desired.get("desired"), dict) else (desired or {})
        desired_mode = str((desired_cfg or {}).get("mode") or "idle")
        status = _status_from_desired_and_state(root_exists=root.exists(), desired_mode=desired_mode, state_age=age)

        active_job_id = None
        try:
            store2: JobStore = request.app.state.job_store
            for j in store2.list_active_jobs():
                if _is_strategy_job(j.command):
                    ap = _argv_opt(j.command, "--account") or ""
                    try:
                        from ghtrader.tq.runtime import canonical_account_profile

                        ap = canonical_account_profile(ap)
                    except Exception:
                        ap = str(ap).strip().lower() or "default"
                    if ap == prof:
                        active_job_id = j.id
                        break
        except Exception:
            active_job_id = None

        return {
            "ok": True,
            "account_profile": prof,
            "root": str(root),
            "exists": bool(root.exists()),
            "status": status,
            "state_age_sec": age,
            "error": _health_error(state),
            "active_job_id": active_job_id,
            "desired": desired,
            "state": state,
            "targets": targets,
            "generated_at": _now_iso(),
        }

    @app.post("/api/strategy/desired", response_class=JSONResponse)
    async def api_strategy_desired(request: Request) -> dict[str, Any]:
        """
        Upsert StrategyRunner desired.json for a profile.

        Payload:
          {account_profile, desired: {mode, symbols, model_name, horizon, threshold_up, threshold_down, position_size, artifacts_dir, poll_interval_sec}}
        """
        if not auth.is_authorized(request):
            raise HTTPException(status_code=401, detail="Unauthorized")
        payload = await request.json()
        ap = str(payload.get("account_profile") or "default").strip() or "default"
        desired_in = payload.get("desired") if isinstance(payload.get("desired"), dict) else {}

        try:
            from ghtrader.trading.strategy_control import StrategyDesired, write_strategy_desired
            from ghtrader.tq.runtime import canonical_account_profile

            prof = canonical_account_profile(ap)
            d = StrategyDesired(
                mode=str(desired_in.get("mode") or "idle").strip().lower() in {"run", "running", "active", "on"} and "run" or "idle",  # type: ignore[arg-type]
                symbols=list(desired_in.get("symbols")) if isinstance(desired_in.get("symbols"), list) else None,
                model_name=str(desired_in.get("model_name") or "xgboost").strip() or "xgboost",
                horizon=int(desired_in.get("horizon") or 50),
                threshold_up=float(desired_in.get("threshold_up") or 0.6),
                threshold_down=float(desired_in.get("threshold_down") or 0.6),
                position_size=int(desired_in.get("position_size") or 1),
                artifacts_dir=str(desired_in.get("artifacts_dir") or "artifacts").strip() or "artifacts",
                poll_interval_sec=float(desired_in.get("poll_interval_sec") or 0.5),
            )
            write_strategy_desired(runs_dir=get_runs_dir(), profile=prof, desired=d)
            return {"ok": True, "account_profile": prof}
        except Exception as e:
            raise HTTPException(status_code=400, detail=f"strategy_desired_failed: {e}")

    # ---------------------------------------------------------------------
    # Explicit Start/Stop endpoints for Gateway and Strategy
    # ---------------------------------------------------------------------

    @app.post("/api/gateway/start", response_class=JSONResponse)
    async def api_gateway_start(request: Request) -> dict[str, Any]:
        """
        Start a gateway job for the selected profile.

        Payload: {account_profile, mode?, symbols?, confirm_live?, ...}

        If mode/symbols/confirm_live are provided, desired.json is updated first.
        Then starts `ghtrader gateway run --account <profile>` as a job.
        """
        if not auth.is_authorized(request):
            raise HTTPException(status_code=401, detail="Unauthorized")
        payload = await request.json()
        ap = str(payload.get("account_profile") or "default").strip() or "default"

        try:
            from ghtrader.tq.runtime import canonical_account_profile

            prof = canonical_account_profile(ap)
        except Exception:
            prof = str(ap).strip().lower() or "default"

        runs_dir = get_runs_dir()

        # Optionally update desired state if mode/symbols provided
        desired_in = payload.get("desired") if isinstance(payload.get("desired"), dict) else None
        if desired_in:
            try:
                from ghtrader.tq.gateway import GatewayDesired, write_gateway_desired

                d = GatewayDesired(
                    mode=str(desired_in.get("mode") or "sim").strip(),  # type: ignore[arg-type]
                    symbols=list(desired_in.get("symbols")) if isinstance(desired_in.get("symbols"), list) else None,
                    executor=str(desired_in.get("executor") or "targetpos").strip().lower() in {"direct"} and "direct" or "targetpos",  # type: ignore[arg-type]
                    sim_account=str(desired_in.get("sim_account") or "tqsim").strip().lower() in {"tqkq"} and "tqkq" or "tqsim",  # type: ignore[arg-type]
                    confirm_live=str(desired_in.get("confirm_live") or "").strip(),
                    max_abs_position=max(0, int(desired_in.get("max_abs_position") or 1)),
                    max_order_size=max(1, int(desired_in.get("max_order_size") or 1)),
                    max_ops_per_sec=max(1, int(desired_in.get("max_ops_per_sec") or 10)),
                    max_daily_loss=(float(desired_in.get("max_daily_loss")) if str(desired_in.get("max_daily_loss") or "").strip() else None),
                    enforce_trading_time=bool(desired_in.get("enforce_trading_time")) if ("enforce_trading_time" in desired_in) else True,
                )
                write_gateway_desired(runs_dir=runs_dir, profile=prof, desired=d)
            except Exception as e:
                raise HTTPException(status_code=400, detail=f"gateway_desired_failed: {e}")

        # Check if already running
        store2: JobStore = request.app.state.job_store
        for j in store2.list_active_jobs():
            if _is_gateway_job(j.command):
                ap2 = _argv_opt(j.command, "--account") or ""
                try:
                    from ghtrader.tq.runtime import canonical_account_profile

                    ap2 = canonical_account_profile(ap2)
                except Exception:
                    ap2 = str(ap2).strip().lower() or "default"
                if ap2 == prof:
                    return {"ok": False, "error": "already_running", "job_id": j.id, "message": f"Gateway for profile '{prof}' is already running."}

        # Start gateway job
        argv = python_module_argv("ghtrader.cli", "gateway", "run", "--account", prof, "--runs-dir", str(runs_dir))
        title = f"gateway {prof}"
        jm2: JobManager = request.app.state.job_manager
        gw_symbols = []
        if desired_in and isinstance(desired_in.get("symbols"), list):
            gw_symbols = [str(s).strip() for s in list(desired_in.get("symbols") or []) if str(s).strip()]
        rec = jm2.start_job(
            JobSpec(
                title=title,
                argv=argv,
                cwd=Path.cwd(),
                metadata={
                    "kind": "gateway",
                    "account_profile": prof,
                    "symbols": gw_symbols,
                },
            )
        )
        return {"ok": True, "account_profile": prof, "job_id": rec.id}

    @app.post("/api/gateway/stop", response_class=JSONResponse)
    async def api_gateway_stop(request: Request) -> dict[str, Any]:
        """
        Stop the running gateway job for the selected profile.

        Payload: {account_profile}
        """
        if not auth.is_authorized(request):
            raise HTTPException(status_code=401, detail="Unauthorized")
        payload = await request.json()
        ap = str(payload.get("account_profile") or "default").strip() or "default"

        try:
            from ghtrader.tq.runtime import canonical_account_profile

            prof = canonical_account_profile(ap)
        except Exception:
            prof = str(ap).strip().lower() or "default"

        store2: JobStore = request.app.state.job_store
        jm2: JobManager = request.app.state.job_manager
        cancelled = False
        job_id = None

        for j in store2.list_active_jobs():
            if _is_gateway_job(j.command):
                ap2 = _argv_opt(j.command, "--account") or ""
                try:
                    from ghtrader.tq.runtime import canonical_account_profile

                    ap2 = canonical_account_profile(ap2)
                except Exception:
                    ap2 = str(ap2).strip().lower() or "default"
                if ap2 == prof:
                    job_id = j.id
                    cancelled = bool(jm2.cancel_job(j.id))
                    break

        # Also set desired mode to idle so supervisor doesn't restart
        runs_dir = get_runs_dir()
        try:
            from dataclasses import replace
            from ghtrader.tq.gateway import read_gateway_desired, write_gateway_desired

            existing = read_gateway_desired(runs_dir=runs_dir, profile=prof)
            if existing and existing.mode != "idle":
                write_gateway_desired(runs_dir=runs_dir, profile=prof, desired=replace(existing, mode="idle"))
        except Exception:
            pass

        if job_id:
            return {"ok": True, "account_profile": prof, "job_id": job_id, "cancelled": cancelled}
        return {"ok": False, "error": "not_running", "message": f"No running gateway job for profile '{prof}'."}

    @app.post("/api/strategy/start", response_class=JSONResponse)
    async def api_strategy_start(request: Request) -> dict[str, Any]:
        """
        Start a strategy job for the selected profile.

        Payload: {account_profile, desired?: {mode, symbols, model_name, horizon, ...}}

        If desired is provided, desired.json is updated first.
        Then starts `ghtrader strategy run --account <profile> ...` as a job.
        """
        if not auth.is_authorized(request):
            raise HTTPException(status_code=401, detail="Unauthorized")
        payload = await request.json()
        ap = str(payload.get("account_profile") or "default").strip() or "default"

        try:
            from ghtrader.tq.runtime import canonical_account_profile

            prof = canonical_account_profile(ap)
        except Exception:
            prof = str(ap).strip().lower() or "default"

        runs_dir = get_runs_dir()

        # Optionally update desired state
        desired_in = payload.get("desired") if isinstance(payload.get("desired"), dict) else None
        if desired_in:
            try:
                from ghtrader.trading.strategy_control import StrategyDesired, write_strategy_desired

                d = StrategyDesired(
                    mode="run",  # type: ignore[arg-type]
                    symbols=list(desired_in.get("symbols")) if isinstance(desired_in.get("symbols"), list) else None,
                    model_name=str(desired_in.get("model_name") or "xgboost").strip() or "xgboost",
                    horizon=int(desired_in.get("horizon") or 50),
                    threshold_up=float(desired_in.get("threshold_up") or 0.6),
                    threshold_down=float(desired_in.get("threshold_down") or 0.6),
                    position_size=int(desired_in.get("position_size") or 1),
                    artifacts_dir=str(desired_in.get("artifacts_dir") or "artifacts").strip() or "artifacts",
                    poll_interval_sec=float(desired_in.get("poll_interval_sec") or 0.5),
                )
                write_strategy_desired(runs_dir=runs_dir, profile=prof, desired=d)
            except Exception as e:
                raise HTTPException(status_code=400, detail=f"strategy_desired_failed: {e}")

        # Read desired config to build argv
        try:
            from ghtrader.trading.strategy_control import read_strategy_desired

            cfg = read_strategy_desired(runs_dir=runs_dir, profile=prof)
        except Exception:
            cfg = None

        if not cfg:
            return {"ok": False, "error": "no_desired", "message": "No strategy desired.json found. Please set desired state first."}

        symbols = cfg.symbols or []
        if not symbols:
            return {"ok": False, "error": "no_symbols", "message": "Symbols required for strategy start."}

        # Check if already running
        store2: JobStore = request.app.state.job_store
        for j in store2.list_active_jobs():
            if _is_strategy_job(j.command):
                ap2 = _argv_opt(j.command, "--account") or ""
                try:
                    from ghtrader.tq.runtime import canonical_account_profile

                    ap2 = canonical_account_profile(ap2)
                except Exception:
                    ap2 = str(ap2).strip().lower() or "default"
                if ap2 == prof:
                    return {"ok": False, "error": "already_running", "job_id": j.id, "message": f"Strategy for profile '{prof}' is already running."}

        # Build argv matching _strategy_supervisor_tick
        model_name = cfg.model_name or "xgboost"
        horizon = str(cfg.horizon or 50)
        threshold_up = str(cfg.threshold_up or 0.6)
        threshold_down = str(cfg.threshold_down or 0.6)
        position_size = str(cfg.position_size or 1)
        artifacts_dir = cfg.artifacts_dir or "artifacts"
        poll_interval_sec = str(cfg.poll_interval_sec or 0.5)

        argv = python_module_argv(
            "ghtrader.cli",
            "strategy",
            "run",
            "--account",
            prof,
            "--model",
            model_name,
            "--horizon",
            horizon,
            "--threshold-up",
            threshold_up,
            "--threshold-down",
            threshold_down,
            "--position-size",
            position_size,
            "--artifacts-dir",
            artifacts_dir,
            "--runs-dir",
            str(runs_dir),
            "--poll-interval-sec",
            poll_interval_sec,
        )
        for s in symbols:
            argv += ["--symbols", str(s)]

        title = f"strategy {prof} {model_name} h={horizon}"
        jm2: JobManager = request.app.state.job_manager
        rec = jm2.start_job(
            JobSpec(
                title=title,
                argv=argv,
                cwd=Path.cwd(),
                metadata={
                    "kind": "strategy",
                    "account_profile": prof,
                    "model": model_name,
                    "horizon": str(cfg.horizon or 50),
                    "symbols": [str(s).strip() for s in symbols if str(s).strip()],
                },
            )
        )
        return {"ok": True, "account_profile": prof, "job_id": rec.id}

    @app.post("/api/strategy/stop", response_class=JSONResponse)
    async def api_strategy_stop(request: Request) -> dict[str, Any]:
        """
        Stop the running strategy job for the selected profile.

        Payload: {account_profile}
        """
        if not auth.is_authorized(request):
            raise HTTPException(status_code=401, detail="Unauthorized")
        payload = await request.json()
        ap = str(payload.get("account_profile") or "default").strip() or "default"

        try:
            from ghtrader.tq.runtime import canonical_account_profile

            prof = canonical_account_profile(ap)
        except Exception:
            prof = str(ap).strip().lower() or "default"

        store2: JobStore = request.app.state.job_store
        jm2: JobManager = request.app.state.job_manager
        cancelled = False
        job_id = None

        for j in store2.list_active_jobs():
            if _is_strategy_job(j.command):
                ap2 = _argv_opt(j.command, "--account") or ""
                try:
                    from ghtrader.tq.runtime import canonical_account_profile

                    ap2 = canonical_account_profile(ap2)
                except Exception:
                    ap2 = str(ap2).strip().lower() or "default"
                if ap2 == prof:
                    job_id = j.id
                    cancelled = bool(jm2.cancel_job(j.id))
                    break

        # Also set desired mode to idle so supervisor doesn't restart
        runs_dir = get_runs_dir()
        try:
            from dataclasses import replace
            from ghtrader.trading.strategy_control import read_strategy_desired, write_strategy_desired

            existing = read_strategy_desired(runs_dir=runs_dir, profile=prof)
            if existing and existing.mode != "idle":
                write_strategy_desired(runs_dir=runs_dir, profile=prof, desired=replace(existing, mode="idle"))
        except Exception:
            pass

        if job_id:
            return {"ok": True, "account_profile": prof, "job_id": job_id, "cancelled": cancelled}
        return {"ok": False, "error": "not_running", "message": f"No running strategy job for profile '{prof}'."}

    @app.get("/api/strategy/runs", response_class=JSONResponse)
    def api_strategy_runs(request: Request, account_profile: str = "", limit: int = 50) -> dict[str, Any]:
        """
        List recent strategy runs under runs/strategy/<run_id>/ (excludes account=<profile> stable dirs).
        """
        if not auth.is_authorized(request):
            raise HTTPException(status_code=401, detail="Unauthorized")

        runs_dir = get_runs_dir()
        root = runs_dir / "strategy"
        lim = max(1, min(int(limit or 50), 200))

        profile_filter = ""
        if str(account_profile or "").strip():
            try:
                from ghtrader.tq.runtime import canonical_account_profile

                profile_filter = canonical_account_profile(str(account_profile))
            except Exception:
                profile_filter = str(account_profile).strip().lower()

        if not root.exists():
            return {"ok": True, "runs": []}

        out: list[dict[str, Any]] = []
        try:
            dirs = [p for p in root.iterdir() if p.is_dir() and not p.name.startswith("account=")]
            # Newest first (run_id is timestamp-prefixed in current implementation).
            dirs = sorted(dirs, key=lambda x: x.name, reverse=True)[:lim]
            for d in dirs:
                cfg = _read_json_file(d / "run_config.json") or {}
                ap = str(cfg.get("account_profile") or "").strip()
                if ap:
                    try:
                        from ghtrader.tq.runtime import canonical_account_profile

                        ap = canonical_account_profile(ap)
                    except Exception:
                        ap = ap.strip().lower()
                if profile_filter and ap != profile_filter:
                    continue
                last_ev = _read_last_jsonl_obj(d / "events.jsonl") if (d / "events.jsonl").exists() else None
                out.append(
                    {
                        "run_id": d.name,
                        "account_profile": ap or None,
                        "symbols": cfg.get("symbols"),
                        "model_name": cfg.get("model_name"),
                        "horizon": cfg.get("horizon"),
                        "position_size": cfg.get("position_size"),
                        "threshold_up": cfg.get("threshold_up"),
                        "threshold_down": cfg.get("threshold_down"),
                        "created_at": cfg.get("created_at"),
                        "last_event_ts": (last_ev or {}).get("ts") if isinstance(last_ev, dict) else None,
                    }
                )
        except Exception as e:
            return {"ok": False, "error": str(e), "runs": []}

        return {"ok": True, "runs": out}

    @app.get("/api/trading/console/status", response_class=JSONResponse)
    def api_trading_console_status(request: Request, account_profile: str = "default", var: str = "") -> dict[str, Any]:
        """
        Unified Trading Console status (Gateway-first):
        - gateway: runs/gateway/account=<profile>/...
        - strategy: runs/strategy/account=<profile>/...
        """
        if not auth.is_authorized(request):
            raise HTTPException(status_code=401, detail="Unauthorized")

        var_filter = _normalize_variety_for_api(var) if str(var or "").strip() else ""
        runs_dir = get_runs_dir()
        try:
            from ghtrader.tq.runtime import canonical_account_profile

            prof = canonical_account_profile(str(account_profile or "default"))
        except Exception:
            prof = str(account_profile or "default").strip() or "default"

        live_enabled = False
        try:
            from ghtrader.config import is_live_enabled

            live_enabled = bool(is_live_enabled())
        except Exception:
            live_enabled = False

        # Compose from existing endpoints (keep behavior consistent and minimize duplication).
        gw = gateway_status_payload(account_profile=prof)
        st = api_strategy_status(request, account_profile=prof)

        # Add derived component statuses for clarity.
        try:
            gw_root = Path(str(gw.get("root") or ""))
            gw_state_path = gw_root / "state.json" if str(gw.get("root") or "") else None
            gw_age = _artifact_age_sec(gw_state_path) if (gw_state_path and gw_state_path.exists()) else None
            gw_desired = gw.get("desired") if isinstance(gw.get("desired"), dict) else {}
            gw_cfg = gw_desired.get("desired") if isinstance(gw_desired.get("desired"), dict) else gw_desired
            gw_mode = str((gw_cfg or {}).get("mode") or "idle")
            gw_status = _status_from_desired_and_state(root_exists=bool(gw.get("exists")), desired_mode=gw_mode, state_age=gw_age)
        except Exception:
            gw_status = "unknown"
            gw_age = None

        # Find active gateway/strategy jobs for this profile
        gateway_job: dict[str, Any] | None = None
        strategy_job: dict[str, Any] | None = None
        try:
            store2: JobStore = request.app.state.job_store
            for j in store2.list_active_jobs():
                if _is_gateway_job(j.command):
                    ap2 = _argv_opt(j.command, "--account") or ""
                    try:
                        from ghtrader.tq.runtime import canonical_account_profile

                        ap2 = canonical_account_profile(ap2)
                    except Exception:
                        ap2 = str(ap2).strip().lower() or "default"
                    if ap2 == prof and gateway_job is None:
                        gateway_job = {"job_id": j.id, "status": j.status, "started_at": j.started_at}
                elif _is_strategy_job(j.command):
                    ap2 = _argv_opt(j.command, "--account") or ""
                    try:
                        from ghtrader.tq.runtime import canonical_account_profile

                        ap2 = canonical_account_profile(ap2)
                    except Exception:
                        ap2 = str(ap2).strip().lower() or "default"
                    if ap2 == prof and strategy_job is None:
                        strategy_job = {"job_id": j.id, "status": j.status, "started_at": j.started_at}
        except Exception:
            pass

        # Compute stale flags: state_age_sec > 30 and desired mode is not idle
        STALE_THRESHOLD_SEC = 30.0
        gw_stale = False
        st_stale = False
        try:
            if gw_age is not None and float(gw_age) > STALE_THRESHOLD_SEC and gw_mode not in {"", "idle"}:
                gw_stale = True
        except Exception:
            pass
        try:
            st_age = st.get("state_age_sec")
            st_desired = st.get("desired") if isinstance(st.get("desired"), dict) else {}
            st_cfg = st_desired.get("desired") if isinstance(st_desired.get("desired"), dict) else st_desired
            st_mode = str((st_cfg or {}).get("mode") or "idle")
            if st_age is not None and float(st_age) > STALE_THRESHOLD_SEC and st_mode not in {"", "idle"}:
                st_stale = True
        except Exception:
            pass

        if var_filter:
            # Gateway desired/effective symbols and snapshots filtered for current variety view.
            gw_desired = gw.get("desired") if isinstance(gw.get("desired"), dict) else None
            if isinstance(gw_desired, dict):
                gw_desired2 = dict(gw_desired)
                inner = gw_desired2.get("desired") if isinstance(gw_desired2.get("desired"), dict) else None
                if isinstance(inner, dict):
                    inner2 = dict(inner)
                    if isinstance(inner2.get("symbols"), list):
                        inner2["symbols"] = [s for s in inner2.get("symbols") or [] if _symbol_matches_variety(str(s), var_filter)]
                    gw_desired2["desired"] = inner2
                elif isinstance(gw_desired2.get("symbols"), list):
                    gw_desired2["symbols"] = [s for s in gw_desired2.get("symbols") or [] if _symbol_matches_variety(str(s), var_filter)]
                gw["desired"] = gw_desired2

            gw_state = gw.get("state") if isinstance(gw.get("state"), dict) else None
            if isinstance(gw_state, dict):
                gw_state2 = dict(gw_state)
                eff = gw_state2.get("effective") if isinstance(gw_state2.get("effective"), dict) else None
                if isinstance(eff, dict):
                    eff2 = dict(eff)
                    if isinstance(eff2.get("symbols"), list):
                        eff2["symbols"] = [s for s in eff2.get("symbols") or [] if _symbol_matches_variety(str(s), var_filter)]
                    gw_state2["effective"] = eff2
                snap = gw_state2.get("last_snapshot") if isinstance(gw_state2.get("last_snapshot"), dict) else None
                if isinstance(snap, dict):
                    snap2 = dict(snap)
                    pos = snap2.get("positions")
                    if isinstance(pos, dict):
                        snap2["positions"] = {k: v for k, v in pos.items() if _symbol_matches_variety(str(k), var_filter)}
                    orders_alive = snap2.get("orders_alive")
                    if isinstance(orders_alive, list):
                        snap2["orders_alive"] = [
                            o
                            for o in orders_alive
                            if isinstance(o, dict) and _symbol_matches_variety(str(o.get("symbol") or ""), var_filter)
                        ]
                    gw_state2["last_snapshot"] = snap2
                gw["state"] = gw_state2

            st_desired = st.get("desired") if isinstance(st.get("desired"), dict) else None
            if isinstance(st_desired, dict):
                st_desired2 = dict(st_desired)
                inner = st_desired2.get("desired") if isinstance(st_desired2.get("desired"), dict) else None
                if isinstance(inner, dict):
                    inner2 = dict(inner)
                    if isinstance(inner2.get("symbols"), list):
                        inner2["symbols"] = [s for s in inner2.get("symbols") or [] if _symbol_matches_variety(str(s), var_filter)]
                    st_desired2["desired"] = inner2
                elif isinstance(st_desired2.get("symbols"), list):
                    st_desired2["symbols"] = [s for s in st_desired2.get("symbols") or [] if _symbol_matches_variety(str(s), var_filter)]
                st["desired"] = st_desired2

            st_state = st.get("state") if isinstance(st.get("state"), dict) else None
            if isinstance(st_state, dict):
                st_state2 = dict(st_state)
                last_targets = st_state2.get("last_targets")
                if isinstance(last_targets, dict):
                    st_state2["last_targets"] = {
                        k: v for k, v in last_targets.items() if _symbol_matches_variety(str(k), var_filter)
                    }
                events = st_state2.get("recent_events")
                if isinstance(events, list):
                    filtered_events: list[Any] = []
                    for e in events:
                        if not isinstance(e, dict):
                            continue
                        e2 = dict(e)
                        targets = e2.get("targets")
                        if isinstance(targets, dict):
                            targets2 = {k: v for k, v in targets.items() if _symbol_matches_variety(str(k), var_filter)}
                            if targets2:
                                e2["targets"] = targets2
                                filtered_events.append(e2)
                                continue
                        sym = str(e2.get("symbol") or "")
                        if sym and not _symbol_matches_variety(sym, var_filter):
                            continue
                        filtered_events.append(e2)
                    st_state2["recent_events"] = filtered_events
                st["state"] = st_state2

        payload = {
            "ok": True,
            "account_profile": prof,
            "live_enabled": bool(live_enabled),
            "state": ("warn" if (gw_stale or st_stale) else "ok"),
            "text": ("state stale" if (gw_stale or st_stale) else "healthy"),
            "error": "",
            "stale": bool(gw_stale or st_stale),
            "updated_at": _now_iso(),
            "gateway": {**gw, "component_status": gw_status, "state_age_sec": gw_age, "stale": gw_stale},
            "strategy": {**st, "stale": st_stale},
            "gateway_job": gateway_job,
            "strategy_job": strategy_job,
            "generated_at": _now_iso(),
        }
        if var_filter:
            payload["var"] = var_filter
        return payload

    app.include_router(models_router)
    app.include_router(accounts_router)
    app.include_router(gateway_router)

    return app


app = create_app()
