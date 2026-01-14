from __future__ import annotations

from dataclasses import asdict
import os
import sys
import threading
import time
import json
import uuid
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import structlog
from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import FileResponse, JSONResponse, PlainTextResponse
from fastapi.staticfiles import StaticFiles

from ghtrader.config import get_artifacts_dir, get_data_dir, get_runs_dir
from ghtrader.control import auth
from ghtrader.control.db import JobStore
from ghtrader.control.jobs import JobManager, JobSpec, python_module_argv
from ghtrader.control.views import build_router

log = structlog.get_logger()

_TQSDK_HEAVY_SUBCOMMANDS = {"download", "download-contract-range", "record", "probe-l5", "update"}

_UI_STATUS_TTL_S = 5.0
_ui_status_at: float = 0.0
_ui_status_payload: dict[str, Any] | None = None
_ui_status_lock = threading.Lock()

_DASH_SUMMARY_TTL_S = 10.0
_dash_summary_at: float = 0.0
_dash_summary_payload: dict[str, Any] | None = None
_dash_summary_lock = threading.Lock()

_BENCHMARKS_TTL_S = 10.0
_benchmarks_at: float = 0.0
_benchmarks_payload: dict[str, Any] | None = None
_benchmarks_lock = threading.Lock()


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


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
    """
    Best-effort extraction of `ghtrader <subcommand>` from argv.
    """
    try:
        for i, t in enumerate(argv):
            if str(t) == "ghtrader.cli" and i + 1 < len(argv):
                return str(argv[i + 1])
        # Fallback for direct `ghtrader <cmd>` style
        if len(argv) >= 2 and str(argv[0]).endswith("ghtrader") and not str(argv[1]).startswith("-"):
            return str(argv[1])
    except Exception:
        return None
    return None


def _is_tqsdk_heavy_job(argv: list[str]) -> bool:
    sub = _job_subcommand(argv) or ""
    return sub in _TQSDK_HEAVY_SUBCOMMANDS


def _scheduler_enabled() -> bool:
    if str(os.environ.get("GHTRADER_DISABLE_TQSDK_SCHEDULER", "")).strip().lower() in {"1", "true", "yes"}:
        return False
    # Avoid background threads during pytest unless explicitly enabled.
    if ("pytest" in sys.modules or os.environ.get("PYTEST_CURRENT_TEST")) and str(
        os.environ.get("GHTRADER_ENABLE_TQSDK_SCHEDULER_IN_TESTS", "")
    ).strip().lower() not in {"1", "true", "yes"}:
        return False
    return True


def _daily_update_targets_from_env() -> list[tuple[str, str]]:
    """
    Parse `GHTRADER_DAILY_UPDATE_TARGETS` into [(EXCHANGE, var), ...].

    Examples:
    - "SHFE:cu"
    - "SHFE:cu,SHFE:au"
    - "SHFE.cu" (also accepted)
    """
    raw = str(os.environ.get("GHTRADER_DAILY_UPDATE_TARGETS", "")).strip()
    if not raw:
        return []
    out: list[tuple[str, str]] = []
    for part in [p.strip() for p in raw.replace(";", ",").split(",") if p.strip()]:
        ex = ""
        v = ""
        if ":" in part:
            ex, v = part.split(":", 1)
        elif "." in part:
            ex, v = part.split(".", 1)
        ex = str(ex).upper().strip()
        v = str(v).lower().strip()
        if ex and v:
            out.append((ex, v))
    # De-dupe while preserving order
    seen: set[str] = set()
    uniq: list[tuple[str, str]] = []
    for ex, v in out:
        k = f"{ex}:{v}"
        if k in seen:
            continue
        seen.add(k)
        uniq.append((ex, v))
    return uniq


def _daily_update_enabled() -> bool:
    if not _daily_update_targets_from_env():
        return False
    if str(os.environ.get("GHTRADER_DISABLE_DAILY_UPDATE", "")).strip().lower() in {"1", "true", "yes"}:
        return False
    # Avoid background threads during pytest unless explicitly enabled.
    if ("pytest" in sys.modules or os.environ.get("PYTEST_CURRENT_TEST")) and str(
        os.environ.get("GHTRADER_ENABLE_DAILY_UPDATE_IN_TESTS", "")
    ).strip().lower() not in {"1", "true", "yes"}:
        return False
    return True


def _daily_update_state_path(*, runs_dir: Path) -> Path:
    return runs_dir / "control" / "cache" / "daily_update" / "state.json"


def _read_json(path: Path) -> dict[str, Any] | None:
    try:
        if not path.exists():
            return None
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return None


def _write_json_atomic(path: Path, obj: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(f".tmp-{uuid.uuid4().hex}")
    tmp.write_text(json.dumps(obj, ensure_ascii=False, indent=2, sort_keys=True, default=str), encoding="utf-8")
    tmp.replace(path)


def _argv_opt(argv: list[str], name: str) -> str | None:
    try:
        for i, t in enumerate(argv):
            if str(t) == str(name) and i + 1 < len(argv):
                return str(argv[i + 1])
    except Exception:
        return None
    return None


def _start_daily_update_scheduler(app: FastAPI) -> None:
    if getattr(app.state, "_daily_update_scheduler_started", False):
        return
    app.state._daily_update_scheduler_started = True

    def _loop() -> None:
        while True:
            try:
                _daily_update_tick(app=app)
            except Exception as e:
                log.warning("daily_update.tick_failed", error=str(e))
            time.sleep(float(os.environ.get("GHTRADER_DAILY_UPDATE_POLL_SECONDS", "60")))

    t = threading.Thread(target=_loop, name="daily-update-scheduler", daemon=True)
    t.start()


def _daily_update_tick(*, app: FastAPI) -> None:
    runs_dir = get_runs_dir()
    data_dir = get_data_dir()
    store: JobStore = app.state.job_store
    jm: JobManager = app.state.job_manager

    targets = _daily_update_targets_from_env()
    if not targets:
        return

    # Determine "today trading day" (UTC) so we enqueue at most once per trading day.
    from ghtrader.trading_calendar import get_trading_calendar

    cal = get_trading_calendar(data_dir=data_dir, refresh=False)
    today = datetime.now(timezone.utc).date()
    today_trading = today
    if cal:
        # last <= today
        lo = 0
        hi = len(cal)
        while lo < hi:
            mid = (lo + hi) // 2
            if cal[mid] <= today:
                lo = mid + 1
            else:
                hi = mid
        idx = lo - 1
        today_trading = cal[idx] if idx >= 0 else today
    else:
        while today_trading.weekday() >= 5:
            today_trading -= timedelta(days=1)

    st_path = _daily_update_state_path(runs_dir=runs_dir)
    state = _read_json(st_path) or {}
    last_by_target: dict[str, str] = dict(state.get("last_enqueued_trading_day") or {})

    active = store.list_active_jobs() + store.list_unstarted_queued_jobs(limit=500)

    for ex, v in targets:
        key = f"{ex}:{v}"
        if str(last_by_target.get(key) or "") == today_trading.isoformat():
            continue

        # Skip if an update job for this target is already queued/running.
        already = False
        for j in active:
            sub = _job_subcommand(j.command) or ""
            if sub != "update":
                continue
            j_ex = _argv_opt(j.command, "--exchange")
            j_v = _argv_opt(j.command, "--var")
            if str(j_ex or "").upper().strip() == ex and str(j_v or "").lower().strip() == v:
                already = True
                break
        if already:
            continue

        recent_days = int(os.environ.get("GHTRADER_DAILY_UPDATE_RECENT_EXPIRED_DAYS", "10"))
        argv = python_module_argv(
            "ghtrader.cli",
            "update",
            "--exchange",
            ex,
            "--var",
            v,
            "--recent-expired-days",
            str(int(recent_days)),
            "--data-dir",
            str(data_dir),
            "--runs-dir",
            str(runs_dir),
        )
        title = f"daily-update {ex}.{v} {today_trading.isoformat()}"
        jm.enqueue_job(JobSpec(title=title, argv=argv, cwd=Path.cwd()))
        last_by_target[key] = today_trading.isoformat()
        log.info("daily_update.enqueued", exchange=ex, var=v, trading_day=today_trading.isoformat())

    state = {"updated_at": _now_iso(), "last_enqueued_trading_day": last_by_target}
    try:
        _write_json_atomic(st_path, state)
    except Exception:
        pass


def _start_tqsdk_scheduler(app: FastAPI, *, max_parallel_default: int = 4) -> None:
    if getattr(app.state, "_tqsdk_scheduler_started", False):
        return
    app.state._tqsdk_scheduler_started = True

    def _loop() -> None:
        while True:
            try:
                store = app.state.job_store
                jm = app.state.job_manager
                max_parallel = int(os.environ.get("GHTRADER_MAX_PARALLEL_TQSDK_JOBS", str(max_parallel_default)))
                max_parallel = max(1, max_parallel)
                _tqsdk_scheduler_tick(store=store, jm=jm, max_parallel=max_parallel)
            except Exception as e:
                log.warning("tqsdk_scheduler.tick_failed", error=str(e))
            time.sleep(1.0)

    t = threading.Thread(target=_loop, name="tqsdk-job-scheduler", daemon=True)
    t.start()


def _tqsdk_scheduler_tick(*, store: JobStore, jm: JobManager, max_parallel: int) -> int:
    """
    Run one scheduling tick: start up to `max_parallel` TqSdk-heavy queued jobs.

    Exposed for unit tests.
    """
    active = [j for j in store.list_active_jobs() if _is_tqsdk_heavy_job(j.command)]
    slots = int(max_parallel) - int(len(active))
    if slots <= 0:
        return 0
    queued = [j for j in store.list_unstarted_queued_jobs(limit=5000) if _is_tqsdk_heavy_job(j.command)]
    started = 0
    for j in queued[:slots]:
        out = jm.start_queued_job(j.id)
        if out is not None and out.pid is not None:
            started += 1
    return started


def _control_root(runs_dir: Path) -> Path:
    return runs_dir / "control"


def _jobs_db_path(runs_dir: Path) -> Path:
    return _control_root(runs_dir) / "jobs.db"


def _logs_dir(runs_dir: Path) -> Path:
    return _control_root(runs_dir) / "logs"


def create_app() -> Any:
    runs_dir = get_runs_dir()
    store = JobStore(_jobs_db_path(runs_dir))
    jm = JobManager(store=store, logs_dir=_logs_dir(runs_dir))
    jm.reconcile()

    app = FastAPI(title="ghTrader Control", version="0.1.0")
    app.state.job_store = store
    app.state.job_manager = jm
    if _scheduler_enabled():
        _start_tqsdk_scheduler(app)
    if _daily_update_enabled():
        _start_daily_update_scheduler(app)

    # Static assets (CSS)
    static_dir = Path(__file__).parent / "static"
    app.mount("/static", StaticFiles(directory=str(static_dir)), name="static")

    # HTML pages
    app.include_router(build_router())

    # JSON API (kept small; UI uses HTML routes above)
    @app.get("/health", response_class=JSONResponse)
    def health() -> dict[str, Any]:
        return {"ok": True}

    @app.get("/api/jobs", response_class=JSONResponse)
    def api_list_jobs(request: Request, limit: int = 200) -> dict[str, Any]:
        if not auth.is_authorized(request):
            raise HTTPException(status_code=401, detail="Unauthorized")
        jobs = store.list_jobs(limit=int(limit))
        return {"jobs": [asdict(j) for j in jobs]}

    @app.get("/api/system", response_class=JSONResponse)
    def api_system(request: Request, include_dir_sizes: bool = False, refresh: str = "none") -> dict[str, Any]:
        """
        Cached system snapshot for the dashboard System page.

        Query params:
        - include_dir_sizes: include cached directory sizes (may still be computing)
        - refresh: none|fast|dir
        """
        if not auth.is_authorized(request):
            raise HTTPException(status_code=401, detail="Unauthorized")

        from ghtrader.control.system_info import system_snapshot

        return system_snapshot(
            data_dir=get_data_dir(),
            runs_dir=get_runs_dir(),
            artifacts_dir=get_artifacts_dir(),
            include_dir_sizes=bool(include_dir_sizes),
            refresh=str(refresh or "none"),
        )

    @app.get("/api/contracts", response_class=JSONResponse)
    def api_contracts(
        request: Request,
        exchange: str = "SHFE",
        var: str = "cu",
        refresh: str = "0",
        refresh_l5: str = "0",
    ) -> dict[str, Any]:
        """
        Contract explorer backend: TqSdk catalog + local lake coverage + L5 status + probe cache.

        Query params:
        - exchange: SHFE
        - var: cu|au|ag
        - refresh: 1 to force refresh of the TqSdk contract catalog cache
        - refresh_l5: 1 to force local L5 rescan (bounded; may still return partial)
        """
        if not auth.is_authorized(request):
            raise HTTPException(status_code=401, detail="Unauthorized")

        ex = str(exchange).upper().strip() or "SHFE"
        v = str(var).lower().strip() or "cu"
        lv = "v2"

        from ghtrader.control.contract_status import compute_contract_statuses
        from ghtrader.tqsdk_catalog import get_contract_catalog
        from ghtrader.tqsdk_l5_probe import load_probe_result

        runs_dir = get_runs_dir()
        data_dir = get_data_dir()

        cat = get_contract_catalog(exchange=ex, var=v, runs_dir=runs_dir, refresh=str(refresh).strip() in {"1", "true", "yes"})
        if not bool(cat.get("ok", False)):
            return {
                "ok": False,
                "exchange": ex,
                "var": v,
                "lake_version": lv,
                "contracts": [],
                "error": str(cat.get("error") or "tqsdk_catalog_failed"),
            }

        # Union: TqSdk catalog (contract-service + pre-2020 cache) + local lake symbols.
        base_contracts = list(cat.get("contracts") or [])
        by_sym: dict[str, dict[str, Any]] = {}
        for r in base_contracts:
            sym = str((r or {}).get("symbol") or "").strip()
            if not sym:
                continue
            by_sym[sym] = dict(r)

        # Local symbols ensure contracts already downloaded are never invisible.
        try:
            ticks_root = data_dir / "lake_v2" / "ticks"
            prefix = f"{ex}.{v}".lower()
            if ticks_root.exists():
                for p in sorted(ticks_root.iterdir(), key=lambda x: x.name):
                    if not p.is_dir() or not p.name.startswith("symbol="):
                        continue
                    sym = p.name.split("=", 1)[-1]
                    if not sym or not str(sym).lower().startswith(prefix):
                        continue
                    if sym not in by_sym:
                        by_sym[sym] = {"symbol": sym, "expired": None, "expire_datetime": None, "catalog_source": "local_lake"}
        except Exception:
            pass

        merged_contracts = [by_sym[s] for s in sorted(by_sym.keys())]
        syms = list(by_sym.keys())

        # QuestDB canonical coverage (best-effort) is used to improve expected ranges.
        cov: dict[str, dict[str, Any]] = {}
        questdb_info: dict[str, Any] = {"ok": False}
        try:
            from ghtrader.config import (
                get_questdb_host,
                get_questdb_pg_dbname,
                get_questdb_pg_password,
                get_questdb_pg_port,
                get_questdb_pg_user,
            )
            from ghtrader.questdb_queries import QuestDBQueryConfig, query_contract_coverage

            tbl = f"ghtrader_ticks_raw_{lv}"
            cfg = QuestDBQueryConfig(
                host=get_questdb_host(),
                pg_port=int(get_questdb_pg_port()),
                pg_user=str(get_questdb_pg_user()),
                pg_password=str(get_questdb_pg_password()),
                pg_dbname=str(get_questdb_pg_dbname()),
            )
            cov = query_contract_coverage(cfg=cfg, table=tbl, symbols=syms, lake_version=lv, ticks_lake="raw")
            questdb_info = {"ok": True, "table": tbl}
        except Exception as e:
            cov = {}
            questdb_info = {"ok": False, "error": str(e)}

        status = compute_contract_statuses(
            exchange=ex,
            var=v,
            lake_version=lv,
            data_dir=data_dir,
            runs_dir=runs_dir,
            contracts=merged_contracts,
            refresh_l5=str(refresh_l5).strip() in {"1", "true", "yes"},
            questdb_cov_by_symbol=cov if questdb_info.get("ok") else None,
        )

        # Attach cached probe results per symbol (if present).
        for r in status.get("contracts") or []:
            sym = str(r.get("symbol") or "").strip()
            if not sym:
                continue
            pr = load_probe_result(symbol=sym, runs_dir=runs_dir)
            if pr:
                r["tqsdk_probe"] = {
                    "probed_day": pr.get("probed_day"),
                    "ticks_rows": pr.get("ticks_rows"),
                    "l5_present": pr.get("l5_present"),
                    "error": pr.get("error"),
                    "updated_at": pr.get("updated_at"),
                }
            else:
                r["tqsdk_probe"] = None

        # Attach QuestDB canonical coverage results (if available).
        for r in status.get("contracts") or []:
            sym = str(r.get("symbol") or "").strip()
            qc = cov.get(sym) if sym and cov else None
            r["questdb_coverage"] = qc
            # Derived per-row DB status for UI convenience.
            db_status = "unknown"
            if bool(questdb_info.get("ok")):
                if not qc or not (qc.get("first_tick_day") or qc.get("last_tick_day")):
                    db_status = "empty"
                else:
                    if r.get("expired") is False:
                        # Active: stale if behind expected_last (today trading day).
                        exp_last = str(r.get("expected_last") or "").strip()
                        last = str(qc.get("last_tick_day") or "").strip()
                        if exp_last and last and last < exp_last:
                            db_status = "stale"
                        else:
                            db_status = "ok"
                    else:
                        db_status = "ok"
            else:
                db_status = "error"
            r["db_status"] = db_status
        status["questdb"] = questdb_info

        status["ok"] = True
        status["exchange"] = ex
        status["var"] = v
        status["lake_version"] = lv
        status["catalog_cached_at"] = cat.get("cached_at")
        status["catalog_source"] = cat.get("source")
        return status

    @app.post("/api/contracts/enqueue-probe-l5", response_class=JSONResponse)
    async def api_contracts_enqueue_probe_l5(request: Request) -> dict[str, Any]:
        """
        Enqueue per-contract L5 probe jobs (runs `ghtrader probe-l5 --symbol ...`).
        """
        if not auth.is_authorized(request):
            raise HTTPException(status_code=401, detail="Unauthorized")
        payload = await request.json()

        runs_dir = get_runs_dir()
        data_dir = get_data_dir()

        symbols: list[str] = []
        raw_syms = payload.get("symbols")
        if isinstance(raw_syms, list):
            symbols = [str(s).strip() for s in raw_syms if str(s).strip()]
        scope = str(payload.get("scope") or "").strip().lower()
        ex = str(payload.get("exchange") or "SHFE").upper().strip()
        v = str(payload.get("var") or "cu").lower().strip()

        if not symbols and scope == "all":
            from ghtrader.tqsdk_catalog import get_contract_catalog

            cat = get_contract_catalog(exchange=ex, var=v, runs_dir=runs_dir, refresh=False)
            if not bool(cat.get("ok", False)):
                return {"ok": False, "error": str(cat.get("error") or "tqsdk_catalog_failed")}
            symbols = [str(r.get("symbol") or "").strip() for r in (cat.get("contracts") or []) if str(r.get("symbol") or "").strip()]

        if not symbols:
            raise HTTPException(status_code=400, detail="No symbols to probe (pass symbols[] or scope=all)")

        enqueued: list[str] = []
        for sym in symbols:
            argv = python_module_argv("ghtrader.cli", "probe-l5", "--symbol", sym, "--data-dir", str(data_dir))
            title = f"probe-l5 {sym}"
            rec = jm.enqueue_job(JobSpec(title=title, argv=argv, cwd=Path.cwd()))
            enqueued.append(rec.id)

        return {"ok": True, "enqueued": enqueued, "count": int(len(enqueued))}

    @app.post("/api/contracts/enqueue-fill", response_class=JSONResponse)
    async def api_contracts_enqueue_fill(request: Request) -> dict[str, Any]:
        """
        Enqueue per-contract download jobs (runs `ghtrader download --symbol ...`).

        Payload:
        - symbols: optional list[str] (explicit)
        - scope: "missing" (default) | "all"
        - exchange/var: used when scope is set (to select contracts)
        - start/end: optional manual bounds (YYYY-MM-DD) used when active-ranges are missing
        """
        if not auth.is_authorized(request):
            raise HTTPException(status_code=401, detail="Unauthorized")
        payload = await request.json()

        runs_dir = get_runs_dir()
        data_dir = get_data_dir()

        symbols: list[str] = []
        raw_syms = payload.get("symbols")
        if isinstance(raw_syms, list):
            symbols = [str(s).strip() for s in raw_syms if str(s).strip()]

        scope = str(payload.get("scope") or "missing").strip().lower()
        ex = str(payload.get("exchange") or "SHFE").upper().strip()
        v = str(payload.get("var") or "cu").lower().strip()
        lv = "v2"

        if not symbols and scope in {"missing", "all"}:
            from ghtrader.control.contract_status import compute_contract_statuses
            from ghtrader.tqsdk_catalog import get_contract_catalog

            cat = get_contract_catalog(exchange=ex, var=v, runs_dir=runs_dir, refresh=False)
            if not bool(cat.get("ok", False)):
                return {"ok": False, "error": str(cat.get("error") or "tqsdk_catalog_failed")}
            status = compute_contract_statuses(
                exchange=ex,
                var=v,
                lake_version=lv,
                data_dir=data_dir,
                runs_dir=runs_dir,
                contracts=list(cat.get("contracts") or []),
                refresh_l5=False,
            )
            rows = list(status.get("contracts") or [])
            if scope == "missing":
                rows = [r for r in rows if str(r.get("status") or "") in {"not-downloaded", "incomplete", "stale"}]
            symbols = [str(r.get("symbol") or "").strip() for r in rows if str(r.get("symbol") or "").strip()]

        if not symbols:
            raise HTTPException(status_code=400, detail="No symbols to fill (pass symbols[] or scope=missing/all)")

        manual_start = str(payload.get("start") or "").strip() or None
        manual_end = str(payload.get("end") or "").strip() or None

        # Build a lookup of expected ranges using local active-ranges cache (no network).
        from ghtrader.control.contract_status import compute_contract_statuses
        from ghtrader.tqsdk_catalog import get_contract_catalog

        cat2 = get_contract_catalog(exchange=ex, var=v, runs_dir=runs_dir, refresh=False)
        by_catalog_sym = {str(r.get("symbol") or "").strip(): r for r in (cat2.get("contracts") or []) if str(r.get("symbol") or "").strip()}
        contracts_for_ranges = [by_catalog_sym.get(s) or {"symbol": s, "expired": None, "expire_datetime": None} for s in symbols]

        # Best-effort: prefer QuestDB canonical bounds when available for better Fill ranges.
        cov: dict[str, dict[str, Any]] = {}
        questdb_ok = False
        try:
            from ghtrader.config import (
                get_questdb_host,
                get_questdb_pg_dbname,
                get_questdb_pg_password,
                get_questdb_pg_port,
                get_questdb_pg_user,
            )
            from ghtrader.questdb_queries import QuestDBQueryConfig, query_contract_coverage

            lv_tbl = f"ghtrader_ticks_raw_{lv}"
            cfg = QuestDBQueryConfig(
                host=get_questdb_host(),
                pg_port=int(get_questdb_pg_port()),
                pg_user=str(get_questdb_pg_user()),
                pg_password=str(get_questdb_pg_password()),
                pg_dbname=str(get_questdb_pg_dbname()),
            )
            cov = query_contract_coverage(
                cfg=cfg,
                table=lv_tbl,
                symbols=[str(r.get("symbol") or "").strip() for r in contracts_for_ranges if str(r.get("symbol") or "").strip()],
                lake_version=lv,
                ticks_lake="raw",
            )
            questdb_ok = True
        except Exception:
            cov = {}
            questdb_ok = False

        ranges = compute_contract_statuses(
            exchange=ex,
            var=v,
            lake_version=lv,
            data_dir=data_dir,
            runs_dir=runs_dir,
            contracts=contracts_for_ranges,
            refresh_l5=False,
            questdb_cov_by_symbol=cov if questdb_ok else None,
        )
        by_sym = {str(r.get("symbol") or ""): r for r in (ranges.get("contracts") or [])}

        enqueued: list[str] = []
        skipped: list[dict[str, Any]] = []
        for sym in symbols:
            r = by_sym.get(sym) or {}
            start = str(r.get("expected_first") or "").strip() or manual_start
            end = str(r.get("expected_last") or "").strip() or manual_end
            if not start or not end:
                skipped.append({"symbol": sym, "reason": "missing_expected_range", "hint": "run download-contract-range once or provide start/end"})
                continue
            argv = python_module_argv(
                "ghtrader.cli",
                "download",
                "--symbol",
                sym,
                "--start",
                start,
                "--end",
                end,
                "--data-dir",
                str(data_dir),
            )
            title = f"download {sym}"
            rec = jm.enqueue_job(JobSpec(title=title, argv=argv, cwd=Path.cwd()))
            enqueued.append(rec.id)

        return {"ok": True, "enqueued": enqueued, "skipped": skipped, "count": int(len(enqueued))}

    @app.post("/api/contracts/enqueue-update", response_class=JSONResponse)
    async def api_contracts_enqueue_update(request: Request) -> dict[str, Any]:
        """
        Enqueue an Update job (runs `ghtrader update ...`) to check remote contract updates.

        Payload:
        - exchange/var: required for variety update
        - symbols: optional list[str] to update a subset
        - recent_expired_days: optional int (default 10)
        - refresh_catalog: optional bool (default true)
        """
        if not auth.is_authorized(request):
            raise HTTPException(status_code=401, detail="Unauthorized")
        payload = await request.json()

        runs_dir = get_runs_dir()
        data_dir = get_data_dir()

        ex = str(payload.get("exchange") or "SHFE").upper().strip()
        v = str(payload.get("var") or "cu").lower().strip()
        recent_days = int(payload.get("recent_expired_days") or 10)
        refresh_catalog = bool(payload.get("refresh_catalog", True))

        symbols: list[str] = []
        raw_syms = payload.get("symbols")
        if isinstance(raw_syms, list):
            symbols = [str(s).strip() for s in raw_syms if str(s).strip()]

        argv = python_module_argv(
            "ghtrader.cli",
            "update",
            "--exchange",
            ex,
            "--var",
            v,
            "--recent-expired-days",
            str(int(recent_days)),
            "--data-dir",
            str(data_dir),
            "--runs-dir",
            str(runs_dir),
        )
        if not refresh_catalog:
            argv += ["--no-refresh-catalog"]
        for s in symbols:
            argv += ["--symbols", s]

        title = f"update {ex}.{v}" if not symbols else (f"update {symbols[0]}" if len(symbols) == 1 else f"update {len(symbols)} symbols")
        jm = request.app.state.job_manager
        rec = jm.enqueue_job(JobSpec(title=title, argv=argv, cwd=Path.cwd()))
        return {"ok": True, "enqueued": [rec.id], "count": 1}

    @app.post("/api/contracts/enqueue-sync-questdb", response_class=JSONResponse)
    async def api_contracts_enqueue_sync_questdb(request: Request) -> dict[str, Any]:
        """
        Start per-contract QuestDB sync jobs (runs `ghtrader db serve-sync --backend questdb --symbol ...`).

        This is a local I/O + DB job (not TqSdk-heavy), so it starts immediately.
        """
        if not auth.is_authorized(request):
            raise HTTPException(status_code=401, detail="Unauthorized")
        payload = await request.json()

        data_dir = get_data_dir()
        runs_dir = get_runs_dir()

        raw_syms = payload.get("symbols")
        if not isinstance(raw_syms, list):
            raise HTTPException(status_code=400, detail="symbols must be a list[str]")
        symbols = [str(s).strip() for s in raw_syms if str(s).strip()]
        if not symbols:
            raise HTTPException(status_code=400, detail="At least one symbol is required")

        lv = "v2"

        # Best-effort: take QuestDB connection defaults from config/env.
        host = "127.0.0.1"
        ilp_port = "9009"
        pg_port = "8812"
        try:
            from ghtrader.config import get_questdb_host, get_questdb_ilp_port, get_questdb_pg_port

            host = str(get_questdb_host())
            ilp_port = str(int(get_questdb_ilp_port()))
            pg_port = str(int(get_questdb_pg_port()))
        except Exception:
            pass

        started: list[str] = []
        for sym in symbols:
            argv = python_module_argv(
                "ghtrader.cli",
                "db",
                "serve-sync",
                "--backend",
                "questdb",
                "--symbol",
                sym,
                "--ticks-lake",
                "raw",
                "--mode",
                "incremental",
                "--data-dir",
                str(data_dir),
                "--runs-dir",
                str(runs_dir),
                "--host",
                host,
                "--questdb-ilp-port",
                ilp_port,
                "--questdb-pg-port",
                pg_port,
            )
            title = f"db serve-sync {sym}"
            rec = jm.start_job(JobSpec(title=title, argv=argv, cwd=Path.cwd()))
            started.append(rec.id)

        return {"ok": True, "started": started, "count": int(len(started))}

    @app.post("/api/contracts/enqueue-audit", response_class=JSONResponse)
    async def api_contracts_enqueue_audit(request: Request) -> dict[str, Any]:
        """
        Start per-contract audit jobs (ticks scope, symbol-filtered).

        Unlike download/probe, audits are local and start immediately.
        """
        if not auth.is_authorized(request):
            raise HTTPException(status_code=401, detail="Unauthorized")
        payload = await request.json()

        data_dir = get_data_dir()
        runs_dir = get_runs_dir()

        raw_syms = payload.get("symbols")
        if not isinstance(raw_syms, list):
            raise HTTPException(status_code=400, detail="symbols must be a list[str]")
        symbols = [str(s).strip() for s in raw_syms if str(s).strip()]
        if not symbols:
            raise HTTPException(status_code=400, detail="At least one symbol is required")

        lv = "v2"

        started: list[str] = []
        for sym in symbols:
            argv = python_module_argv(
                "ghtrader.cli",
                "audit",
                "--scope",
                "ticks",
                "--symbol",
                sym,
                "--data-dir",
                str(data_dir),
                "--runs-dir",
                str(runs_dir),
            )
            title = f"audit ticks {sym}"
            rec = jm.start_job(JobSpec(title=title, argv=argv, cwd=Path.cwd()))
            started.append(rec.id)

        return {"ok": True, "started": started, "count": int(len(started))}

    @app.post("/api/jobs", response_class=JSONResponse)
    async def api_create_job(request: Request) -> dict[str, Any]:
        if not auth.is_authorized(request):
            raise HTTPException(status_code=401, detail="Unauthorized")

        payload = await request.json()
        title = str(payload.get("title") or "job")
        argv = payload.get("argv")
        if not isinstance(argv, list) or not all(isinstance(x, str) for x in argv):
            raise HTTPException(status_code=400, detail="argv must be a list[str]")

        cwd = Path(str(payload.get("cwd") or Path.cwd()))
        rec = jm.start_job(JobSpec(title=title, argv=list(argv), cwd=cwd))
        return {"job": asdict(rec)}

    @app.post("/api/jobs/{job_id}/cancel", response_class=JSONResponse)
    def api_cancel_job(request: Request, job_id: str) -> dict[str, Any]:
        if not auth.is_authorized(request):
            raise HTTPException(status_code=401, detail="Unauthorized")
        ok = jm.cancel_job(job_id)
        return {"ok": bool(ok)}

    @app.get("/api/jobs/{job_id}/log", response_class=PlainTextResponse)
    def api_job_log(request: Request, job_id: str, max_bytes: int = 64000) -> str:
        if not auth.is_authorized(request):
            raise HTTPException(status_code=401, detail="Unauthorized")
        return jm.read_log_tail(job_id, max_bytes=int(max_bytes))

    @app.get("/api/jobs/{job_id}/log/download")
    def api_job_log_download(request: Request, job_id: str) -> Any:
        if not auth.is_authorized(request):
            raise HTTPException(status_code=401, detail="Unauthorized")
        job = store.get_job(job_id)
        if job is None:
            raise HTTPException(status_code=404, detail="Job not found")
        if not job.log_path:
            raise HTTPException(status_code=404, detail="No log path for job")

        p = Path(str(job.log_path)).resolve()
        if not p.exists():
            raise HTTPException(status_code=404, detail="Log file not found")

        logs_root = _logs_dir(get_runs_dir()).resolve()
        if logs_root not in p.parents:
            raise HTTPException(status_code=400, detail="Invalid log path")

        return FileResponse(path=str(p), filename=p.name, media_type="text/plain")

    @app.get("/api/jobs/{job_id}/ingest_status", response_class=JSONResponse)
    def api_job_ingest_status(request: Request, job_id: str) -> dict[str, Any]:
        if not auth.is_authorized(request):
            raise HTTPException(status_code=401, detail="Unauthorized")
        job = store.get_job(job_id)
        if job is None:
            raise HTTPException(status_code=404, detail="Job not found")
        from ghtrader.control.ingest_status import ingest_status_for_job

        status = ingest_status_for_job(
            job_id=job.id,
            command=job.command,
            log_path=job.log_path,
            default_data_dir=get_data_dir(),
        )
        # Attach minimal job metadata for UI convenience.
        status.update(
            {
                "job_status": job.status,
                "title": job.title,
                "source": job.source,
                "created_at": job.created_at,
                "started_at": job.started_at,
            }
        )
        return status

    @app.get("/api/ingest/status", response_class=JSONResponse)
    def api_ingest_status(request: Request, limit: int = 200) -> dict[str, Any]:
        if not auth.is_authorized(request):
            raise HTTPException(status_code=401, detail="Unauthorized")
        from ghtrader.control.ingest_status import ingest_status_for_job, parse_ingest_command

        jobs = store.list_jobs(limit=int(limit))
        out: list[dict[str, Any]] = []
        for job in jobs:
            if job.status not in {"queued", "running"}:
                continue
            kind = parse_ingest_command(job.command).get("kind")
            if kind not in {"download", "download_contract_range", "record"}:
                continue
            status = ingest_status_for_job(
                job_id=job.id,
                command=job.command,
                log_path=job.log_path,
                default_data_dir=get_data_dir(),
            )
            status.update(
                {
                    "job_status": job.status,
                    "title": job.title,
                    "source": job.source,
                    "created_at": job.created_at,
                    "started_at": job.started_at,
                }
            )
            out.append(status)
        return {"jobs": out}

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
            from ghtrader.config import (
                get_questdb_host,
                get_questdb_pg_dbname,
                get_questdb_pg_password,
                get_questdb_pg_port,
                get_questdb_pg_user,
            )
            from ghtrader.questdb_queries import QuestDBQueryConfig, query_sql_read_only

            cfg = QuestDBQueryConfig(
                host=str(get_questdb_host()),
                pg_port=int(get_questdb_pg_port()),
                pg_user=str(get_questdb_pg_user()),
                pg_password=str(get_questdb_pg_password()),
                pg_dbname=str(get_questdb_pg_dbname()),
            )
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
    def api_dashboard_summary(request: Request) -> dict[str, Any]:
        """
        Aggregated KPIs for the dashboard home page.
        """
        if not auth.is_authorized(request):
            raise HTTPException(status_code=401, detail="Unauthorized")

        global _dash_summary_at, _dash_summary_payload
        now = time.time()
        with _dash_summary_lock:
            if _dash_summary_payload is not None and (now - float(_dash_summary_at)) <= float(_DASH_SUMMARY_TTL_S):
                return dict(_dash_summary_payload)

        data_dir = get_data_dir()
        runs_dir = get_runs_dir()
        artifacts_dir = get_artifacts_dir()

        # Job counts
        jobs = store.list_jobs(limit=200)
        running_count = len([j for j in jobs if j.status == "running"])
        queued_count = len([j for j in jobs if j.status == "queued"])

        # QuestDB reachability
        questdb_ok = False
        try:
            from ghtrader.config import (
                get_questdb_host,
                get_questdb_pg_dbname,
                get_questdb_pg_password,
                get_questdb_pg_port,
                get_questdb_pg_user,
            )
            import psycopg  # type: ignore

            with psycopg.connect(
                user=str(get_questdb_pg_user()),
                password=str(get_questdb_pg_password()),
                host=str(get_questdb_host()),
                port=int(get_questdb_pg_port()),
                dbname=str(get_questdb_pg_dbname()),
                connect_timeout=1,
            ) as conn:
                with conn.cursor() as cur:
                    cur.execute("SELECT 1")
                    cur.fetchone()
            questdb_ok = True
        except Exception:
            questdb_ok = False

        # Data symbols counts (v2-only; keep legacy v1 fields as zero)
        data_status = ""
        try:
            v2_syms = _iter_symbol_dirs(data_dir / "lake_v2" / "ticks")
            v1_syms: set[str] = set()
            union_syms = set(v2_syms)
            data_status = "lake_v2" if v2_syms else "empty"
        except Exception:
            v2_syms = set()
            v1_syms = set()
            union_syms = set()
            data_status = "error"

        # Model count (count actual model files, not arbitrary artifacts/ subdirs)
        model_files: list[dict[str, Any]] = []
        model_status = ""
        try:
            model_files = _scan_model_files(artifacts_dir)
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

        # Pipeline status (simplified)
        data_symbols = int(len(union_syms))
        pipeline = {
            "ingest": {"state": "ok" if data_symbols > 0 else "warn", "text": f"{data_symbols} symbols"},
            "sync": {"state": "ok" if questdb_ok else "error", "text": "connected" if questdb_ok else "offline"},
            "schedule": {"state": "unknown", "text": "--"},
            "main_l5": {"state": "unknown", "text": "--"},
            "build": {"state": "unknown", "text": "--"},
            "train": {"state": "ok" if model_count > 0 else "warn", "text": f"{model_count} models"},
        }

        # Check schedule exists
        try:
            schedule_dir = data_dir / "rolls" / "shfe_main_schedule" / "var=cu"
            if schedule_dir.exists() and (schedule_dir / "schedule.parquet").exists():
                pipeline["schedule"] = {"state": "ok", "text": "cu ready"}
        except Exception:
            pass

        # Check main_l5 exists
        try:
            main_l5_dir = data_dir / "lake_v2" / "main_l5" / "ticks"
            if main_l5_dir.exists():
                derived = [p for p in main_l5_dir.iterdir() if p.is_dir() and p.name.startswith("symbol=")]
                if derived:
                    pipeline["main_l5"] = {"state": "ok", "text": f"{len(derived)} derived"}
        except Exception:
            pass

        # Check features exist
        try:
            features_dir = data_dir / "features"
            if features_dir.exists():
                feat_symbols = [p for p in features_dir.iterdir() if p.is_dir() and p.name.startswith("symbol=")]
                if feat_symbols:
                    pipeline["build"] = {"state": "ok", "text": f"{len(feat_symbols)} symbols"}
        except Exception:
            pass

        payload = {
            "ok": True,
            "running_count": running_count,
            "queued_count": queued_count,
            "questdb_ok": questdb_ok,
            "data_symbols": data_symbols,
            "data_symbols_v1": int(len(v1_syms)),
            "data_symbols_v2": int(len(v2_syms)),
            "data_symbols_union": data_symbols,
            "data_status": data_status,
            "model_count": model_count,
            "model_status": model_status,
            "trading_mode": trading_mode,
            "trading_status": trading_status,
            "pipeline": pipeline,
        }
        with _dash_summary_lock:
            _dash_summary_payload = dict(payload)
            _dash_summary_at = time.time()
        return payload

    @app.get("/api/ui/status", response_class=JSONResponse)
    def api_ui_status(request: Request) -> dict[str, Any]:
        """
        Lightweight status endpoint for global UI chrome (topbar).

        Cached to avoid doing QuestDB connection checks too frequently.
        """
        if not auth.is_authorized(request):
            raise HTTPException(status_code=401, detail="Unauthorized")

        global _ui_status_at, _ui_status_payload
        now = time.time()
        with _ui_status_lock:
            if _ui_status_payload is not None and (now - float(_ui_status_at)) <= float(_UI_STATUS_TTL_S):
                return dict(_ui_status_payload)

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
            from ghtrader.config import (
                get_questdb_host,
                get_questdb_pg_dbname,
                get_questdb_pg_password,
                get_questdb_pg_port,
                get_questdb_pg_user,
            )
            import psycopg  # type: ignore

            with psycopg.connect(
                user=str(get_questdb_pg_user()),
                password=str(get_questdb_pg_password()),
                host=str(get_questdb_host()),
                port=int(get_questdb_pg_port()),
                dbname=str(get_questdb_pg_dbname()),
                connect_timeout=1,
            ) as conn:
                with conn.cursor() as cur:
                    cur.execute("SELECT 1")
                    cur.fetchone()
            questdb_ok = True
        except Exception:
            questdb_ok = False

        payload = {
            "ok": True,
            "questdb_ok": bool(questdb_ok),
            "gpu_status": gpu_status,
            "running_count": int(running_count),
            "queued_count": int(queued_count),
        }
        with _ui_status_lock:
            _ui_status_payload = dict(payload)
            _ui_status_at = time.time()
        return payload

    @app.get("/api/models/inventory", response_class=JSONResponse)
    def api_models_inventory(request: Request) -> dict[str, Any]:
        """
        List trained model artifacts from artifacts/ directory.
        """
        if not auth.is_authorized(request):
            raise HTTPException(status_code=401, detail="Unauthorized")

        artifacts_dir = get_artifacts_dir()
        models: list[dict[str, Any]] = []

        try:
            if not artifacts_dir.exists():
                return {"ok": True, "models": []}

            def _human_size(size_bytes: int) -> str:
                for unit in ["B", "KB", "MB", "GB"]:
                    if size_bytes < 1024:
                        return f"{size_bytes:.1f} {unit}"
                    size_bytes /= 1024
                return f"{size_bytes:.1f} TB"

            from datetime import datetime, timezone

            files = _scan_model_files(artifacts_dir)
            files_sorted = sorted(files, key=lambda x: float(x.get("mtime") or 0.0), reverse=True)
            for f in files_sorted:
                try:
                    mtime = float(f.get("mtime") or 0.0)
                    created_at = datetime.fromtimestamp(mtime, tz=timezone.utc).isoformat()
                    p = Path(str(f.get("path") or ""))
                    rel = str(p.relative_to(artifacts_dir)) if artifacts_dir in p.parents else str(p.name)
                    size_bytes = int(f.get("size_bytes") or 0)
                    models.append(
                        {
                            "name": rel,
                            "model_type": str(f.get("model_type") or ""),
                            "symbol": str(f.get("symbol") or ""),
                            "horizon": int(f.get("horizon") or 0),
                            "namespace": str(f.get("namespace") or ""),
                            "created_at": created_at,
                            "size_bytes": size_bytes,
                            "size_human": _human_size(size_bytes),
                            "path": str(p),
                        }
                    )
                except Exception:
                    continue

        except Exception as e:
            log.warning("api_models_inventory.error", error=str(e))
            return {"ok": False, "models": [], "error": str(e)}

        return {"ok": True, "models": models}

    @app.get("/api/models/benchmarks", response_class=JSONResponse)
    def api_models_benchmarks(request: Request, limit: int = 20) -> dict[str, Any]:
        """
        List recent benchmark summaries from runs/benchmarks/**/<run_id>.json.
        """
        if not auth.is_authorized(request):
            raise HTTPException(status_code=401, detail="Unauthorized")

        global _benchmarks_at, _benchmarks_payload
        now = time.time()
        with _benchmarks_lock:
            if _benchmarks_payload is not None and (now - float(_benchmarks_at)) <= float(_BENCHMARKS_TTL_S):
                payload = dict(_benchmarks_payload)
                # Apply limit at read time (cheap)
                payload["benchmarks"] = list(payload.get("benchmarks") or [])[: max(1, int(limit or 20))]
                return payload

        runs_dir = get_runs_dir()
        root = runs_dir / "benchmarks"
        if not root.exists():
            return {"ok": True, "benchmarks": []}

        out: list[dict[str, Any]] = []
        from datetime import datetime, timezone

        try:
            for p in root.rglob("*.json"):
                try:
                    if not p.is_file():
                        continue
                    rel = p.relative_to(runs_dir).as_posix()
                    st = p.stat()
                    created_at = datetime.fromtimestamp(float(st.st_mtime), tz=timezone.utc).isoformat()
                    payload = json.loads(p.read_text(encoding="utf-8"))
                    offline = payload.get("offline") if isinstance(payload.get("offline"), dict) else {}
                    latency = payload.get("latency") if isinstance(payload.get("latency"), dict) else {}
                    out.append(
                        {
                            "run_id": str(payload.get("run_id") or p.stem),
                            "timestamp": str(payload.get("timestamp") or ""),
                            "model_type": str(payload.get("model_type") or ""),
                            "symbol": str(payload.get("symbol") or ""),
                            "horizon": int(payload.get("horizon") or 0),
                            "accuracy": offline.get("accuracy"),
                            "f1_macro": offline.get("f1_macro"),
                            "log_loss": offline.get("log_loss"),
                            "ece": offline.get("ece"),
                            "inference_p95_ms": latency.get("inference_p95_ms"),
                            "created_at": created_at,
                            "path": rel,
                        }
                    )
                except Exception:
                    continue
        except Exception as e:
            return {"ok": False, "benchmarks": [], "error": str(e)}

        out = sorted(out, key=lambda x: (str(x.get("timestamp") or ""), str(x.get("created_at") or "")), reverse=True)
        payload2 = {"ok": True, "benchmarks": out[: max(1, int(limit or 20))]}
        with _benchmarks_lock:
            _benchmarks_payload = dict(payload2)
            _benchmarks_at = time.time()
        return payload2

    @app.get("/api/trading/status", response_class=JSONResponse)
    def api_trading_status(request: Request) -> dict[str, Any]:
        """
        Get current trading run status from runs/trading/.
        """
        if not auth.is_authorized(request):
            raise HTTPException(status_code=401, detail="Unauthorized")

        runs_dir = get_runs_dir()
        trading_dir = runs_dir / "trading"

        live_enabled = False
        try:
            from ghtrader.config import is_live_enabled

            live_enabled = bool(is_live_enabled())
        except Exception:
            live_enabled = False

        result: dict[str, Any] = {
            "ok": True,
            "active": False,
            "live_enabled": bool(live_enabled),
            "mode": "idle",
            "monitor_only": None,
            "run_id": None,
            "snapshot_ts": None,
            "executor": None,
            "model": None,
            "model_name": None,
            "symbols": [],
            "symbols_requested": [],
            "symbols_resolved": [],
            "account": None,
            "positions": None,
            "position": None,
            "orders_alive": None,
            "pnl": None,
            "risk": None,
        }

        try:
            if not trading_dir.exists():
                return result

            # Find most recent run with recent activity.
            runs = sorted([d for d in trading_dir.iterdir() if d.is_dir()], key=lambda x: x.name, reverse=True)
            if not runs:
                return result

            import time as t

            latest = None
            for d in runs:
                sf = d / "snapshots.jsonl"
                if not sf.exists():
                    continue
                try:
                    if (t.time() - float(sf.stat().st_mtime)) < 300.0:
                        latest = d
                        break
                except Exception:
                    continue

            if latest is None:
                return result

            snapshots_file = latest / "snapshots.jsonl"
            config_file = latest / "run_config.json"

            result["active"] = True
            result["run_id"] = latest.name

            # Read config
            if config_file.exists():
                try:
                    config = json.loads(config_file.read_text())
                    result["mode"] = config.get("mode", "unknown") or "unknown"
                    result["monitor_only"] = bool(config.get("monitor_only")) if ("monitor_only" in config) else None
                    result["executor"] = config.get("executor") or None
                    result["model_name"] = config.get("model_name") or None
                    # Backwards-compat field for UI
                    result["model"] = result.get("model_name")
                    sy_req = config.get("symbols_requested")
                    sy_res = config.get("symbols_resolved")
                    if isinstance(sy_req, list):
                        result["symbols_requested"] = [str(x) for x in sy_req if str(x)]
                    if isinstance(sy_res, list):
                        result["symbols_resolved"] = [str(x) for x in sy_res if str(x)]
                    result["symbols"] = list(result.get("symbols_requested") or result.get("symbols_resolved") or [])
                    limits = config.get("limits") if isinstance(config.get("limits"), dict) else {}
                    result["risk"] = {
                        "max_position": limits.get("max_abs_position"),
                        "max_order_size": limits.get("max_order_size"),
                        "max_ops_per_sec": limits.get("max_ops_per_sec"),
                        "max_daily_loss": None,
                    }
                except Exception:
                    pass

            # Read latest snapshot
            if snapshots_file.exists():
                try:
                    with open(snapshots_file, "rb") as f:
                        # Read last 32KB to find last line
                        f.seek(0, 2)
                        size = f.tell()
                        offset = max(0, size - 32 * 1024)
                        f.seek(offset)
                        chunk = f.read().decode("utf-8", errors="ignore")
                    lines = [ln for ln in chunk.splitlines() if ln.strip()]
                    if lines:
                        last_snapshot = json.loads(lines[-1])
                        result["snapshot_ts"] = last_snapshot.get("ts")
                        acct = last_snapshot.get("account") if isinstance(last_snapshot.get("account"), dict) else None
                        if acct is not None:
                            acct2 = dict(acct)
                            bal = acct2.get("balance")
                            fp = acct2.get("float_profit")
                            try:
                                if bal is not None and fp is not None:
                                    acct2["equity"] = float(bal) + float(fp)
                                else:
                                    acct2["equity"] = None
                            except Exception:
                                acct2["equity"] = None
                            result["account"] = acct2
                            # Default P&L: float_profit if present else position_profit.
                            try:
                                if acct2.get("float_profit") is not None:
                                    result["pnl"] = float(acct2.get("float_profit"))
                                elif acct2.get("position_profit") is not None:
                                    result["pnl"] = float(acct2.get("position_profit"))
                            except Exception:
                                pass

                        pos = last_snapshot.get("positions") if isinstance(last_snapshot.get("positions"), dict) else None
                        if pos is not None:
                            result["positions"] = pos

                            # Back-compat: a single "position" summary for the primary execution symbol.
                            primary = None
                            sy_res = list(result.get("symbols_resolved") or [])
                            sy_req = list(result.get("symbols_requested") or [])
                            if sy_res:
                                primary = sy_res[0]
                            elif sy_req:
                                primary = sy_req[0]
                            else:
                                sy_snap = last_snapshot.get("symbols")
                                if isinstance(sy_snap, list) and sy_snap:
                                    primary = str(sy_snap[0])
                            if primary and isinstance(pos.get(primary), dict) and ("error" not in pos.get(primary)):
                                p0 = dict(pos.get(primary) or {})
                                try:
                                    vlong = int(p0.get("volume_long", 0) or 0)
                                    vshort = int(p0.get("volume_short", 0) or 0)
                                    net = int(vlong) - int(vshort)
                                except Exception:
                                    vlong = 0
                                    vshort = 0
                                    net = 0
                                result["position"] = {"symbol": primary, "volume": net, "volume_long": vlong, "volume_short": vshort}

                        orders = last_snapshot.get("orders_alive")
                        if isinstance(orders, list):
                            result["orders_alive"] = orders
                except Exception:
                    pass

        except Exception as e:
            log.warning("api_trading_status.error", error=str(e))
            result["ok"] = False
            result["error"] = str(e)

        return result

    return app


app = create_app()

