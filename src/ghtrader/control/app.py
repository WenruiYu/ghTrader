from __future__ import annotations

from dataclasses import asdict
import os
import sys
import threading
import time
import json
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

_TQSDK_HEAVY_SUBCOMMANDS = {"download", "download-contract-range", "record", "probe-l5"}


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
        lake_version: str = "v1",
        refresh: str = "0",
        refresh_l5: str = "0",
    ) -> dict[str, Any]:
        """
        Contract explorer backend: TqSdk catalog + local lake coverage + L5 status + probe cache.

        Query params:
        - exchange: SHFE
        - var: cu|au|ag
        - lake_version: v1|v2
        - refresh: 1 to force refresh of the TqSdk contract catalog cache
        - refresh_l5: 1 to force local L5 rescan (bounded; may still return partial)
        """
        if not auth.is_authorized(request):
            raise HTTPException(status_code=401, detail="Unauthorized")

        ex = str(exchange).upper().strip() or "SHFE"
        v = str(var).lower().strip() or "cu"
        lv = str(lake_version).lower().strip() or "v1"
        lv = "v2" if lv == "v2" else "v1"

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

        status = compute_contract_statuses(
            exchange=ex,
            var=v,
            lake_version=lv,
            data_dir=data_dir,
            runs_dir=runs_dir,
            contracts=list(cat.get("contracts") or []),
            refresh_l5=str(refresh_l5).strip() in {"1", "true", "yes"},
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

        # Attach QuestDB canonical coverage when available.
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
            syms = [str(r.get("symbol") or "").strip() for r in (status.get("contracts") or []) if str(r.get("symbol") or "").strip()]
            cov = query_contract_coverage(cfg=cfg, table=tbl, symbols=syms, lake_version=lv, ticks_lake="raw")
            for r in status.get("contracts") or []:
                sym = str(r.get("symbol") or "").strip()
                r["questdb_coverage"] = cov.get(sym) if sym else None
            status["questdb"] = {"ok": True, "table": tbl}
        except Exception as e:
            for r in status.get("contracts") or []:
                r["questdb_coverage"] = None
            status["questdb"] = {"ok": False, "error": str(e)}

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
        - exchange/var/lake_version: used when scope is set (to select contracts)
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
        lv = str(payload.get("lake_version") or "v1").lower().strip()
        lv = "v2" if lv == "v2" else "v1"

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

        ranges = compute_contract_statuses(
            exchange=ex,
            var=v,
            lake_version=lv,
            data_dir=data_dir,
            runs_dir=runs_dir,
            contracts=contracts_for_ranges,
            refresh_l5=False,
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
                "--lake-version",
                lv,
            )
            title = f"download {sym} lake={lv}"
            rec = jm.enqueue_job(JobSpec(title=title, argv=argv, cwd=Path.cwd()))
            enqueued.append(rec.id)

        return {"ok": True, "enqueued": enqueued, "skipped": skipped, "count": int(len(enqueued))}

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

        lv = str(payload.get("lake_version") or "v1").lower().strip()
        lv = "v2" if lv == "v2" else "v1"

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
                "--lake-version",
                lv,
            )
            title = f"audit ticks {sym} lake={lv}"
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

    @app.post("/api/db/query", response_class=JSONResponse)
    async def api_db_query(request: Request) -> dict[str, Any]:
        """
        Read-only DuckDB query endpoint (guarded).

        Intended for local SSH-forwarded dashboard use only.
        """
        if not auth.is_authorized(request):
            raise HTTPException(status_code=401, detail="Unauthorized")

        payload = await request.json()
        query = str(payload.get("query") or "").strip()
        include_metrics = bool(payload.get("include_metrics") or False)
        try:
            limit = int(payload.get("limit") or 200)
        except Exception:
            limit = 200
        limit = max(1, min(limit, 500))

        if not query:
            raise HTTPException(status_code=400, detail="query is required")

        try:
            from ghtrader.db import DuckDBBackend, DuckDBConfig, DuckDBNotInstalled

            data_dir = get_data_dir()
            runs_dir = get_runs_dir()
            db_path = data_dir / "ghtrader.duckdb"

            # Prefer a prepared DB file; fall back to in-memory views.
            if db_path.exists():
                try:
                    backend = DuckDBBackend(config=DuckDBConfig(db_path=db_path, read_only=True))
                    with backend.connect() as con:
                        df = backend.query_df_limited(con=con, sql=query, limit=limit)
                    return {"columns": df.columns.tolist(), "rows": df.astype(str).to_dict(orient="records")}
                except Exception:
                    pass

            backend = DuckDBBackend(config=DuckDBConfig(db_path=None, read_only=False))
            with backend.connect() as con:
                backend.init_views(con=con, data_dir=data_dir)
                if include_metrics:
                    backend.ingest_runs_metrics(con=con, runs_dir=runs_dir)
                df = backend.query_df_limited(con=con, sql=query, limit=limit)
            return {"columns": df.columns.tolist(), "rows": df.astype(str).to_dict(orient="records")}
        except DuckDBNotInstalled as e:
            raise HTTPException(status_code=500, detail=str(e)) from e
        except Exception as e:
            raise HTTPException(status_code=400, detail=str(e)) from e

    return app


app = create_app()

