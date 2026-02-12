from __future__ import annotations

import json
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from pathlib import Path
from typing import Any

import structlog

from fastapi import APIRouter, HTTPException, Request
from fastapi.responses import PlainTextResponse, RedirectResponse
from fastapi.templating import Jinja2Templates

from ghtrader.config import get_artifacts_dir, get_data_dir, get_runs_dir
from ghtrader.control import auth
from ghtrader.control.jobs import JobSpec, python_module_argv
from ghtrader.control.settings import get_tqsdk_scheduler_state, set_tqsdk_scheduler_max_parallel
from ghtrader.control.system_info import cpu_mem_info, disk_usage, gpu_info

log = structlog.get_logger()

_DATA_PAGE_CACHE_TTL_S = 5.0
_DATA_PAGE_CACHE: dict[str, tuple[float, Any]] = {}
_DATA_PAGE_CACHE_LOCK = threading.Lock()


def _data_page_cache_get(key: str, *, ttl_s: float | None = None) -> Any | None:
    ttl = float(_DATA_PAGE_CACHE_TTL_S if ttl_s is None else ttl_s)
    now = time.time()
    with _DATA_PAGE_CACHE_LOCK:
        item = _DATA_PAGE_CACHE.get(key)
        if not item:
            return None
        ts, payload = item
        if (now - float(ts)) > ttl:
            _DATA_PAGE_CACHE.pop(key, None)
            return None
        return payload


def _data_page_cache_set(key: str, payload: Any) -> None:
    with _DATA_PAGE_CACHE_LOCK:
        _DATA_PAGE_CACHE[key] = (time.time(), payload)


def _data_page_cache_clear() -> None:
    with _DATA_PAGE_CACHE_LOCK:
        _DATA_PAGE_CACHE.clear()


def build_router() -> Any:
    router = APIRouter()

    templates = Jinja2Templates(directory=str(Path(__file__).parent / "templates"))

    def _require_auth(request: Request) -> None:
        if not auth.is_authorized(request):
            raise HTTPException(status_code=401, detail="Unauthorized")

    def _token_qs(request: Request) -> str:
        return auth.token_query_string(request)

    @router.get("/")
    def index(request: Request):
        _require_auth(request)
        store = request.app.state.job_store
        jobs = store.list_jobs(limit=50)
        running = [j for j in jobs if j.status == "running"]
        queued = [j for j in jobs if j.status == "queued"]

        data_dir = get_data_dir()
        l5_start_date = ""
        l5_start_error = ""
        latest_trading_day = ""
        latest_trading_error = ""
        try:
            from ghtrader.config import get_l5_start_date

            l5_start_date = get_l5_start_date().isoformat()
        except Exception as e:
            l5_start_error = str(e)
        try:
            from ghtrader.data.trading_calendar import latest_trading_day as _latest_trading_day

            latest_trading_day = _latest_trading_day(data_dir=data_dir, refresh=False, allow_download=True).isoformat()
        except Exception as e:
            latest_trading_error = str(e)

        return templates.TemplateResponse(
            request,
            "index.html",
            {
                "request": request,
                "title": "ghTrader Dashboard",
                "token_qs": _token_qs(request),
                "jobs": jobs,
                "running_count": len(running),
                "queued_count": len(queued),
                "recent_count": len(jobs),
                "l5_start_date": l5_start_date,
                "l5_start_error": l5_start_error,
                "latest_trading_day": latest_trading_day,
                "latest_trading_error": latest_trading_error,
            },
        )

    @router.get("/jobs")
    def jobs(request: Request):
        _require_auth(request)
        store = request.app.state.job_store
        jobs = store.list_jobs(limit=200)
        running = [j for j in jobs if j.status == "running"]
        data_dir = get_data_dir()
        l5_start_date = ""
        l5_start_error = ""
        latest_trading_day = ""
        latest_trading_error = ""
        try:
            from ghtrader.config import get_l5_start_date

            l5_start_date = get_l5_start_date().isoformat()
        except Exception as e:
            l5_start_error = str(e)
        try:
            from ghtrader.data.trading_calendar import latest_trading_day as _latest_trading_day

            latest_trading_day = _latest_trading_day(data_dir=data_dir, refresh=False, allow_download=True).isoformat()
        except Exception as e:
            latest_trading_error = str(e)
        return templates.TemplateResponse(
            request,
            "jobs.html",
            {
                "request": request,
                "title": "Jobs",
                "token_qs": _token_qs(request),
                "jobs": jobs,
                "running_count": len(running),
                "l5_start_date": l5_start_date,
                "l5_start_error": l5_start_error,
                "latest_trading_day": latest_trading_day,
                "latest_trading_error": latest_trading_error,
            },
        )

    # ---------------------------------------------------------------------
    # Models page
    # ---------------------------------------------------------------------

    @router.get("/models")
    def models_page(request: Request):
        _require_auth(request)
        store = request.app.state.job_store
        jobs = store.list_jobs(limit=50)
        running = [j for j in jobs if j.status == "running"]
        return templates.TemplateResponse(
            request,
            "models.html",
            {
                "request": request,
                "title": "Models",
                "token_qs": _token_qs(request),
                "running_count": len(running),
            },
        )

    # ---------------------------------------------------------------------
    # Trading page
    # ---------------------------------------------------------------------

    @router.get("/trading")
    def trading_page(request: Request):
        _require_auth(request)
        store = request.app.state.job_store
        jobs = store.list_jobs(limit=50)
        running = [j for j in jobs if j.status == "running"]

        return templates.TemplateResponse(
            request,
            "trading.html",
            {
                "request": request,
                "title": "Trading",
                "token_qs": _token_qs(request),
                "running_count": len(running),
            },
        )

    # ---------------------------------------------------------------------
    # Ops -> Data redirect (backward compatibility)
    # ---------------------------------------------------------------------

    @router.get("/ops")
    def ops_redirect(request: Request):
        """Redirect /ops to /data for backward compatibility (unified workflow in Contracts tab)."""
        _require_auth(request)
        token_qs = _token_qs(request)
        # Redirect to the unified Data Hub (Contracts tab has the 8-step workflow)
        return RedirectResponse(url=f"/data{token_qs}#contracts", status_code=303)

    @router.get("/ops/ingest")
    def ops_ingest_redirect(request: Request):
        _require_auth(request)
        return RedirectResponse(url=f"/data{_token_qs(request)}#ingest", status_code=303)

    @router.get("/ops/build")
    def ops_build_redirect(request: Request):
        _require_auth(request)
        return RedirectResponse(url=f"/data{_token_qs(request)}#build", status_code=303)

    @router.get("/ops/model")
    def ops_model_redirect(request: Request):
        _require_auth(request)
        return RedirectResponse(url=f"/models{_token_qs(request)}", status_code=303)

    @router.get("/ops/eval")
    def ops_eval_redirect(request: Request):
        _require_auth(request)
        return RedirectResponse(url=f"/models{_token_qs(request)}", status_code=303)

    @router.get("/ops/trading")
    def ops_trading_redirect(request: Request):
        _require_auth(request)
        return RedirectResponse(url=f"/trading{_token_qs(request)}", status_code=303)

    @router.get("/ops/locks")
    def ops_locks_redirect(request: Request):
        _require_auth(request)
        return RedirectResponse(url=f"/data{_token_qs(request)}#locks", status_code=303)

    @router.get("/ops/integrity")
    def ops_integrity_redirect(request: Request):
        _require_auth(request)
        return RedirectResponse(url=f"/data{_token_qs(request)}#integrity", status_code=303)

    @router.get("/ops/integrity/report/{name}")
    def ops_integrity_report_redirect(request: Request, name: str):
        """Redirect to /data/integrity/report for backward compatibility."""
        _require_auth(request)
        return RedirectResponse(url=f"/data/integrity/report/{name}{_token_qs(request)}", status_code=303)

    # ---------------------------------------------------------------------
    # Data Hub routes (consolidated from Ops)
    # ---------------------------------------------------------------------

    @router.get("/data/integrity/report/{name}")
    def data_integrity_report(request: Request, name: str):
        _require_auth(request)
        if "/" in name or "\\" in name or not name.endswith(".json"):
            raise HTTPException(status_code=400, detail="invalid report name")
        p = get_runs_dir() / "audit" / name
        if not p.exists():
            raise HTTPException(status_code=404, detail="report not found")
        return PlainTextResponse(p.read_text(), media_type="application/json")

    @router.get("/data/main-l5-validate/report/{name}")
    def data_main_l5_validate_report(request: Request, name: str):
        _require_auth(request)
        if "/" in name or "\\" in name or not name.endswith(".json"):
            raise HTTPException(status_code=400, detail="invalid report name")
        p = get_runs_dir() / "control" / "reports" / "main_l5_validate" / name
        if not p.exists():
            raise HTTPException(status_code=404, detail="report not found")
        return PlainTextResponse(p.read_text(), media_type="application/json")

    @router.post("/data/ingest/download")
    async def data_ingest_download(request: Request):
        _require_auth(request)
        raise HTTPException(status_code=410, detail="download endpoint removed; use main-schedule + main-l5")

    @router.post("/data/ingest/download_contract_range")
    async def data_ingest_download_contract_range(request: Request):
        _require_auth(request)
        raise HTTPException(
            status_code=410,
            detail="download-contract-range endpoint removed; use main-schedule + main-l5",
        )

    @router.post("/data/ingest/record")
    async def data_ingest_record(request: Request):
        _require_auth(request)
        raise HTTPException(status_code=410, detail="record endpoint removed in current PRD phase")

    @router.post("/data/settings/tqsdk_scheduler")
    async def data_settings_tqsdk_scheduler(request: Request):
        _require_auth(request)
        form = await request.form()
        max_parallel = form.get("max_parallel")
        persist_raw = str(form.get("persist") or "1").strip().lower()
        persist = persist_raw not in {"0", "false", "no", "off"}

        try:
            set_tqsdk_scheduler_max_parallel(runs_dir=get_runs_dir(), max_parallel=max_parallel, persist=bool(persist))
        except Exception as e:
            raise HTTPException(status_code=400, detail=str(e))

        return RedirectResponse(url=f"/data{_token_qs(request)}#ingest", status_code=303)

    @router.post("/data/build/main_schedule")
    async def data_build_main_schedule(request: Request):
        _require_auth(request)
        form = await request.form()
        var = str(form.get("variety") or "cu").strip()
        data_dir = str(form.get("data_dir") or "data").strip()
        argv = python_module_argv(
            "ghtrader.cli",
            "main-schedule",
            "--var",
            var,
            "--data-dir",
            data_dir,
        )
        title = f"main-schedule {var} env->latest"
        jm = request.app.state.job_manager
        rec = jm.start_job(JobSpec(title=title, argv=argv, cwd=Path.cwd()))
        return RedirectResponse(url=f"/jobs/{rec.id}{_token_qs(request)}", status_code=303)

    @router.post("/data/build/main_l5")
    async def data_build_main_l5(request: Request):
        _require_auth(request)
        form = await request.form()
        var = str(form.get("variety") or "cu").strip()
        derived_symbol = str(form.get("derived_symbol") or f"KQ.m@SHFE.{var}").strip()
        data_dir = str(form.get("data_dir") or "data").strip()
        update_mode = str(form.get("update_mode") or "0").strip().lower() in {"1", "true", "yes", "on"}
        if not var or not derived_symbol:
            raise HTTPException(status_code=400, detail="variety/derived_symbol required")
        argv = python_module_argv(
            "ghtrader.cli",
            "main-l5",
            "--var",
            var,
            "--symbol",
            derived_symbol,
            "--data-dir",
            data_dir,
        )
        if update_mode:
            argv += ["--update"]
        title = f"main-l5 {var} {derived_symbol}"
        jm = request.app.state.job_manager
        rec = jm.start_job(JobSpec(title=title, argv=argv, cwd=Path.cwd()))
        return RedirectResponse(url=f"/jobs/{rec.id}{_token_qs(request)}", status_code=303)

    @router.post("/data/build/build")
    async def data_build_build(request: Request):
        _require_auth(request)
        form = await request.form()
        symbol = str(form.get("symbol") or "").strip()
        horizons = str(form.get("horizons") or "10,50,200").strip()
        threshold_k = str(form.get("threshold_k") or "1").strip()
        ticks_kind = str(form.get("ticks_kind") or "main_l5").strip()
        overwrite = str(form.get("overwrite") or "false").strip().lower() == "true"
        data_dir = str(form.get("data_dir") or "data").strip()
        if not symbol:
            raise HTTPException(status_code=400, detail="symbol required")
        argv = python_module_argv(
            "ghtrader.cli",
            "build",
            "--symbol",
            symbol,
            "--data-dir",
            data_dir,
            "--horizons",
            horizons,
            "--threshold-k",
            threshold_k,
            "--ticks-kind",
            ticks_kind,
            "--overwrite" if overwrite else "--no-overwrite",
        )
        title = f"build {symbol} ticks_kind={ticks_kind} overwrite={overwrite}"
        jm = request.app.state.job_manager
        rec = jm.start_job(JobSpec(title=title, argv=argv, cwd=Path.cwd()))
        return RedirectResponse(url=f"/jobs/{rec.id}{_token_qs(request)}", status_code=303)

    @router.post("/data/integrity/audit")
    async def data_integrity_audit(request: Request):
        _require_auth(request)
        raise HTTPException(status_code=410, detail="integrity audit endpoint removed in current PRD phase")

    # ---------------------------------------------------------------------
    # Legacy /ops/* POST routes (redirect to /data/*)
    # ---------------------------------------------------------------------

    # Legacy /ops/* POST routes - forward to new /data/* handlers
    @router.post("/ops/ingest/download")
    async def ops_ingest_download(request: Request):
        return await data_ingest_download(request)

    @router.post("/ops/ingest/download_contract_range")
    async def ops_ingest_download_contract_range(request: Request):
        return await data_ingest_download_contract_range(request)

    @router.post("/ops/ingest/update_variety")
    async def ops_ingest_update_variety(request: Request):
        _require_auth(request)
        raise HTTPException(status_code=410, detail="update-variety endpoint removed in current PRD phase")

    @router.post("/ops/ingest/record")
    async def ops_ingest_record(request: Request):
        return await data_ingest_record(request)

    @router.post("/ops/settings/tqsdk_scheduler")
    async def ops_settings_tqsdk_scheduler(request: Request):
        return await data_settings_tqsdk_scheduler(request)

    @router.post("/ops/build/build")
    async def ops_build_build(request: Request):
        return await data_build_build(request)

    @router.post("/ops/build/main_schedule")
    async def ops_build_main_schedule(request: Request):
        return await data_build_main_schedule(request)

    @router.post("/ops/build/main_l5")
    async def ops_build_main_l5(request: Request):
        return await data_build_main_l5(request)

    # Note: /ops/model/* and /ops/eval/* routes are kept as-is since they
    # redirect to /models page which is separate from the Data Hub consolidation.
    # These routes submit jobs and redirect to job detail, so they work independently.
    @router.post("/models/model/train")
    @router.post("/ops/model/train")
    async def ops_model_train(request: Request):
        _require_auth(request)
        form = await request.form()
        model = str(form.get("model") or "").strip()
        symbol = str(form.get("symbol") or "").strip()
        data_dir = str(form.get("data_dir") or "data").strip()
        artifacts_dir = str(form.get("artifacts_dir") or "artifacts").strip()
        horizon = str(form.get("horizon") or "50").strip()
        gpus = str(form.get("gpus") or "1").strip()
        epochs = str(form.get("epochs") or "50").strip()
        batch_size = str(form.get("batch_size") or "256").strip()
        seq_len = str(form.get("seq_len") or "100").strip()
        lr = str(form.get("lr") or "0.001").strip()
        ddp = str(form.get("ddp") or "true").strip().lower() == "true"
        if not model or not symbol:
            raise HTTPException(status_code=400, detail="model/symbol required")
        argv = python_module_argv(
            "ghtrader.cli",
            "train",
            "--model",
            model,
            "--symbol",
            symbol,
            "--data-dir",
            data_dir,
            "--artifacts-dir",
            artifacts_dir,
            "--horizon",
            horizon,
            "--gpus",
            gpus,
            "--epochs",
            epochs,
            "--batch-size",
            batch_size,
            "--seq-len",
            seq_len,
            "--lr",
            lr,
            "--ddp" if ddp else "--no-ddp",
        )
        title = f"train {model} {symbol}"
        jm = request.app.state.job_manager
        rec = jm.start_job(JobSpec(title=title, argv=argv, cwd=Path.cwd()))
        return RedirectResponse(url=f"/jobs/{rec.id}{_token_qs(request)}", status_code=303)

    @router.post("/models/model/sweep")
    @router.post("/ops/model/sweep")
    async def ops_model_sweep(request: Request):
        _require_auth(request)
        form = await request.form()
        symbol = str(form.get("symbol") or "").strip()
        model = str(form.get("model") or "deeplob").strip()
        data_dir = str(form.get("data_dir") or "data").strip()
        artifacts_dir = str(form.get("artifacts_dir") or "artifacts").strip()
        runs_dir_str = str(form.get("runs_dir") or "runs").strip()
        n_trials = str(form.get("n_trials") or "20").strip()
        n_cpus = str(form.get("n_cpus") or "8").strip()
        n_gpus = str(form.get("n_gpus") or "1").strip()
        if not symbol:
            raise HTTPException(status_code=400, detail="symbol required")
        argv = python_module_argv(
            "ghtrader.cli",
            "sweep",
            "--symbol",
            symbol,
            "--model",
            model,
            "--data-dir",
            data_dir,
            "--artifacts-dir",
            artifacts_dir,
            "--runs-dir",
            runs_dir_str,
            "--n-trials",
            n_trials,
            "--n-cpus",
            n_cpus,
            "--n-gpus",
            n_gpus,
        )
        title = f"sweep {model} {symbol}"
        jm = request.app.state.job_manager
        rec = jm.start_job(JobSpec(title=title, argv=argv, cwd=Path.cwd()))
        return RedirectResponse(url=f"/jobs/{rec.id}{_token_qs(request)}", status_code=303)

    @router.post("/models/eval/benchmark")
    @router.post("/ops/eval/benchmark")
    async def ops_eval_benchmark(request: Request):
        _require_auth(request)
        form = await request.form()
        model = str(form.get("model") or "").strip()
        symbol = str(form.get("symbol") or "").strip()
        data_dir = str(form.get("data_dir") or "data").strip()
        artifacts_dir = str(form.get("artifacts_dir") or "artifacts").strip()
        runs_dir_str = str(form.get("runs_dir") or "runs").strip()
        horizon = str(form.get("horizon") or "50").strip()
        if not model or not symbol:
            raise HTTPException(status_code=400, detail="model/symbol required")
        argv = python_module_argv(
            "ghtrader.cli",
            "benchmark",
            "--model",
            model,
            "--symbol",
            symbol,
            "--data-dir",
            data_dir,
            "--artifacts-dir",
            artifacts_dir,
            "--runs-dir",
            runs_dir_str,
            "--horizon",
            horizon,
        )
        title = f"benchmark {model} {symbol}"
        jm = request.app.state.job_manager
        rec = jm.start_job(JobSpec(title=title, argv=argv, cwd=Path.cwd()))
        return RedirectResponse(url=f"/jobs/{rec.id}{_token_qs(request)}", status_code=303)

    @router.post("/models/eval/compare")
    @router.post("/ops/eval/compare")
    async def ops_eval_compare(request: Request):
        _require_auth(request)
        form = await request.form()
        symbol = str(form.get("symbol") or "").strip()
        models = str(form.get("models") or "").strip()
        data_dir = str(form.get("data_dir") or "data").strip()
        artifacts_dir = str(form.get("artifacts_dir") or "artifacts").strip()
        runs_dir_str = str(form.get("runs_dir") or "runs").strip()
        horizon = str(form.get("horizon") or "50").strip()
        if not symbol:
            raise HTTPException(status_code=400, detail="symbol required")
        argv = python_module_argv(
            "ghtrader.cli",
            "compare",
            "--symbol",
            symbol,
            "--models",
            models,
            "--data-dir",
            data_dir,
            "--artifacts-dir",
            artifacts_dir,
            "--runs-dir",
            runs_dir_str,
            "--horizon",
            horizon,
        )
        title = f"compare {symbol}"
        jm = request.app.state.job_manager
        rec = jm.start_job(JobSpec(title=title, argv=argv, cwd=Path.cwd()))
        return RedirectResponse(url=f"/jobs/{rec.id}{_token_qs(request)}", status_code=303)

    @router.post("/models/eval/backtest")
    @router.post("/ops/eval/backtest")
    async def ops_eval_backtest(request: Request):
        _require_auth(request)
        form = await request.form()
        model = str(form.get("model") or "").strip()
        symbol = str(form.get("symbol") or "").strip()
        start_date = str(form.get("start_date") or "").strip()
        end_date = str(form.get("end_date") or "").strip()
        data_dir = str(form.get("data_dir") or "data").strip()
        artifacts_dir = str(form.get("artifacts_dir") or "artifacts").strip()
        runs_dir_str = str(form.get("runs_dir") or "runs").strip()
        if not model or not symbol or not start_date or not end_date:
            raise HTTPException(status_code=400, detail="model/symbol/start_date/end_date required")
        argv = python_module_argv(
            "ghtrader.cli",
            "backtest",
            "--model",
            model,
            "--symbol",
            symbol,
            "--start",
            start_date,
            "--end",
            end_date,
            "--data-dir",
            data_dir,
            "--artifacts-dir",
            artifacts_dir,
            "--runs-dir",
            runs_dir_str,
        )
        title = f"backtest {model} {symbol}"
        jm = request.app.state.job_manager
        rec = jm.start_job(JobSpec(title=title, argv=argv, cwd=Path.cwd()))
        return RedirectResponse(url=f"/jobs/{rec.id}{_token_qs(request)}", status_code=303)

    @router.post("/models/eval/paper")
    @router.post("/ops/eval/paper")
    async def ops_eval_paper(request: Request):
        _require_auth(request)
        form = await request.form()
        model = str(form.get("model") or "").strip()
        symbols = str(form.get("symbols") or "").strip()
        artifacts_dir = str(form.get("artifacts_dir") or "artifacts").strip()
        if not model or not symbols:
            raise HTTPException(status_code=400, detail="model/symbols required")
        argv = python_module_argv("ghtrader.cli", "paper", "--model", model)
        for s in [s.strip() for s in symbols.split(",") if s.strip()]:
            argv += ["--symbols", s]
        argv += ["--artifacts-dir", artifacts_dir]
        title = f"paper {model}"
        jm = request.app.state.job_manager
        rec = jm.start_job(JobSpec(title=title, argv=argv, cwd=Path.cwd()))
        return RedirectResponse(url=f"/jobs/{rec.id}{_token_qs(request)}", status_code=303)

    @router.post("/models/eval/daily_train")
    @router.post("/ops/eval/daily_train")
    async def ops_eval_daily_train(request: Request):
        _require_auth(request)
        form = await request.form()
        symbols = str(form.get("symbols") or "").strip()
        model = str(form.get("model") or "deeplob").strip()
        horizon = str(form.get("horizon") or "50").strip()
        lookback_days = str(form.get("lookback_days") or "30").strip()
        data_dir = str(form.get("data_dir") or "data").strip()
        artifacts_dir = str(form.get("artifacts_dir") or "artifacts").strip()
        runs_dir_str = str(form.get("runs_dir") or "runs").strip()
        if not symbols:
            raise HTTPException(status_code=400, detail="symbols required")
        argv = python_module_argv("ghtrader.cli", "daily-train")
        for s in [s.strip() for s in symbols.split(",") if s.strip()]:
            argv += ["--symbols", s]
        argv += [
            "--model",
            model,
            "--data-dir",
            data_dir,
            "--artifacts-dir",
            artifacts_dir,
            "--runs-dir",
            runs_dir_str,
            "--horizon",
            horizon,
            "--lookback-days",
            lookback_days,
        ]
        title = f"daily-train {model}"
        jm = request.app.state.job_manager
        rec = jm.start_job(JobSpec(title=title, argv=argv, cwd=Path.cwd()))
        return RedirectResponse(url=f"/jobs/{rec.id}{_token_qs(request)}", status_code=303)

    @router.post("/ops/integrity/audit")
    async def ops_integrity_audit(request: Request):
        return await data_integrity_audit(request)

    @router.post("/jobs/start")
    async def jobs_start(request: Request):
        _require_auth(request)
        form = await request.form()

        job_type = str(form.get("job_type") or "").strip()
        symbol_or_var = str(form.get("symbol_or_var") or "").strip()
        data_dir = Path(str(form.get("data_dir") or "data"))

        # Build argv for a known-safe set of job types (no shell).
        if job_type == "download_contract_range":
            raise HTTPException(status_code=410, detail="download_contract_range job type removed in current PRD phase")
        elif job_type == "build":
            symbol = symbol_or_var
            if not symbol:
                raise HTTPException(status_code=400, detail="symbol_or_var must be a symbol for build")
            argv = python_module_argv(
                "ghtrader.cli",
                "build",
                "--symbol",
                symbol,
                "--data-dir",
                str(data_dir),
            )
            title = f"build {symbol}"
        elif job_type == "train":
            symbol = symbol_or_var
            if not symbol:
                raise HTTPException(status_code=400, detail="symbol_or_var must be a symbol for train")
            model = str(form.get("model") or "xgboost").strip()
            horizon = str(form.get("horizon") or "50").strip()
            artifacts_dir = str(form.get("artifacts_dir") or "artifacts").strip()
            argv = python_module_argv(
                "ghtrader.cli",
                "train",
                "--model",
                model,
                "--symbol",
                symbol,
                "--data-dir",
                str(data_dir),
                "--artifacts-dir",
                artifacts_dir,
                "--horizon",
                horizon,
            )
            title = f"train {model} {symbol}"
        elif job_type == "benchmark":
            symbol = symbol_or_var
            if not symbol:
                raise HTTPException(status_code=400, detail="symbol_or_var must be a symbol for benchmark")
            model = str(form.get("model") or "xgboost").strip()
            horizon = str(form.get("horizon") or "50").strip()
            artifacts_dir = str(form.get("artifacts_dir") or "artifacts").strip()
            runs_dir = str(get_runs_dir())
            argv = python_module_argv(
                "ghtrader.cli",
                "benchmark",
                "--model",
                model,
                "--symbol",
                symbol,
                "--data-dir",
                str(data_dir),
                "--artifacts-dir",
                artifacts_dir,
                "--runs-dir",
                runs_dir,
                "--horizon",
                horizon,
            )
            title = f"benchmark {model} {symbol}"
        elif job_type == "main_schedule":
            var = symbol_or_var or "cu"
            argv = python_module_argv(
                "ghtrader.cli",
                "main-schedule",
                "--var",
                var,
                "--data-dir",
                str(data_dir),
            )
            title = f"main-schedule {var} env->latest"
        elif job_type == "main_l5":
            var = symbol_or_var or "cu"
            derived_symbol = str(form.get("derived_symbol") or "").strip()
            update_mode = str(form.get("update_mode") or "0").strip().lower() in {"1", "true", "yes", "on"}
            argv = python_module_argv("ghtrader.cli", "main-l5", "--var", var)
            if derived_symbol:
                argv += ["--symbol", derived_symbol]
            if update_mode:
                argv += ["--update"]
            title = f"main-l5 {var}"
        else:
            raise HTTPException(status_code=400, detail=f"Unknown job_type: {job_type}")

        jm = request.app.state.job_manager
        rec = jm.start_job(JobSpec(title=title, argv=argv, cwd=Path.cwd()))
        return RedirectResponse(url=f"/jobs/{rec.id}{_token_qs(request)}", status_code=303)

    @router.get("/jobs/{job_id}")
    def job_detail(request: Request, job_id: str):
        _require_auth(request)
        store = request.app.state.job_store
        jm = request.app.state.job_manager
        job = store.get_job(job_id)
        if job is None:
            raise HTTPException(status_code=404, detail="Job not found")
        log_text = jm.read_log_tail(job_id)
        return templates.TemplateResponse(
            request,
            "job_detail.html",
            {
                "request": request,
                "title": f"Job {job_id}",
                "token_qs": _token_qs(request),
                "job": job,
                "log_text": log_text,
            },
        )

    @router.post("/jobs/{job_id}/cancel")
    def job_cancel(request: Request, job_id: str):
        _require_auth(request)
        jm = request.app.state.job_manager
        jm.cancel_job(job_id)
        return RedirectResponse(url=f"/jobs/{job_id}{_token_qs(request)}", status_code=303)

    @router.get("/data")
    def data_page(request: Request):
        _require_auth(request)
        store = request.app.state.job_store
        runs_dir = get_runs_dir()

        jobs = store.list_jobs(limit=200)
        running = [j for j in jobs if j.status == "running"]
        queued = [j for j in jobs if j.status == "queued"]

        data_dir = get_data_dir()
        lv = "v2"


        # Locks
        locks = []
        try:
            from ghtrader.control.locks import LockStore

            locks = LockStore(runs_dir / "control" / "jobs.db").list_locks()
        except Exception:
            locks = []

        # Audit reports / QuestDB / coverage / validation can be cached + parallelized.
        t0 = time.time()
        reports = _data_page_cache_get("data_page:audit_reports")
        questdb = _data_page_cache_get("data_page:questdb_status")

        l5_start_date = ""
        l5_start_error = ""
        latest_trading_day = ""
        latest_trading_error = ""
        try:
            from ghtrader.config import get_l5_start_date

            l5_start_date = get_l5_start_date().isoformat()
        except Exception as e:
            l5_start_error = str(e)
        try:
            from ghtrader.data.trading_calendar import latest_trading_day as _latest_trading_day

            latest_trading_day = _latest_trading_day(data_dir=data_dir, refresh=False, allow_download=True).isoformat()
        except Exception as e:
            latest_trading_error = str(e)

        # Coverage watermarks (best-effort, default CU)
        main_l5_coverage: dict[str, Any] = {}
        main_schedule_coverage: dict[str, Any] = {}
        coverage_error = ""
        main_l5_validation: dict[str, Any] = {}
        validation_error = ""
        coverage_var = "cu"
        coverage_symbol = f"KQ.m@SHFE.{coverage_var}"

        coverage_payload = _data_page_cache_get(f"data_page:coverage:{coverage_symbol}")
        validation_payload = _data_page_cache_get(f"data_page:validation:{coverage_symbol}")

        tasks: dict[str, Any] = {}

        def _load_reports() -> list[str]:
            out: list[str] = []
            try:
                reports_dir = runs_dir / "audit"
                if reports_dir.exists():
                    out = sorted([p.name for p in reports_dir.glob("*.json")], reverse=True)[:50]
            except Exception:
                out = []
            return out

        def _load_questdb_status() -> dict[str, Any]:
            out: dict[str, Any] = {"ok": False}
            try:
                from ghtrader.config import (
                    get_questdb_host,
                    get_questdb_ilp_port,
                    get_questdb_pg_port,
                )
                from ghtrader.questdb.client import questdb_reachable_pg

                host = get_questdb_host()
                pg_port = int(get_questdb_pg_port())
                ilp_port = int(get_questdb_ilp_port())
                out.update({"host": host, "pg_port": pg_port, "ilp_port": ilp_port})

                q = questdb_reachable_pg(connect_timeout_s=2, retries=1, backoff_s=0.2)
                out["ok"] = bool(q.get("ok"))
                if not out["ok"] and q.get("error"):
                    out["error"] = str(q.get("error"))
            except Exception as e:
                out["ok"] = False
                out["error"] = str(e)
            return out

        cfg = None
        if coverage_payload is None or validation_payload is None:
            try:
                from ghtrader.questdb.client import make_questdb_query_config_from_env

                cfg = make_questdb_query_config_from_env()
            except Exception:
                cfg = None

        def _load_coverage() -> dict[str, Any]:
            out = {"main_schedule_coverage": {}, "main_l5_coverage": {}, "coverage_error": ""}
            if cfg is None:
                out["coverage_error"] = "questdb config unavailable"
                return out
            try:
                from ghtrader.questdb.main_schedule import fetch_main_schedule_state
                from ghtrader.questdb.queries import query_symbol_day_bounds

                sched = fetch_main_schedule_state(
                    cfg=cfg,
                    exchange="SHFE",
                    variety=coverage_var,
                )
                hashes = sched.get("schedule_hashes") or set()
                sched["schedule_hashes"] = sorted([str(h) for h in hashes if str(h).strip()])
                cov = query_symbol_day_bounds(
                    cfg=cfg,
                    table="ghtrader_ticks_main_l5_v2",
                    symbols=[coverage_symbol],
                    dataset_version="v2",
                    ticks_kind="main_l5",
                    l5_only=True,
                )
                out["main_schedule_coverage"] = sched
                out["main_l5_coverage"] = dict(cov.get(coverage_symbol) or {})
            except Exception as e:
                out["coverage_error"] = str(e)
            return out

        def _load_validation() -> dict[str, Any]:
            out = {"main_l5_validation": {}, "validation_error": ""}
            if cfg is None:
                out["validation_error"] = "questdb config unavailable"
                return out
            try:
                from ghtrader.questdb.main_l5_validate import (
                    fetch_latest_main_l5_validate_summary,
                    fetch_main_l5_validate_overview,
                    fetch_main_l5_validate_top_gap_days,
                    fetch_main_l5_validate_top_lag_days,
                )
                from ghtrader.data.main_l5_validation import read_latest_validation_report

                overview = fetch_main_l5_validate_overview(cfg=cfg, symbol=coverage_symbol)
                latest_rows = fetch_latest_main_l5_validate_summary(cfg=cfg, symbol=coverage_symbol, limit=1)
                latest = latest_rows[0] if latest_rows else {}
                top_gap_days = fetch_main_l5_validate_top_gap_days(cfg=cfg, symbol=coverage_symbol, limit=8)
                top_lag_days = fetch_main_l5_validate_top_lag_days(cfg=cfg, symbol=coverage_symbol, limit=8)
                if overview.get("days_total"):
                    out["main_l5_validation"] = {
                        "status": "ok"
                        if (
                            overview.get("missing_days", 0) == 0
                            and overview.get("missing_segments", 0) == 0
                            and overview.get("missing_half_seconds", 0) == 0
                        )
                        else "warn",
                        "last_day": overview.get("last_day"),
                        "checked_days": overview.get("days_total"),
                        "missing_days": overview.get("missing_days"),
                        "missing_segments_total": overview.get("missing_segments"),
                        "missing_seconds_total": overview.get("missing_seconds"),
                        "missing_half_seconds_total": overview.get("missing_half_seconds"),
                        "expected_seconds_strict_total": overview.get("expected_seconds_strict_total"),
                        "total_segments": overview.get("total_segments"),
                        "max_gap_s": overview.get("max_gap_s"),
                        "gap_threshold_s": overview.get("gap_threshold_s"),
                        "missing_seconds_ratio": overview.get("missing_seconds_ratio"),
                        "gap_buckets_total": {
                            "2_5": overview.get("gap_bucket_2_5"),
                            "6_15": overview.get("gap_bucket_6_15"),
                            "16_30": overview.get("gap_bucket_16_30"),
                            "gt_30": overview.get("gap_bucket_gt_30"),
                        },
                        "gap_count_gt_30s": overview.get("gap_count_gt_30"),
                        "p95_lag_s": overview.get("p95_lag_s"),
                        "max_lag_s": overview.get("max_lag_s"),
                        "cadence_mode": latest.get("cadence_mode"),
                        "two_plus_ratio": latest.get("two_plus_ratio"),
                        "last_run": latest.get("updated_at"),
                        "top_gap_days": top_gap_days,
                        "top_lag_days": top_lag_days,
                    }
                rep = read_latest_validation_report(
                    runs_dir=runs_dir,
                    exchange="SHFE",
                    variety=coverage_var,
                    derived_symbol=coverage_symbol,
                )
                if rep and rep.get("_path"):
                    out["main_l5_validation"]["report_name"] = Path(str(rep.get("_path"))).name
                if rep.get("created_at") and not out["main_l5_validation"].get("last_run"):
                    out["main_l5_validation"]["last_run"] = rep.get("created_at")
                    for key in (
                        "max_gap_s",
                        "gap_threshold_s",
                        "seconds_with_one_tick_total",
                        "missing_seconds_ratio",
                        "gap_buckets_total",
                        "gap_buckets_by_session_total",
                        "gap_count_gt_30s",
                    ):
                        if key in rep and key not in out["main_l5_validation"]:
                            out["main_l5_validation"][key] = rep.get(key)
            except Exception as e:
                out["validation_error"] = str(e)
            return out


        if reports is None:
            tasks["reports"] = _load_reports
        if questdb is None:
            tasks["questdb"] = _load_questdb_status
        if coverage_payload is None:
            tasks["coverage"] = _load_coverage
        if validation_payload is None:
            tasks["validation"] = _load_validation

        if tasks:
            with ThreadPoolExecutor(max_workers=min(4, len(tasks))) as executor:
                future_map = {executor.submit(fn): name for name, fn in tasks.items()}
                for fut in as_completed(future_map):
                    name = future_map[fut]
                    try:
                        result = fut.result()
                    except Exception as e:
                        result = {"error": str(e)}
                    if name == "reports":
                        reports = result if isinstance(result, list) else []
                        _data_page_cache_set("data_page:audit_reports", reports)
                    elif name == "questdb":
                        questdb = result if isinstance(result, dict) else {"ok": False, "error": "invalid"}
                        _data_page_cache_set("data_page:questdb_status", questdb)
                    elif name == "coverage":
                        coverage_payload = result if isinstance(result, dict) else {}
                        _data_page_cache_set(f"data_page:coverage:{coverage_symbol}", coverage_payload)
                    elif name == "validation":
                        validation_payload = result if isinstance(result, dict) else {}
                        _data_page_cache_set(f"data_page:validation:{coverage_symbol}", validation_payload)

        if reports is None:
            reports = []
        if questdb is None:
            questdb = {"ok": False, "error": "questdb unavailable"}
        if coverage_payload is not None:
            main_schedule_coverage = dict((coverage_payload or {}).get("main_schedule_coverage") or {})
            main_l5_coverage = dict((coverage_payload or {}).get("main_l5_coverage") or {})
            coverage_error = str((coverage_payload or {}).get("coverage_error") or "")
        if validation_payload is not None:
            main_l5_validation = dict((validation_payload or {}).get("main_l5_validation") or {})
            validation_error = str((validation_payload or {}).get("validation_error") or "")

        try:
            log.debug(
                "data_page.timing",
                coverage_symbol=coverage_symbol,
                cache_hits=int(
                    sum(
                        1
                        for k in (
                            reports is not None,
                            questdb is not None,
                            coverage_payload is not None,
                            validation_payload is not None,
                        )
                        if k
                    )
                ),
                ms=int((time.time() - t0) * 1000),
            )
        except Exception:
            pass

        # TqSdk scheduler settings (max parallel heavy jobs)
        tqsdk_scheduler = get_tqsdk_scheduler_state(runs_dir=runs_dir)

        # Coverage lists can get very large; keep /data page load fast by default.
        # The Contracts tab will lazy-load any detailed coverage via API.
        return templates.TemplateResponse(
            request,
            "data.html",
            {
                "request": request,
                "title": "Data Hub",
                "token_qs": _token_qs(request),
                "running_count": len(running),
                "queued_count": len(queued),
                "dataset_version": lv,
                "ticks_v2": [],
                "main_l5_v2": [],
                "features": [],
                "labels": [],
                "questdb": questdb,
                "locks": locks,
                "reports": reports,
                "l5_start_date": l5_start_date,
                "l5_start_error": l5_start_error,
                "latest_trading_day": latest_trading_day,
                "latest_trading_error": latest_trading_error,
                "tqsdk_scheduler": tqsdk_scheduler,
                "coverage_var": coverage_var,
                "coverage_symbol": coverage_symbol,
                "main_schedule_coverage": main_schedule_coverage,
                "main_l5_coverage": main_l5_coverage,
                "coverage_error": coverage_error,
                "main_l5_validation": main_l5_validation,
                "validation_error": validation_error,
            },
        )

    @router.get("/explorer")
    def explorer_page(request: Request):
        _require_auth(request)
        store = request.app.state.job_store
        jobs = store.list_jobs(limit=50)
        running = [j for j in jobs if j.status == "running"]

        q = "SELECT count() AS n_ticks FROM ghtrader_ticks_main_l5_v2"
        return templates.TemplateResponse(
            request,
            "explorer.html",
            {
                "request": request,
                "title": "SQL Explorer",
                "token_qs": _token_qs(request),
                "running_count": len(running),
                "query": q,
                "limit": 200,
                "columns": [],
                "rows": [],
                "error": "",
            },
        )

    @router.post("/explorer")
    async def explorer_run(request: Request):
        _require_auth(request)
        form = await request.form()
        query = str(form.get("query") or "").strip()
        limit_raw = str(form.get("limit") or "200").strip()

        try:
            limit = int(limit_raw)
        except Exception:
            limit = 200
        limit = max(1, min(limit, 500))

        columns: list[str] = []
        rows: list[dict[str, str]] = []
        err = ""

        if not query:
            err = "Query is required."
        else:
            try:
                from ghtrader.questdb.client import make_questdb_query_config_from_env
                from ghtrader.questdb.queries import query_sql_read_only

                cfg = make_questdb_query_config_from_env()
                columns, rows = query_sql_read_only(cfg=cfg, query=query, limit=int(limit), connect_timeout_s=2)
            except Exception as e:
                err = str(e)

        return templates.TemplateResponse(
            request,
            "explorer.html",
            {
                "request": request,
                "title": "SQL Explorer",
                "token_qs": _token_qs(request),
                "query": query,
                "limit": limit,
                "columns": columns,
                "rows": rows,
                "error": err,
            },
        )

    @router.get("/system")
    def system_page(request: Request):
        _require_auth(request)
        store = request.app.state.job_store
        jobs = store.list_jobs(limit=50)
        running = [j for j in jobs if j.status == "running"]

        data_dir = get_data_dir()
        runs_dir = get_runs_dir()
        artifacts_dir = get_artifacts_dir()

        return templates.TemplateResponse(
            request,
            "system.html",
            {
                "request": request,
                "title": "System",
                "token_qs": _token_qs(request),
                "running_count": len(running),
                # Render fast and fetch live metrics via /api/system (JS).
                "paths": [
                    {"key": "data", "path": str(data_dir), "exists": bool(data_dir.exists())},
                    {"key": "runs", "path": str(runs_dir), "exists": bool(runs_dir.exists())},
                    {"key": "artifacts", "path": str(artifacts_dir), "exists": bool(artifacts_dir.exists())},
                ],
            },
        )

    return router

