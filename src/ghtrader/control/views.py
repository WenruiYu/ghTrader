from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from fastapi import APIRouter, HTTPException, Request
from fastapi.responses import PlainTextResponse, RedirectResponse
from fastapi.templating import Jinja2Templates

from ghtrader.config import get_artifacts_dir, get_data_dir, get_lake_version, get_runs_dir
from ghtrader.control import auth
from ghtrader.control.coverage import scan_partitioned_store
from ghtrader.control.jobs import JobSpec, python_module_argv
from ghtrader.control.system_info import cpu_mem_info, disk_usage, gpu_info


def build_router() -> Any:
    router = APIRouter()

    templates = Jinja2Templates(directory=str(Path(__file__).parent / "templates"))

    def _require_auth(request: Request) -> None:
        if not auth.is_authorized(request):
            raise HTTPException(status_code=401, detail="Unauthorized")

    def _token_qs(request: Request) -> str:
        return auth.token_query_string(request)

    def _read_last_jsonl_line(path: Path, *, max_bytes: int = 64 * 1024) -> dict[str, Any] | None:
        if not path.exists():
            return None
        try:
            with open(path, "rb") as f:
                f.seek(0, 2)
                size = f.tell()
                offset = max(0, size - max_bytes)
                f.seek(offset)
                chunk = f.read().decode("utf-8", errors="ignore")
            lines = [ln for ln in chunk.splitlines() if ln.strip()]
            if not lines:
                return None
            return json.loads(lines[-1])
        except Exception:
            return None

    @router.get("/")
    def index(request: Request):
        _require_auth(request)
        store = request.app.state.job_store
        jobs = store.list_jobs(limit=50)
        running = [j for j in jobs if j.status == "running"]
        return templates.TemplateResponse(
            "index.html",
            {
                "request": request,
                "title": "ghTrader Control",
                "token_qs": _token_qs(request),
                "jobs": jobs,
                "running_count": len(running),
                "recent_count": len(jobs),
            },
        )

    @router.get("/jobs")
    def jobs(request: Request):
        _require_auth(request)
        store = request.app.state.job_store
        jobs = store.list_jobs(limit=200)
        return templates.TemplateResponse(
            "jobs.html",
            {
                "request": request,
                "title": "Jobs",
                "token_qs": _token_qs(request),
                "jobs": jobs,
            },
        )

    # ---------------------------------------------------------------------
    # Ops pages (full-form parity with CLI)
    # ---------------------------------------------------------------------

    @router.get("/ops/ingest")
    def ops_ingest(request: Request):
        _require_auth(request)
        store = request.app.state.job_store
        ingest_statuses: list[dict[str, Any]] = []
        try:
            from ghtrader.control.ingest_status import ingest_status_for_job, parse_ingest_command

            for job in store.list_jobs(limit=200):
                if job.status not in {"queued", "running"}:
                    continue
                kind = parse_ingest_command(job.command).get("kind")
                if kind not in {"download", "download_contract_range", "record"}:
                    continue
                s = ingest_status_for_job(
                    job_id=job.id,
                    command=job.command,
                    log_path=job.log_path,
                    default_data_dir=get_data_dir(),
                )
                s.update(
                    {
                        "job_status": job.status,
                        "title": job.title,
                        "source": job.source,
                        "created_at": job.created_at,
                        "started_at": job.started_at,
                    }
                )
                ingest_statuses.append(s)
        except Exception:
            ingest_statuses = []
        return templates.TemplateResponse(
            "ops_ingest.html",
            {"request": request, "title": "Ingest", "token_qs": _token_qs(request), "ingest_statuses": ingest_statuses},
        )

    @router.get("/ops/build")
    def ops_build(request: Request):
        _require_auth(request)
        return templates.TemplateResponse(
            "ops_build.html",
            {"request": request, "title": "Build", "token_qs": _token_qs(request)},
        )

    @router.get("/ops/model")
    def ops_model(request: Request):
        _require_auth(request)
        return templates.TemplateResponse(
            "ops_model.html",
            {"request": request, "title": "Model", "token_qs": _token_qs(request)},
        )

    @router.get("/ops/eval")
    def ops_eval(request: Request):
        _require_auth(request)
        return templates.TemplateResponse(
            "ops_eval.html",
            {"request": request, "title": "Eval", "token_qs": _token_qs(request)},
        )

    @router.get("/ops/trading")
    def ops_trading(request: Request):
        _require_auth(request)
        runs_dir = get_runs_dir()
        trading_dir = runs_dir / "trading"
        runs: list[dict[str, Any]] = []
        if trading_dir.exists():
            for p in sorted([d for d in trading_dir.iterdir() if d.is_dir()], key=lambda x: x.name, reverse=True)[:50]:
                last = _read_last_jsonl_line(p / "snapshots.jsonl")
                runs.append(
                    {
                        "run_id": p.name,
                        "last_ts": (last or {}).get("ts", ""),
                        "balance": ((last or {}).get("account") or {}).get("balance", ""),
                    }
                )
        return templates.TemplateResponse(
            "ops_trading.html",
            {"request": request, "title": "Trading", "token_qs": _token_qs(request), "runs": runs},
        )

    @router.get("/ops/trading/run/{run_id}")
    def ops_trading_run(request: Request, run_id: str):
        _require_auth(request)
        if "/" in run_id or "\\" in run_id:
            raise HTTPException(status_code=400, detail="invalid run id")
        root = get_runs_dir() / "trading" / run_id
        if not root.exists():
            raise HTTPException(status_code=404, detail="run not found")
        last = _read_last_jsonl_line(root / "snapshots.jsonl")
        return templates.TemplateResponse(
            "trading_run.html",
            {
                "request": request,
                "title": f"Trading {run_id}",
                "token_qs": _token_qs(request),
                "run_id": run_id,
                "root": str(root),
                "config_path": str(root / "run_config.json"),
                "snapshots_path": str(root / "snapshots.jsonl"),
                "events_path": str(root / "events.jsonl"),
                "last_snapshot": json.dumps(last or {}, indent=2, default=str),
            },
        )

    @router.get("/ops/locks")
    def ops_locks(request: Request):
        _require_auth(request)
        runs_dir = get_runs_dir()
        from ghtrader.control.locks import LockStore

        locks = LockStore(runs_dir / "control" / "jobs.db").list_locks()
        return templates.TemplateResponse(
            "ops_locks.html",
            {"request": request, "title": "Locks", "token_qs": _token_qs(request), "locks": locks},
        )

    @router.get("/ops/integrity")
    def ops_integrity(request: Request):
        _require_auth(request)
        runs_dir = get_runs_dir()
        reports_dir = runs_dir / "audit"
        reports = []
        if reports_dir.exists():
            reports = sorted([p.name for p in reports_dir.glob("*.json")], reverse=True)[:50]
        return templates.TemplateResponse(
            "ops_integrity.html",
            {"request": request, "title": "Integrity", "token_qs": _token_qs(request), "reports": reports},
        )

    @router.get("/ops/integrity/report/{name}")
    def ops_integrity_report(request: Request, name: str):
        _require_auth(request)
        if "/" in name or "\\" in name or not name.endswith(".json"):
            raise HTTPException(status_code=400, detail="invalid report name")
        p = get_runs_dir() / "audit" / name
        if not p.exists():
            raise HTTPException(status_code=404, detail="report not found")
        return PlainTextResponse(p.read_text(), media_type="application/json")

    @router.post("/ops/ingest/download")
    async def ops_ingest_download(request: Request):
        _require_auth(request)
        form = await request.form()
        symbol = str(form.get("symbol") or "").strip()
        start_date = str(form.get("start_date") or "").strip()
        end_date = str(form.get("end_date") or "").strip()
        data_dir = str(form.get("data_dir") or "data").strip()
        chunk_days = str(form.get("chunk_days") or "5").strip()
        lake_version = str(form.get("lake_version") or "v1").strip()
        if not symbol or not start_date or not end_date:
            raise HTTPException(status_code=400, detail="symbol/start_date/end_date required")
        argv = python_module_argv(
            "ghtrader.cli",
            "download",
            "--symbol",
            symbol,
            "--start",
            start_date,
            "--end",
            end_date,
            "--data-dir",
            data_dir,
            "--chunk-days",
            chunk_days,
            "--lake-version",
            lake_version,
        )
        title = f"download {symbol} {start_date}->{end_date} lake={lake_version}"
        jm = request.app.state.job_manager
        rec = jm.start_job(JobSpec(title=title, argv=argv, cwd=Path.cwd()))
        return RedirectResponse(url=f"/jobs/{rec.id}{_token_qs(request)}", status_code=303)

    @router.post("/ops/ingest/download_contract_range")
    async def ops_ingest_download_contract_range(request: Request):
        _require_auth(request)
        form = await request.form()
        exchange = str(form.get("exchange") or "SHFE").strip()
        var = str(form.get("variety") or "").strip()
        start_contract = str(form.get("start_contract") or "").strip()
        end_contract = str(form.get("end_contract") or "").strip()
        data_dir = str(form.get("data_dir") or "data").strip()
        chunk_days = str(form.get("chunk_days") or "5").strip()
        refresh_akshare = str(form.get("refresh_akshare") or "false").strip().lower() == "true"
        lake_version = str(form.get("lake_version") or "v1").strip()
        if not var or not start_contract or not end_contract:
            raise HTTPException(status_code=400, detail="var/start_contract/end_contract required")
        argv = python_module_argv(
            "ghtrader.cli",
            "download-contract-range",
            "--exchange",
            exchange,
            "--var",
            var,
            "--start-contract",
            start_contract,
            "--end-contract",
            end_contract,
            "--data-dir",
            data_dir,
            "--chunk-days",
            chunk_days,
            "--refresh-akshare" if refresh_akshare else "--no-refresh-akshare",
            "--lake-version",
            lake_version,
        )
        title = f"download-contract-range {var} {start_contract}->{end_contract} lake={lake_version}"
        jm = request.app.state.job_manager
        rec = jm.start_job(JobSpec(title=title, argv=argv, cwd=Path.cwd()))
        return RedirectResponse(url=f"/jobs/{rec.id}{_token_qs(request)}", status_code=303)

    @router.post("/ops/ingest/record")
    async def ops_ingest_record(request: Request):
        _require_auth(request)
        form = await request.form()
        symbols = str(form.get("symbols") or "").strip()
        data_dir = str(form.get("data_dir") or "data").strip()
        lake_version = str(form.get("lake_version") or "v1").strip()
        if not symbols:
            raise HTTPException(status_code=400, detail="symbols required")
        argv = python_module_argv("ghtrader.cli", "record")
        for s in [s.strip() for s in symbols.split(",") if s.strip()]:
            argv += ["--symbols", s]
        argv += ["--data-dir", data_dir]
        argv += ["--lake-version", lake_version]
        title = f"record {symbols} lake={lake_version}"
        jm = request.app.state.job_manager
        rec = jm.start_job(JobSpec(title=title, argv=argv, cwd=Path.cwd()))
        return RedirectResponse(url=f"/jobs/{rec.id}{_token_qs(request)}", status_code=303)

    @router.post("/ops/build/build")
    async def ops_build_build(request: Request):
        _require_auth(request)
        form = await request.form()
        symbol = str(form.get("symbol") or "").strip()
        horizons = str(form.get("horizons") or "10,50,200").strip()
        threshold_k = str(form.get("threshold_k") or "1").strip()
        ticks_lake = str(form.get("ticks_lake") or "raw").strip()
        overwrite = str(form.get("overwrite") or "false").strip().lower() == "true"
        data_dir = str(form.get("data_dir") or "data").strip()
        lake_version = str(form.get("lake_version") or "v1").strip()
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
            "--ticks-lake",
            ticks_lake,
            "--overwrite" if overwrite else "--no-overwrite",
            "--lake-version",
            lake_version,
        )
        title = f"build {symbol} ticks_lake={ticks_lake} overwrite={overwrite} lake={lake_version}"
        jm = request.app.state.job_manager
        rec = jm.start_job(JobSpec(title=title, argv=argv, cwd=Path.cwd()))
        return RedirectResponse(url=f"/jobs/{rec.id}{_token_qs(request)}", status_code=303)

    @router.post("/ops/build/main_schedule")
    async def ops_build_main_schedule(request: Request):
        _require_auth(request)
        form = await request.form()
        var = str(form.get("variety") or "cu").strip()
        start_date = str(form.get("start_date") or "").strip()
        end_date = str(form.get("end_date") or "").strip()
        threshold = str(form.get("threshold") or "1.1").strip()
        refresh_akshare = str(form.get("refresh_akshare") or "false").strip().lower() == "true"
        data_dir = str(form.get("data_dir") or "data").strip()
        if not start_date or not end_date:
            raise HTTPException(status_code=400, detail="start_date/end_date required")
        argv = python_module_argv(
            "ghtrader.cli",
            "main-schedule",
            "--var",
            var,
            "--start",
            start_date,
            "--end",
            end_date,
            "--threshold",
            threshold,
            "--data-dir",
            data_dir,
            "--refresh-akshare" if refresh_akshare else "--no-refresh-akshare",
        )
        title = f"main-schedule {var} {start_date}->{end_date}"
        jm = request.app.state.job_manager
        rec = jm.start_job(JobSpec(title=title, argv=argv, cwd=Path.cwd()))
        return RedirectResponse(url=f"/jobs/{rec.id}{_token_qs(request)}", status_code=303)

    @router.post("/ops/build/main_depth")
    async def ops_build_main_depth(request: Request):
        _require_auth(request)
        form = await request.form()
        derived_symbol = str(form.get("derived_symbol") or "").strip()
        schedule_path = str(form.get("schedule_path") or "").strip()
        overwrite = str(form.get("overwrite") or "false").strip().lower() == "true"
        data_dir = str(form.get("data_dir") or "data").strip()
        lake_version = str(form.get("lake_version") or "v1").strip()
        if not derived_symbol or not schedule_path:
            raise HTTPException(status_code=400, detail="derived_symbol/schedule_path required")
        argv = python_module_argv(
            "ghtrader.cli",
            "main-depth",
            "--symbol",
            derived_symbol,
            "--schedule-path",
            schedule_path,
            "--data-dir",
            data_dir,
            "--overwrite" if overwrite else "--no-overwrite",
            "--lake-version",
            lake_version,
        )
        title = f"main-depth {derived_symbol} lake={lake_version}"
        jm = request.app.state.job_manager
        rec = jm.start_job(JobSpec(title=title, argv=argv, cwd=Path.cwd()))
        return RedirectResponse(url=f"/jobs/{rec.id}{_token_qs(request)}", status_code=303)

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

    @router.post("/ops/model/sweep")
    async def ops_model_sweep(request: Request):
        _require_auth(request)
        form = await request.form()
        symbol = str(form.get("symbol") or "").strip()
        model = str(form.get("model") or "deeplob").strip()
        data_dir = str(form.get("data_dir") or "data").strip()
        artifacts_dir = str(form.get("artifacts_dir") or "artifacts").strip()
        runs_dir = str(form.get("runs_dir") or "runs").strip()
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
            runs_dir,
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

    @router.post("/ops/eval/benchmark")
    async def ops_eval_benchmark(request: Request):
        _require_auth(request)
        form = await request.form()
        model = str(form.get("model") or "").strip()
        symbol = str(form.get("symbol") or "").strip()
        data_dir = str(form.get("data_dir") or "data").strip()
        artifacts_dir = str(form.get("artifacts_dir") or "artifacts").strip()
        runs_dir = str(form.get("runs_dir") or "runs").strip()
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
            runs_dir,
            "--horizon",
            horizon,
        )
        title = f"benchmark {model} {symbol}"
        jm = request.app.state.job_manager
        rec = jm.start_job(JobSpec(title=title, argv=argv, cwd=Path.cwd()))
        return RedirectResponse(url=f"/jobs/{rec.id}{_token_qs(request)}", status_code=303)

    @router.post("/ops/eval/compare")
    async def ops_eval_compare(request: Request):
        _require_auth(request)
        form = await request.form()
        symbol = str(form.get("symbol") or "").strip()
        models = str(form.get("models") or "").strip()
        data_dir = str(form.get("data_dir") or "data").strip()
        artifacts_dir = str(form.get("artifacts_dir") or "artifacts").strip()
        runs_dir = str(form.get("runs_dir") or "runs").strip()
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
            runs_dir,
            "--horizon",
            horizon,
        )
        title = f"compare {symbol}"
        jm = request.app.state.job_manager
        rec = jm.start_job(JobSpec(title=title, argv=argv, cwd=Path.cwd()))
        return RedirectResponse(url=f"/jobs/{rec.id}{_token_qs(request)}", status_code=303)

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
        runs_dir = str(form.get("runs_dir") or "runs").strip()
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
            runs_dir,
        )
        title = f"backtest {model} {symbol}"
        jm = request.app.state.job_manager
        rec = jm.start_job(JobSpec(title=title, argv=argv, cwd=Path.cwd()))
        return RedirectResponse(url=f"/jobs/{rec.id}{_token_qs(request)}", status_code=303)

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
        runs_dir = str(form.get("runs_dir") or "runs").strip()
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
            runs_dir,
            "--horizon",
            horizon,
            "--lookback-days",
            lookback_days,
        ]
        title = f"daily-train {model}"
        jm = request.app.state.job_manager
        rec = jm.start_job(JobSpec(title=title, argv=argv, cwd=Path.cwd()))
        return RedirectResponse(url=f"/jobs/{rec.id}{_token_qs(request)}", status_code=303)

    @router.post("/ops/trading/trade")
    async def ops_trading_trade(request: Request):
        _require_auth(request)
        form = await request.form()
        mode = str(form.get("mode") or "paper").strip()
        monitor_only = str(form.get("monitor_only") or "").strip().lower() in {"true", "1", "yes", "on"}
        sim_account = str(form.get("sim_account") or "tqsim").strip()
        executor = str(form.get("executor") or "targetpos").strip()
        model = str(form.get("model") or "").strip()
        symbols = str(form.get("symbols") or "").strip()
        horizon = str(form.get("horizon") or "50").strip()
        threshold_up = str(form.get("threshold_up") or "0.6").strip()
        threshold_down = str(form.get("threshold_down") or "0.6").strip()
        position_size = str(form.get("position_size") or "1").strip()
        data_dir = str(form.get("data_dir") or "data").strip()
        artifacts_dir = str(form.get("artifacts_dir") or "artifacts").strip()
        runs_dir = str(form.get("runs_dir") or "runs").strip()
        max_position = str(form.get("max_position") or "1").strip()
        max_order_size = str(form.get("max_order_size") or "1").strip()
        max_ops_per_sec = str(form.get("max_ops_per_sec") or "10").strip()
        max_daily_loss = str(form.get("max_daily_loss") or "").strip()
        enforce_trading_time = str(form.get("enforce_trading_time") or "true").strip().lower() == "true"
        tp_price = str(form.get("tp_price") or "ACTIVE").strip()
        tp_offset_priority = str(form.get("tp_offset_priority") or "今昨,开").strip()
        direct_price_mode = str(form.get("direct_price_mode") or "ACTIVE").strip()
        direct_advanced = str(form.get("direct_advanced") or "").strip()
        confirm_live = str(form.get("confirm_live") or "").strip()
        snapshot_interval_sec = str(form.get("snapshot_interval_sec") or "10").strip()

        if not model or not symbols:
            raise HTTPException(status_code=400, detail="model/symbols required")

        argv = python_module_argv(
            "ghtrader.cli",
            "trade",
            "--mode",
            mode,
            "--monitor-only" if monitor_only else "--no-monitor-only",
            "--sim-account",
            sim_account,
            "--executor",
            executor,
            "--model",
            model,
            "--data-dir",
            data_dir,
            "--artifacts-dir",
            artifacts_dir,
            "--runs-dir",
            runs_dir,
            "--horizon",
            horizon,
            "--threshold-up",
            threshold_up,
            "--threshold-down",
            threshold_down,
            "--position-size",
            position_size,
            "--max-position",
            max_position,
            "--max-order-size",
            max_order_size,
            "--max-ops-per-sec",
            max_ops_per_sec,
            "--tp-price",
            tp_price,
            "--tp-offset-priority",
            tp_offset_priority,
            "--direct-price-mode",
            direct_price_mode,
            "--snapshot-interval-sec",
            snapshot_interval_sec,
        )
        if not enforce_trading_time:
            argv.append("--no-enforce-trading-time")
        if max_daily_loss:
            argv += ["--max-daily-loss", max_daily_loss]
        if direct_advanced:
            argv += ["--direct-advanced", direct_advanced]
        if confirm_live:
            argv += ["--confirm-live", confirm_live]
        for s in [s.strip() for s in symbols.split(",") if s.strip()]:
            argv += ["--symbols", s]

        title = f"trade {mode} {executor} {model}"
        jm = request.app.state.job_manager
        rec = jm.start_job(JobSpec(title=title, argv=argv, cwd=Path.cwd()))
        return RedirectResponse(url=f"/jobs/{rec.id}{_token_qs(request)}", status_code=303)

    @router.post("/ops/integrity/audit")
    async def ops_integrity_audit(request: Request):
        _require_auth(request)
        form = await request.form()
        scopes = form.getlist("scopes")
        data_dir = str(form.get("data_dir") or "data").strip()
        runs_dir = str(form.get("runs_dir") or "runs").strip()
        lake_version = str(form.get("lake_version") or "v1").strip()
        scopes = [s for s in scopes if s]
        if not scopes:
            scopes = ["all"]
        argv = python_module_argv(
            "ghtrader.cli",
            "audit",
            *sum([["--scope", s] for s in scopes], []),
            "--data-dir",
            data_dir,
            "--runs-dir",
            runs_dir,
            "--lake-version",
            lake_version,
        )
        title = f"audit {','.join(scopes)} lake={lake_version}"
        jm = request.app.state.job_manager
        rec = jm.start_job(JobSpec(title=title, argv=argv, cwd=Path.cwd()))
        return RedirectResponse(url=f"/jobs/{rec.id}{_token_qs(request)}", status_code=303)

    @router.post("/jobs/start")
    async def jobs_start(request: Request):
        _require_auth(request)
        form = await request.form()

        job_type = str(form.get("job_type") or "").strip()
        symbol_or_var = str(form.get("symbol_or_var") or "").strip()
        data_dir = Path(str(form.get("data_dir") or "data"))
        lake_version = str(form.get("lake_version") or "v1").strip()

        # Build argv for a known-safe set of job types (no shell).
        if job_type == "download_contract_range":
            var = symbol_or_var or "cu"
            start_contract = str(form.get("start_contract") or "1601").strip()
            end_contract = str(form.get("end_contract") or "auto").strip()
            chunk_days = str(form.get("chunk_days") or "5").strip()
            argv = python_module_argv(
                "ghtrader.cli",
                "download-contract-range",
                "--exchange",
                "SHFE",
                "--var",
                var,
                "--start-contract",
                start_contract,
                "--end-contract",
                end_contract,
                "--data-dir",
                str(data_dir),
                "--chunk-days",
                chunk_days,
                "--lake-version",
                lake_version,
            )
            title = f"download-contract-range {var} {start_contract}->{end_contract} lake={lake_version}"
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
                "--lake-version",
                lake_version,
            )
            title = f"build {symbol} lake={lake_version}"
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
            start_date = str(form.get("start_date") or "").strip()
            end_date = str(form.get("end_date") or "").strip()
            threshold = str(form.get("threshold") or "1.1").strip()
            if not start_date or not end_date:
                raise HTTPException(status_code=400, detail="start_date and end_date are required for main_schedule")
            argv = python_module_argv(
                "ghtrader.cli",
                "main-schedule",
                "--var",
                var,
                "--start",
                start_date,
                "--end",
                end_date,
                "--threshold",
                threshold,
                "--data-dir",
                str(data_dir),
            )
            title = f"main-schedule {var} {start_date}->{end_date}"
        elif job_type == "main_depth":
            derived_symbol = str(form.get("derived_symbol") or "KQ.m@SHFE.cu").strip()
            schedule_path = str(form.get("schedule_path") or "").strip()
            if not schedule_path:
                raise HTTPException(status_code=400, detail="schedule_path is required for main_depth")
            argv = python_module_argv(
                "ghtrader.cli",
                "main-depth",
                "--symbol",
                derived_symbol,
                "--schedule-path",
                schedule_path,
                "--data-dir",
                str(data_dir),
                "--lake-version",
                lake_version,
            )
            title = f"main-depth {derived_symbol} lake={lake_version}"
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
    def data_page(request: Request, lake_version: str | None = None):
        _require_auth(request)
        data_dir = get_data_dir()
        lv = str(lake_version or "").strip().lower()
        if lv not in {"v1", "v2", "both"}:
            lv = get_lake_version()

        show_v1 = lv in {"v1", "both"}
        show_v2 = lv in {"v2", "both"}

        ticks_root_v1 = data_dir / "lake" / "ticks"
        ticks_root_v2 = data_dir / "lake_v2" / "ticks"
        main_l5_root_v1 = data_dir / "lake" / "main_l5" / "ticks"
        main_l5_root_v2 = data_dir / "lake_v2" / "main_l5" / "ticks"
        features_root = data_dir / "features"
        labels_root = data_dir / "labels"

        return templates.TemplateResponse(
            "data.html",
            {
                "request": request,
                "title": "Data coverage",
                "token_qs": _token_qs(request),
                "lake_version": lv,
                "show_v1": show_v1,
                "show_v2": show_v2,
                "ticks_v1": scan_partitioned_store(ticks_root_v1) if show_v1 else [],
                "ticks_v2": scan_partitioned_store(ticks_root_v2) if show_v2 else [],
                "main_l5_v1": scan_partitioned_store(main_l5_root_v1) if show_v1 else [],
                "main_l5_v2": scan_partitioned_store(main_l5_root_v2) if show_v2 else [],
                "features": scan_partitioned_store(features_root),
                "labels": scan_partitioned_store(labels_root),
            },
        )

    @router.get("/explorer")
    def explorer_page(request: Request):
        _require_auth(request)
        q = "SELECT COUNT(*) AS n_ticks FROM ticks_raw_v2"
        return templates.TemplateResponse(
            "explorer.html",
            {
                "request": request,
                "title": "SQL Explorer",
                "token_qs": _token_qs(request),
                "query": q,
                "limit": 200,
                "include_metrics": False,
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
        include_metrics = str(form.get("include_metrics") or "false").strip().lower() == "true"

        try:
            limit = int(limit_raw)
        except Exception:
            limit = 200
        limit = max(1, min(limit, 500))

        columns: list[str] = []
        rows: list[dict[str, Any]] = []
        err = ""

        if not query:
            err = "Query is required."
        else:
            try:
                from ghtrader.db import DuckDBBackend, DuckDBConfig, DuckDBNotInstalled

                data_dir = get_data_dir()
                runs_dir = get_runs_dir()
                db_path = data_dir / "ghtrader.duckdb"

                # Prefer a prepared DB file (created via `ghtrader db init`).
                tried_file = False
                if db_path.exists():
                    tried_file = True
                    try:
                        backend = DuckDBBackend(config=DuckDBConfig(db_path=db_path, read_only=True))
                        with backend.connect() as con:
                            df = backend.query_df_limited(con=con, sql=query, limit=limit)
                        columns = df.columns.tolist()
                        rows = df.astype(str).to_dict(orient="records")
                    except Exception:
                        # Fall back to in-memory views if the DB file is missing views/tables.
                        columns = []
                        rows = []

                if not rows:
                    backend = DuckDBBackend(config=DuckDBConfig(db_path=None, read_only=False))
                    with backend.connect() as con:
                        backend.init_views(con=con, data_dir=data_dir)
                        if include_metrics:
                            backend.ingest_runs_metrics(con=con, runs_dir=runs_dir)
                        df = backend.query_df_limited(con=con, sql=query, limit=limit)
                    columns = df.columns.tolist()
                    rows = df.astype(str).to_dict(orient="records")

                if tried_file and db_path.exists() and not rows:
                    err = "DuckDB file exists but query failed; fell back to in-memory views (consider running `ghtrader db init`)."
            except DuckDBNotInstalled:
                err = "DuckDB is not installed on the server. Install with: pip install -e '.[db,control]'"
            except Exception as e:
                err = str(e)

        return templates.TemplateResponse(
            "explorer.html",
            {
                "request": request,
                "title": "SQL Explorer",
                "token_qs": _token_qs(request),
                "query": query,
                "limit": limit,
                "include_metrics": include_metrics,
                "columns": columns,
                "rows": rows,
                "error": err,
            },
        )

    @router.get("/system")
    def system_page(request: Request):
        _require_auth(request)
        data_dir = get_data_dir()
        runs_dir = get_runs_dir()
        artifacts_dir = get_artifacts_dir()
        disks = [
            (str(data_dir), disk_usage(data_dir)),
            (str(runs_dir), disk_usage(runs_dir)),
            (str(artifacts_dir), disk_usage(artifacts_dir)),
        ]
        return templates.TemplateResponse(
            "system.html",
            {
                "request": request,
                "title": "System",
                "token_qs": _token_qs(request),
                "cpu_mem": cpu_mem_info(),
                "disks": disks,
                "gpu_info": gpu_info(),
            },
        )

    return router

