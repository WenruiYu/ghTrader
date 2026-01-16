from __future__ import annotations

import json
from datetime import datetime
from pathlib import Path
from typing import Any

from fastapi import APIRouter, HTTPException, Request
from fastapi.responses import PlainTextResponse, RedirectResponse
from fastapi.templating import Jinja2Templates

from ghtrader.config import get_artifacts_dir, get_data_dir, get_runs_dir
from ghtrader.control import auth
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
        queued = [j for j in jobs if j.status == "queued"]

        # Defaults for schedule builder quick actions
        default_schedule_start = "2015-01-01"
        default_schedule_end = datetime.now().date().isoformat()

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
                "default_schedule_start": default_schedule_start,
                "default_schedule_end": default_schedule_end,
            },
        )

    @router.get("/jobs")
    def jobs(request: Request):
        _require_auth(request)
        store = request.app.state.job_store
        jobs = store.list_jobs(limit=200)
        running = [j for j in jobs if j.status == "running"]
        return templates.TemplateResponse(
            request,
            "jobs.html",
            {
                "request": request,
                "title": "Jobs",
                "token_qs": _token_qs(request),
                "jobs": jobs,
                "running_count": len(running),
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

        # Trading runs
        runs_dir = get_runs_dir()
        trading_dir = runs_dir / "trading"
        runs: list[dict[str, Any]] = []
        try:
            if trading_dir.exists():
                for p in sorted([d for d in trading_dir.iterdir() if d.is_dir()], key=lambda x: x.name, reverse=True)[:50]:
                    state = None
                    try:
                        sf = p / "state.json"
                        if sf.exists():
                            state = json.loads(sf.read_text(encoding="utf-8"))
                    except Exception:
                        state = None
                    last = None
                    if isinstance(state, dict) and isinstance(state.get("last_snapshot"), dict):
                        last = state.get("last_snapshot")
                    else:
                        last = _read_last_jsonl_line(p / "snapshots.jsonl")
                    cfg = {}
                    try:
                        cf = p / "run_config.json"
                        if cf.exists():
                            cfg = json.loads(cf.read_text(encoding="utf-8"))
                    except Exception:
                        cfg = {}
                    runs.append(
                        {
                            "run_id": p.name,
                            "last_ts": (last or {}).get("ts", ""),
                            "balance": ((last or {}).get("account") or {}).get("balance", ""),
                            "mode": cfg.get("mode", ""),
                            "account_profile": cfg.get("account_profile", "default") or "default",
                            "monitor_only": bool(cfg.get("monitor_only")) if ("monitor_only" in cfg) else None,
                        }
                    )
        except Exception:
            runs = []

        return templates.TemplateResponse(
            request,
            "trading.html",
            {
                "request": request,
                "title": "Trading",
                "token_qs": _token_qs(request),
                "running_count": len(running),
                "runs": runs,
            },
        )

    # ---------------------------------------------------------------------
    # Ops pages (full-form parity with CLI)
    # ---------------------------------------------------------------------

    @router.get("/ops")
    def ops(request: Request):
        _require_auth(request)
        store = request.app.state.job_store

        jobs = store.list_jobs(limit=200)
        running_count = len([j for j in jobs if j.status == "running"])
        queued_count = len([j for j in jobs if j.status == "queued"])
        recent_count = len(jobs)

        # Ingest statuses (same as legacy /ops/ingest page)
        ingest_statuses: list[dict[str, Any]] = []
        try:
            from ghtrader.control.ingest_status import ingest_status_for_job, parse_ingest_command

            for job in jobs:
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

        # Trading runs (same as legacy /ops/trading page)
        runs_dir = get_runs_dir()
        trading_dir = runs_dir / "trading"
        runs: list[dict[str, Any]] = []
        try:
            if trading_dir.exists():
                for p in sorted([d for d in trading_dir.iterdir() if d.is_dir()], key=lambda x: x.name, reverse=True)[:50]:
                    state = None
                    try:
                        sf = p / "state.json"
                        if sf.exists():
                            state = json.loads(sf.read_text(encoding="utf-8"))
                    except Exception:
                        state = None
                    last = None
                    if isinstance(state, dict) and isinstance(state.get("last_snapshot"), dict):
                        last = state.get("last_snapshot")
                    else:
                        last = _read_last_jsonl_line(p / "snapshots.jsonl")
                    cfg = {}
                    try:
                        cf = p / "run_config.json"
                        if cf.exists():
                            cfg = json.loads(cf.read_text(encoding="utf-8"))
                    except Exception:
                        cfg = {}
                    runs.append(
                        {
                            "run_id": p.name,
                            "last_ts": (last or {}).get("ts", ""),
                            "balance": ((last or {}).get("account") or {}).get("balance", ""),
                            "mode": cfg.get("mode", ""),
                            "account_profile": cfg.get("account_profile", "default") or "default",
                            "monitor_only": bool(cfg.get("monitor_only")) if ("monitor_only" in cfg) else None,
                        }
                    )
        except Exception:
            runs = []

        # Locks (same as legacy /ops/locks page)
        locks = []
        try:
            from ghtrader.control.locks import LockStore

            locks = LockStore(runs_dir / "control" / "jobs.db").list_locks()
        except Exception:
            locks = []

        # Audit reports (same as legacy /ops/integrity page)
        reports = []
        try:
            reports_dir = runs_dir / "audit"
            if reports_dir.exists():
                reports = sorted([p.name for p in reports_dir.glob("*.json")], reverse=True)[:50]
        except Exception:
            reports = []

        # QuestDB reachability (best-effort).
        questdb: dict[str, Any] = {"ok": False}
        try:
            from ghtrader.config import (
                get_questdb_host,
                get_questdb_ilp_port,
                get_questdb_pg_dbname,
                get_questdb_pg_password,
                get_questdb_pg_port,
                get_questdb_pg_user,
            )

            host = get_questdb_host()
            pg_port = int(get_questdb_pg_port())
            ilp_port = int(get_questdb_ilp_port())
            questdb.update({"host": host, "pg_port": pg_port, "ilp_port": ilp_port})
            try:
                import psycopg  # type: ignore

                with psycopg.connect(
                    user=str(get_questdb_pg_user()),
                    password=str(get_questdb_pg_password()),
                    host=str(host),
                    port=int(pg_port),
                    dbname=str(get_questdb_pg_dbname()),
                    connect_timeout=1,
                ) as conn:
                    with conn.cursor() as cur:
                        cur.execute("SELECT 1")
                        cur.fetchone()
                questdb["ok"] = True
            except Exception as e:
                questdb["ok"] = False
                questdb["error"] = str(e)
        except Exception as e:
            questdb["ok"] = False
            questdb["error"] = str(e)

        # Defaults for schedule builder quick actions
        default_schedule_start = "2015-01-01"
        default_schedule_end = datetime.now().date().isoformat()

        return templates.TemplateResponse(
            request,
            "ops.html",
            {
                "request": request,
                "title": "Operations",
                "token_qs": _token_qs(request),
                "running_count": running_count,
                "queued_count": queued_count,
                "recent_count": recent_count,
                "ingest_statuses": ingest_statuses,
                "runs": runs,
                "locks": locks,
                "reports": reports,
                "questdb": questdb,
                "default_schedule_start": default_schedule_start,
                "default_schedule_end": default_schedule_end,
            },
        )

    @router.get("/ops/ingest")
    def ops_ingest(request: Request):
        _require_auth(request)
        return RedirectResponse(url=f"/ops{_token_qs(request)}#ingest", status_code=303)

    @router.get("/ops/build")
    def ops_build(request: Request):
        _require_auth(request)
        return RedirectResponse(url=f"/ops{_token_qs(request)}#build", status_code=303)

    @router.get("/ops/model")
    def ops_model(request: Request):
        _require_auth(request)
        return RedirectResponse(url=f"/models{_token_qs(request)}", status_code=303)

    @router.get("/ops/eval")
    def ops_eval(request: Request):
        _require_auth(request)
        return RedirectResponse(url=f"/models{_token_qs(request)}", status_code=303)

    @router.get("/ops/trading")
    def ops_trading(request: Request):
        _require_auth(request)
        return RedirectResponse(url=f"/trading{_token_qs(request)}", status_code=303)

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
            request,
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
        return RedirectResponse(url=f"/ops{_token_qs(request)}#locks", status_code=303)

    @router.get("/ops/integrity")
    def ops_integrity(request: Request):
        _require_auth(request)
        return RedirectResponse(url=f"/ops{_token_qs(request)}#integrity", status_code=303)

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
        )
        title = f"download {symbol} {start_date}->{end_date}"
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
        start_date = str(form.get("start_date") or "").strip()
        end_date = str(form.get("end_date") or "").strip()
        data_dir = str(form.get("data_dir") or "data").strip()
        chunk_days = str(form.get("chunk_days") or "5").strip()
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
        )
        if start_date:
            argv += ["--start-date", start_date]
        if end_date:
            argv += ["--end-date", end_date]
        title = f"download-contract-range {var} {start_contract}->{end_contract}"
        jm = request.app.state.job_manager
        rec = jm.start_job(JobSpec(title=title, argv=argv, cwd=Path.cwd()))
        return RedirectResponse(url=f"/jobs/{rec.id}{_token_qs(request)}", status_code=303)

    @router.post("/ops/ingest/update_variety")
    async def ops_ingest_update_variety(request: Request):
        _require_auth(request)
        form = await request.form()
        exchange = str(form.get("exchange") or "SHFE").strip()
        var = str(form.get("variety") or "").strip()
        recent_days = str(form.get("recent_expired_days") or "10").strip()
        data_dir = str(form.get("data_dir") or "data").strip()
        runs_dir = str(form.get("runs_dir") or "runs").strip()
        if not var:
            raise HTTPException(status_code=400, detail="variety required")
        argv = python_module_argv(
            "ghtrader.cli",
            "update",
            "--exchange",
            exchange,
            "--var",
            var,
            "--recent-expired-days",
            recent_days,
            "--data-dir",
            data_dir,
            "--runs-dir",
            runs_dir,
        )
        title = f"update {exchange}.{var}"
        jm = request.app.state.job_manager
        # Update is TqSdk-heavy; enqueue to respect max-parallel scheduler.
        rec = jm.enqueue_job(JobSpec(title=title, argv=argv, cwd=Path.cwd()))
        return RedirectResponse(url=f"/jobs/{rec.id}{_token_qs(request)}", status_code=303)

    @router.post("/ops/ingest/record")
    async def ops_ingest_record(request: Request):
        _require_auth(request)
        form = await request.form()
        symbols = str(form.get("symbols") or "").strip()
        data_dir = str(form.get("data_dir") or "data").strip()
        if not symbols:
            raise HTTPException(status_code=400, detail="symbols required")
        argv = python_module_argv("ghtrader.cli", "record")
        for s in [s.strip() for s in symbols.split(",") if s.strip()]:
            argv += ["--symbols", s]
        argv += ["--data-dir", data_dir]
        title = f"record {symbols}"
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
        )
        title = f"build {symbol} ticks_lake={ticks_lake} overwrite={overwrite}"
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
        )
        title = f"main-schedule {var} {start_date}->{end_date}"
        jm = request.app.state.job_manager
        rec = jm.start_job(JobSpec(title=title, argv=argv, cwd=Path.cwd()))
        return RedirectResponse(url=f"/jobs/{rec.id}{_token_qs(request)}", status_code=303)

    @router.post("/ops/build/main_l5")
    async def ops_build_main_l5(request: Request):
        _require_auth(request)
        form = await request.form()
        var = str(form.get("variety") or "cu").strip()
        derived_symbol = str(form.get("derived_symbol") or f"KQ.m@SHFE.{var}").strip()
        overwrite = str(form.get("overwrite") or "false").strip().lower() == "true"
        if not var or not derived_symbol:
            raise HTTPException(status_code=400, detail="variety/derived_symbol required")
        argv = python_module_argv(
            "ghtrader.cli",
            "main-l5",
            "--var",
            var,
            "--symbol",
            derived_symbol,
            "--overwrite" if overwrite else "--no-overwrite",
        )
        title = f"main-l5 {var} {derived_symbol}"
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
        account_profile = str(form.get("account_profile") or form.get("account") or "default").strip() or "default"
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
            "--account",
            account_profile,
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

        title = f"trade {account_profile} {mode} {executor} {model}"
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
        exchange = str(form.get("exchange") or "").strip()
        variety = str(form.get("variety") or "").strip()
        refresh_catalog = str(form.get("refresh_catalog") or "").strip().lower() in {"true", "1", "yes", "on"}
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
        )
        if exchange:
            argv += ["--exchange", exchange]
        if variety:
            argv += ["--var", variety]
        if refresh_catalog:
            argv += ["--refresh-catalog"]
        title = f"audit {','.join(scopes)}"
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
            )
            title = f"download-contract-range {var} {start_contract}->{end_contract}"
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
        elif job_type == "main_l5":
            var = symbol_or_var or "cu"
            derived_symbol = str(form.get("derived_symbol") or "").strip()
            overwrite = str(form.get("overwrite") or "0").strip().lower() in {"1", "true", "yes", "on"}
            argv = python_module_argv("ghtrader.cli", "main-l5", "--var", var)
            if derived_symbol:
                argv += ["--symbol", derived_symbol]
            argv += ["--overwrite" if overwrite else "--no-overwrite"]
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
        jobs = store.list_jobs(limit=50)
        running = [j for j in jobs if j.status == "running"]

        data_dir = get_data_dir()
        lv = "v2"

        # QuestDB reachability (best-effort) for sync tab
        questdb: dict[str, Any] = {"ok": False}
        try:
            from ghtrader.config import (
                get_questdb_host,
                get_questdb_ilp_port,
                get_questdb_pg_dbname,
                get_questdb_pg_password,
                get_questdb_pg_port,
                get_questdb_pg_user,
            )

            host = get_questdb_host()
            pg_port = int(get_questdb_pg_port())
            ilp_port = int(get_questdb_ilp_port())
            questdb.update({"host": host, "pg_port": pg_port, "ilp_port": ilp_port})
            try:
                import psycopg  # type: ignore

                with psycopg.connect(
                    user=str(get_questdb_pg_user()),
                    password=str(get_questdb_pg_password()),
                    host=str(host),
                    port=int(pg_port),
                    dbname=str(get_questdb_pg_dbname()),
                ) as conn:
                    with conn.cursor() as cur:
                        cur.execute("SELECT 1")
                        cur.fetchone()
                questdb["ok"] = True
            except Exception as e:
                questdb["ok"] = False
                questdb["error"] = str(e)
        except Exception as e:
            questdb["ok"] = False
            questdb["error"] = str(e)

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
                "lake_version": lv,
                "ticks_v2": [],
                "main_l5_v2": [],
                "features": [],
                "labels": [],
                "questdb": questdb,
            },
        )

    @router.get("/explorer")
    def explorer_page(request: Request):
        _require_auth(request)
        store = request.app.state.job_store
        jobs = store.list_jobs(limit=50)
        running = [j for j in jobs if j.status == "running"]

        q = "SELECT count() AS n_ticks FROM ghtrader_ticks_raw_v2"
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
                from ghtrader.questdb_client import make_questdb_query_config_from_env
                from ghtrader.questdb_queries import query_sql_read_only

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

