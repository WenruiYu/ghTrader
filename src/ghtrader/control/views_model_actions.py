from __future__ import annotations

from pathlib import Path
from typing import Any, Callable

from fastapi import APIRouter, HTTPException, Request
from fastapi.responses import RedirectResponse

from ghtrader.control.jobs import JobSpec, python_module_argv
from ghtrader.control.views_helpers import safe_int as _safe_int

RequireAuth = Callable[[Request], None]
BuildTrainJobArgv = Callable[..., tuple[list[str], dict[str, Any]]]
HasActiveDdpTraining = Callable[[Request], bool]
InferVarietyFromSymbol = Callable[[str], str | None]
JobRedirectUrl = Callable[..., str]


def register_model_action_routes(
    *,
    router: APIRouter,
    require_auth: RequireAuth,
    build_train_job_argv: BuildTrainJobArgv,
    has_active_ddp_training: HasActiveDdpTraining,
    infer_variety_from_symbol: InferVarietyFromSymbol,
    job_redirect_url: JobRedirectUrl,
) -> None:
    def _start_job_and_redirect(
        *,
        request: Request,
        title: str,
        argv: list[str],
        variety_symbol: str | None = None,
    ) -> RedirectResponse:
        jm = request.app.state.job_manager
        rec = jm.start_job(JobSpec(title=title, argv=argv, cwd=Path.cwd()))
        return RedirectResponse(
            url=job_redirect_url(request, rec.id, variety=infer_variety_from_symbol(variety_symbol or "")),
            status_code=303,
        )

    def _split_symbols(raw_symbols: str) -> list[str]:
        return [s.strip() for s in str(raw_symbols or "").split(",") if s.strip()]

    @router.post("/models/model/train")
    async def ops_model_train(request: Request):
        require_auth(request)
        form = await request.form()
        model = str(form.get("model") or "").strip()
        symbol = str(form.get("symbol") or "").strip()
        data_dir = str(form.get("data_dir") or "data").strip()
        artifacts_dir = str(form.get("artifacts_dir") or "artifacts").strip()
        horizon = str(form.get("horizon") or "50").strip()
        gpus = _safe_int(form.get("gpus"), 1, min_value=1, max_value=8)
        epochs = str(form.get("epochs") or "50").strip()
        batch_size = str(form.get("batch_size") or "256").strip()
        seq_len = str(form.get("seq_len") or "100").strip()
        lr = str(form.get("lr") or "0.001").strip()
        ddp = str(form.get("ddp") or "true").strip().lower() == "true"
        num_workers = _safe_int(form.get("num_workers"), 0, min_value=0, max_value=512)
        prefetch_factor = _safe_int(form.get("prefetch_factor"), 2, min_value=1, max_value=64)
        pin_memory = str(form.get("pin_memory") or "auto").strip().lower() or "auto"
        if pin_memory not in {"auto", "true", "false"}:
            pin_memory = "auto"
        if not model or not symbol:
            raise HTTPException(status_code=400, detail="model/symbol required")
        argv, launch_meta = build_train_job_argv(
            model=model,
            symbol=symbol,
            data_dir=data_dir,
            artifacts_dir=artifacts_dir,
            horizon=horizon,
            epochs=epochs,
            batch_size=batch_size,
            seq_len=seq_len,
            lr=lr,
            gpus=gpus,
            ddp_requested=ddp,
            num_workers=num_workers,
            prefetch_factor=prefetch_factor,
            pin_memory=pin_memory,
        )
        title = (
            f"train {model} {symbol} "
            f"mode={launch_meta['mode']} "
            f"gpus={launch_meta['effective_gpus']}"
        )
        return _start_job_and_redirect(request=request, title=title, argv=argv, variety_symbol=symbol)

    @router.post("/models/model/sweep")
    async def ops_model_sweep(request: Request):
        require_auth(request)
        form = await request.form()
        symbol = str(form.get("symbol") or "").strip()
        model = str(form.get("model") or "deeplob").strip()
        data_dir = str(form.get("data_dir") or "data").strip()
        artifacts_dir = str(form.get("artifacts_dir") or "artifacts").strip()
        runs_dir_str = str(form.get("runs_dir") or "runs").strip()
        n_trials = str(form.get("n_trials") or "20").strip()
        n_cpus = str(form.get("n_cpus") or "8").strip()
        requested_n_gpus = _safe_int(form.get("n_gpus"), 1, min_value=0, max_value=8)
        if not symbol:
            raise HTTPException(status_code=400, detail="symbol required")
        n_gpus_eff = requested_n_gpus
        sweep_note = ""
        if requested_n_gpus > 0 and has_active_ddp_training(request):
            # DDP speedup-first governance: keep sweeps off GPU while multi-GPU train runs.
            n_gpus_eff = 0
            sweep_note = "cpu_only_ddp_train_active"
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
            str(n_gpus_eff),
        )
        title = f"sweep {model} {symbol} n_gpus={n_gpus_eff}"
        if sweep_note:
            title += f" {sweep_note}"
        return _start_job_and_redirect(request=request, title=title, argv=argv, variety_symbol=symbol)

    @router.post("/models/eval/benchmark")
    async def ops_eval_benchmark(request: Request):
        require_auth(request)
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
        return _start_job_and_redirect(request=request, title=title, argv=argv, variety_symbol=symbol)

    @router.post("/models/eval/compare")
    async def ops_eval_compare(request: Request):
        require_auth(request)
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
        return _start_job_and_redirect(request=request, title=title, argv=argv, variety_symbol=symbol)

    @router.post("/models/eval/backtest")
    async def ops_eval_backtest(request: Request):
        require_auth(request)
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
        return _start_job_and_redirect(request=request, title=title, argv=argv, variety_symbol=symbol)

    @router.post("/models/eval/paper")
    async def ops_eval_paper(request: Request):
        require_auth(request)
        form = await request.form()
        model = str(form.get("model") or "").strip()
        symbols = str(form.get("symbols") or "").strip()
        artifacts_dir = str(form.get("artifacts_dir") or "artifacts").strip()
        if not model or not symbols:
            raise HTTPException(status_code=400, detail="model/symbols required")
        argv = python_module_argv("ghtrader.cli", "paper", "--model", model)
        symbol_list = _split_symbols(symbols)
        for s in symbol_list:
            argv += ["--symbols", s]
        argv += ["--artifacts-dir", artifacts_dir]
        title = f"paper {model}"
        first_sym = symbol_list[0] if symbol_list else ""
        return _start_job_and_redirect(request=request, title=title, argv=argv, variety_symbol=first_sym)

    @router.post("/models/eval/daily_train")
    async def ops_eval_daily_train(request: Request):
        require_auth(request)
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
        symbol_list = _split_symbols(symbols)
        for s in symbol_list:
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
        first_sym = symbol_list[0] if symbol_list else ""
        return _start_job_and_redirect(request=request, title=title, argv=argv, variety_symbol=first_sym)
