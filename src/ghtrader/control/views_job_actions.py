from __future__ import annotations

from pathlib import Path
from typing import Any, Callable

from fastapi import APIRouter, HTTPException, Request
from fastapi.responses import RedirectResponse

from ghtrader.config import get_runs_dir
from ghtrader.control.jobs import JobSpec, python_module_argv

RequireAuth = Callable[[Request], None]
FormVariety = Callable[..., str]
PageVariety = Callable[..., str]
InferVarietyFromSymbol = Callable[[str], str | None]
DerivedSymbolForVariety = Callable[[str], str]
StartOrReuseJobByCommand = Callable[..., Any]
JobRedirectUrl = Callable[..., str]
TokenQueryString = Callable[[Request], str]
RequestVarietyHint = Callable[[Request], str]


def _build_jobs_start_payload(
    *,
    form: Any,
    job_type: str,
    symbol_or_var: str,
    data_dir: Path,
    default_job_var: str,
    page_variety: PageVariety,
    infer_variety_from_symbol: InferVarietyFromSymbol,
    derived_symbol_for_variety: DerivedSymbolForVariety,
) -> tuple[str, list[str], str, dict[str, Any] | None, bool]:
    job_var = str(default_job_var or "").strip()
    job_metadata: dict[str, Any] | None = None
    reuse_existing = False

    if job_type == "build":
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
        job_var = infer_variety_from_symbol(symbol) or job_var
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
        job_var = infer_variety_from_symbol(symbol) or job_var
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
        job_var = infer_variety_from_symbol(symbol) or job_var
    elif job_type == "main_schedule":
        var_raw = symbol_or_var or str(form.get("variety") or "").strip()
        if not var_raw:
            raise HTTPException(status_code=400, detail="symbol_or_var must be a variety for main_schedule")
        var = page_variety(var_raw)
        argv = python_module_argv(
            "ghtrader.cli",
            "main-schedule",
            "--var",
            var,
            "--data-dir",
            str(data_dir),
        )
        title = f"main-schedule {var} env->latest"
        job_var = var
        job_metadata = {
            "kind": "main_schedule",
            "exchange": "SHFE",
            "variety": var,
            "symbol": derived_symbol_for_variety(var),
        }
        reuse_existing = True
    elif job_type == "main_l5":
        var_raw = symbol_or_var or str(form.get("variety") or "").strip()
        if not var_raw:
            raise HTTPException(status_code=400, detail="symbol_or_var must be a variety for main_l5")
        var = page_variety(var_raw)
        derived_symbol = str(form.get("derived_symbol") or "").strip() or derived_symbol_for_variety(var)
        update_mode = str(form.get("update_mode") or "0").strip().lower() in {"1", "true", "yes", "on"}
        argv = python_module_argv("ghtrader.cli", "main-l5", "--var", var, "--symbol", derived_symbol)
        if update_mode:
            argv += ["--update"]
        title = f"main-l5 {var} {derived_symbol}"
        job_var = var
        job_metadata = {
            "kind": "main_l5",
            "exchange": "SHFE",
            "variety": var,
            "symbol": derived_symbol,
        }
        reuse_existing = True
    else:
        raise HTTPException(status_code=400, detail=f"Unknown job_type: {job_type}")

    return title, argv, job_var, job_metadata, reuse_existing


def register_job_action_routes(
    *,
    router: APIRouter,
    require_auth: RequireAuth,
    form_variety: FormVariety,
    page_variety: PageVariety,
    infer_variety_from_symbol: InferVarietyFromSymbol,
    derived_symbol_for_variety: DerivedSymbolForVariety,
    start_or_reuse_job_by_command: StartOrReuseJobByCommand,
    job_redirect_url: JobRedirectUrl,
    token_qs: TokenQueryString,
    request_variety_hint: RequestVarietyHint,
) -> None:
    @router.post("/jobs/start")
    async def jobs_start(request: Request):
        require_auth(request)
        form = await request.form()

        job_type = str(form.get("job_type") or "").strip()
        symbol_or_var = str(form.get("symbol_or_var") or "").strip()
        data_dir = Path(str(form.get("data_dir") or "data"))
        job_var_default = form_variety(form.get("variety"), strict=False)

        title, argv, job_var, job_metadata, reuse_existing = _build_jobs_start_payload(
            form=form,
            job_type=job_type,
            symbol_or_var=symbol_or_var,
            data_dir=data_dir,
            default_job_var=job_var_default,
            page_variety=page_variety,
            infer_variety_from_symbol=infer_variety_from_symbol,
            derived_symbol_for_variety=derived_symbol_for_variety,
        )

        if reuse_existing:
            rec = start_or_reuse_job_by_command(
                request=request,
                title=title,
                argv=argv,
                cwd=Path.cwd(),
                metadata=job_metadata,
            )
        else:
            jm = request.app.state.job_manager
            rec = jm.start_job(JobSpec(title=title, argv=argv, cwd=Path.cwd(), metadata=job_metadata))
        return RedirectResponse(url=job_redirect_url(request, rec.id, variety=job_var), status_code=303)

    @router.post("/jobs/{job_id}/cancel")
    def job_cancel(request: Request, job_id: str):
        require_auth(request)
        jm = request.app.state.job_manager
        jm.cancel_job(job_id)
        nav_var = request_variety_hint(request)
        token = token_qs(request)
        extra_var = f"&var={nav_var}" if token else f"?var={nav_var}"
        return RedirectResponse(url=f"/jobs/{job_id}{token}{extra_var}", status_code=303)
