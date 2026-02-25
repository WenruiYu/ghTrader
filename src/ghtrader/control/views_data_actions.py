from __future__ import annotations

from pathlib import Path
from typing import Any, Callable

from fastapi import APIRouter, HTTPException, Request
from fastapi.responses import PlainTextResponse, RedirectResponse

from ghtrader.config import get_runs_dir
from ghtrader.control.jobs import JobSpec, python_module_argv
from ghtrader.control.settings import set_tqsdk_scheduler_max_parallel

RequireAuth = Callable[[Request], None]
FormVariety = Callable[..., str]
VarPage = Callable[..., str]
TokenQueryString = Callable[[Request], str]
StartOrReuseJobByCommand = Callable[..., Any]
JobRedirectUrl = Callable[..., str]
DerivedSymbolForVariety = Callable[[str], str]
InferVarietyFromSymbol = Callable[[str], str | None]
ClearDataPageCache = Callable[[], None]


def register_data_action_routes(
    *,
    router: APIRouter,
    require_auth: RequireAuth,
    form_variety: FormVariety,
    var_page: VarPage,
    token_qs: TokenQueryString,
    start_or_reuse_job_by_command: StartOrReuseJobByCommand,
    job_redirect_url: JobRedirectUrl,
    derived_symbol_for_variety: DerivedSymbolForVariety,
    infer_variety_from_symbol: InferVarietyFromSymbol,
    clear_data_page_cache: ClearDataPageCache,
) -> None:
    @router.get("/data/integrity/report/{name}")
    def data_integrity_report(request: Request, name: str):
        require_auth(request)
        if "/" in name or "\\" in name or not name.endswith(".json"):
            raise HTTPException(status_code=400, detail="invalid report name")
        p = get_runs_dir() / "audit" / name
        if not p.exists():
            raise HTTPException(status_code=404, detail="report not found")
        return PlainTextResponse(p.read_text(), media_type="application/json")

    @router.get("/data/main-l5-validate/report/{name}")
    def data_main_l5_validate_report(request: Request, name: str):
        require_auth(request)
        if "/" in name or "\\" in name or not name.endswith(".json"):
            raise HTTPException(status_code=400, detail="invalid report name")
        p = get_runs_dir() / "control" / "reports" / "main_l5_validate" / name
        if not p.exists():
            raise HTTPException(status_code=404, detail="report not found")
        return PlainTextResponse(p.read_text(), media_type="application/json")

    @router.post("/data/settings/tqsdk_scheduler")
    async def data_settings_tqsdk_scheduler(request: Request):
        require_auth(request)
        form = await request.form()
        variety = form_variety(form.get("variety"), strict=False)
        max_parallel = form.get("max_parallel")
        persist_raw = str(form.get("persist") or "1").strip().lower()
        persist = persist_raw not in {"0", "false", "no", "off"}

        try:
            set_tqsdk_scheduler_max_parallel(runs_dir=get_runs_dir(), max_parallel=max_parallel, persist=bool(persist))
        except Exception as e:
            raise HTTPException(status_code=400, detail=str(e))

        return RedirectResponse(url=f"{var_page('data', variety=variety, token_qs=token_qs(request))}#ingest", status_code=303)

    @router.post("/data/build/main_schedule")
    async def data_build_main_schedule(request: Request):
        require_auth(request)
        form = await request.form()
        var = form_variety(form.get("variety"), strict=True)
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
        rec = start_or_reuse_job_by_command(
            request=request,
            title=title,
            argv=argv,
            cwd=Path.cwd(),
            metadata={
                "kind": "main_schedule",
                "exchange": "SHFE",
                "variety": var,
                "symbol": derived_symbol_for_variety(var),
            },
        )
        clear_data_page_cache()
        return RedirectResponse(url=job_redirect_url(request, rec.id, variety=var), status_code=303)

    @router.post("/data/build/main_l5")
    async def data_build_main_l5(request: Request):
        require_auth(request)
        form = await request.form()
        var = form_variety(form.get("variety"), strict=True)
        derived_symbol = str(form.get("derived_symbol") or derived_symbol_for_variety(var)).strip()
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
        rec = start_or_reuse_job_by_command(
            request=request,
            title=title,
            argv=argv,
            cwd=Path.cwd(),
            metadata={
                "kind": "main_l5",
                "exchange": "SHFE",
                "variety": var,
                "symbol": derived_symbol,
            },
        )
        clear_data_page_cache()
        return RedirectResponse(url=job_redirect_url(request, rec.id, variety=var), status_code=303)

    @router.post("/data/build/build")
    async def data_build_build(request: Request):
        require_auth(request)
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
        return RedirectResponse(
            url=job_redirect_url(request, rec.id, variety=infer_variety_from_symbol(symbol)),
            status_code=303,
        )
