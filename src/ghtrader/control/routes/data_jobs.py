from __future__ import annotations

from pathlib import Path
from typing import Any, Callable

from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import JSONResponse

from ghtrader.control.jobs import JobSpec, python_module_argv


def mount_data_job_routes(
    app: FastAPI,
    *,
    is_authorized: Callable[[Request], bool],
    normalize_variety_for_api: Callable[[str | None], str],
    derived_symbol_for_variety: Callable[..., str],
    get_runs_dir: Callable[[], Path],
    get_data_dir: Callable[[], Path],
    data_page_cache_clear: Callable[[], None],
    clear_data_quality_cache: Callable[[], None],
) -> None:
    @app.post("/api/data/enqueue-l5-start", response_class=JSONResponse)
    async def api_data_enqueue_l5_start(request: Request) -> dict[str, Any]:
        """
        Enqueue an L5-start computation job (runs `ghtrader data l5-start ...`).
        """
        if not is_authorized(request):
            raise HTTPException(status_code=401, detail="Unauthorized")
        payload = await request.json()

        runs_dir_default = get_runs_dir()
        data_dir_default = get_data_dir()
        runs_dir = Path(str(payload.get("runs_dir") or str(runs_dir_default)).strip() or str(runs_dir_default))
        data_dir = Path(str(payload.get("data_dir") or str(data_dir_default)).strip() or str(data_dir_default))

        ex = str(payload.get("exchange") or "SHFE").upper().strip()
        v = normalize_variety_for_api(payload.get("var"))
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
                        data_page_cache_clear()
                        clear_data_quality_cache()
                        return {"ok": True, "enqueued": [started.id], "count": 1, "deduped": True, "started": bool(started.pid)}
                    data_page_cache_clear()
                    clear_data_quality_cache()
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
                    "symbol": derived_symbol_for_variety(v, exchange=ex),
                    "symbols": symbols,
                },
            )
        )
        data_page_cache_clear()
        clear_data_quality_cache()
        return {"ok": True, "enqueued": [rec.id], "count": 1, "deduped": False}

    @app.post("/api/data/enqueue-main-l5-validate", response_class=JSONResponse)
    async def api_data_enqueue_main_l5_validate(request: Request) -> dict[str, Any]:
        """
        Enqueue a main_l5 validation job (runs `ghtrader data main-l5-validate ...`).
        """
        if not is_authorized(request):
            raise HTTPException(status_code=401, detail="Unauthorized")
        payload = await request.json()

        runs_dir = get_runs_dir()
        data_dir = get_data_dir()

        ex = str(payload.get("exchange") or "SHFE").upper().strip()
        v = normalize_variety_for_api(payload.get("var"))
        derived_symbol = str(payload.get("derived_symbol") or derived_symbol_for_variety(v, exchange=ex)).strip()
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
                        data_page_cache_clear()
                        clear_data_quality_cache()
                        return {
                            "ok": True,
                            "enqueued": [started.id],
                            "count": 1,
                            "deduped": True,
                            "started": bool(started.pid),
                        }
                    data_page_cache_clear()
                    clear_data_quality_cache()
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
        data_page_cache_clear()
        clear_data_quality_cache()
        return {
            "ok": True,
            "enqueued": [rec.id],
            "count": 1,
            "deduped": False,
            "policy_preview": policy_preview,
        }
