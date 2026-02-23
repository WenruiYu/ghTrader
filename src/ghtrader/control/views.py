from __future__ import annotations

import json
import os
import re
import shutil
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from pathlib import Path
from typing import Any

import structlog

from fastapi import APIRouter, HTTPException, Request
from fastapi.responses import PlainTextResponse, RedirectResponse
from fastapi.templating import Jinja2Templates

from ghtrader.config_service import get_config_resolver
from ghtrader.config import get_artifacts_dir, get_data_dir, get_env, get_runs_dir
from ghtrader.control import auth
from ghtrader.control.job_metadata import infer_job_metadata, merge_job_metadata
from ghtrader.control.jobs import JobSpec, python_module_argv
from ghtrader.control.settings import get_tqsdk_scheduler_state, set_tqsdk_scheduler_max_parallel
from ghtrader.control.system_info import cpu_mem_info, disk_usage, gpu_info
from ghtrader.control.cache import TTLCacheMap
from ghtrader.control.variety_context import (
    allowed_varieties as _allowed_varieties,
    default_variety as _default_variety,
    derived_symbol_for_variety as _derived_symbol_for_variety,
    infer_variety_from_symbol as _infer_variety_from_symbol_ctx,
    require_supported_variety as _require_supported_variety,
    symbol_matches_variety as _symbol_matches_variety,
)
from ghtrader.control.views_config import register_config_routes
from ghtrader.control.views_helpers import safe_int as _safe_int
from ghtrader.control.views_system import register_system_routes

log = structlog.get_logger()

_DATA_PAGE_CACHE_TTL_S = 5.0
_DATA_PAGE_CACHE = TTLCacheMap()


def _data_page_cache_get(key: str, *, ttl_s: float | None = None) -> Any | None:
    ttl = float(_DATA_PAGE_CACHE_TTL_S if ttl_s is None else ttl_s)
    return _DATA_PAGE_CACHE.get(str(key), ttl_s=ttl, now=time.time())


def _data_page_cache_set(key: str, payload: Any) -> None:
    _DATA_PAGE_CACHE.set(str(key), payload, now=time.time())


def _data_page_cache_clear() -> None:
    _DATA_PAGE_CACHE.clear()


def build_router() -> Any:
    router = APIRouter()

    templates = Jinja2Templates(directory=str(Path(__file__).parent / "templates"))

    def _require_auth(request: Request) -> None:
        if not auth.is_authorized(request):
            raise HTTPException(status_code=401, detail="Unauthorized")

    def _token_qs(request: Request) -> str:
        return auth.token_query_string(request)

    def _page_variety(variety: str | None) -> str:
        try:
            return _require_supported_variety(variety)
        except Exception as e:
            raise HTTPException(status_code=404, detail=str(e))

    def _variety_nav_ctx(
        variety: str,
        *,
        section: str = "",
        page_title: str = "",
        breadcrumbs: list[dict[str, str]] | None = None,
    ) -> dict[str, Any]:
        v = _page_variety(variety)
        section_key = str(section or "").strip().lower()
        section_title_map = {
            "dashboard": "Dashboard",
            "jobs": "Jobs",
            "data": "Data Hub",
            "models": "Models",
            "trading": "Trading",
            "config": "Configuration",
            "sql": "SQL Explorer",
            "system": "System",
        }
        current_title = str(page_title or section_title_map.get(section_key) or "Workspace").strip()
        crumb_list = breadcrumbs or [
            {"label": "Workspace", "href": f"/v/{v}/dashboard"},
            {"label": current_title, "href": ""},
        ]
        return {
            "variety": v,
            "allowed_varieties": list(_allowed_varieties()),
            "variety_path_prefix": f"/v/{v}",
            "page_context": {
                "section": section_key,
                "page_title": current_title,
                "workspace_label": f"{v.upper()} workspace",
                "breadcrumbs": crumb_list,
            },
        }

    def _var_page(path_suffix: str, *, variety: str | None = None, token_qs: str = "") -> str:
        v = str(variety or _default_variety()).strip().lower() or _default_variety()
        suffix = str(path_suffix or "").strip().lstrip("/")
        return f"/v/{v}/{suffix}{token_qs}"

    def _form_variety(raw: Any, *, strict: bool = False) -> str:
        v = str(raw or "").strip().lower()
        if not v:
            if strict:
                raise HTTPException(status_code=400, detail="variety is required")
            return _default_variety()
        if strict:
            try:
                return _require_supported_variety(v)
            except Exception as e:
                raise HTTPException(status_code=400, detail=str(e))
        return v if v in _allowed_varieties() else _default_variety()

    def _request_variety_hint(request: Request) -> str:
        raw = str(request.query_params.get("var") or "").strip().lower()
        return raw if raw in _allowed_varieties() else _default_variety()

    def _infer_variety_from_symbol(symbol: str) -> str | None:
        return _infer_variety_from_symbol_ctx(symbol)

    def _job_redirect_url(request: Request, job_id: str, *, variety: str | None = None) -> str:
        token_qs = _token_qs(request)
        if variety:
            sep = "&" if token_qs else "?"
            return f"/jobs/{job_id}{token_qs}{sep}var={variety}"
        return f"/jobs/{job_id}{token_qs}"

    def _cuda_device_count() -> int:
        try:
            import torch

            return max(0, int(torch.cuda.device_count()))
        except Exception:
            return 0

    def _resolve_torchrun_path() -> str | None:
        cand = str(get_env("GHTRADER_TORCHRUN_BIN", "torchrun") or "").strip() or "torchrun"
        return shutil.which(cand)

    def _has_active_ddp_training(request: Request) -> bool:
        store = request.app.state.job_store
        active = []
        try:
            active = store.list_active_jobs() + store.list_unstarted_queued_jobs(limit=2000)
        except Exception:
            return False
        for j in active:
            cmd = [str(x) for x in (j.command or [])]
            title = str(j.title or "").lower()
            if not cmd and not title:
                continue
            # torchrun --nproc_per_node>1 ... ghtrader.cli train
            if cmd and ("torchrun" in Path(cmd[0]).name.lower() or any("torchrun" in p.lower() for p in cmd)):
                if "ghtrader.cli" in cmd and "train" in cmd:
                    for p in cmd:
                        if p.startswith("--nproc_per_node="):
                            n = _safe_int(p.split("=", 1)[1], 1, min_value=1)
                            if n > 1:
                                return True
            # fallback parser for training jobs without explicit torchrun marker in title.
            if title.startswith("train ") and "mode=torchrun-ddp-" in title:
                return True
        return False

    def _job_summary(*, request: Request, limit: int) -> dict[str, Any]:
        store = request.app.state.job_store
        jobs = store.list_jobs(limit=int(limit))
        running = [j for j in jobs if j.status == "running"]
        queued = [j for j in jobs if j.status == "queued"]
        return {
            "jobs": jobs,
            "running": running,
            "queued": queued,
            "running_count": len(running),
            "queued_count": len(queued),
        }

    def _find_existing_job_by_command(*, request: Request, argv: list[str]) -> Any | None:
        """
        Best-effort dedupe for long-running data build jobs.
        """
        store = request.app.state.job_store
        target = [str(x) for x in list(argv)]
        try:
            active = store.list_active_jobs() + store.list_unstarted_queued_jobs(limit=2000)
        except Exception:
            return None
        for job in active:
            cmd = [str(x) for x in (job.command or [])]
            if cmd == target:
                return job
        return None

    def _start_or_reuse_job_by_command(
        *, request: Request, title: str, argv: list[str], cwd: Path, metadata: dict[str, Any] | None = None
    ) -> Any:
        jm = request.app.state.job_manager
        auto_metadata = infer_job_metadata(argv=argv, title=title)
        merged_metadata = merge_job_metadata(auto_metadata, metadata)
        existing = _find_existing_job_by_command(request=request, argv=argv)
        if existing is not None:
            if merged_metadata:
                try:
                    store = request.app.state.job_store
                    current_meta = dict(existing.metadata) if isinstance(getattr(existing, "metadata", None), dict) else None
                    next_meta = merge_job_metadata(current_meta, merged_metadata)
                    if next_meta and next_meta != current_meta:
                        store.update_job(existing.id, metadata=next_meta)
                except Exception:
                    pass
            if existing.pid is None and str(existing.status or "").strip().lower() == "queued":
                try:
                    started = jm.start_queued_job(existing.id)
                    if started is not None:
                        return started
                except Exception:
                    pass
            return existing
        return jm.start_job(JobSpec(title=title, argv=argv, cwd=cwd, metadata=merged_metadata))

    def _job_matches_variety(job: Any, variety: str) -> bool:
        v = _page_variety(variety)
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
        cmd = [str(x) for x in (job.command or [])]
        cmd_lc = [x.lower() for x in cmd]
        for i, token in enumerate(cmd_lc[:-1]):
            if token == "--var" and cmd_lc[i + 1] == v:
                return True
        for token in cmd_lc:
            if _symbol_matches_variety(token, v):
                return True
        title = str(job.title or "").lower()
        if _symbol_matches_variety(title, v):
            return True
        if re.search(rf"(^|\s){re.escape(v)}(\s|$)", title):
            return True
        return False

    def _l5_calendar_context(*, data_dir: Path, variety: str) -> dict[str, str]:
        l5_start_date = ""
        l5_start_error = ""
        l5_start_env_key = ""
        l5_start_env_expected_key = ""
        l5_start_note = ""
        latest_trading_day = ""
        latest_trading_error = ""
        try:
            from ghtrader.config import get_l5_start_date_with_source, l5_start_env_key as _l5_start_env_key

            l5_start_env_expected_key = _l5_start_env_key(variety=variety)
            d, used_key = get_l5_start_date_with_source(variety=variety)
            l5_start_date = d.isoformat()
            l5_start_env_key = str(used_key or l5_start_env_expected_key)
            if l5_start_env_expected_key and l5_start_env_key and l5_start_env_key != l5_start_env_expected_key:
                l5_start_note = f"{l5_start_env_expected_key} not set; using {l5_start_env_key}"
        except Exception as e:
            l5_start_error = str(e)
        try:
            from ghtrader.data.trading_calendar import latest_trading_day as _latest_trading_day

            latest_trading_day = _latest_trading_day(data_dir=data_dir, refresh=False, allow_download=True).isoformat()
        except Exception as e:
            latest_trading_error = str(e)
        return {
            "l5_start_date": l5_start_date,
            "l5_start_error": l5_start_error,
            "l5_start_env_key": l5_start_env_key,
            "l5_start_env_expected_key": l5_start_env_expected_key,
            "l5_start_note": l5_start_note,
            "latest_trading_day": latest_trading_day,
            "latest_trading_error": latest_trading_error,
        }

    def _pipeline_guardrails_context() -> dict[str, Any]:
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
        total_workers = _safe_int(total_workers_raw, 0, min_value=0) if total_workers_raw else 0
        segment_workers = _safe_int(segment_workers_raw, 0, min_value=0) if segment_workers_raw else 0

        health_gate_strict = bool(enforce_health_schedule and enforce_health_main_l5)
        worker_mode = "bounded" if (total_workers > 0 or segment_workers > 0) else "auto"
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
            "main_l5_worker_mode": worker_mode,
        }

    def _validation_profile_suggestion(variety: str) -> dict[str, Any]:
        v = str(variety or "").strip().lower()
        profiles: dict[str, dict[str, Any]] = {
            "cu": {
                "label": "CU sparse-source profile",
                "note": "CU is naturally sparse; use a looser gap threshold for manual validate runs.",
                "gap_threshold_s": 8.0,
                "strict_ratio": 0.65,
                "max_segments_per_day": 300,
            },
            "ag": {
                "label": "AG balanced profile",
                "note": "AG keeps stricter thresholds after night-session attribution fixes.",
                "gap_threshold_s": 5.0,
                "strict_ratio": 0.8,
                "max_segments_per_day": 200,
            },
            "au": {
                "label": "AU conservative profile",
                "note": "AU may hit isolated provider-anomaly days; review missing-day list before blocking.",
                "gap_threshold_s": 5.0,
                "strict_ratio": 0.75,
                "max_segments_per_day": 200,
            },
        }
        return dict(profiles.get(v) or {})

    def _pipeline_health_context(
        *,
        main_schedule_coverage: dict[str, Any],
        main_l5_coverage: dict[str, Any],
        main_l5_validation: dict[str, Any],
        coverage_error: str,
        validation_error: str,
    ) -> dict[str, dict[str, Any]]:
        schedule_days = _safe_int(main_schedule_coverage.get("n_days"), 0, min_value=0)
        hashes_raw = main_schedule_coverage.get("schedule_hashes") or []
        hashes: list[str] = [str(h) for h in hashes_raw if str(h).strip()]
        hash_count = len(hashes)

        if coverage_error:
            schedule_state = "error"
            schedule_text = "coverage query failed"
        elif schedule_days <= 0:
            schedule_state = "error"
            schedule_text = "missing"
        elif hash_count == 1:
            schedule_state = "ok"
            schedule_text = f"{schedule_days}d · hash ok"
        else:
            schedule_state = "warn"
            schedule_text = f"{schedule_days}d · {hash_count} hashes"

        main_days = _safe_int(main_l5_coverage.get("n_days"), 0, min_value=0)
        if coverage_error:
            main_l5_state = "error"
            main_l5_text = "coverage query failed"
        elif schedule_days <= 0 and main_days <= 0:
            main_l5_state = "warn"
            main_l5_text = "wait for schedule"
        elif main_days <= 0:
            main_l5_state = "error"
            main_l5_text = "empty"
        elif schedule_days > 0:
            miss = max(0, int(schedule_days - main_days))
            if miss > 0:
                main_l5_state = "warn"
                main_l5_text = f"{main_days}/{schedule_days}d"
            else:
                main_l5_state = "ok"
                main_l5_text = f"{main_days}/{schedule_days}d"
        else:
            main_l5_state = "ok"
            main_l5_text = f"{main_days}d"

        val_status = str(main_l5_validation.get("status") or "").strip().lower()
        val_state = str(main_l5_validation.get("state") or "").strip().lower()
        val_overall = str(main_l5_validation.get("overall_state") or "").strip().lower()
        val_status_label = str(main_l5_validation.get("status_label") or "").strip().lower()
        status_token = val_overall or val_status or val_state or val_status_label
        checked_days = _safe_int(main_l5_validation.get("checked_days"), 0, min_value=0)
        missing_days = _safe_int(main_l5_validation.get("missing_days"), 0, min_value=0)
        missing_segments = _safe_int(main_l5_validation.get("missing_segments_total"), 0, min_value=0)
        if validation_error:
            validation_state = "error"
            validation_text = "summary unavailable"
        elif status_token == "error":
            validation_state = "error"
            validation_text = (
                f"{missing_days}/{checked_days}d miss · {missing_segments} seg"
                if checked_days > 0
                else "error"
            )
        elif status_token == "noop":
            validation_state = "ok"
            validation_text = "up-to-date (noop)"
        elif checked_days <= 0:
            validation_state = "warn"
            validation_text = "not run"
        elif status_token == "ok":
            validation_state = "ok"
            validation_text = f"{checked_days}d clean"
        elif status_token == "warn":
            validation_state = "warn"
            validation_text = (
                f"{missing_days}/{checked_days}d miss · {missing_segments} seg"
                if checked_days > 0
                else "warn"
            )
        else:
            validation_state = "warn"
            validation_text = (
                f"{missing_days}/{checked_days}d miss · {missing_segments} seg"
                if checked_days > 0
                else "warn"
            )

        return {
            "schedule": {"state": schedule_state, "text": schedule_text},
            "main_l5": {"state": main_l5_state, "text": main_l5_text},
            "validation": {"state": validation_state, "text": validation_text},
        }

    def _build_train_job_argv(
        *,
        model: str,
        symbol: str,
        data_dir: str,
        artifacts_dir: str,
        horizon: str,
        epochs: str,
        batch_size: str,
        seq_len: str,
        lr: str,
        gpus: int,
        ddp_requested: bool,
        num_workers: int,
        prefetch_factor: int,
        pin_memory: str,
    ) -> tuple[list[str], dict[str, Any]]:
        deep_models = {"deeplob", "transformer", "tcn", "tlob", "ssm"}
        model_norm = str(model).strip().lower()
        requested_gpus = _safe_int(gpus, 1, min_value=1, max_value=8)
        cuda_count = _cuda_device_count()

        effective_gpus = requested_gpus
        use_ddp = bool(ddp_requested) and model_norm in deep_models and requested_gpus > 1
        launch_mode = "single-process"
        notes: list[str] = []

        if use_ddp and cuda_count > 0 and effective_gpus > cuda_count:
            effective_gpus = max(1, min(8, int(cuda_count)))
            notes.append(f"gpu_clamped_to_{effective_gpus}")
        if use_ddp and effective_gpus <= 1:
            use_ddp = False
            notes.append("ddp_disabled_insufficient_gpu")

        torchrun_path = _resolve_torchrun_path() if use_ddp else None
        cli_gpus = effective_gpus
        if use_ddp and not torchrun_path:
            # Health check failed: keep training alive via single-process fallback.
            use_ddp = False
            cli_gpus = 1
            launch_mode = "fallback-single-no-torchrun"
            notes.append("ddp_disabled_no_torchrun")
        elif use_ddp and torchrun_path:
            launch_mode = f"torchrun-ddp-{effective_gpus}"
            notes.append("ddp_enabled")
        else:
            if model_norm not in deep_models and requested_gpus > 1:
                cli_gpus = 1
                notes.append("non_deep_model_forces_single_gpu")
            launch_mode = "single-process"

        cli_args = [
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
            str(cli_gpus),
            "--epochs",
            epochs,
            "--batch-size",
            batch_size,
            "--seq-len",
            seq_len,
            "--lr",
            lr,
            "--num-workers",
            str(max(0, int(num_workers))),
            "--prefetch-factor",
            str(max(1, int(prefetch_factor))),
            "--pin-memory",
            (str(pin_memory).strip().lower() if str(pin_memory).strip() else "auto"),
            "--ddp" if use_ddp else "--no-ddp",
        ]

        if use_ddp and torchrun_path:
            argv = [
                str(torchrun_path),
                "--standalone",
                "--nnodes=1",
                f"--nproc_per_node={effective_gpus}",
                "-m",
                "ghtrader.cli",
                *cli_args,
            ]
        else:
            argv = python_module_argv("ghtrader.cli", *cli_args)

        return (
            argv,
            {
                "mode": launch_mode,
                "requested_gpus": requested_gpus,
                "effective_gpus": int(effective_gpus if use_ddp else cli_gpus),
                "cuda_count": int(cuda_count),
                "notes": notes,
            },
        )

    @router.get("/")
    def index_redirect(request: Request):
        _require_auth(request)
        return RedirectResponse(url=_var_page("dashboard", token_qs=_token_qs(request)), status_code=303)

    @router.get("/v/{variety}/dashboard")
    def index(request: Request, variety: str):
        _require_auth(request)
        v = _page_variety(variety)
        summary = _job_summary(request=request, limit=50)
        jobs_filtered = [j for j in list(summary["jobs"]) if _job_matches_variety(j, v)]
        running_count = len([j for j in jobs_filtered if str(j.status) == "running"])
        queued_count = len([j for j in jobs_filtered if str(j.status) == "queued"])
        data_dir = get_data_dir()
        calendar_ctx = _l5_calendar_context(data_dir=data_dir, variety=v)
        guardrails = _pipeline_guardrails_context()

        return templates.TemplateResponse(
            request,
            "index.html",
            {
                "request": request,
                "title": "ghTrader Dashboard",
                "token_qs": _token_qs(request),
                "jobs": jobs_filtered,
                "running_count": running_count,
                "queued_count": queued_count,
                "recent_count": len(jobs_filtered),
                "guardrails": guardrails,
                **calendar_ctx,
                **_variety_nav_ctx(v, section="dashboard", page_title="Dashboard"),
            },
        )

    @router.get("/jobs")
    def jobs_redirect(request: Request):
        _require_auth(request)
        return RedirectResponse(url=_var_page("jobs", token_qs=_token_qs(request)), status_code=303)

    @router.get("/v/{variety}/jobs")
    def jobs(request: Request, variety: str):
        _require_auth(request)
        v = _page_variety(variety)
        summary = _job_summary(request=request, limit=200)
        jobs_scope = str(request.query_params.get("scope") or "var").strip().lower()
        page = _safe_int(request.query_params.get("page"), 1, min_value=1)
        per_page = _safe_int(request.query_params.get("per_page"), 50, min_value=20, max_value=200)
        show_all = jobs_scope == "all"
        jobs_all = list(summary["jobs"])
        jobs_filtered_all = jobs_all if show_all else [j for j in jobs_all if _job_matches_variety(j, v)]
        running_count = len([j for j in jobs_filtered_all if str(j.status) == "running"])
        status_counts = {
            "running": len([j for j in jobs_filtered_all if str(j.status) == "running"]),
            "queued": len([j for j in jobs_filtered_all if str(j.status) == "queued"]),
            "succeeded": len([j for j in jobs_filtered_all if str(j.status) == "succeeded"]),
            "failed": len([j for j in jobs_filtered_all if str(j.status) == "failed"]),
            "cancelled": len([j for j in jobs_filtered_all if str(j.status) == "cancelled"]),
        }
        total_jobs = len(jobs_filtered_all)
        total_pages = max(1, (total_jobs + per_page - 1) // per_page)
        if page > total_pages:
            page = total_pages
        offset = (page - 1) * per_page
        jobs_filtered = jobs_filtered_all[offset : offset + per_page]
        data_dir = get_data_dir()
        calendar_ctx = _l5_calendar_context(data_dir=data_dir, variety=v)
        guardrails = _pipeline_guardrails_context()
        return templates.TemplateResponse(
            request,
            "jobs.html",
            {
                "request": request,
                "title": "Jobs",
                "token_qs": _token_qs(request),
                "jobs": jobs_filtered,
                "running_count": running_count,
                "jobs_status_counts": status_counts,
                "jobs_scope": "all" if show_all else "var",
                "jobs_pagination": {
                    "page": page,
                    "per_page": per_page,
                    "total": total_jobs,
                    "total_pages": total_pages,
                    "has_prev": page > 1,
                    "has_next": page < total_pages,
                    "prev_page": max(1, page - 1),
                    "next_page": min(total_pages, page + 1),
                },
                "guardrails": guardrails,
                **calendar_ctx,
                **_variety_nav_ctx(v, section="jobs", page_title="Jobs"),
            },
        )

    # ---------------------------------------------------------------------
    # Models page
    # ---------------------------------------------------------------------

    @router.get("/models")
    def models_redirect(request: Request):
        _require_auth(request)
        return RedirectResponse(url=_var_page("models", token_qs=_token_qs(request)), status_code=303)

    @router.get("/v/{variety}/models")
    def models_page(request: Request, variety: str):
        _require_auth(request)
        v = _page_variety(variety)
        store = request.app.state.job_store
        jobs = store.list_jobs(limit=50)
        running = [j for j in jobs if j.status == "running" and _job_matches_variety(j, v)]
        return templates.TemplateResponse(
            request,
            "models.html",
            {
                "request": request,
                "title": "Models",
                "token_qs": _token_qs(request),
                "running_count": len(running),
                **_variety_nav_ctx(v, section="models", page_title="Models"),
            },
        )

    # ---------------------------------------------------------------------
    # Trading page
    # ---------------------------------------------------------------------

    @router.get("/trading")
    def trading_redirect(request: Request):
        _require_auth(request)
        return RedirectResponse(url=_var_page("trading", token_qs=_token_qs(request)), status_code=303)

    @router.get("/v/{variety}/trading")
    def trading_page(request: Request, variety: str):
        _require_auth(request)
        v = _page_variety(variety)
        store = request.app.state.job_store
        jobs = store.list_jobs(limit=50)
        running = [j for j in jobs if j.status == "running" and _job_matches_variety(j, v)]

        return templates.TemplateResponse(
            request,
            "trading.html",
            {
                "request": request,
                "title": "Trading",
                "token_qs": _token_qs(request),
                "running_count": len(running),
                **_variety_nav_ctx(v, section="trading", page_title="Trading"),
            },
        )

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

    @router.post("/data/settings/tqsdk_scheduler")
    async def data_settings_tqsdk_scheduler(request: Request):
        _require_auth(request)
        form = await request.form()
        variety = _form_variety(form.get("variety"), strict=False)
        max_parallel = form.get("max_parallel")
        persist_raw = str(form.get("persist") or "1").strip().lower()
        persist = persist_raw not in {"0", "false", "no", "off"}

        try:
            set_tqsdk_scheduler_max_parallel(runs_dir=get_runs_dir(), max_parallel=max_parallel, persist=bool(persist))
        except Exception as e:
            raise HTTPException(status_code=400, detail=str(e))

        return RedirectResponse(url=f"{_var_page('data', variety=variety, token_qs=_token_qs(request))}#ingest", status_code=303)

    @router.post("/data/build/main_schedule")
    async def data_build_main_schedule(request: Request):
        _require_auth(request)
        form = await request.form()
        var = _form_variety(form.get("variety"), strict=True)
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
        rec = _start_or_reuse_job_by_command(
            request=request,
            title=title,
            argv=argv,
            cwd=Path.cwd(),
            metadata={
                "kind": "main_schedule",
                "exchange": "SHFE",
                "variety": var,
                "symbol": _derived_symbol_for_variety(var),
            },
        )
        _data_page_cache_clear()
        return RedirectResponse(url=_job_redirect_url(request, rec.id, variety=var), status_code=303)

    @router.post("/data/build/main_l5")
    async def data_build_main_l5(request: Request):
        _require_auth(request)
        form = await request.form()
        var = _form_variety(form.get("variety"), strict=True)
        derived_symbol = str(form.get("derived_symbol") or _derived_symbol_for_variety(var)).strip()
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
        rec = _start_or_reuse_job_by_command(
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
        _data_page_cache_clear()
        return RedirectResponse(url=_job_redirect_url(request, rec.id, variety=var), status_code=303)

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
        return RedirectResponse(url=_job_redirect_url(request, rec.id, variety=_infer_variety_from_symbol(symbol)), status_code=303)

    # Models/Eval form actions
    @router.post("/models/model/train")
    async def ops_model_train(request: Request):
        _require_auth(request)
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
        argv, launch_meta = _build_train_job_argv(
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
        jm = request.app.state.job_manager
        rec = jm.start_job(JobSpec(title=title, argv=argv, cwd=Path.cwd()))
        return RedirectResponse(url=_job_redirect_url(request, rec.id, variety=_infer_variety_from_symbol(symbol)), status_code=303)

    @router.post("/models/model/sweep")
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
        requested_n_gpus = _safe_int(form.get("n_gpus"), 1, min_value=0, max_value=8)
        if not symbol:
            raise HTTPException(status_code=400, detail="symbol required")
        n_gpus_eff = requested_n_gpus
        sweep_note = ""
        if requested_n_gpus > 0 and _has_active_ddp_training(request):
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
        jm = request.app.state.job_manager
        rec = jm.start_job(JobSpec(title=title, argv=argv, cwd=Path.cwd()))
        return RedirectResponse(url=_job_redirect_url(request, rec.id, variety=_infer_variety_from_symbol(symbol)), status_code=303)

    @router.post("/models/eval/benchmark")
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
        return RedirectResponse(url=_job_redirect_url(request, rec.id, variety=_infer_variety_from_symbol(symbol)), status_code=303)

    @router.post("/models/eval/compare")
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
        return RedirectResponse(url=_job_redirect_url(request, rec.id, variety=_infer_variety_from_symbol(symbol)), status_code=303)

    @router.post("/models/eval/backtest")
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
        return RedirectResponse(url=_job_redirect_url(request, rec.id, variety=_infer_variety_from_symbol(symbol)), status_code=303)

    @router.post("/models/eval/paper")
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
        first_sym = next((s.strip() for s in symbols.split(",") if s.strip()), "")
        return RedirectResponse(
            url=_job_redirect_url(request, rec.id, variety=_infer_variety_from_symbol(first_sym)),
            status_code=303,
        )

    @router.post("/models/eval/daily_train")
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
        first_sym = next((s.strip() for s in symbols.split(",") if s.strip()), "")
        return RedirectResponse(
            url=_job_redirect_url(request, rec.id, variety=_infer_variety_from_symbol(first_sym)),
            status_code=303,
        )

    @router.post("/jobs/start")
    async def jobs_start(request: Request):
        _require_auth(request)
        form = await request.form()

        job_type = str(form.get("job_type") or "").strip()
        symbol_or_var = str(form.get("symbol_or_var") or "").strip()
        data_dir = Path(str(form.get("data_dir") or "data"))
        job_var = _form_variety(form.get("variety"), strict=False)
        job_metadata: dict[str, Any] | None = None

        # Build argv for a known-safe set of job types (no shell).
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
            job_var = _infer_variety_from_symbol(symbol) or job_var
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
            job_var = _infer_variety_from_symbol(symbol) or job_var
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
            job_var = _infer_variety_from_symbol(symbol) or job_var
        elif job_type == "main_schedule":
            var_raw = symbol_or_var or str(form.get("variety") or "").strip()
            if not var_raw:
                raise HTTPException(status_code=400, detail="symbol_or_var must be a variety for main_schedule")
            var = _page_variety(var_raw)
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
                "symbol": _derived_symbol_for_variety(var),
            }
        elif job_type == "main_l5":
            var_raw = symbol_or_var or str(form.get("variety") or "").strip()
            if not var_raw:
                raise HTTPException(status_code=400, detail="symbol_or_var must be a variety for main_l5")
            var = _page_variety(var_raw)
            derived_symbol = str(form.get("derived_symbol") or "").strip() or _derived_symbol_for_variety(var)
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
        else:
            raise HTTPException(status_code=400, detail=f"Unknown job_type: {job_type}")

        if job_type in {"main_schedule", "main_l5"}:
            rec = _start_or_reuse_job_by_command(
                request=request, title=title, argv=argv, cwd=Path.cwd(), metadata=job_metadata
            )
        else:
            jm = request.app.state.job_manager
            rec = jm.start_job(JobSpec(title=title, argv=argv, cwd=Path.cwd(), metadata=job_metadata))
        return RedirectResponse(url=_job_redirect_url(request, rec.id, variety=job_var), status_code=303)

    @router.get("/jobs/{job_id}")
    def job_detail(request: Request, job_id: str):
        _require_auth(request)
        nav_var = _request_variety_hint(request)
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
                **_variety_nav_ctx(
                    nav_var,
                    section="jobs",
                    page_title=f"Job {job_id}",
                    breadcrumbs=[
                        {"label": "Workspace", "href": f"/v/{nav_var}/dashboard"},
                        {"label": "Jobs", "href": f"/v/{nav_var}/jobs{_token_qs(request)}"},
                        {"label": f"Job {job_id}", "href": ""},
                    ],
                ),
            },
        )

    @router.post("/jobs/{job_id}/cancel")
    def job_cancel(request: Request, job_id: str):
        _require_auth(request)
        jm = request.app.state.job_manager
        jm.cancel_job(job_id)
        nav_var = _request_variety_hint(request)
        extra_var = f"&var={nav_var}" if _token_qs(request) else f"?var={nav_var}"
        return RedirectResponse(url=f"/jobs/{job_id}{_token_qs(request)}{extra_var}", status_code=303)

    @router.get("/data")
    def data_redirect(request: Request):
        _require_auth(request)
        return RedirectResponse(url=_var_page("data", token_qs=_token_qs(request)), status_code=303)

    @router.get("/v/{variety}/data")
    def data_page(request: Request, variety: str):
        _require_auth(request)
        v = _page_variety(variety)
        summary = _job_summary(request=request, limit=200)
        jobs_filtered = [j for j in list(summary["jobs"]) if _job_matches_variety(j, v)]
        running_count = len([j for j in jobs_filtered if str(j.status) == "running"])
        queued_count = len([j for j in jobs_filtered if str(j.status) == "queued"])
        runs_dir = get_runs_dir()

        data_dir = get_data_dir()
        calendar_ctx = _l5_calendar_context(data_dir=data_dir, variety=v)
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

        # Coverage watermarks (best-effort, variety-scoped)
        main_l5_coverage: dict[str, Any] = {}
        main_schedule_coverage: dict[str, Any] = {}
        coverage_error = ""
        main_l5_validation: dict[str, Any] = {}
        validation_error = ""
        coverage_var = v
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
                from ghtrader.questdb.main_schedule import fetch_main_schedule_state
                from ghtrader.data.main_l5_validation import read_latest_validation_report

                schedule_hash_filter = None
                try:
                    sched = fetch_main_schedule_state(cfg=cfg, exchange="SHFE", variety=coverage_var)
                    hashes_raw = sched.get("schedule_hashes") or set()
                    hashes = sorted([str(h) for h in hashes_raw if str(h).strip()])
                    if len(hashes) == 1:
                        schedule_hash_filter = hashes[0]
                except Exception:
                    schedule_hash_filter = None

                overview = fetch_main_l5_validate_overview(
                    cfg=cfg,
                    symbol=coverage_symbol,
                    schedule_hash=schedule_hash_filter,
                )
                latest_rows = fetch_latest_main_l5_validate_summary(
                    cfg=cfg,
                    symbol=coverage_symbol,
                    schedule_hash=schedule_hash_filter,
                    limit=1,
                )
                latest = latest_rows[0] if latest_rows else {}
                top_gap_days = fetch_main_l5_validate_top_gap_days(
                    cfg=cfg,
                    symbol=coverage_symbol,
                    schedule_hash=schedule_hash_filter,
                    limit=8,
                )
                top_lag_days = fetch_main_l5_validate_top_lag_days(
                    cfg=cfg,
                    symbol=coverage_symbol,
                    schedule_hash=schedule_hash_filter,
                    limit=8,
                )
                if overview.get("days_total"):
                    base_status = (
                        "ok"
                        if (
                            overview.get("missing_days", 0) == 0
                            and overview.get("missing_segments", 0) == 0
                            and overview.get("missing_half_seconds", 0) == 0
                        )
                        else "warn"
                    )
                    out["main_l5_validation"] = {
                        "status": base_status,
                        "status_label": base_status,
                        "state": base_status,
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
                        "schedule_hash": (schedule_hash_filter or latest.get("schedule_hash")),
                        "top_gap_days": top_gap_days,
                        "top_lag_days": top_lag_days,
                    }
                rep = read_latest_validation_report(
                    runs_dir=runs_dir,
                    exchange="SHFE",
                    variety=coverage_var,
                    derived_symbol=coverage_symbol,
                )
                rep_usable = isinstance(rep, dict)
                if rep_usable and schedule_hash_filter:
                    rep_sh = str((rep or {}).get("schedule_hash") or "").strip()
                    if rep_sh and rep_sh != str(schedule_hash_filter):
                        rep_usable = False

                if not out["main_l5_validation"] and rep_usable:
                    rep_state = str((rep or {}).get("state") or "").strip().lower()
                    if rep_state == "noop":
                        rep_status = "ok"
                        rep_status_label = "noop"
                    elif rep_state in {"ok", "warn", "error"}:
                        rep_status = rep_state
                        rep_status_label = rep_state
                    else:
                        rep_status = ("ok" if bool((rep or {}).get("ok")) else "warn")
                        rep_status_label = rep_status
                    out["main_l5_validation"] = {
                        "status": rep_status,
                        "status_label": rep_status_label,
                        "state": (rep_state or rep_status),
                        "last_run": (rep or {}).get("created_at"),
                        "max_gap_s": (rep or {}).get("max_gap_s"),
                        "gap_threshold_s": (rep or {}).get("gap_threshold_s"),
                        "missing_seconds_ratio": (rep or {}).get("missing_seconds_ratio"),
                        "schedule_hash": (rep or {}).get("schedule_hash"),
                    }
                if rep_usable and (rep or {}).get("_path"):
                    out["main_l5_validation"]["report_name"] = Path(str(rep.get("_path"))).name
                if rep_usable and isinstance(rep, dict):
                    rep_state = str(rep.get("state") or "").strip().lower()
                    if rep_state == "noop":
                        out["main_l5_validation"]["status"] = "ok"
                        out["main_l5_validation"]["status_label"] = "noop"
                        out["main_l5_validation"]["state"] = "noop"
                    elif rep_state in {"ok", "warn", "error"}:
                        out["main_l5_validation"]["status"] = rep_state
                        out["main_l5_validation"]["status_label"] = rep_state
                        out["main_l5_validation"]["state"] = rep_state
                    if rep.get("created_at"):
                        out["main_l5_validation"]["last_run"] = rep.get("created_at")
                    merge_if_missing = (
                        "checked_days",
                        "missing_days",
                        "missing_segments_total",
                        "missing_seconds_total",
                        "missing_half_seconds_total",
                        "expected_seconds_strict_total",
                        "total_segments",
                        "max_gap_s",
                        "gap_threshold_s",
                        "cadence_mode",
                        "two_plus_ratio",
                        "schedule_hash",
                    )
                    for key in merge_if_missing:
                        if key in rep and key not in out["main_l5_validation"]:
                            out["main_l5_validation"][key] = rep.get(key)
                    merge_from_report = (
                        "reason",
                        "reason_code",
                        "action_hint",
                        "strict_ratio",
                        "state",
                        "overall_state",
                        "engineering_state",
                        "source_state",
                        "policy_state",
                        "missing_seconds_ratio",
                        "missing_half_seconds_state",
                        "missing_half_seconds_ratio",
                        "missing_half_seconds_info_ratio",
                        "missing_half_seconds_block_ratio",
                        "source_missing_days_count",
                        "source_missing_days_tolerance",
                        "source_missing_days_blocking",
                        "source_blocking",
                        "policy_sources",
                        "policy_profile",
                        "ticks_outside_sessions_seconds_total",
                        "expected_seconds_strict_fixed_total",
                        "gap_threshold_s_by_session",
                        "seconds_with_one_tick_total",
                        "gap_buckets_total",
                        "gap_buckets_by_session_total",
                        "gap_count_gt_30s",
                        "tqsdk_check",
                    )
                    for key in merge_from_report:
                        if key in rep:
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
        guardrails = _pipeline_guardrails_context()
        pipeline_health = _pipeline_health_context(
            main_schedule_coverage=main_schedule_coverage,
            main_l5_coverage=main_l5_coverage,
            main_l5_validation=main_l5_validation,
            coverage_error=coverage_error,
            validation_error=validation_error,
        )

        # Coverage lists can get very large; keep /data page load fast by default.
        # The Contracts tab will lazy-load any detailed coverage via API.
        return templates.TemplateResponse(
            request,
            "data.html",
            {
                "request": request,
                "title": "Data Hub",
                "token_qs": _token_qs(request),
                "running_count": running_count,
                "queued_count": queued_count,
                "dataset_version": lv,
                "ticks_v2": [],
                "main_l5_v2": [],
                "features": [],
                "labels": [],
                "questdb": questdb,
                "locks": locks,
                "reports": reports,
                **calendar_ctx,
                "tqsdk_scheduler": tqsdk_scheduler,
                "coverage_var": coverage_var,
                "coverage_symbol": coverage_symbol,
                "main_schedule_coverage": main_schedule_coverage,
                "main_l5_coverage": main_l5_coverage,
                "coverage_error": coverage_error,
                "main_l5_validation": main_l5_validation,
                "validation_error": validation_error,
                "guardrails": guardrails,
                "pipeline_health": pipeline_health,
                "validation_profile_suggestion": _validation_profile_suggestion(v),
                **_variety_nav_ctx(v, section="data", page_title="Data Hub"),
            },
        )

    register_config_routes(
        router=router,
        templates=templates,
        require_auth=_require_auth,
        token_qs=_token_qs,
        page_variety=_page_variety,
        variety_nav_ctx=_variety_nav_ctx,
        var_page=_var_page,
        job_summary=_job_summary,
        job_matches_variety=_job_matches_variety,
    )
    register_system_routes(
        router=router,
        templates=templates,
        require_auth=_require_auth,
        token_qs=_token_qs,
        request_variety_hint=_request_variety_hint,
        variety_nav_ctx=_variety_nav_ctx,
    )

    return router

