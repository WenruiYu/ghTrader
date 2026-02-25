from __future__ import annotations

import shutil
from pathlib import Path
from typing import Any

from fastapi import APIRouter, HTTPException, Request
from fastapi.templating import Jinja2Templates

from ghtrader.config import get_env
from ghtrader.control.app_helpers import job_matches_variety as _job_matches_variety_shared
from ghtrader.control import auth
from ghtrader.control.variety_context import (
    allowed_varieties as _allowed_varieties,
    default_variety as _default_variety,
    derived_symbol_for_variety as _derived_symbol_for_variety,
    infer_variety_from_symbol as _infer_variety_from_symbol_ctx,
    require_supported_variety as _require_supported_variety,
)
from ghtrader.control.views_pipeline_helpers import (
    l5_calendar_context as _l5_calendar_context,
    pipeline_guardrails_context as _pipeline_guardrails_context,
    pipeline_health_context as _pipeline_health_context,
    validation_profile_suggestion as _validation_profile_suggestion,
)
from ghtrader.control.views_data_page import (
    clear_data_page_cache as _clear_data_page_cache,
    register_data_page_routes,
)
from ghtrader.control.views_data_actions import register_data_action_routes
from ghtrader.control.views_workspace_pages import register_workspace_page_routes
from ghtrader.control.views_job_detail import register_job_detail_routes
from ghtrader.control.views_model_actions import register_model_action_routes
from ghtrader.control.views_job_actions import register_job_action_routes
from ghtrader.control.views_training_helpers import (
    build_train_job_argv as _build_train_job_argv_impl,
    has_active_ddp_training as _has_active_ddp_training_impl,
)
from ghtrader.control.views_config import register_config_routes
from ghtrader.control.views_system import register_system_routes
from ghtrader.control.views_helpers import (
    variety_nav_ctx as _variety_nav_ctx_impl,
    job_summary as _job_summary_impl,
    start_or_reuse_job_by_command as _start_or_reuse_job_by_command_impl,
)

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
        return _variety_nav_ctx_impl(
            variety,
            section=section,
            page_title=page_title,
            breadcrumbs=breadcrumbs,
            page_variety=_page_variety,
            allowed_varieties=_allowed_varieties,
        )

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
        return _has_active_ddp_training_impl(store=store)

    def _job_summary(*, request: Request, limit: int) -> dict[str, Any]:
        return _job_summary_impl(request=request, limit=limit)

    def _start_or_reuse_job_by_command(
        *, request: Request, title: str, argv: list[str], cwd: Path, metadata: dict[str, Any] | None = None
    ) -> Any:
        return _start_or_reuse_job_by_command_impl(
            request=request, title=title, argv=argv, cwd=cwd, metadata=metadata,
        )

    def _job_matches_variety(job: Any, variety: str) -> bool:
        return _job_matches_variety_shared(job, _page_variety(variety))

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
        return _build_train_job_argv_impl(
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
            ddp_requested=ddp_requested,
            num_workers=num_workers,
            prefetch_factor=prefetch_factor,
            pin_memory=pin_memory,
            cuda_device_count=_cuda_device_count,
            resolve_torchrun_path=_resolve_torchrun_path,
        )

    register_workspace_page_routes(
        router=router,
        templates=templates,
        require_auth=_require_auth,
        token_qs=_token_qs,
        var_page=_var_page,
        page_variety=_page_variety,
        job_summary=_job_summary,
        job_matches_variety=_job_matches_variety,
        variety_nav_ctx=_variety_nav_ctx,
        l5_calendar_context=_l5_calendar_context,
        pipeline_guardrails_context=_pipeline_guardrails_context,
    )
    register_job_detail_routes(
        router=router,
        templates=templates,
        require_auth=_require_auth,
        token_qs=_token_qs,
        request_variety_hint=_request_variety_hint,
        variety_nav_ctx=_variety_nav_ctx,
    )
    register_data_page_routes(
        router=router,
        templates=templates,
        require_auth=_require_auth,
        token_qs=_token_qs,
        var_page=_var_page,
        page_variety=_page_variety,
        job_summary=_job_summary,
        job_matches_variety=_job_matches_variety,
        variety_nav_ctx=_variety_nav_ctx,
        l5_calendar_context=_l5_calendar_context,
        pipeline_guardrails_context=_pipeline_guardrails_context,
        pipeline_health_context=_pipeline_health_context,
        validation_profile_suggestion=_validation_profile_suggestion,
    )
    register_data_action_routes(
        router=router,
        require_auth=_require_auth,
        form_variety=_form_variety,
        var_page=_var_page,
        token_qs=_token_qs,
        start_or_reuse_job_by_command=_start_or_reuse_job_by_command,
        job_redirect_url=_job_redirect_url,
        derived_symbol_for_variety=_derived_symbol_for_variety,
        infer_variety_from_symbol=_infer_variety_from_symbol,
        clear_data_page_cache=_clear_data_page_cache,
    )

    register_model_action_routes(
        router=router,
        require_auth=_require_auth,
        build_train_job_argv=_build_train_job_argv,
        has_active_ddp_training=_has_active_ddp_training,
        infer_variety_from_symbol=_infer_variety_from_symbol,
        job_redirect_url=_job_redirect_url,
    )
    register_job_action_routes(
        router=router,
        require_auth=_require_auth,
        form_variety=_form_variety,
        page_variety=_page_variety,
        infer_variety_from_symbol=_infer_variety_from_symbol,
        derived_symbol_for_variety=_derived_symbol_for_variety,
        start_or_reuse_job_by_command=_start_or_reuse_job_by_command,
        job_redirect_url=_job_redirect_url,
        token_qs=_token_qs,
        request_variety_hint=_request_variety_hint,
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

