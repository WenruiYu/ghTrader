from __future__ import annotations

from fastapi import FastAPI

from ghtrader.config import get_artifacts_dir, get_data_dir, get_runs_dir
from ghtrader.control import auth
from ghtrader.control.app_helpers import (
    dashboard_guardrails_context as _dashboard_guardrails_context,
    job_matches_variety as _job_matches_variety,
    normalize_variety_for_api as _normalize_variety_for_api,
    scan_model_files as _scan_model_files,
    ui_state_payload as _ui_state_payload,
)
from ghtrader.control.cache import TTLCacheSlot
from ghtrader.control.routes import build_api_router, build_root_router
from ghtrader.control.routes.accounts import router as accounts_router
from ghtrader.control.routes.data_jobs import mount_data_job_routes
from ghtrader.control.routes.data_reports import mount_data_report_routes
from ghtrader.control.routes.dashboard_status import mount_dashboard_status_routes
from ghtrader.control.routes.gateway import gateway_status_payload, router as gateway_router
from ghtrader.control.routes.models import router as models_router
from ghtrader.control.routes.questdb_query import mount_questdb_query_route
from ghtrader.control.routes.quality import mount_quality_routes
from ghtrader.control.routes.quality_data import mount_quality_data_routes
from ghtrader.control.routes.trading_runtime import mount_trading_runtime_routes
from ghtrader.control.state_helpers import (
    artifact_age_sec as _artifact_age_sec,
    health_error as _health_error,
    read_json_file as _read_json_file,
    read_last_jsonl_obj as _read_last_jsonl_obj,
    read_redis_json as _read_redis_json,
    read_state_with_revision as _read_state_with_revision,
    status_from_desired_and_state as _status_from_desired_and_state,
)
from ghtrader.control.supervisor_helpers import (
    argv_opt as _argv_opt,
    is_gateway_job as _is_gateway_job,
    is_strategy_job as _is_strategy_job,
)
from ghtrader.control.variety_context import (
    derived_symbol_for_variety as _derived_symbol_for_variety,
    symbol_matches_variety as _symbol_matches_variety,
)
from ghtrader.control.views import build_router
from ghtrader.control.views_data_page import clear_data_page_cache as _data_page_cache_clear
from ghtrader.control.websocket_manager import mount_dashboard_websocket
from ghtrader.util.json_io import read_json as _read_json
from ghtrader.util.time import now_iso as _now_iso

_UI_STATUS_TTL_S = 5.0
_ui_status_cache = TTLCacheSlot()

_DASH_SUMMARY_TTL_S = 10.0
_dash_summary_cache = TTLCacheSlot()

_DATA_QUALITY_TTL_S = 60.0
_data_quality_cache = TTLCacheSlot()


def _clear_data_quality_cache() -> None:
    _data_quality_cache.clear()


def mount_all_routes(app: FastAPI) -> None:
    # HTML pages
    app.include_router(build_router())
    app.include_router(build_api_router())
    app.include_router(build_root_router())

    # WebSocket + API endpoints
    mount_dashboard_websocket(app)
    mount_quality_routes(
        app,
        is_authorized=auth.is_authorized,
        normalize_variety_for_api=_normalize_variety_for_api,
        derived_symbol_for_variety=_derived_symbol_for_variety,
        get_runs_dir=get_runs_dir,
    )
    mount_quality_data_routes(
        app,
        is_authorized=auth.is_authorized,
        normalize_variety_for_api=_normalize_variety_for_api,
        derived_symbol_for_variety=_derived_symbol_for_variety,
        get_runs_dir=get_runs_dir,
        read_json=_read_json,
        now_iso=_now_iso,
        data_quality_cache=_data_quality_cache,
        data_quality_ttl_s=_DATA_QUALITY_TTL_S,
    )
    mount_data_report_routes(
        app,
        is_authorized=auth.is_authorized,
        normalize_variety_for_api=_normalize_variety_for_api,
        get_runs_dir=get_runs_dir,
        read_json=_read_json,
    )
    mount_data_job_routes(
        app,
        is_authorized=auth.is_authorized,
        normalize_variety_for_api=_normalize_variety_for_api,
        derived_symbol_for_variety=_derived_symbol_for_variety,
        get_runs_dir=get_runs_dir,
        get_data_dir=get_data_dir,
        data_page_cache_clear=_data_page_cache_clear,
        clear_data_quality_cache=_clear_data_quality_cache,
    )
    mount_questdb_query_route(
        app,
        is_authorized=auth.is_authorized,
    )
    mount_dashboard_status_routes(
        app,
        is_authorized=auth.is_authorized,
        normalize_variety_for_api=_normalize_variety_for_api,
        get_data_dir=get_data_dir,
        get_runs_dir=get_runs_dir,
        get_artifacts_dir=get_artifacts_dir,
        now_iso=_now_iso,
        job_matches_variety=_job_matches_variety,
        symbol_matches_variety=_symbol_matches_variety,
        scan_model_files=_scan_model_files,
        ui_state_payload=_ui_state_payload,
        dashboard_guardrails_context=_dashboard_guardrails_context,
        derived_symbol_for_variety=_derived_symbol_for_variety,
        dash_summary_cache=_dash_summary_cache,
        dash_summary_ttl_s=_DASH_SUMMARY_TTL_S,
        ui_status_cache=_ui_status_cache,
        ui_status_ttl_s=_UI_STATUS_TTL_S,
    )
    mount_trading_runtime_routes(
        app,
        is_authorized=auth.is_authorized,
        get_runs_dir=get_runs_dir,
        now_iso=_now_iso,
        read_state_with_revision=_read_state_with_revision,
        read_redis_json=_read_redis_json,
        read_json_file=_read_json_file,
        read_last_jsonl_obj=_read_last_jsonl_obj,
        artifact_age_sec=_artifact_age_sec,
        health_error=_health_error,
        status_from_desired_and_state=_status_from_desired_and_state,
        is_gateway_job=_is_gateway_job,
        is_strategy_job=_is_strategy_job,
        argv_opt=_argv_opt,
        normalize_variety_for_api=_normalize_variety_for_api,
        symbol_matches_variety=_symbol_matches_variety,
        gateway_status_payload=gateway_status_payload,
    )

    # Domain routers
    app.include_router(models_router)
    app.include_router(accounts_router)
    app.include_router(gateway_router)
