from __future__ import annotations

from pathlib import Path

from fastapi import FastAPI
from fastapi.staticfiles import StaticFiles

from ghtrader.config_service import enforce_no_legacy_env
from ghtrader.config import get_runs_dir
from ghtrader.control.app_lifecycle import (
    _apply_persisted_scheduler_settings,
    _attach_runtime_state,
    _build_runtime_components,
    _install_http_observability_middleware,
    _start_background_workers,
)
from ghtrader.control.route_mounts import mount_all_routes

def create_app() -> FastAPI:
    runs_dir = get_runs_dir()
    enforce_no_legacy_env()
    store, jm = _build_runtime_components(runs_dir=runs_dir)
    _apply_persisted_scheduler_settings(runs_dir=runs_dir)

    app = FastAPI(title="ghTrader Control", version="0.1.0")
    _install_http_observability_middleware(app)
    _attach_runtime_state(app, store=store, jm=jm)
    _start_background_workers(app)

    # Static assets (CSS)
    static_dir = Path(__file__).parent / "static"
    app.mount("/static", StaticFiles(directory=str(static_dir)), name="static")

    mount_all_routes(app)

    return app
