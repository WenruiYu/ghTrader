from __future__ import annotations

import importlib
from pathlib import Path

import pytest


def _set_control_env(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("GHTRADER_RUNS_DIR", str(tmp_path / "runs"))
    monkeypatch.setenv("GHTRADER_DATA_DIR", str(tmp_path / "data"))
    monkeypatch.setenv("GHTRADER_ARTIFACTS_DIR", str(tmp_path / "artifacts"))


def test_create_app_initializes_runtime_state_and_core_routes(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    _set_control_env(tmp_path, monkeypatch)
    factory_mod = importlib.import_module("ghtrader.control.app_factory")
    importlib.reload(factory_mod)

    app = factory_mod.create_app()

    assert getattr(app.state, "job_store", None) is not None
    assert getattr(app.state, "job_manager", None) is not None
    paths = {str(getattr(route, "path", "")) for route in app.routes}
    assert "/ws/dashboard" in paths
    assert "/api/data/reports" in paths
    assert "/api/trading/console/status" in paths
    assert "/static" in paths


def test_app_factory_uses_dedicated_route_mount_module(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    _set_control_env(tmp_path, monkeypatch)
    factory_mod = importlib.import_module("ghtrader.control.app_factory")
    importlib.reload(factory_mod)
    mount_mod = importlib.import_module("ghtrader.control.route_mounts")

    mount_fn = getattr(factory_mod, "mount_all_routes")
    assert getattr(mount_fn, "__module__", "") == "ghtrader.control.route_mounts"
    assert mount_fn is getattr(mount_mod, "mount_all_routes")


def test_app_factory_uses_dedicated_lifecycle_module(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    _set_control_env(tmp_path, monkeypatch)
    factory_mod = importlib.import_module("ghtrader.control.app_factory")
    importlib.reload(factory_mod)
    lifecycle_mod = importlib.import_module("ghtrader.control.app_lifecycle")

    expected = (
        "_build_runtime_components",
        "_apply_persisted_scheduler_settings",
        "_install_http_observability_middleware",
        "_attach_runtime_state",
        "_start_background_workers",
    )
    for name in expected:
        fn = getattr(factory_mod, name)
        assert getattr(fn, "__module__", "") == "ghtrader.control.app_lifecycle"
        assert fn is getattr(lifecycle_mod, name)


def test_control_app_module_is_compatibility_shim(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    _set_control_env(tmp_path, monkeypatch)
    app_mod = importlib.import_module("ghtrader.control.app")
    importlib.reload(app_mod)
    factory_mod = importlib.import_module("ghtrader.control.app_factory")
    ws_mod = importlib.import_module("ghtrader.control.websocket_manager")

    assert getattr(app_mod, "create_app") is getattr(factory_mod, "create_app")
    assert getattr(app_mod, "ConnectionManager") is getattr(ws_mod, "ConnectionManager")
    assert getattr(app_mod, "mount_dashboard_websocket") is getattr(ws_mod, "mount_dashboard_websocket")
    app = getattr(app_mod, "app", None)
    assert app is not None
    assert getattr(app, "title", "") == "ghTrader Control"


def test_bootstrap_heavy_job_classifier_and_paths(tmp_path: Path) -> None:
    bootstrap_mod = importlib.import_module("ghtrader.control.bootstrap")

    assert bootstrap_mod.is_tqsdk_heavy_job(["python", "-m", "ghtrader.cli", "account", "run"]) is True
    assert bootstrap_mod.is_tqsdk_heavy_job(["python", "-m", "ghtrader.cli", "data", "health"]) is True
    assert bootstrap_mod.is_tqsdk_heavy_job(["python", "-m", "ghtrader.cli", "data", "build-main-l5"]) is False

    runs_dir = tmp_path / "runs"
    assert bootstrap_mod.control_root(runs_dir) == runs_dir / "control"
    assert bootstrap_mod.jobs_db_path(runs_dir) == runs_dir / "control" / "jobs.db"
    assert bootstrap_mod.logs_dir(runs_dir) == runs_dir / "control" / "logs"
