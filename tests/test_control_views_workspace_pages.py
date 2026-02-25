from __future__ import annotations

import importlib
from pathlib import Path

import pytest


def test_views_register_workspace_page_routes_is_loaded_from_dedicated_module() -> None:
    views_mod = importlib.import_module("ghtrader.control.views")
    assert (
        getattr(getattr(views_mod, "register_workspace_page_routes"), "__module__", "")
        == "ghtrader.control.views_workspace_pages"
    )


def test_views_workspace_page_routes_are_mounted_from_dedicated_module(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("GHTRADER_RUNS_DIR", str(tmp_path / "runs"))
    monkeypatch.setenv("GHTRADER_DATA_DIR", str(tmp_path / "data"))
    monkeypatch.setenv("GHTRADER_ARTIFACTS_DIR", str(tmp_path / "artifacts"))

    app_mod = importlib.import_module("ghtrader.control.app")
    importlib.reload(app_mod)

    expected_get_paths = {
        "/",
        "/v/{variety}/dashboard",
        "/jobs",
        "/v/{variety}/jobs",
        "/models",
        "/v/{variety}/models",
        "/trading",
        "/v/{variety}/trading",
    }
    route_modules: dict[str, str] = {}
    for route in app_mod.app.routes:
        path = str(getattr(route, "path", ""))
        if path not in expected_get_paths:
            continue
        methods = set(getattr(route, "methods", set()) or set())
        if "GET" not in methods:
            continue
        route_modules[path] = str(getattr(getattr(route, "endpoint", None), "__module__", ""))

    assert set(route_modules.keys()) == expected_get_paths
    assert all(mod == "ghtrader.control.views_workspace_pages" for mod in route_modules.values())
