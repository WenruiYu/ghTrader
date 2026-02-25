from __future__ import annotations

import importlib
from pathlib import Path

import pytest


def test_views_register_data_action_routes_is_loaded_from_dedicated_module() -> None:
    views_mod = importlib.import_module("ghtrader.control.views")
    assert (
        getattr(getattr(views_mod, "register_data_action_routes"), "__module__", "")
        == "ghtrader.control.views_data_actions"
    )


def test_views_data_action_routes_are_mounted_from_dedicated_module(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("GHTRADER_RUNS_DIR", str(tmp_path / "runs"))
    monkeypatch.setenv("GHTRADER_DATA_DIR", str(tmp_path / "data"))
    monkeypatch.setenv("GHTRADER_ARTIFACTS_DIR", str(tmp_path / "artifacts"))

    app_mod = importlib.import_module("ghtrader.control.app")
    importlib.reload(app_mod)

    expected_get_paths = {
        "/data/integrity/report/{name}",
        "/data/main-l5-validate/report/{name}",
    }
    expected_post_paths = {
        "/data/settings/tqsdk_scheduler",
        "/data/build/main_schedule",
        "/data/build/main_l5",
        "/data/build/build",
    }
    route_modules: dict[str, str] = {}
    for route in app_mod.app.routes:
        path = str(getattr(route, "path", ""))
        methods = set(getattr(route, "methods", set()) or set())
        if path in expected_get_paths and "GET" in methods:
            route_modules[f"GET {path}"] = str(getattr(getattr(route, "endpoint", None), "__module__", ""))
        if path in expected_post_paths and "POST" in methods:
            route_modules[f"POST {path}"] = str(getattr(getattr(route, "endpoint", None), "__module__", ""))

    expected_keys = {f"GET {p}" for p in expected_get_paths} | {f"POST {p}" for p in expected_post_paths}
    assert set(route_modules.keys()) == expected_keys
    assert all(mod == "ghtrader.control.views_data_actions" for mod in route_modules.values())
