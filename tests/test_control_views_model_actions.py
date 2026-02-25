from __future__ import annotations

import importlib
from pathlib import Path

import pytest


def test_views_register_model_action_routes_is_loaded_from_dedicated_module() -> None:
    views_mod = importlib.import_module("ghtrader.control.views")
    assert (
        getattr(getattr(views_mod, "register_model_action_routes"), "__module__", "")
        == "ghtrader.control.views_model_actions"
    )


def test_views_model_action_routes_are_mounted_from_dedicated_module(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("GHTRADER_RUNS_DIR", str(tmp_path / "runs"))
    monkeypatch.setenv("GHTRADER_DATA_DIR", str(tmp_path / "data"))
    monkeypatch.setenv("GHTRADER_ARTIFACTS_DIR", str(tmp_path / "artifacts"))

    app_mod = importlib.import_module("ghtrader.control.app")
    importlib.reload(app_mod)

    expected_paths = {
        "/models/model/train",
        "/models/model/sweep",
        "/models/eval/benchmark",
        "/models/eval/compare",
        "/models/eval/backtest",
        "/models/eval/paper",
        "/models/eval/daily_train",
    }
    route_modules: dict[str, str] = {}
    for route in app_mod.app.routes:
        if getattr(route, "path", "") not in expected_paths:
            continue
        methods = set(getattr(route, "methods", set()) or set())
        if "POST" not in methods:
            continue
        route_modules[str(route.path)] = str(getattr(getattr(route, "endpoint", None), "__module__", ""))

    assert set(route_modules.keys()) == expected_paths
    assert all(mod == "ghtrader.control.views_model_actions" for mod in route_modules.values())
