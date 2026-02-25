from __future__ import annotations

import importlib
from pathlib import Path

import pytest


def test_views_register_job_detail_routes_is_loaded_from_dedicated_module() -> None:
    views_mod = importlib.import_module("ghtrader.control.views")
    assert (
        getattr(getattr(views_mod, "register_job_detail_routes"), "__module__", "")
        == "ghtrader.control.views_job_detail"
    )


def test_views_job_detail_route_is_mounted_from_dedicated_module(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("GHTRADER_RUNS_DIR", str(tmp_path / "runs"))
    monkeypatch.setenv("GHTRADER_DATA_DIR", str(tmp_path / "data"))
    monkeypatch.setenv("GHTRADER_ARTIFACTS_DIR", str(tmp_path / "artifacts"))

    app_mod = importlib.import_module("ghtrader.control.app")
    importlib.reload(app_mod)

    route_modules: dict[str, str] = {}
    for route in app_mod.app.routes:
        if str(getattr(route, "path", "")) != "/jobs/{job_id}":
            continue
        methods = set(getattr(route, "methods", set()) or set())
        if "GET" not in methods:
            continue
        route_modules[str(route.path)] = str(getattr(getattr(route, "endpoint", None), "__module__", ""))

    assert set(route_modules.keys()) == {"/jobs/{job_id}"}
    assert route_modules["/jobs/{job_id}"] == "ghtrader.control.views_job_detail"
