from __future__ import annotations

import importlib
from pathlib import Path


def test_views_data_page_loader_bindings_are_from_dedicated_module() -> None:
    orchestrator_mod = importlib.import_module("ghtrader.control.views_data_page_orchestrator")

    assert (
        getattr(getattr(orchestrator_mod, "_load_reports_snapshot"), "__module__", "")
        == "ghtrader.control.views_data_page_loaders"
    )
    assert (
        getattr(getattr(orchestrator_mod, "_load_questdb_status_snapshot"), "__module__", "")
        == "ghtrader.control.views_data_page_loaders"
    )
    assert (
        getattr(getattr(orchestrator_mod, "_load_coverage_snapshot"), "__module__", "")
        == "ghtrader.control.views_data_page_loaders"
    )
    assert (
        getattr(getattr(orchestrator_mod, "_load_validation_snapshot"), "__module__", "")
        == "ghtrader.control.views_data_page_loaders"
    )


def test_load_coverage_snapshot_without_cfg_reports_unavailable() -> None:
    mod = importlib.import_module("ghtrader.control.views_data_page_loaders")
    out = mod.load_coverage_snapshot(
        cfg=None,
        coverage_var="cu",
        coverage_symbol="KQ.m@SHFE.cu",
    )
    assert out["main_schedule_coverage"] == {}
    assert out["main_l5_coverage"] == {}
    assert str(out["coverage_error"]) == "questdb config unavailable"


def test_load_validation_snapshot_without_cfg_reports_unavailable(tmp_path: Path) -> None:
    mod = importlib.import_module("ghtrader.control.views_data_page_loaders")
    out = mod.load_validation_snapshot(
        cfg=None,
        runs_dir=tmp_path,
        coverage_var="cu",
        coverage_symbol="KQ.m@SHFE.cu",
    )
    assert out["main_l5_validation"] == {}
    assert str(out["validation_error"]) == "questdb config unavailable"
