from __future__ import annotations

import importlib
from pathlib import Path


def test_views_data_page_orchestrator_binding_is_dedicated_module() -> None:
    page_mod = importlib.import_module("ghtrader.control.views_data_page")
    assert (
        getattr(getattr(page_mod, "_resolve_data_page_snapshot"), "__module__", "")
        == "ghtrader.control.views_data_page_orchestrator"
    )


def test_resolve_data_page_snapshot_populates_missing_cache(tmp_path: Path, monkeypatch) -> None:
    mod = importlib.import_module("ghtrader.control.views_data_page_orchestrator")

    monkeypatch.setattr(mod, "_make_qdb_cfg", lambda: object())
    monkeypatch.setattr(mod, "_load_reports_snapshot", lambda **kwargs: ["audit-1.json"])  # type: ignore[no-untyped-call]
    monkeypatch.setattr(mod, "_load_questdb_status_snapshot", lambda: {"ok": True, "host": "127.0.0.1"})
    monkeypatch.setattr(
        mod,
        "_load_coverage_snapshot",
        lambda **kwargs: {  # type: ignore[no-untyped-call]
            "main_schedule_coverage": {"n_days": 2},
            "main_l5_coverage": {"n_days": 1},
            "coverage_error": "",
        },
    )
    monkeypatch.setattr(
        mod,
        "_load_validation_snapshot",
        lambda **kwargs: {  # type: ignore[no-untyped-call]
            "main_l5_validation": {"status": "ok", "checked_days": 2},
            "validation_error": "",
        },
    )

    cache: dict[str, object] = {}
    out = mod.resolve_data_page_snapshot(
        runs_dir=tmp_path,
        coverage_var="cu",
        cache_get=lambda k: cache.get(k),
        cache_set=lambda k, v: cache.__setitem__(k, v),
    )

    assert out["coverage_symbol"] == "KQ.m@SHFE.cu"
    assert out["reports"] == ["audit-1.json"]
    assert out["questdb"]["ok"] is True
    assert out["main_schedule_coverage"]["n_days"] == 2
    assert out["main_l5_validation"]["status"] == "ok"
    assert "data_page:audit_reports" in cache
    assert "data_page:questdb_status" in cache
    assert "data_page:coverage:KQ.m@SHFE.cu" in cache
    assert "data_page:validation:KQ.m@SHFE.cu" in cache


def test_resolve_data_page_snapshot_surfaces_loader_exception_errors(tmp_path: Path, monkeypatch) -> None:
    mod = importlib.import_module("ghtrader.control.views_data_page_orchestrator")

    monkeypatch.setattr(mod, "_make_qdb_cfg", lambda: object())
    monkeypatch.setattr(mod, "_load_reports_snapshot", lambda **kwargs: [])  # type: ignore[no-untyped-call]
    monkeypatch.setattr(mod, "_load_questdb_status_snapshot", lambda: {"ok": True})

    def _raise_coverage(**kwargs):  # type: ignore[no-untyped-call]
        raise RuntimeError("coverage boom")

    def _raise_validation(**kwargs):  # type: ignore[no-untyped-call]
        raise RuntimeError("validation boom")

    monkeypatch.setattr(mod, "_load_coverage_snapshot", _raise_coverage)
    monkeypatch.setattr(mod, "_load_validation_snapshot", _raise_validation)

    cache: dict[str, object] = {}
    out = mod.resolve_data_page_snapshot(
        runs_dir=tmp_path,
        coverage_var="cu",
        cache_get=lambda k: cache.get(k),
        cache_set=lambda k, v: cache.__setitem__(k, v),
    )

    assert out["coverage_error"] == "coverage boom"
    assert out["validation_error"] == "validation boom"
    assert cache["data_page:coverage:KQ.m@SHFE.cu"]["coverage_error"] == "coverage boom"  # type: ignore[index]
    assert cache["data_page:validation:KQ.m@SHFE.cu"]["validation_error"] == "validation boom"  # type: ignore[index]


def test_resolve_data_page_snapshot_logs_preload_cache_hits(tmp_path: Path, monkeypatch) -> None:
    mod = importlib.import_module("ghtrader.control.views_data_page_orchestrator")

    class _LogRecorder:
        def __init__(self) -> None:
            self.calls: list[tuple[str, dict[str, object]]] = []

        def debug(self, event: str, **kwargs) -> None:
            self.calls.append((event, kwargs))

    log_recorder = _LogRecorder()
    monkeypatch.setattr(mod, "log", log_recorder)
    monkeypatch.setattr(mod, "_make_qdb_cfg", lambda: object())
    monkeypatch.setattr(
        mod,
        "_load_coverage_snapshot",
        lambda **kwargs: {  # type: ignore[no-untyped-call]
            "main_schedule_coverage": {"n_days": 2},
            "main_l5_coverage": {"n_days": 1},
            "coverage_error": "",
        },
    )
    monkeypatch.setattr(
        mod,
        "_load_validation_snapshot",
        lambda **kwargs: {  # type: ignore[no-untyped-call]
            "main_l5_validation": {"status": "ok", "checked_days": 2},
            "validation_error": "",
        },
    )

    cache: dict[str, object] = {
        "data_page:audit_reports": ["cached-audit.json"],
        "data_page:questdb_status": {"ok": True, "host": "127.0.0.1"},
    }
    mod.resolve_data_page_snapshot(
        runs_dir=tmp_path,
        coverage_var="cu",
        cache_get=lambda k: cache.get(k),
        cache_set=lambda k, v: cache.__setitem__(k, v),
    )

    assert log_recorder.calls
    event, payload = log_recorder.calls[-1]
    assert event == "data_page.timing"
    assert payload["cache_hits"] == 2
