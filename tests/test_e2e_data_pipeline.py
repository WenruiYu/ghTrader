from __future__ import annotations

import json
import os
from pathlib import Path
import shutil

import pytest


def test_run_daily_pipeline_end_to_end_with_stubbed_dependencies(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    import ghtrader.datasets.features as features_mod
    import ghtrader.datasets.labels as labels_mod
    import ghtrader.regime as regime_mod
    import ghtrader.research.eval as eval_mod
    import ghtrader.research.models as models_mod
    import ghtrader.research.pipeline as pipeline_mod
    import ghtrader.tq.eval as tq_eval_mod

    assert hasattr(features_mod, "FactorEngine"), "FactorEngine missing from features module"
    assert hasattr(labels_mod, "build_labels_for_symbol"), "build_labels_for_symbol missing"
    assert hasattr(regime_mod, "train_regime_model"), "train_regime_model missing"
    assert hasattr(models_mod, "train_model"), "train_model missing"
    assert hasattr(tq_eval_mod, "run_backtest"), "run_backtest missing from tq.eval"
    assert hasattr(eval_mod, "PromotionGate"), "PromotionGate missing from research.eval"
    assert hasattr(eval_mod, "evaluate_and_promote"), "evaluate_and_promote missing"

    data_dir = tmp_path / "data"
    artifacts_dir = tmp_path / "artifacts"
    runs_dir = tmp_path / "runs"

    class _FakeFactorEngine:
        def build_features_for_symbol(self, *, symbol: str, data_dir: Path, ticks_kind: str = "main_l5") -> None:
            out = Path(data_dir) / "features" / symbol
            out.mkdir(parents=True, exist_ok=True)
            (out / f"{ticks_kind}.ok").write_text("ok", encoding="utf-8")

    class _FakeMetrics:
        def to_dict(self) -> dict[str, float]:
            return {"sharpe": 1.23, "max_drawdown": 0.08, "win_rate": 0.57}

    class _FakeGate:
        pass

    def _fake_build_labels_for_symbol(*, symbol: str, data_dir: Path, horizons: list[int], ticks_kind: str) -> None:
        out = Path(data_dir) / "labels" / symbol
        out.mkdir(parents=True, exist_ok=True)
        (out / f"{ticks_kind}_h{int(horizons[0])}.ok").write_text("ok", encoding="utf-8")

    def _fake_train_regime_model(*, symbol: str, artifacts_dir: Path, **_kwargs) -> Path:
        out = Path(artifacts_dir) / "regime" / symbol
        out.mkdir(parents=True, exist_ok=True)
        path = out / "regime.pkl"
        path.write_bytes(b"regime")
        return path

    def _fake_train_model(
        *,
        model_type: str,
        symbol: str,
        artifacts_dir: Path,
        horizon: int,
        **_kwargs,
    ) -> Path:
        out = Path(artifacts_dir) / symbol / model_type
        out.mkdir(parents=True, exist_ok=True)
        model_path = out / f"model_h{int(horizon)}.pt"
        model_path.write_bytes(b"candidate-model")
        return model_path

    def _fake_run_backtest(**_kwargs) -> _FakeMetrics:
        return _FakeMetrics()

    def _fake_evaluate_and_promote(*, candidate_path: Path, production_path: Path, **_kwargs) -> bool:
        production_path.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(candidate_path, production_path)
        return True

    monkeypatch.setattr(features_mod, "FactorEngine", _FakeFactorEngine)
    monkeypatch.setattr(labels_mod, "build_labels_for_symbol", _fake_build_labels_for_symbol)
    monkeypatch.setattr(regime_mod, "train_regime_model", _fake_train_regime_model)
    monkeypatch.setattr(models_mod, "train_model", _fake_train_model)
    monkeypatch.setattr(tq_eval_mod, "run_backtest", _fake_run_backtest)
    monkeypatch.setattr(eval_mod, "PromotionGate", _FakeGate)
    monkeypatch.setattr(eval_mod, "evaluate_and_promote", _fake_evaluate_and_promote)

    report = pipeline_mod.run_daily_pipeline(
        symbols=["SHFE.cu2505"],
        model_type="mlp",
        data_dir=data_dir,
        artifacts_dir=artifacts_dir,
        runs_dir=runs_dir,
        horizon=10,
        lookback_days=3,
        backtest_days=1,
    )

    symbol_report = report["results"]["SHFE.cu2505"]
    assert symbol_report["status"] == "success"
    assert symbol_report["steps"]["promote"]["gate_passed"] is True
    assert symbol_report["steps"]["backtest"]["metrics"]["sharpe"] == pytest.approx(1.23)

    production = artifacts_dir / "production" / "SHFE.cu2505" / "mlp" / "model_h10.pt"
    assert production.exists()

    report_path = runs_dir / "daily_pipeline" / str(report["run_id"]) / "report.json"
    assert report_path.exists()
    payload = json.loads(report_path.read_text(encoding="utf-8"))
    assert payload["results"]["SHFE.cu2505"]["status"] == "success"


@pytest.mark.integration
def test_live_data_pipeline_catalog_probe_optional(tmp_path: Path) -> None:
    if str(os.environ.get("GHTRADER_RUN_E2E_PIPELINE_LIVE", "false")).strip().lower() != "true":
        pytest.skip("Set GHTRADER_RUN_E2E_PIPELINE_LIVE=true to enable live pipeline smoke test")
    pytest.importorskip("tqsdk")

    from ghtrader.tq.catalog import get_contract_catalog

    out = get_contract_catalog(
        exchange="SHFE",
        var="cu",
        runs_dir=tmp_path,
        refresh=True,
        offline=False,
    )
    if not bool(out.get("ok")):
        pytest.skip(f"live catalog probe unavailable: {out.get('error')}")
    assert len(list(out.get("contracts") or [])) > 0
