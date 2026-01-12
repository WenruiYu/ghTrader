from __future__ import annotations

import numpy as np
import pytest

import ghtrader.distributed as distu
from ghtrader.models import DeepLOBModel, TCNModel, TLOBModel, SSMModel, create_model


class TestModelFactory:
    def test_create_tcn(self):
        m = create_model("tcn", n_features=11, seq_len=20)
        assert m.name == "tcn"

    def test_create_tlob(self):
        m = create_model("tlob", n_features=11, seq_len=20)
        assert m.name == "tlob"

    def test_create_ssm(self):
        m = create_model("ssm", n_features=11, seq_len=20)
        assert m.name == "ssm"


@pytest.mark.parametrize("model_cls", [TCNModel, TLOBModel, SSMModel])
def test_fit_predict_smoke(model_cls):
    n = 200
    n_features = 11
    X = np.random.randn(n, n_features).astype("float32")
    y = np.random.randint(0, 3, size=n).astype("int64")

    m = model_cls(n_features=n_features, seq_len=20, device="cpu")
    m.fit(X, y, epochs=1, batch_size=32, lr=1e-3, ddp=False)

    probs = m.predict_proba(X)
    assert probs.shape[0] == n


class TestDeepLOBModel:
    def test_ddp_flag_attempts_setup_distributed(self, monkeypatch):
        calls = {"n": 0}

        def _fake_setup(*_args, **_kwargs):
            calls["n"] += 1
            return False

        monkeypatch.setattr(distu, "setup_distributed", _fake_setup)
        m = DeepLOBModel(n_features=11, seq_len=20, device="cpu")
        X = np.random.randn(120, 11).astype("float32")
        y = np.random.randint(0, 3, size=120).astype("int64")
        m.fit(X, y, epochs=1, batch_size=32, ddp=True)
        assert calls["n"] >= 1

    def test_ddp_disabled_does_not_setup_distributed(self, monkeypatch):
        def _boom(*_args, **_kwargs):
            raise AssertionError("setup_distributed should not be called")

        monkeypatch.setattr(distu, "setup_distributed", _boom)
        m = DeepLOBModel(n_features=11, seq_len=20, device="cpu")
        X = np.random.randn(120, 11).astype("float32")
        y = np.random.randint(0, 3, size=120).astype("int64")
        m.fit(X, y, epochs=1, batch_size=32, ddp=False)

