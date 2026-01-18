from __future__ import annotations

import os
import subprocess
import sys
from pathlib import Path

import pytest


@pytest.mark.integration
@pytest.mark.ddp_integration
def test_ddp_training_smoke(tmp_path: Path):
    """
    Minimal torchrun smoke test.

    Skipped by default unless GHTRADER_RUN_DDP_TESTS=true and credentials are set.
    """
    env = os.environ.copy()
    env["WORLD_SIZE"] = env.get("WORLD_SIZE", "2")

    script = tmp_path / "ddp_smoke.py"
    script.write_text(
        """
import numpy as np
from ghtrader.research.models import DeepLOBModel

X = np.random.randn(200, 11).astype("float32")
y = np.random.randint(0, 3, size=200).astype("int64")
m = DeepLOBModel(n_features=11, seq_len=20, device="cpu")
m.fit(X, y, epochs=1, batch_size=32, ddp=True)
print("ok")
"""
    )

    cmd = [
        "torchrun",
        "--standalone",
        "--nproc_per_node=2",
        str(script),
    ]
    res = subprocess.run(cmd, capture_output=True, text=True, env=env)
    assert res.returncode == 0, res.stderr

