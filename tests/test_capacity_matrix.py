from __future__ import annotations

import json
from pathlib import Path

from click.testing import CliRunner

from ghtrader.cli import main


def test_capacity_matrix_command_writes_report(tmp_path: Path) -> None:
    runner = CliRunner()
    runs_dir = tmp_path / "runs"
    res = runner.invoke(main, ["capacity-matrix", "--runs-dir", str(runs_dir), "--smoke"])
    assert res.exit_code == 0

    p = Path(res.output.strip())
    assert p.exists()
    payload = json.loads(p.read_text(encoding="utf-8"))
    assert isinstance(payload.get("matrix"), list)
    assert any(item.get("id") == "ddp-8gpu-stability" for item in payload["matrix"])
    assert "smoke" in payload
