from __future__ import annotations

import json
from pathlib import Path

from click.testing import CliRunner

from ghtrader.cli import main


def test_research_loop_template_command(tmp_path: Path) -> None:
    runner = CliRunner()
    runs_dir = tmp_path / "runs"
    res = runner.invoke(
        main,
        [
            "research-loop-template",
            "--symbol",
            "KQ.m@SHFE.cu",
            "--model",
            "deeplob",
            "--horizon",
            "50",
            "--runs-dir",
            str(runs_dir),
            "--owner",
            "tester",
            "--hypothesis",
            "longer context improves stability",
        ],
    )
    assert res.exit_code == 0

    out_path = Path(res.output.strip())
    assert out_path.exists()
    payload = json.loads(out_path.read_text(encoding="utf-8"))
    assert payload["scope"]["model"] == "deeplob"
    assert payload["scope"]["symbol"] == "KQ.m@SHFE.cu"
    assert payload["learn"]["decision"] == "pending"
