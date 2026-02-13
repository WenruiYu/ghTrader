from __future__ import annotations

import re
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from ghtrader.util.json_io import write_json_atomic


def _slug(raw: str) -> str:
    return re.sub(r"[^a-z0-9]+", "-", str(raw or "").lower()).strip("-")


def build_research_loop_template(
    *,
    symbol: str,
    model: str,
    horizon: int,
    owner: str = "",
    hypothesis: str = "",
) -> dict[str, Any]:
    now = datetime.now(timezone.utc)
    run_id = now.strftime("%Y%m%d_%H%M%S")
    sym = str(symbol).strip()
    mdl = str(model).strip().lower()
    hz = int(horizon)

    return {
        "schema_version": 1,
        "run_id": run_id,
        "created_at": now.isoformat(),
        "status": "draft",
        "owner": str(owner).strip() or None,
        "scope": {
            "symbol": sym,
            "model": mdl,
            "horizon": hz,
        },
        # Borrowed process pattern:
        # - Qlib: config-driven workflow spec
        # - RD-Agent: propose -> implement -> evaluate -> learn loop
        # - Kronos: long-context experiment axis for sequence models
        "pattern_sources": ["Qlib-config-workflow", "RD-Agent-closed-loop", "Kronos-long-sequence-axis"],
        "propose": {
            "hypothesis": str(hypothesis).strip() or "TODO: state a falsifiable hypothesis",
            "expected_metric_delta": {
                "accuracy_min_delta": 0.0,
                "f1_min_delta": 0.0,
            },
            "risk_notes": [],
        },
        "implement": {
            "change_plan": [],
            "config_overrides": {
                "train": {},
                "data": {},
                "control": {},
            },
        },
        "evaluate": {
            "required_jobs": [
                f"ghtrader benchmark --model {mdl} --symbol {sym} --horizon {hz}",
                f"ghtrader compare --symbol {sym} --horizon {hz}",
            ],
            "capacity_checks": [
                "data-plane throughput check",
                "queue/cancel/log API stability check",
                "ddp smoke check when gpus>1",
            ],
            "acceptance_gates": {
                "must_improve_accuracy": False,
                "must_not_regress_latency": True,
                "must_not_break_ops_compat": True,
            },
            "results": {},
        },
        "learn": {
            "decision": "pending",  # promote|iterate|rollback
            "lessons": [],
            "next_hypothesis": "",
        },
        "kronos_track": {
            "enabled": mdl in {"deeplob", "transformer", "tcn", "tlob", "ssm", "lobert", "kanformer"},
            "sequence_lengths": [100, 200, 400],
            "notes": "Use only as an experiment axis; do not add external runtime dependency.",
        },
    }


def write_research_loop_template(*, runs_dir: Path, template: dict[str, Any]) -> Path:
    scope = dict(template.get("scope") or {})
    symbol = _slug(str(scope.get("symbol") or "symbol"))
    model = _slug(str(scope.get("model") or "model"))
    run_id = str(template.get("run_id") or "run")
    out_dir = Path(runs_dir) / "research_loop" / f"symbol={symbol}" / f"model={model}"
    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / f"iteration_{run_id}.json"
    write_json_atomic(out_path, template)
    return out_path
