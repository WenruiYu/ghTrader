from __future__ import annotations

import json
import shutil
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def capacity_matrix_template() -> list[dict[str, Any]]:
    return [
        {
            "id": "data-throughput-main-l5",
            "domain": "data",
            "metric": "rows_per_second",
            "target": ">= baseline * 2.0",
            "notes": "main_l5 ingest/build throughput on multi-day batches",
            "status": "planned",
        },
        {
            "id": "questdb-query-latency",
            "domain": "data",
            "metric": "p95_query_ms",
            "target": "<= 500",
            "notes": "coverage/validation queries under concurrent readers",
            "status": "planned",
        },
        {
            "id": "control-api-concurrency",
            "domain": "control",
            "metric": "cancel_and_log_stability",
            "target": "no errors under concurrent job operations",
            "notes": "queue/cancel/log API behavior remains deterministic",
            "status": "planned",
        },
        {
            "id": "ddp-8gpu-stability",
            "domain": "training",
            "metric": "ddp_run_completion_rate",
            "target": "100% for smoke runs, no artifact corruption",
            "notes": "torchrun nproc_per_node=8 path and rank0-only artifact writes",
            "status": "planned",
        },
    ]


def capacity_smoke_snapshot() -> dict[str, Any]:
    try:
        import torch

        gpu_count = int(torch.cuda.device_count())
    except Exception:
        gpu_count = 0

    torchrun_path = shutil.which("torchrun")

    questdb_ok = False
    questdb_error = ""
    try:
        from ghtrader.questdb.client import questdb_reachable_pg

        q = questdb_reachable_pg(connect_timeout_s=1, retries=1, backoff_s=0.2)
        questdb_ok = bool(q.get("ok"))
        questdb_error = str(q.get("error") or "")
    except Exception as e:
        questdb_ok = False
        questdb_error = str(e)

    return {
        "gpu_count": gpu_count,
        "torchrun_available": bool(torchrun_path),
        "torchrun_path": torchrun_path,
        "questdb_ok": bool(questdb_ok),
        "questdb_error": questdb_error,
    }


def build_capacity_report(*, include_smoke: bool = True) -> dict[str, Any]:
    report: dict[str, Any] = {
        "schema_version": 1,
        "generated_at": _now_iso(),
        "matrix": capacity_matrix_template(),
    }
    if include_smoke:
        report["smoke"] = capacity_smoke_snapshot()
    return report


def write_capacity_report(*, runs_dir: Path, report: dict[str, Any]) -> Path:
    out_dir = Path(runs_dir) / "capacity"
    out_dir.mkdir(parents=True, exist_ok=True)
    run_id = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    out_path = out_dir / f"capacity_matrix_{run_id}.json"
    out_path.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")
    return out_path
