from __future__ import annotations

from typing import Any

OPS_COMPAT_VERSION = "2026-02"

# Legacy /ops routes are retained as compatibility aliases while canonical
# entrypoints are consolidated under /data, /models, /trading.
OPS_COMPAT_RULES: list[dict[str, str]] = [
    {"legacy": "/ops", "canonical": "/data#contracts", "mode": "redirect"},
    {"legacy": "/ops/ingest", "canonical": "/data#ingest", "mode": "redirect"},
    {"legacy": "/ops/build", "canonical": "/data#build", "mode": "redirect"},
    {"legacy": "/ops/model", "canonical": "/models", "mode": "redirect"},
    {"legacy": "/ops/eval", "canonical": "/models#benchmarks", "mode": "redirect"},
    {"legacy": "/ops/trading", "canonical": "/trading", "mode": "redirect"},
    {"legacy": "/ops/locks", "canonical": "/data#locks", "mode": "redirect"},
    {"legacy": "/ops/integrity", "canonical": "/data#integrity", "mode": "redirect"},
    {"legacy": "/ops/integrity/report/{name}", "canonical": "/data/integrity/report/{name}", "mode": "alias"},
    {"legacy": "/ops/settings/tqsdk_scheduler", "canonical": "/data/settings/tqsdk_scheduler", "mode": "alias"},
    {"legacy": "/ops/build/build", "canonical": "/data/build/build", "mode": "alias"},
    {"legacy": "/ops/build/main_schedule", "canonical": "/data/build/main_schedule", "mode": "alias"},
    {"legacy": "/ops/build/main_l5", "canonical": "/data/build/main_l5", "mode": "alias"},
    {"legacy": "/ops/model/train", "canonical": "/models/model/train", "mode": "alias"},
    {"legacy": "/ops/model/sweep", "canonical": "/models/model/sweep", "mode": "alias"},
    {"legacy": "/ops/eval/benchmark", "canonical": "/models/eval/benchmark", "mode": "alias"},
    {"legacy": "/ops/eval/compare", "canonical": "/models/eval/compare", "mode": "alias"},
    {"legacy": "/ops/eval/backtest", "canonical": "/models/eval/backtest", "mode": "alias"},
    {"legacy": "/ops/eval/paper", "canonical": "/models/eval/paper", "mode": "alias"},
    {"legacy": "/ops/eval/daily_train", "canonical": "/models/eval/daily_train", "mode": "alias"},
    {"legacy": "/ops/ingest/download", "canonical": "/data/ingest/download", "mode": "removed_410"},
    {"legacy": "/ops/ingest/download_contract_range", "canonical": "/data/ingest/download_contract_range", "mode": "removed_410"},
    {"legacy": "/ops/ingest/record", "canonical": "/data/ingest/record", "mode": "removed_410"},
    {"legacy": "/ops/ingest/update_variety", "canonical": "/data/ingest/update_variety", "mode": "removed_410"},
    {"legacy": "/ops/integrity/audit", "canonical": "/data/integrity/audit", "mode": "removed_410"},
]


def canonical_for_legacy(path: str) -> str | None:
    p = str(path or "").strip()
    for rule in OPS_COMPAT_RULES:
        if rule["legacy"] == p:
            return rule["canonical"]
    return None


def ops_compat_contract() -> dict[str, Any]:
    return {
        "version": OPS_COMPAT_VERSION,
        "policy": "compatibility-layer-only",
        "canonical_roots": ["/data", "/models", "/trading"],
        "legacy_prefix": "/ops",
        "rules": list(OPS_COMPAT_RULES),
    }
