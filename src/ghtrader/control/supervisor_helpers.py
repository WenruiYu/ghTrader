from __future__ import annotations

from pathlib import Path
from typing import Any

from ghtrader.control.job_command import extract_cli_subcommands
from ghtrader.util.json_io import read_json


def argv_opt(argv: list[str], name: str) -> str | None:
    try:
        for i, t in enumerate(argv):
            if str(t) == str(name) and i + 1 < len(argv):
                return str(argv[i + 1])
    except Exception:
        return None
    return None


def is_gateway_job(argv: list[str]) -> bool:
    sub1, sub2 = extract_cli_subcommands(argv)
    return bool((sub1 or "") == "gateway" and (sub2 or "") in {"run"})


def is_strategy_job(argv: list[str]) -> bool:
    sub1, sub2 = extract_cli_subcommands(argv)
    return bool((sub1 or "") == "strategy" and (sub2 or "") in {"run"})


def scan_gateway_desired(*, runs_dir: Path) -> dict[str, str]:
    """
    Return mapping {profile -> desired_mode} for gateway profiles.

    Source: runs/gateway/account=<profile>/desired.json
    """
    root = runs_dir / "gateway"
    out: dict[str, str] = {}
    try:
        if not root.exists():
            return out
        for d in [p for p in root.iterdir() if p.is_dir() and p.name.startswith("account=")]:
            prof = d.name.split("=", 1)[-1] if "=" in d.name else d.name
            desired = read_json(d / "desired.json") or {}
            cfg = desired.get("desired") if isinstance(desired.get("desired"), dict) else desired
            mode = str((cfg or {}).get("mode") or "idle").strip().lower()
            if prof:
                out[str(prof)] = mode
    except Exception:
        return {}
    return out


def scan_strategy_desired(*, runs_dir: Path) -> dict[str, dict[str, Any]]:
    """
    Return mapping {profile -> desired_config_dict} for strategies.

    Source: runs/strategy/account=<profile>/desired.json
    """
    root = runs_dir / "strategy"
    out: dict[str, dict[str, Any]] = {}
    try:
        if not root.exists():
            return out
        for d in [p for p in root.iterdir() if p.is_dir() and p.name.startswith("account=")]:
            prof = d.name.split("=", 1)[-1] if "=" in d.name else d.name
            desired = read_json(d / "desired.json") or {}
            cfg = desired.get("desired") if isinstance(desired.get("desired"), dict) else desired
            cfg = cfg if isinstance(cfg, dict) else {}
            if prof:
                out[str(prof)] = dict(cfg)
    except Exception:
        return {}
    return out


__all__ = ["argv_opt", "is_gateway_job", "is_strategy_job", "scan_gateway_desired", "scan_strategy_desired"]

