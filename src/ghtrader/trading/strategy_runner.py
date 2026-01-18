"""
StrategyRunner (AI) for AccountGateway (PRD §5.12.0).

This module must NOT import `tqsdk` directly. It consumes market snapshots produced by
AccountGateway under runs/gateway/account=<PROFILE>/state.json, and writes targets to:
  runs/gateway/account=<PROFILE>/targets.json
"""

from __future__ import annotations

import json
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Literal

import numpy as np
import structlog

from ghtrader.util.json_io import read_json, write_json_atomic
from ghtrader.research.models import load_model
from ghtrader.trading.strategy_control import StrategyStateWriter

log = structlog.get_logger()

ModelType = Literal["logistic", "xgboost", "lightgbm", "deeplob", "transformer", "tcn", "tlob", "ssm"]


from ghtrader.util.time import now_iso as _now_iso


from ghtrader.util.safe_parse import safe_float as _safe_float


def _canonical_profile(p: str) -> str:
    try:
        from ghtrader.tq.runtime import canonical_account_profile

        return canonical_account_profile(p)
    except Exception:
        return str(p or "").strip().lower() or "default"


def gateway_state_path(*, runs_dir: Path, account_profile: str) -> Path:
    prof = _canonical_profile(account_profile)
    return runs_dir / "gateway" / f"account={prof}" / "state.json"


def gateway_targets_path(*, runs_dir: Path, account_profile: str) -> Path:
    prof = _canonical_profile(account_profile)
    return runs_dir / "gateway" / f"account={prof}" / "targets.json"


@dataclass(frozen=True)
class StrategyConfig:
    account_profile: str
    symbols: list[str]
    model_name: ModelType = "xgboost"
    horizon: int = 50
    threshold_up: float = 0.6
    threshold_down: float = 0.6
    position_size: int = 1
    artifacts_dir: Path = Path("artifacts")
    runs_dir: Path = Path("runs")
    poll_interval_sec: float = 0.5


def compute_target(
    probs: np.ndarray,
    *,
    threshold_up: float,
    threshold_down: float,
    position_size: int,
) -> int:
    """
    Convert class probabilities into a target net position.

    Assumes 3-class: [down, flat, up].
    """
    p = np.asarray(probs, dtype=float).reshape(-1)
    if p.size < 3:
        return 0
    if float(p[2]) > float(threshold_up):
        return int(position_size)
    if float(p[0]) > float(threshold_down):
        return -int(position_size)
    return 0


def _find_model_path(*, model_dir: Path, horizon: int) -> Path:
    files = sorted([p for p in model_dir.glob(f"model_h{int(horizon)}.*") if not p.name.endswith(".meta.json")])
    if not files:
        raise FileNotFoundError(f"No model found under {model_dir} for horizon={horizon}")
    return files[0]


def _read_enabled_factors(*, model_dir: Path, horizon: int) -> list[str]:
    meta = read_json(model_dir / f"model_h{int(horizon)}.meta.json") or {}
    ef = meta.get("enabled_factors") if isinstance(meta, dict) else None
    if isinstance(ef, list) and ef and all(isinstance(x, str) and x.strip() for x in ef):
        return [str(x).strip() for x in ef if str(x).strip()]
    raise RuntimeError(f"Missing enabled_factors in model metadata: {model_dir}/model_h{int(horizon)}.meta.json")


class StrategyWriter:
    def __init__(self, *, runs_dir: Path, account_profile: str) -> None:
        self.runs_dir = runs_dir
        self.account_profile = _canonical_profile(account_profile)
        base = time.strftime("%Y%m%d_%H%M%S")
        self.run_id = f"{base}_{self.account_profile}"
        self.root = runs_dir / "strategy" / self.run_id
        self.root.mkdir(parents=True, exist_ok=True)
        self._evt = self.root / "events.jsonl"
        self._cfg = self.root / "run_config.json"

    def write_config(self, cfg: dict[str, Any]) -> None:
        payload = {"created_at": _now_iso(), **(cfg or {})}
        self._cfg.write_text(json.dumps(payload, ensure_ascii=False, indent=2, default=str), encoding="utf-8")

    def event(self, evt: dict[str, Any]) -> None:
        e2 = {"ts": _now_iso(), **(evt or {})}
        with open(self._evt, "a", encoding="utf-8") as f:
            f.write(json.dumps(e2, ensure_ascii=False, default=str) + "\n")


def write_gateway_targets(
    *,
    runs_dir: Path,
    account_profile: str,
    targets: dict[str, int],
    meta: dict[str, Any],
) -> None:
    p = gateway_targets_path(runs_dir=runs_dir, account_profile=account_profile)
    payload = {
        "schema_version": 1,
        "updated_at": _now_iso(),
        "account_profile": _canonical_profile(account_profile),
        "targets": {str(k): int(v) for k, v in (targets or {}).items()},
        "meta": dict(meta or {}),
    }
    write_json_atomic(p, payload)


def run_strategy_runner(cfg: StrategyConfig) -> None:
    """
    Long-running StrategyRunner loop:
    - reads gateway market ticks from state.json
    - computes features + model inference
    - writes targets.json for the gateway to consume
    """
    prof = _canonical_profile(cfg.account_profile)
    symbols = [str(s).strip() for s in (cfg.symbols or []) if str(s).strip()]
    if not symbols:
        raise ValueError("symbols is required")

    from ghtrader.datasets.features import FactorEngine

    writer = StrategyWriter(runs_dir=cfg.runs_dir, account_profile=prof)
    statew = StrategyStateWriter(runs_dir=cfg.runs_dir, profile=prof)
    writer.write_config(
        {
            "account_profile": prof,
            "symbols": list(symbols),
            "model_name": cfg.model_name,
            "horizon": int(cfg.horizon),
            "threshold_up": float(cfg.threshold_up),
            "threshold_down": float(cfg.threshold_down),
            "position_size": int(cfg.position_size),
            "artifacts_dir": str(cfg.artifacts_dir),
        }
    )
    writer.event({"type": "strategy_start"})
    statew.set_health(ok=True, running=True, error="", last_loop_at=_now_iso())
    statew.set_effective(
        run_id=writer.run_id,
        run_root=str(writer.root),
        account_profile=prof,
        symbols=list(symbols),
        model_name=str(cfg.model_name),
        horizon=int(cfg.horizon),
        threshold_up=float(cfg.threshold_up),
        threshold_down=float(cfg.threshold_down),
        position_size=int(cfg.position_size),
        artifacts_dir=str(cfg.artifacts_dir),
        poll_interval_sec=float(cfg.poll_interval_sec),
    )
    statew.append_event({"type": "strategy_start", "run_id": writer.run_id})

    # Models + factor specs
    models: dict[str, Any] = {}
    enabled_factors: dict[str, list[str]] = {}
    engines: dict[str, Any] = {}

    for sym in symbols:
        mdir = cfg.artifacts_dir / sym / cfg.model_name
        mp = _find_model_path(model_dir=mdir, horizon=cfg.horizon)
        models[sym] = load_model(cfg.model_name, mp)  # type: ignore[arg-type]
        enabled_factors[sym] = _read_enabled_factors(model_dir=mdir, horizon=cfg.horizon)
        engines[sym] = FactorEngine(enabled_factors=enabled_factors[sym])
        writer.event({"type": "model_loaded", "symbol": sym, "path": str(mp), "n_factors": len(enabled_factors[sym])})
        statew.append_event({"type": "model_loaded", "symbol": sym, "path": str(mp), "n_factors": len(enabled_factors[sym])})

    last_tick_dt: dict[str, str] = {s: "" for s in symbols}
    last_targets: dict[str, int] = {}

    while True:
        statew.set_health(ok=True, running=True, error="", last_loop_at=_now_iso())
        try:
            st_path = gateway_state_path(runs_dir=cfg.runs_dir, account_profile=prof)
            st = read_json(st_path) or {}
            market = st.get("market") if isinstance(st.get("market"), dict) else {}
            ticks = market.get("ticks") if isinstance(market.get("ticks"), dict) else {}
        except Exception as e:
            ticks = {}
            statew.set_health(ok=False, running=True, error=str(e), last_loop_at=_now_iso())
            statew.append_event({"type": "gateway_state_read_failed", "error": str(e)})

        desired_targets: dict[str, int] = {}
        probs_meta: dict[str, Any] = {}

        for sym in symbols:
            tick = ticks.get(sym)
            if not isinstance(tick, dict):
                continue
            dt = str(tick.get("datetime") or tick.get("datetime_ns") or "")
            if dt and dt == last_tick_dt.get(sym):
                continue
            last_tick_dt[sym] = dt

            try:
                feats = engines[sym].compute_incremental(tick)
                # Stable order: enabled_factors list
                xs = [float(feats.get(k) or 0.0) for k in enabled_factors[sym]]
                X = np.asarray(xs, dtype=float).reshape(1, -1)
                X = np.nan_to_num(X, nan=0.0)
                probs = models[sym].predict_proba(X)
                p = probs[-1] if hasattr(probs, "__len__") else probs
                p = np.asarray(p, dtype=float).reshape(-1)
                probs_meta[sym] = [float(x) for x in p[:3].tolist()] if p.size >= 3 else []
                desired_targets[sym] = compute_target(
                    p,
                    threshold_up=float(cfg.threshold_up),
                    threshold_down=float(cfg.threshold_down),
                    position_size=int(cfg.position_size),
                )
            except Exception as e:
                writer.event({"type": "predict_failed", "symbol": sym, "error": str(e)})
                statew.append_event({"type": "predict_failed", "symbol": sym, "error": str(e)})
                desired_targets[sym] = 0

        if desired_targets and desired_targets != last_targets:
            last_targets = dict(desired_targets)
            meta = {
                "strategy_run_id": writer.run_id,
                "model_name": cfg.model_name,
                "horizon": int(cfg.horizon),
                "threshold_up": float(cfg.threshold_up),
                "threshold_down": float(cfg.threshold_down),
                "position_size": int(cfg.position_size),
                "probs": probs_meta,
            }
            write_gateway_targets(runs_dir=cfg.runs_dir, account_profile=prof, targets=last_targets, meta=meta)
            writer.event({"type": "target_change", "targets": dict(last_targets)})
            statew.set_targets(dict(last_targets), dict(meta))
            statew.append_event({"type": "target_change", "targets": dict(last_targets)})
        else:
            # Keep state fresh even when targets don't change.
            statew.flush_state()

        time.sleep(float(max(0.05, cfg.poll_interval_sec)))


__all__ = [
    "StrategyConfig",
    "compute_target",
    "gateway_state_path",
    "gateway_targets_path",
    "run_strategy_runner",
    "write_gateway_targets",
]

