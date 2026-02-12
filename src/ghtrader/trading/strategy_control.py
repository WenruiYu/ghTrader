"""
Strategy control-plane artifacts (PRD §5.11.1).

This module provides a stable per-account-profile directory for StrategyRunner supervision
and dashboard reads.

Layout:
  runs/strategy/account=<PROFILE>/
    desired.json   # written by dashboard/operators (desired config)
    state.json     # atomic fast-read (written by StrategyRunner)
    events.jsonl   # append-only events (written by StrategyRunner)
"""

from __future__ import annotations

import json
from collections import deque
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Literal

import redis

from ghtrader.util.json_io import read_json, write_json_atomic
from ghtrader.tq.runtime import canonical_account_profile

StrategyMode = Literal["idle", "run"]

STRATEGY_DESIRED_SCHEMA_VERSION = 1
STRATEGY_STATE_SCHEMA_VERSION = 1


from ghtrader.util.time import now_iso as _now_iso


from ghtrader.util.safe_parse import safe_float as _safe_float, safe_int as _safe_int


def _jsonable(obj: Any) -> Any:
    if obj is None:
        return None
    if isinstance(obj, (str, int, float, bool)):
        return obj
    if isinstance(obj, dict):
        return {str(k): _jsonable(v) for k, v in obj.items()}
    if isinstance(obj, (list, tuple)):
        return [_jsonable(v) for v in obj]
    try:
        if hasattr(obj, "item"):
            return _jsonable(obj.item())  # type: ignore[attr-defined]
    except Exception:
        pass
    return str(obj)


def strategy_root(*, runs_dir: Path, profile: str) -> Path:
    p = canonical_account_profile(profile)
    return runs_dir / "strategy" / f"account={p}"


def strategy_desired_path(*, runs_dir: Path, profile: str) -> Path:
    return strategy_root(runs_dir=runs_dir, profile=profile) / "desired.json"


def strategy_state_path(*, runs_dir: Path, profile: str) -> Path:
    return strategy_root(runs_dir=runs_dir, profile=profile) / "state.json"


def strategy_events_path(*, runs_dir: Path, profile: str) -> Path:
    return strategy_root(runs_dir=runs_dir, profile=profile) / "events.jsonl"


@dataclass(frozen=True)
class StrategyDesired:
    mode: StrategyMode = "idle"
    symbols: list[str] | None = None
    model_name: str = "xgboost"
    horizon: int = 50
    threshold_up: float = 0.6
    threshold_down: float = 0.6
    position_size: int = 1
    artifacts_dir: str = "artifacts"
    poll_interval_sec: float = 0.5

    def symbols_list(self) -> list[str]:
        if not self.symbols:
            return []
        out: list[str] = []
        for s in self.symbols:
            ss = str(s or "").strip()
            if ss and ss not in out:
                out.append(ss)
        return out


def _parse_mode(x: Any) -> StrategyMode:
    s = str(x or "").strip().lower()
    if s in {"idle", "run"}:
        return s  # type: ignore[return-value]
    if s in {"running", "active", "on"}:
        return "run"
    return "idle"


def _parse_strategy_desired_cfg(cfg: dict[str, Any]) -> StrategyDesired:
    mode = _parse_mode(cfg.get("mode"))
    symbols = list(cfg.get("symbols")) if isinstance(cfg.get("symbols"), list) else None
    model_name = str(cfg.get("model_name") or "xgboost").strip() or "xgboost"
    horizon = int(_safe_int(cfg.get("horizon")) or 50)
    threshold_up = float(_safe_float(cfg.get("threshold_up")) or 0.6)
    threshold_down = float(_safe_float(cfg.get("threshold_down")) or 0.6)
    position_size = int(_safe_int(cfg.get("position_size")) or 1)
    artifacts_dir = str(cfg.get("artifacts_dir") or "artifacts").strip() or "artifacts"
    poll_interval_sec = float(_safe_float(cfg.get("poll_interval_sec")) or 0.5)
    return StrategyDesired(
        mode=mode,
        symbols=symbols,
        model_name=model_name,
        horizon=horizon,
        threshold_up=threshold_up,
        threshold_down=threshold_down,
        position_size=position_size,
        artifacts_dir=artifacts_dir,
        poll_interval_sec=poll_interval_sec,
    )


def read_strategy_desired(*, runs_dir: Path, profile: str, redis_client: redis.Redis | None = None) -> StrategyDesired:
    """
    Read desired state (Redis preferred, file fallback); returns defaults if missing/invalid.
    """
    prof = canonical_account_profile(profile)
    if redis_client is not None:
        try:
            raw = redis_client.get(f"ghtrader:strategy:desired:{prof}")
            if raw:
                obj = json.loads(raw)
                if isinstance(obj, dict):
                    cfg = obj.get("desired") if isinstance(obj.get("desired"), dict) else obj
                    if isinstance(cfg, dict):
                        return _parse_strategy_desired_cfg(cfg)
        except Exception:
            pass

    p = strategy_desired_path(runs_dir=runs_dir, profile=profile)
    obj = read_json(p)
    if not isinstance(obj, dict):
        return StrategyDesired()
    cfg = obj.get("desired") if isinstance(obj.get("desired"), dict) else obj
    try:
        return _parse_strategy_desired_cfg(cfg)
    except Exception:
        return StrategyDesired()


def write_strategy_desired(*, runs_dir: Path, profile: str, desired: StrategyDesired, redis_client: redis.Redis | None = None) -> None:
    root = strategy_root(runs_dir=runs_dir, profile=profile)
    prof = canonical_account_profile(profile)
    payload: dict[str, Any] = {
        "schema_version": int(STRATEGY_DESIRED_SCHEMA_VERSION),
        "updated_at": _now_iso(),
        "account_profile": prof,
        "desired": _jsonable(
            {
                "mode": desired.mode,
                "symbols": desired.symbols_list(),
                "model_name": desired.model_name,
                "horizon": int(desired.horizon),
                "threshold_up": float(desired.threshold_up),
                "threshold_down": float(desired.threshold_down),
                "position_size": int(desired.position_size),
                "artifacts_dir": str(desired.artifacts_dir),
                "poll_interval_sec": float(desired.poll_interval_sec),
            }
        ),
    }
    write_json_atomic(root / "desired.json", payload)
    try:
        rc = redis_client
        if rc is None:
            rc = redis.Redis(host="localhost", port=6379, db=0, decode_responses=True)
        rc.set(f"ghtrader:strategy:desired:{prof}", json.dumps(payload, default=str))
        rc.publish(f"ghtrader:strategy:updates:{prof}", json.dumps(payload, default=str))
    except Exception:
        pass


class StrategyStateWriter:
    """
    Writes a stable per-profile state surface for the dashboard and supervisor.
    """

    def __init__(self, *, runs_dir: Path, profile: str, recent_events_max: int = 50, redis_client: redis.Redis | None = None) -> None:
        self.runs_dir = runs_dir
        self.profile = canonical_account_profile(profile)
        self.redis_client = redis_client
        self.root = strategy_root(runs_dir=runs_dir, profile=profile)
        self.root.mkdir(parents=True, exist_ok=True)

        self._state_path = strategy_state_path(runs_dir=runs_dir, profile=profile)
        self._evt_path = strategy_events_path(runs_dir=runs_dir, profile=profile)

        self._health: dict[str, Any] = {"ok": False, "running": False, "error": "", "last_loop_at": ""}
        self._effective: dict[str, Any] = {}
        self._last_targets: dict[str, Any] = {}
        self._last_meta: dict[str, Any] = {}
        self._recent_events: deque[dict[str, Any]] = deque(maxlen=int(max(1, recent_events_max)))

    def set_health(self, **fields: Any) -> None:
        self._health.update(fields)

    def set_effective(self, **fields: Any) -> None:
        self._effective.update(fields)

    def set_targets(self, targets: dict[str, Any] | None, meta: dict[str, Any] | None) -> None:
        self._last_targets = dict(targets or {})
        self._last_meta = dict(meta or {})

    def append_event(self, evt: dict[str, Any]) -> None:
        e2 = {"ts": _now_iso(), **(evt or {})}
        try:
            with open(self._evt_path, "a", encoding="utf-8") as f:
                f.write(json.dumps(_jsonable(e2), ensure_ascii=False, default=str) + "\n")
        except Exception:
            pass
        try:
            self._recent_events.append(dict(e2))
        except Exception:
            pass
        self.flush_state()

    def flush_state(self) -> None:
        payload = {
            "schema_version": int(STRATEGY_STATE_SCHEMA_VERSION),
            "updated_at": _now_iso(),
            "account_profile": self.profile,
            "health": _jsonable(self._health),
            "effective": _jsonable(self._effective),
            "last_targets": _jsonable(self._last_targets),
            "last_meta": _jsonable(self._last_meta),
            "recent_events": _jsonable(list(self._recent_events)),
        }
        if self.redis_client:
            try:
                self.redis_client.publish(f"ghtrader:strategy:updates:{self.profile}", json.dumps(payload, default=str))
                self.redis_client.set(f"ghtrader:strategy:state:{self.profile}", json.dumps(payload, default=str))
            except Exception:
                pass
        try:
            write_json_atomic(self._state_path, payload)
        except Exception:
            return None


__all__ = [
    "StrategyDesired",
    "StrategyMode",
    "StrategyStateWriter",
    "read_strategy_desired",
    "strategy_desired_path",
    "strategy_events_path",
    "strategy_root",
    "strategy_state_path",
    "write_strategy_desired",
]

