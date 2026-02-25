"""
StrategyRunner (AI) for AccountGateway (PRD §5.11.1).

This module must NOT import `tqsdk` directly. It consumes market snapshots from
AccountGateway over the local ZMQ hot path, and writes targets to:
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
import zmq
import redis

from ghtrader.util.json_io import read_json, write_json_atomic
from ghtrader.util.observability import get_store
from ghtrader.research.models import load_model
from ghtrader.trading.strategy_control import StrategyStateWriter

log = structlog.get_logger()
_obs_store = get_store("trading.strategy_runner")

ModelType = Literal["logistic", "xgboost", "lightgbm", "deeplob", "transformer", "tcn", "tlob", "ssm"]
HotPathState = Literal["starting", "healthy", "suspect", "degraded"]


from ghtrader.util.time import now_iso as _now_iso

def _canonical_profile(p: str) -> str:
    try:
        from ghtrader.tq.runtime import canonical_account_profile

        return canonical_account_profile(p)
    except Exception:
        return str(p or "").strip().lower() or "default"


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


def _ts_to_iso(ts: float | None) -> str:
    if ts is None:
        return ""
    try:
        return datetime.fromtimestamp(float(ts), tz=timezone.utc).isoformat().replace("+00:00", "Z")
    except Exception:
        return ""


@dataclass
class _ZmqConnectionHealthFsm:
    """
    Stabilize ZMQ hot-path health transitions to avoid one-tick flapping.
    """

    enter_degraded_after_failures: int = 3
    recover_after_successes: int = 2
    state: HotPathState = "starting"
    consecutive_failures: int = 0
    consecutive_successes: int = 0
    last_success_at: float | None = None
    last_failure_at: float | None = None
    last_reason: str = ""

    def __post_init__(self) -> None:
        self.enter_degraded_after_failures = max(1, int(self.enter_degraded_after_failures))
        self.recover_after_successes = max(1, int(self.recover_after_successes))
        s = str(self.state or "starting").strip().lower()
        if s not in {"starting", "healthy", "suspect", "degraded"}:
            s = "starting"
        self.state = s  # type: ignore[assignment]

    def record_failure(self, *, reason: str, now_ts: float | None = None) -> bool:
        prev = self.state
        now_s = float(time.time() if now_ts is None else now_ts)
        self.last_failure_at = now_s
        self.last_reason = str(reason or "")
        self.consecutive_failures = int(self.consecutive_failures) + 1
        self.consecutive_successes = 0
        if prev == "degraded":
            # Stay degraded until recover_after_successes is satisfied.
            self.state = "degraded"
        elif int(self.consecutive_failures) >= int(self.enter_degraded_after_failures):
            self.state = "degraded"
        else:
            self.state = "suspect"
        return self.state != prev

    def record_success(self, *, now_ts: float | None = None) -> bool:
        prev = self.state
        now_s = float(time.time() if now_ts is None else now_ts)
        self.last_success_at = now_s
        self.consecutive_successes = int(self.consecutive_successes) + 1
        self.consecutive_failures = 0
        if self.state == "degraded":
            if int(self.consecutive_successes) >= int(self.recover_after_successes):
                self.state = "healthy"
        else:
            self.state = "healthy"
        if self.state == "healthy":
            self.last_reason = ""
        return self.state != prev


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
    - reads gateway market ticks from ZMQ hot path
    - computes features + model inference
    - writes targets.json for the gateway to consume
    - handles symbol roll detection (resets FactorEngine state on underlying change)
    """
    prof = _canonical_profile(cfg.account_profile)
    
    # ZMQ Setup
    zmq_ctx = zmq.Context()
    sub_socket = zmq_ctx.socket(zmq.SUB)
    sub_socket.connect(f"ipc:///tmp/ghtrader_gateway_pub_{prof}.ipc")
    sub_socket.setsockopt_string(zmq.SUBSCRIBE, "")

    req_endpoint = f"ipc:///tmp/ghtrader_gateway_rep_{prof}.ipc"

    def _new_req_socket() -> Any:
        s = zmq_ctx.socket(zmq.REQ)
        s.connect(req_endpoint)
        s.setsockopt(zmq.LINGER, 0)
        s.setsockopt(zmq.SNDTIMEO, 100)
        s.setsockopt(zmq.RCVTIMEO, 100)
        try:
            s.setsockopt(zmq.REQ_RELAXED, 1)
            s.setsockopt(zmq.REQ_CORRELATE, 1)
        except Exception:
            pass
        return s

    req_socket = _new_req_socket()

    requested_symbols = [str(s).strip() for s in (cfg.symbols or []) if str(s).strip()]
    if not requested_symbols:
        raise ValueError("symbols is required")

    from ghtrader.datasets.features import FactorEngine

    def _connect_local_redis() -> redis.Redis | None:
        try:
            rc = redis.Redis(host="localhost", port=6379, db=0, decode_responses=True)
            rc.ping()
            return rc
        except Exception:
            return None

    # Redis Setup (Warm Path)
    redis_client = _connect_local_redis()
    warm_path_degraded = redis_client is None
    warm_path_reason = "redis_unavailable" if warm_path_degraded else ""
    redis_retry_interval_s = 3.0
    redis_healthcheck_interval_s = 5.0
    last_redis_retry_at = 0.0
    last_redis_healthcheck_at = 0.0

    writer = StrategyWriter(runs_dir=cfg.runs_dir, account_profile=prof)
    statew = StrategyStateWriter(runs_dir=cfg.runs_dir, profile=prof, redis_client=redis_client)
    writer.write_config(
        {
            "account_profile": prof,
            "symbols": list(requested_symbols),
            "model_name": cfg.model_name,
            "horizon": int(cfg.horizon),
            "threshold_up": float(cfg.threshold_up),
            "threshold_down": float(cfg.threshold_down),
            "position_size": int(cfg.position_size),
            "artifacts_dir": str(cfg.artifacts_dir),
        }
    )
    writer.event({"type": "strategy_start"})
    hot_path_fsm = _ZmqConnectionHealthFsm()
    statew.set_health(ok=True, running=True, error="", last_loop_at=_now_iso())
    statew.set_effective(
        run_id=writer.run_id,
        run_root=str(writer.root),
        account_profile=prof,
        symbols=list(requested_symbols),
        model_name=str(cfg.model_name),
        horizon=int(cfg.horizon),
        threshold_up=float(cfg.threshold_up),
        threshold_down=float(cfg.threshold_down),
        position_size=int(cfg.position_size),
        artifacts_dir=str(cfg.artifacts_dir),
        poll_interval_sec=float(cfg.poll_interval_sec),
        warm_path_degraded=bool(warm_path_degraded),
        warm_path_reason=str(warm_path_reason),
        hot_path_state=str(hot_path_fsm.state),
        hot_path_consecutive_failures=0,
        hot_path_consecutive_successes=0,
        hot_path_last_success_at="",
        hot_path_last_failure_at="",
        no_new_targets=False,
        halt_reason="",
    )
    statew.append_event({"type": "strategy_start", "run_id": writer.run_id})

    def _sync_hot_path_state(*, prev_state: HotPathState, reason: str) -> None:
        halt_active = str(hot_path_fsm.state) == "degraded"
        halt_reason = str(hot_path_fsm.last_reason or reason or "") if halt_active else ""
        statew.set_effective(
            hot_path_state=str(hot_path_fsm.state),
            hot_path_consecutive_failures=int(hot_path_fsm.consecutive_failures),
            hot_path_consecutive_successes=int(hot_path_fsm.consecutive_successes),
            hot_path_last_success_at=_ts_to_iso(hot_path_fsm.last_success_at),
            hot_path_last_failure_at=_ts_to_iso(hot_path_fsm.last_failure_at),
            no_new_targets=bool(halt_active),
            halt_reason=str(halt_reason),
        )
        if prev_state == hot_path_fsm.state:
            return
        evt = {
            "type": "hot_path_state_changed",
            "from": str(prev_state),
            "to": str(hot_path_fsm.state),
            "reason": str(reason or hot_path_fsm.last_reason or ""),
            "consecutive_failures": int(hot_path_fsm.consecutive_failures),
            "consecutive_successes": int(hot_path_fsm.consecutive_successes),
        }
        writer.event(evt)
        statew.append_event(evt)
        if prev_state != "degraded" and hot_path_fsm.state == "degraded":
            halt_evt = {"type": "safe_halt_entered", "reason": str(halt_reason or "gateway_hot_path_degraded")}
            writer.event(halt_evt)
            statew.append_event(halt_evt)
        elif prev_state == "degraded" and hot_path_fsm.state != "degraded":
            clear_evt = {"type": "safe_halt_cleared", "reason": "gateway_stream_restored"}
            writer.event(clear_evt)
            statew.append_event(clear_evt)

    def _set_warm_path_state(*, degraded: bool, reason: str = "") -> None:
        nonlocal warm_path_degraded, warm_path_reason
        degraded_b = bool(degraded)
        reason_s = str(reason or "") if degraded_b else ""
        changed = (degraded_b != warm_path_degraded) or (reason_s != warm_path_reason)
        warm_path_degraded = degraded_b
        warm_path_reason = reason_s
        statew.set_effective(warm_path_degraded=bool(warm_path_degraded), warm_path_reason=str(warm_path_reason))
        if changed:
            evt = {"type": "warm_path_state", "degraded": bool(warm_path_degraded), "reason": str(warm_path_reason)}
            writer.event(evt)
            statew.append_event(evt)

    def _ensure_redis_connection() -> None:
        nonlocal redis_client, last_redis_retry_at, last_redis_healthcheck_at
        now_t = time.time()
        if redis_client is not None and (now_t - float(last_redis_healthcheck_at)) >= float(redis_healthcheck_interval_s):
            last_redis_healthcheck_at = now_t
            try:
                redis_client.ping()
            except Exception:
                redis_client = None
                statew.redis_client = None
                _set_warm_path_state(degraded=True, reason="redis_ping_failed")

        if redis_client is not None:
            return
        if (now_t - float(last_redis_retry_at)) < float(redis_retry_interval_s):
            return
        last_redis_retry_at = now_t
        rc = _connect_local_redis()
        if rc is None:
            _set_warm_path_state(degraded=True, reason="redis_unavailable")
            return
        redis_client = rc
        statew.redis_client = rc
        _set_warm_path_state(degraded=False, reason="")
        evt = {"type": "warm_path_recovered", "component": "redis"}
        writer.event(evt)
        statew.append_event(evt)

    # Models + factor specs (keyed by requested symbol for artifact lookup)
    models: dict[str, Any] = {}
    enabled_factors: dict[str, list[str]] = {}
    engines: dict[str, Any] = {}

    def _init_engines_for_symbol(sym: str) -> None:
        """Initialize or reinitialize model and engine for a requested symbol."""
        mdir = cfg.artifacts_dir / sym / cfg.model_name
        mp = _find_model_path(model_dir=mdir, horizon=cfg.horizon)
        models[sym] = load_model(cfg.model_name, mp)  # type: ignore[arg-type]
        enabled_factors[sym] = _read_enabled_factors(model_dir=mdir, horizon=cfg.horizon)
        engines[sym] = FactorEngine(enabled_factors=enabled_factors[sym])
        writer.event({"type": "model_loaded", "symbol": sym, "path": str(mp), "n_factors": len(enabled_factors[sym])})
        statew.append_event({"type": "model_loaded", "symbol": sym, "path": str(mp), "n_factors": len(enabled_factors[sym])})

    for sym in requested_symbols:
        _init_engines_for_symbol(sym)

    last_tick_dt: dict[str, str] = {s: "" for s in requested_symbols}
    last_targets: dict[str, int] = {}

    # Track execution symbols from gateway for roll detection
    last_symbol_mapping: dict[str, str] = {}  # requested_symbol -> execution_symbol

    last_zmq_timeout_event_at = 0.0
    while True:
        loop_started = time.perf_counter()
        _ensure_redis_connection()
        try:
            # ZMQ Poll (Hot Path)
            if not sub_socket.poll(timeout=1000):
                now_ts = time.time()
                statew.set_health(ok=False, running=True, error="gateway_zmq_timeout", last_loop_at=_now_iso())
                prev_state: HotPathState = hot_path_fsm.state
                hot_path_fsm.record_failure(reason="gateway_zmq_timeout", now_ts=now_ts)
                _sync_hot_path_state(prev_state=prev_state, reason="gateway_zmq_timeout")
                if (now_ts - last_zmq_timeout_event_at) >= 5.0:
                    evt = {"type": "gateway_state_timeout", "channel": "zmq_hot_path"}
                    writer.event(evt)
                    statew.append_event(evt)
                    last_zmq_timeout_event_at = now_ts
                _obs_store.observe(metric="loop", latency_s=(time.perf_counter() - loop_started), ok=False)
                _obs_store.observe(metric="gateway_state_timeout", ok=False)
                statew.flush_state()
                continue
            msg = sub_socket.recv_json()
            st = msg.get("payload", {})
            statew.set_health(ok=True, running=True, error="", last_loop_at=_now_iso())
            prev_state = hot_path_fsm.state
            hot_path_fsm.record_success(now_ts=time.time())
            _sync_hot_path_state(prev_state=prev_state, reason="gateway_stream_restored")
            market = st.get("market") if isinstance(st.get("market"), dict) else {}
            ticks = market.get("ticks") if isinstance(market.get("ticks"), dict) else {}
            effective = st.get("effective") if isinstance(st.get("effective"), dict) else {}
            symbol_mapping = effective.get("symbol_mapping") if isinstance(effective.get("symbol_mapping"), dict) else {}
        except Exception as e:
            ticks = {}
            symbol_mapping = {}
            _obs_store.observe(metric="gateway_state_read", ok=False)
            statew.set_health(ok=False, running=True, error=str(e), last_loop_at=_now_iso())
            prev_state = hot_path_fsm.state
            hot_path_fsm.record_failure(reason="gateway_state_read_failed", now_ts=time.time())
            _sync_hot_path_state(prev_state=prev_state, reason="gateway_state_read_failed")
            statew.append_event({"type": "gateway_state_read_failed", "error": str(e)})

        # Detect roll: if execution symbol changed for any requested symbol, reset engines
        for req_sym in requested_symbols:
            prev_exec = last_symbol_mapping.get(req_sym, "")
            curr_exec = symbol_mapping.get(req_sym, "")
            if prev_exec and curr_exec and prev_exec != curr_exec:
                # Roll detected: reset FactorEngine state for this symbol
                writer.event({
                    "type": "symbol_roll_reset",
                    "requested_symbol": req_sym,
                    "old_execution": prev_exec,
                    "new_execution": curr_exec,
                })
                statew.append_event({
                    "type": "symbol_roll_reset",
                    "requested_symbol": req_sym,
                    "old_execution": prev_exec,
                    "new_execution": curr_exec,
                })
                try:
                    _init_engines_for_symbol(req_sym)
                    last_tick_dt[req_sym] = ""  # Force re-process
                except Exception as e:
                    writer.event({"type": "roll_reset_failed", "symbol": req_sym, "error": str(e)})
                    statew.append_event({"type": "roll_reset_failed", "symbol": req_sym, "error": str(e)})
        last_symbol_mapping = dict(symbol_mapping) if symbol_mapping else {}

        desired_targets: dict[str, int] = {}
        probs_meta: dict[str, Any] = {}

        for req_sym in requested_symbols:
            # Get ticks using execution symbol from mapping (gateway writes ticks by execution symbol)
            exec_sym = symbol_mapping.get(req_sym, req_sym)
            tick = ticks.get(exec_sym)
            if not isinstance(tick, dict):
                continue
            dt = str(tick.get("datetime") or tick.get("datetime_ns") or "")
            if dt and dt == last_tick_dt.get(req_sym):
                continue
            last_tick_dt[req_sym] = dt

            try:
                # Use req_sym for model/engine lookup (artifacts stored by requested symbol)
                feats = engines[req_sym].compute_incremental(tick)
                # Stable order: enabled_factors list
                xs = [float(feats.get(k) or 0.0) for k in enabled_factors[req_sym]]
                X = np.asarray(xs, dtype=float).reshape(1, -1)
                X = np.nan_to_num(X, nan=0.0)
                probs = models[req_sym].predict_proba(X)
                p = probs[-1] if hasattr(probs, "__len__") else probs
                p = np.asarray(p, dtype=float).reshape(-1)
                probs_meta[exec_sym] = [float(x) for x in p[:3].tolist()] if p.size >= 3 else []
                # Targets keyed by execution symbol (gateway executes on these)
                desired_targets[exec_sym] = compute_target(
                    p,
                    threshold_up=float(cfg.threshold_up),
                    threshold_down=float(cfg.threshold_down),
                    position_size=int(cfg.position_size),
                )
            except Exception as e:
                writer.event({"type": "predict_failed", "symbol": req_sym, "exec_symbol": exec_sym, "error": str(e)})
                statew.append_event({"type": "predict_failed", "symbol": req_sym, "exec_symbol": exec_sym, "error": str(e)})
                _obs_store.observe(metric="predict", ok=False)
                desired_targets[exec_sym] = 0

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
            
            # ZMQ Send Targets
            try:
                req_socket.send_json({"type": "set_targets", "targets": {str(k): int(v) for k, v in last_targets.items()}})
                ack = req_socket.recv_json()
                if not (isinstance(ack, dict) and str(ack.get("status") or "").lower() == "ok"):
                    raise RuntimeError(f"gateway_ack_error: {ack}")
                _obs_store.observe(metric="gateway_ack", ok=True)
            except Exception as e:
                writer.event({"type": "gateway_ack_failed", "error": str(e)})
                statew.append_event({"type": "gateway_ack_failed", "error": str(e)})
                _obs_store.observe(metric="gateway_ack", ok=False)
                try:
                    req_socket.close(0)
                except Exception:
                    pass
                req_socket = _new_req_socket()

            writer.event({"type": "target_change", "targets": dict(last_targets)})
            statew.set_targets(dict(last_targets), dict(meta))
            statew.append_event({"type": "target_change", "targets": dict(last_targets)})
        else:
            # Keep state fresh even when targets don't change.
            statew.flush_state()
        _obs_store.observe(metric="loop", latency_s=(time.perf_counter() - loop_started), ok=True)

        # No sleep in ZMQ mode (driven by SUB poll)
        # time.sleep(float(max(0.05, cfg.poll_interval_sec)))


__all__ = [
    "StrategyConfig",
    "compute_target",
    "gateway_targets_path",
    "run_strategy_runner",
    "write_gateway_targets",
]

