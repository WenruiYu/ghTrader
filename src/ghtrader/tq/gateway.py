"""
AccountGateway (OMS/EMS) for TqSdk accounts (PRD §5.11.1).

This module intentionally contains direct TqSdk integration. Non-`ghtrader.tq.*` modules must not
import `tqsdk` directly.

Audit mirrors (file-based; local-only):
  runs/gateway/account=<PROFILE>/
    desired.json     # desired configuration (written by dashboard/operators)
    targets.json     # latest targets audit snapshot (written by StrategyRunner)
    commands.jsonl   # operator commands (append-only)
    state.json       # atomic last state (fast dashboard reads)
    events.jsonl     # append-only events
    snapshots.jsonl  # append-only account snapshots
"""

from __future__ import annotations

import json
import os
import time
from collections import deque
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Literal

import redis
import structlog
import zmq

from ghtrader.config import env_bool, env_int, get_tqsdk_auth, is_live_enabled, load_config
from ghtrader.util.json_io import read_json, write_json_atomic
from ghtrader.util.observability import get_store

from .runtime import canonical_account_profile, create_tq_account, snapshot_account_state, trading_day_from_ts_ns

log = structlog.get_logger()
_obs_store = get_store("trading.gateway")


def _resolve_symbols_for_execution(
    *,
    requested_symbols: list[str],
    data_dir: Path,
    previous_mapping: dict[str, str],
) -> tuple[dict[str, str], list[str], bool]:
    """
    Resolve requested symbols (which may include continuous aliases like KQ.m@SHFE.cu) to
    execution symbols (specific contracts like SHFE.cu2602).

    Returns:
        (mapping, execution_symbols, rolled) where:
        - mapping: {requested_symbol: execution_symbol}
        - execution_symbols: list of resolved symbols (for subscriptions)
        - rolled: True if any execution symbol changed vs previous_mapping
    """
    from ghtrader.trading.symbol_resolver import is_continuous_alias, resolve_trading_symbol

    mapping: dict[str, str] = {}
    execution_symbols: list[str] = []
    rolled = False

    for req_sym in requested_symbols:
        try:
            if is_continuous_alias(req_sym):
                exec_sym = resolve_trading_symbol(symbol=req_sym, data_dir=data_dir)
            else:
                exec_sym = req_sym
        except Exception as e:
            log.warning("symbol_resolution_failed", symbol=req_sym, error=str(e))
            # Fallback: use the requested symbol as-is (gateway may still work for specific contracts)
            exec_sym = req_sym

        mapping[req_sym] = exec_sym
        if exec_sym not in execution_symbols:
            execution_symbols.append(exec_sym)

        # Check for roll
        if previous_mapping.get(req_sym) and previous_mapping[req_sym] != exec_sym:
            rolled = True

    return mapping, execution_symbols, rolled

GatewayMode = Literal["idle", "paper", "sim", "live_monitor", "live_trade"]
ExecutorType = Literal["targetpos", "direct"]

DESIRED_SCHEMA_VERSION = 1
TARGETS_SCHEMA_VERSION = 1
STATE_SCHEMA_VERSION = 1


from ghtrader.util.time import now_iso as _now_iso


from ghtrader.util.safe_parse import safe_float as _safe_float, safe_int as _safe_int


def _jsonable(obj: Any) -> Any:
    """
    Best-effort conversion to JSON-serializable primitives (avoids numpy/pandas scalars).
    """
    if obj is None:
        return None
    if isinstance(obj, (str, int, float, bool)):
        return obj
    if isinstance(obj, dict):
        return {str(k): _jsonable(v) for k, v in obj.items()}
    if isinstance(obj, (list, tuple)):
        return [_jsonable(v) for v in obj]
    # Common scalar-ish types
    try:
        if hasattr(obj, "item"):
            return _jsonable(obj.item())  # type: ignore[attr-defined]
    except Exception:
        pass
    return str(obj)


def gateway_root(*, runs_dir: Path, profile: str) -> Path:
    p = canonical_account_profile(profile)
    return runs_dir / "gateway" / f"account={p}"


def desired_path(*, runs_dir: Path, profile: str) -> Path:
    return gateway_root(runs_dir=runs_dir, profile=profile) / "desired.json"


def targets_path(*, runs_dir: Path, profile: str) -> Path:
    return gateway_root(runs_dir=runs_dir, profile=profile) / "targets.json"


def commands_path(*, runs_dir: Path, profile: str) -> Path:
    return gateway_root(runs_dir=runs_dir, profile=profile) / "commands.jsonl"


def commands_cursor_path(*, runs_dir: Path, profile: str) -> Path:
    """
    Cursor file for idempotent commands.jsonl consumption across restarts.

    Stores the byte offset into commands.jsonl (append-only).
    """
    return gateway_root(runs_dir=runs_dir, profile=profile) / "commands.cursor.json"


def state_path(*, runs_dir: Path, profile: str) -> Path:
    return gateway_root(runs_dir=runs_dir, profile=profile) / "state.json"


def events_path(*, runs_dir: Path, profile: str) -> Path:
    return gateway_root(runs_dir=runs_dir, profile=profile) / "events.jsonl"


def snapshots_path(*, runs_dir: Path, profile: str) -> Path:
    return gateway_root(runs_dir=runs_dir, profile=profile) / "snapshots.jsonl"


@dataclass(frozen=True)
class GatewayDesired:
    mode: GatewayMode = "idle"
    # market symbols the gateway should subscribe to (execution symbols)
    symbols: list[str] | None = None
    executor: ExecutorType = "targetpos"
    sim_account: Literal["tqsim", "tqkq"] = "tqsim"
    # live trade gate (second step); required when mode=live_trade
    confirm_live: str = ""
    # minimal risk knobs (gateway enforces; StrategyRunner should not exceed)
    max_abs_position: int = 1
    max_order_size: int = 1
    max_ops_per_sec: int = 10
    max_daily_loss: float | None = None
    enforce_trading_time: bool = True

    def symbols_list(self) -> list[str]:
        if not self.symbols:
            return []
        out: list[str] = []
        for s in self.symbols:
            ss = str(s or "").strip()
            if ss and ss not in out:
                out.append(ss)
        return out


def _parse_gateway_mode(x: Any) -> GatewayMode:
    s = str(x or "").strip().lower()
    if s in {"idle", "paper", "sim", "live_monitor", "live_trade"}:
        return s  # type: ignore[return-value]
    # Back-compat: allow PRD-style tuples encoded as strings
    if s in {"live", "monitor", "monitor_only"}:
        return "live_monitor"
    return "idle"


def _parse_gateway_desired_cfg(cfg: dict[str, Any]) -> GatewayDesired:
    return GatewayDesired(
        mode=_parse_gateway_mode(cfg.get("mode")),
        symbols=list(cfg.get("symbols")) if isinstance(cfg.get("symbols"), list) else None,
        executor=str(cfg.get("executor") or "targetpos").strip().lower() in {"direct"} and "direct" or "targetpos",
        sim_account=str(cfg.get("sim_account") or "tqsim").strip().lower() in {"tqkq"} and "tqkq" or "tqsim",
        confirm_live=str(cfg.get("confirm_live") or "").strip(),
        max_abs_position=max(0, int(cfg.get("max_abs_position") or 1)),
        max_order_size=max(1, int(cfg.get("max_order_size") or 1)),
        max_ops_per_sec=max(1, int(cfg.get("max_ops_per_sec") or 10)),
        max_daily_loss=_safe_float(cfg.get("max_daily_loss")),
        enforce_trading_time=bool(cfg.get("enforce_trading_time")) if ("enforce_trading_time" in cfg) else True,
    )


def read_gateway_desired(*, runs_dir: Path, profile: str, redis_client: redis.Redis | None = None) -> GatewayDesired:
    """
    Read desired state (Redis preferred, file fallback); return defaults if missing/invalid.
    """
    prof = canonical_account_profile(profile)
    if redis_client is not None:
        try:
            raw = redis_client.get(f"ghtrader:gateway:desired:{prof}")
            if raw:
                obj = json.loads(raw)
                if isinstance(obj, dict):
                    cfg = obj.get("desired") if isinstance(obj.get("desired"), dict) else obj
                    if isinstance(cfg, dict):
                        return _parse_gateway_desired_cfg(cfg)
        except Exception:
            pass

    p = desired_path(runs_dir=runs_dir, profile=profile)
    obj = read_json(p)
    if not isinstance(obj, dict):
        return GatewayDesired()
    cfg = obj.get("desired") if isinstance(obj.get("desired"), dict) else obj

    try:
        return _parse_gateway_desired_cfg(cfg)
    except Exception:
        return GatewayDesired()


def write_gateway_desired(*, runs_dir: Path, profile: str, desired: GatewayDesired, redis_client: redis.Redis | None = None) -> None:
    root = gateway_root(runs_dir=runs_dir, profile=profile)
    prof = canonical_account_profile(profile)
    payload: dict[str, Any] = {
        "schema_version": int(DESIRED_SCHEMA_VERSION),
        "updated_at": _now_iso(),
        "account_profile": prof,
        "desired": _jsonable(
            {
                "mode": desired.mode,
                "symbols": desired.symbols_list(),
                "executor": desired.executor,
                "sim_account": desired.sim_account,
                "confirm_live": desired.confirm_live,
                "max_abs_position": int(desired.max_abs_position),
                "max_order_size": int(desired.max_order_size),
                "max_ops_per_sec": int(desired.max_ops_per_sec),
                "max_daily_loss": desired.max_daily_loss,
                "enforce_trading_time": bool(desired.enforce_trading_time),
            }
        ),
    }
    write_json_atomic(root / "desired.json", payload)
    try:
        rc = redis_client
        if rc is None:
            rc = redis.Redis(host="localhost", port=6379, db=0, decode_responses=True)
        rc.set(f"ghtrader:gateway:desired:{prof}", json.dumps(payload, default=str))
        rc.publish(f"ghtrader:gateway:updates:{prof}", json.dumps(payload, default=str))
    except Exception:
        pass


def read_gateway_targets(*, runs_dir: Path, profile: str) -> dict[str, Any]:
    """
    Read targets.json. Returns a dict:
      {updated_at, targets: {symbol: target_net}, meta: {...}}
    """
    p = targets_path(runs_dir=runs_dir, profile=profile)
    obj = read_json(p)
    if not isinstance(obj, dict):
        return {"schema_version": TARGETS_SCHEMA_VERSION, "updated_at": "", "targets": {}, "meta": {}}
    targets = obj.get("targets") if isinstance(obj.get("targets"), dict) else {}
    meta = obj.get("meta") if isinstance(obj.get("meta"), dict) else {}
    out_targets: dict[str, int] = {}
    for k, v in (targets or {}).items():
        sym = str(k or "").strip()
        if not sym:
            continue
        iv = _safe_int(v)
        if iv is None:
            continue
        out_targets[sym] = int(iv)
    return {"schema_version": int(obj.get("schema_version") or TARGETS_SCHEMA_VERSION), "updated_at": str(obj.get("updated_at") or ""), "targets": out_targets, "meta": dict(meta)}


class GatewayWriter:
    def __init__(self, *, runs_dir: Path, profile: str, pub_socket: zmq.Socket | None = None, redis_client: redis.Redis | None = None) -> None:
        self.runs_dir = runs_dir
        self.pub_socket = pub_socket
        self.redis_client = redis_client
        self.profile = canonical_account_profile(profile)
        self.root = gateway_root(runs_dir=runs_dir, profile=profile)
        self.root.mkdir(parents=True, exist_ok=True)

        self._state_path = state_path(runs_dir=runs_dir, profile=profile)
        self._evt_path = events_path(runs_dir=runs_dir, profile=profile)
        self._snap_path = snapshots_path(runs_dir=runs_dir, profile=profile)

        self._last_snapshot: dict[str, Any] | None = None
        self._recent_events: deque[dict[str, Any]] = deque(maxlen=50)
        self._health: dict[str, Any] = {"ok": False}
        self._effective: dict[str, Any] = {"mode": "idle"}
        self._market: dict[str, Any] = {}
        self._state_revision = self._load_last_state_revision()
        self._redis_publish_failures_total = 0
        self._redis_set_failures_total = 0
        self._redis_write_fail_streak = 0
        self._redis_last_write_error = ""
        self._redis_last_write_failed_at = ""

    def _load_last_state_revision(self) -> int:
        try:
            obj = read_json(self._state_path)
            if isinstance(obj, dict):
                rev = int(obj.get("state_revision") or 0)
                return int(rev) if int(rev) >= 0 else 0
        except Exception:
            pass
        return 0

    def set_health(self, **fields: Any) -> None:
        self._health.update(fields)

    def set_effective(self, **fields: Any) -> None:
        self._effective.update(fields)

    def set_market(self, market: dict[str, Any]) -> None:
        # Market data should be small and JSON-serializable.
        self._market = dict(market or {})

    def flush_state(self) -> None:
        self._write_state()

    def _write_state(self) -> None:
        self._state_revision = int(self._state_revision) + 1
        payload = {
            "schema_version": int(STATE_SCHEMA_VERSION),
            "state_revision": int(self._state_revision),
            "updated_at": _now_iso(),
            "account_profile": self.profile,
            "health": _jsonable(self._health),
            "effective": _jsonable(self._effective),
            "market": _jsonable(self._market),
            "last_snapshot": _jsonable(self._last_snapshot) if isinstance(self._last_snapshot, dict) else None,
            "recent_events": _jsonable(list(self._recent_events)),
        }
        if self.pub_socket:
            try:
                # Publish state to ZMQ subscribers
                self.pub_socket.send_json(payload)
            except Exception:
                pass
        
        redis_publish_failed = False
        redis_set_failed = False
        redis_last_error = ""
        if self.redis_client:
            try:
                # Publish to Redis channel for WebSocket
                self.redis_client.publish(f"ghtrader:gateway:updates:{self.profile}", json.dumps(payload, default=str))
            except Exception as e:
                redis_publish_failed = True
                redis_last_error = str(e)
                self._redis_publish_failures_total = int(self._redis_publish_failures_total) + 1
            try:
                # Set hot state key
                self.redis_client.set(f"ghtrader:gateway:state:{self.profile}", json.dumps(payload, default=str))
            except Exception as e:
                redis_set_failed = True
                redis_last_error = str(e)
                self._redis_set_failures_total = int(self._redis_set_failures_total) + 1
            if redis_publish_failed or redis_set_failed:
                self._redis_write_fail_streak = int(self._redis_write_fail_streak) + 1
                self._redis_last_write_error = str(redis_last_error or "redis_state_write_failed")
                self._redis_last_write_failed_at = _now_iso()
            else:
                self._redis_write_fail_streak = 0
                self._redis_last_write_error = ""

        try:
            write_json_atomic(self._state_path, payload)
        except Exception:
            return None

    def redis_write_status(self) -> dict[str, Any]:
        return {
            "publish_failures_total": int(self._redis_publish_failures_total),
            "set_failures_total": int(self._redis_set_failures_total),
            "fail_streak": int(self._redis_write_fail_streak),
            "last_error": str(self._redis_last_write_error),
            "last_failed_at": str(self._redis_last_write_failed_at),
        }

    def append_event(self, evt: dict[str, Any]) -> None:
        try:
            e2 = {"ts": _now_iso(), **(evt or {})}
        except Exception:
            e2 = {"ts": _now_iso(), "type": "event"}
        try:
            with open(self._evt_path, "a", encoding="utf-8") as f:
                f.write(json.dumps(_jsonable(e2), default=str) + "\n")
        except Exception:
            pass
        try:
            self._recent_events.append(dict(e2))
        except Exception:
            pass
        self._write_state()

    def append_snapshot(self, snap: dict[str, Any]) -> None:
        try:
            with open(self._snap_path, "a", encoding="utf-8") as f:
                f.write(json.dumps(_jsonable(snap), default=str) + "\n")
        except Exception:
            pass
        try:
            self._last_snapshot = dict(snap)
        except Exception:
            self._last_snapshot = None
        self._write_state()


def _read_commands_cursor(*, runs_dir: Path, profile: str) -> int:
    p = commands_cursor_path(runs_dir=runs_dir, profile=profile)
    obj = read_json(p)
    if not isinstance(obj, dict):
        return 0
    try:
        off = int(obj.get("offset") or 0)
    except Exception:
        off = 0
    return max(0, int(off))


def _write_commands_cursor(*, runs_dir: Path, profile: str, offset: int) -> None:
    p = commands_cursor_path(runs_dir=runs_dir, profile=profile)
    write_json_atomic(p, {"updated_at": _now_iso(), "offset": int(max(0, int(offset)))})


def risk_kill_state_path(*, runs_dir: Path, profile: str) -> Path:
    return gateway_root(runs_dir=runs_dir, profile=profile) / "risk_kill.json"


def _read_risk_kill_state(*, runs_dir: Path, profile: str) -> dict[str, Any]:
    obj = read_json(risk_kill_state_path(runs_dir=runs_dir, profile=profile))
    if not isinstance(obj, dict):
        return {"active": False, "reason": "", "updated_at": "", "activated_at": ""}
    return {
        "active": bool(obj.get("active")),
        "reason": str(obj.get("reason") or ""),
        "updated_at": str(obj.get("updated_at") or ""),
        "activated_at": str(obj.get("activated_at") or ""),
    }


def _write_risk_kill_state(*, runs_dir: Path, profile: str, active: bool, reason: str) -> dict[str, Any]:
    prev = _read_risk_kill_state(runs_dir=runs_dir, profile=profile)
    now_s = _now_iso()
    activated_at = str(prev.get("activated_at") or "")
    if bool(active):
        if not activated_at:
            activated_at = now_s
    else:
        activated_at = ""
    payload = {
        "schema_version": 1,
        "updated_at": now_s,
        "account_profile": canonical_account_profile(profile),
        "active": bool(active),
        "reason": str(reason or ""),
        "activated_at": activated_at,
    }
    write_json_atomic(risk_kill_state_path(runs_dir=runs_dir, profile=profile), payload)
    return payload


def _clear_risk_kill_state(*, runs_dir: Path, profile: str) -> None:
    p = risk_kill_state_path(runs_dir=runs_dir, profile=profile)
    try:
        if p.exists():
            p.unlink()
    except Exception:
        pass


def _connect_local_redis() -> redis.Redis | None:
    try:
        rc = redis.Redis(host="localhost", port=6379, db=0, decode_responses=True)
        rc.ping()
        return rc
    except Exception:
        return None


class _CommandIdDeduper:
    """Bounded duplicate-command filter with TTL + capacity limits."""

    def __init__(self, *, max_ids: int, ttl_s: float) -> None:
        self.max_ids = max(1, int(max_ids))
        self.ttl_s = max(1.0, float(ttl_s))
        self._ts_by_id: dict[str, float] = {}
        self._fifo: deque[tuple[float, str]] = deque()

    def _prune(self, *, now_ts: float) -> None:
        cutoff = float(now_ts) - float(self.ttl_s)
        while self._fifo and (self._fifo[0][0] < cutoff or len(self._ts_by_id) > self.max_ids):
            ts0, cid0 = self._fifo.popleft()
            cur = self._ts_by_id.get(cid0)
            if cur is None:
                continue
            # Remove only if this FIFO entry still represents the latest stamp for cid0.
            if float(cur) == float(ts0):
                del self._ts_by_id[cid0]

    def is_duplicate_and_remember(self, cid: str, *, now_ts: float | None = None) -> bool:
        cc = str(cid or "").strip()
        if not cc:
            return True
        now_s = float(time.time() if now_ts is None else now_ts)
        self._prune(now_ts=now_s)
        prev = self._ts_by_id.get(cc)
        if prev is not None and (now_s - float(prev)) <= float(self.ttl_s):
            return True
        self._ts_by_id[cc] = now_s
        self._fifo.append((now_s, cc))
        self._prune(now_ts=now_s)
        return False


def _should_emit_hot_path_heartbeat(*, now_ts: float, last_emit_at: float, interval_s: float) -> bool:
    try:
        interval = max(0.1, float(interval_s))
        return (float(now_ts) - float(last_emit_at)) >= interval
    except Exception:
        return True


def _close_api(api: Any) -> None:
    try:
        api.close()
    except Exception:
        return None


def run_gateway(
    *,
    account_profile: str,
    runs_dir: Path,
    snapshot_interval_sec: float = 10.0,
    poll_interval_sec: float = 0.2,
) -> None:
    """
    Long-running AccountGateway loop.

    MVP scope: monitoring + transparent state persistence.
    Execution/risk integration is added in later phases.
    """
    load_config()
    prof = canonical_account_profile(account_profile)

    # ZMQ Setup (Hot Path)
    zmq_ctx = zmq.Context()
    pub_socket = zmq_ctx.socket(zmq.PUB)
    pub_socket.bind(f"ipc:///tmp/ghtrader_gateway_pub_{prof}.ipc")
    
    rep_socket = zmq_ctx.socket(zmq.REP)
    rep_socket.bind(f"ipc:///tmp/ghtrader_gateway_rep_{prof}.ipc")

    # Redis Setup (Warm Path)
    redis_client = _connect_local_redis()

    writer = GatewayWriter(runs_dir=runs_dir, profile=prof, pub_socket=pub_socket, redis_client=redis_client)

    from ghtrader.trading.execution import RiskLimits, clamp_target_position

    from .execution import DirectOrderExecutor, TargetPosExecutor
    from .runtime import is_in_trading_time

    api = None
    account = None
    current_mode: GatewayMode = "idle"
    current_symbols: list[str] = []

    quotes: dict[str, Any] = {}
    tick_serials: dict[str, Any] = {}
    positions: dict[str, Any] = {}
    local_time_record: dict[str, float] = {}
    market: dict[str, Any] = {"ticks": {}, "quotes": {}}
    last_market_flush_at = 0.0

    exec_targetpos: TargetPosExecutor | None = None
    exec_direct: DirectOrderExecutor | None = None
    last_targets: dict[str, int] = {}
    zmq_targets: dict[str, int] = {}
    start_balance: float | None = None
    risk_kill_state = _read_risk_kill_state(runs_dir=runs_dir, profile=prof)
    risk_kill_active = bool(risk_kill_state.get("active"))
    risk_kill_reason = str(risk_kill_state.get("reason") or "")
    risk_kill_updated_at = str(risk_kill_state.get("updated_at") or "")
    risk_kill_pending_enforce = bool(risk_kill_active)
    warm_path_degraded = redis_client is None
    warm_path_reason = "redis_unavailable" if warm_path_degraded else ""
    redis_retry_interval_s = 3.0
    last_redis_retry_at = 0.0

    last_snapshot_at = 0.0
    last_wait_ok_at = 0.0

    # Symbol resolution: track mapping from requested (possibly continuous) to execution symbols
    symbol_mapping: dict[str, str] = {}  # requested_symbol -> execution_symbol
    last_resolution_at = 0.0
    resolution_interval_sec = 60.0  # Re-resolve periodically to catch trading day changes
    hot_path_heartbeat_interval_s = float(env_int("GHTRADER_GATEWAY_HOT_PATH_HEARTBEAT_S", 1))
    last_hot_path_heartbeat_at = 0.0

    # Get data_dir for symbol resolution
    from ghtrader.config import get_data_dir
    data_dir = get_data_dir()

    writer.set_health(ok=False, connected=False, last_wait_update_at="", error="")
    writer.set_effective(
        mode="idle",
        symbols=[],
        executor="targetpos",
        risk_kill_active=bool(risk_kill_active),
        risk_kill_reason=str(risk_kill_reason),
        risk_kill_updated_at=str(risk_kill_updated_at),
        warm_path_degraded=bool(warm_path_degraded),
        warm_path_reason=str(warm_path_reason),
    )
    writer.append_event({"type": "gateway_start"})
    if risk_kill_active:
        writer.append_event({"type": "risk_kill_rehydrated", "reason": risk_kill_reason or "persisted_state"})

    cmd_offset = _read_commands_cursor(runs_dir=runs_dir, profile=prof)
    redis_cmd_stream = f"ghtrader:commands:{prof}"
    redis_cmd_id = "$"
    cmd_dedupe_max_ids = int(env_int("GHTRADER_GATEWAY_CMD_DEDUPE_MAX_IDS", 20000))
    cmd_dedupe_ttl_s = float(env_int("GHTRADER_GATEWAY_CMD_DEDUPE_TTL_S", 24 * 3600))
    command_deduper = _CommandIdDeduper(max_ids=cmd_dedupe_max_ids, ttl_s=cmd_dedupe_ttl_s)
    redis_state_write_fail_streak_degrade = max(
        1,
        int(env_int("GHTRADER_GATEWAY_REDIS_WRITE_FAIL_STREAK_DEGRADE", 3)),
    )

    def _set_warm_path_state(*, degraded: bool, reason: str = "") -> None:
        nonlocal warm_path_degraded, warm_path_reason
        degraded_b = bool(degraded)
        reason_s = str(reason or "") if degraded_b else ""
        changed = (degraded_b != warm_path_degraded) or (reason_s != warm_path_reason)
        warm_path_degraded = degraded_b
        warm_path_reason = reason_s
        writer.set_effective(warm_path_degraded=bool(warm_path_degraded), warm_path_reason=str(warm_path_reason))
        if changed:
            writer.append_event(
                {
                    "type": "warm_path_state",
                    "degraded": bool(warm_path_degraded),
                    "reason": str(warm_path_reason),
                }
            )

    def _ensure_redis_connection() -> None:
        nonlocal redis_client, last_redis_retry_at
        if redis_client is not None:
            return
        now_t = time.time()
        if (now_t - float(last_redis_retry_at)) < float(redis_retry_interval_s):
            return
        last_redis_retry_at = now_t
        rc = _connect_local_redis()
        if rc is None:
            _set_warm_path_state(degraded=True, reason="redis_unavailable")
            return
        redis_client = rc
        writer.redis_client = redis_client
        _set_warm_path_state(degraded=False, reason="")
        writer.append_event({"type": "warm_path_recovered", "component": "redis"})

    def _sync_redis_state_write_health() -> None:
        stats = writer.redis_write_status()
        fail_streak = int(stats.get("fail_streak") or 0)
        writer.set_effective(
            redis_state_publish_failures_total=int(stats.get("publish_failures_total") or 0),
            redis_state_set_failures_total=int(stats.get("set_failures_total") or 0),
            redis_state_write_fail_streak=int(fail_streak),
            redis_state_last_error=str(stats.get("last_error") or ""),
            redis_state_last_failed_at=str(stats.get("last_failed_at") or ""),
        )
        if redis_client is not None and fail_streak >= int(redis_state_write_fail_streak_degrade):
            if not (bool(warm_path_degraded) and str(warm_path_reason) == "redis_state_write_failed"):
                _set_warm_path_state(degraded=True, reason="redis_state_write_failed")
            return
        if (
            redis_client is not None
            and fail_streak == 0
            and bool(warm_path_degraded)
            and str(warm_path_reason) == "redis_state_write_failed"
        ):
            _set_warm_path_state(degraded=False, reason="")

    def _enforce_risk_kill_actions() -> None:
        try:
            if exec_direct is not None:
                exec_direct.cancel_all_alive()
            if exec_targetpos is not None:
                for s in list(current_symbols):
                    exec_targetpos.set_target(s, 0)
        except Exception:
            pass
        try:
            write_gateway_desired(
                runs_dir=runs_dir,
                profile=prof,
                desired=GatewayDesired(mode="idle"),
                redis_client=redis_client,
            )
        except Exception:
            pass

    def _activate_risk_kill(*, reason: str, details: dict[str, Any] | None = None) -> None:
        nonlocal risk_kill_active, risk_kill_reason, risk_kill_updated_at, risk_kill_pending_enforce
        if risk_kill_active:
            return
        risk_kill_active = True
        risk_kill_pending_enforce = True
        risk_kill_reason = str(reason or "")
        st = _write_risk_kill_state(
            runs_dir=runs_dir,
            profile=prof,
            active=True,
            reason=str(risk_kill_reason),
        )
        risk_kill_updated_at = str(st.get("updated_at") or _now_iso())
        writer.set_effective(
            risk_kill_active=True,
            risk_kill_reason=str(risk_kill_reason),
            risk_kill_updated_at=str(risk_kill_updated_at),
        )
        evt = {"type": "risk_kill", "reason": str(risk_kill_reason)}
        if isinstance(details, dict):
            evt.update(details)
        writer.append_event(evt)
        _enforce_risk_kill_actions()

    def _reset_risk_kill(*, reason: str) -> None:
        nonlocal risk_kill_active, risk_kill_reason, risk_kill_updated_at, risk_kill_pending_enforce
        was_active = bool(risk_kill_active)
        risk_kill_active = False
        risk_kill_pending_enforce = False
        risk_kill_reason = ""
        risk_kill_updated_at = _now_iso()
        _clear_risk_kill_state(runs_dir=runs_dir, profile=prof)
        writer.set_effective(
            risk_kill_active=False,
            risk_kill_reason="",
            risk_kill_updated_at=str(risk_kill_updated_at),
        )
        writer.append_event(
            {
                "type": "risk_kill_reset",
                "reason": str(reason or "operator_reset"),
                "was_active": bool(was_active),
            }
        )

    if risk_kill_active:
        _enforce_risk_kill_actions()

    while True:
        loop_started = time.perf_counter()
        loop_ok = True
        now_ts = time.time()
        _sync_redis_state_write_health()
        if _should_emit_hot_path_heartbeat(
            now_ts=now_ts,
            last_emit_at=last_hot_path_heartbeat_at,
            interval_s=hot_path_heartbeat_interval_s,
        ):
            writer.flush_state()
            last_hot_path_heartbeat_at = now_ts
        _ensure_redis_connection()
        desired = read_gateway_desired(runs_dir=runs_dir, profile=prof, redis_client=redis_client)
        desired_mode = desired.mode
        desired_symbols = desired.symbols_list()

        # Enforce live-trade two-step gate:
        # - env var allows live routing
        # - desired.confirm_live requires explicit ack token
        effective_mode: GatewayMode = desired_mode
        if desired_mode == "live_trade":
            if desired.confirm_live != "I_UNDERSTAND":
                effective_mode = "live_monitor"
            elif not bool(is_live_enabled()):
                effective_mode = "live_monitor"

        # (Re)connect if mode changed across account types.
        need_reconnect = False
        if effective_mode != current_mode:
            need_reconnect = True
        if need_reconnect:
            if api is not None:
                writer.append_event({"type": "gateway_reconnect", "from": current_mode, "to": effective_mode})
                _close_api(api)
            api = None
            account = None
            quotes = {}
            tick_serials = {}
            positions = {}
            local_time_record = {}
            market = {"ticks": {}, "quotes": {}}
            last_market_flush_at = 0.0
            exec_targetpos = None
            exec_direct = None
            last_targets = {}
            zmq_targets = {}
            start_balance = None
            if risk_kill_active:
                risk_kill_pending_enforce = True
            current_mode = effective_mode
            current_symbols = []

            try:
                if effective_mode == "idle":
                    writer.set_health(ok=True, connected=False, last_wait_update_at=_now_iso(), error="")
                    writer.set_effective(mode="idle", symbols=[], executor=desired.executor)
                elif effective_mode == "paper":
                    # Data-only TqApi (no account).
                    auth = get_tqsdk_auth()
                    from tqsdk import TqApi  # type: ignore

                    api = TqApi(auth=auth)
                    writer.set_effective(mode="paper", symbols=desired_symbols, executor=desired.executor)
                    writer.set_health(ok=True, connected=True, last_wait_update_at=_now_iso(), error="")
                elif effective_mode == "sim":
                    account = create_tq_account(mode="sim", sim_account=desired.sim_account, monitor_only=False, account_profile=prof)
                    from tqsdk import TqApi  # type: ignore

                    api = TqApi(account=account, auth=get_tqsdk_auth())
                    writer.set_effective(mode="sim", symbols=desired_symbols, executor=desired.executor, sim_account=desired.sim_account)
                    writer.set_health(ok=True, connected=True, last_wait_update_at=_now_iso(), error="")
                else:
                    # live_monitor or live_trade
                    monitor_only = effective_mode != "live_trade"
                    account = create_tq_account(mode="live", sim_account=desired.sim_account, monitor_only=monitor_only, account_profile=prof)
                    from tqsdk import TqApi  # type: ignore

                    api = TqApi(account=account, auth=get_tqsdk_auth())
                    writer.set_effective(mode=effective_mode, symbols=desired_symbols, executor=desired.executor, monitor_only=monitor_only)
                    writer.set_health(ok=True, connected=True, last_wait_update_at=_now_iso(), error="")

                # Optional defense-in-depth: attach TqSdk risk rules when requested.
                if api is not None and env_bool("GHTRADER_GATEWAY_ENABLE_TQSDK_RISK_RULES", False):
                    try:
                        from tqsdk.risk_rule import TqRuleOrderRateLimit  # type: ignore

                        api.add_risk_rule(TqRuleOrderRateLimit(api, limit_per_second=int(max(1, desired.max_ops_per_sec))))  # type: ignore[call-arg]
                        writer.append_event({"type": "gateway_risk_rule_enabled", "rule": "TqRuleOrderRateLimit"})
                    except Exception as e:
                        writer.append_event({"type": "gateway_risk_rule_failed", "rule": "TqRuleOrderRateLimit", "error": str(e)})
            except Exception as e:
                writer.set_health(ok=False, connected=False, last_wait_update_at=_now_iso(), error=str(e))
                writer.append_event({"type": "gateway_connect_failed", "error": str(e), "mode": effective_mode})
                _obs_store.observe(metric="connect", ok=False)
                loop_ok = False
                api = None
                account = None
                time.sleep(1.0)
                _obs_store.observe(metric="loop", latency_s=(time.perf_counter() - loop_started), ok=False)
                continue

        # Manage subscriptions (best-effort; only if api is connected and mode is not idle).
        if api is not None:
            # Resolve continuous aliases to execution symbols (with roll detection)
            now_t = time.time()
            need_resolution = (
                desired_symbols != list(symbol_mapping.keys())
                or (now_t - last_resolution_at) > resolution_interval_sec
            )

            execution_symbols = current_symbols
            rolled = False
            if need_resolution and desired_symbols:
                try:
                    new_mapping, execution_symbols, rolled = _resolve_symbols_for_execution(
                        requested_symbols=desired_symbols,
                        data_dir=data_dir,
                        previous_mapping=symbol_mapping,
                    )
                    symbol_mapping = new_mapping
                    last_resolution_at = now_t

                    if rolled:
                        writer.append_event({
                            "type": "symbol_roll",
                            "mapping": dict(symbol_mapping),
                            "reason": "trading_day_change",
                        })
                        # On roll: cancel all and flatten positions before switching
                        try:
                            if exec_direct is not None:
                                exec_direct.cancel_all_alive()
                            if exec_targetpos is not None:
                                for s in list(current_symbols):
                                    exec_targetpos.set_target(s, 0)
                        except Exception as e:
                            writer.append_event({"type": "roll_flatten_failed", "error": str(e)})
                except Exception as e:
                    log.warning("symbol_resolution_error", error=str(e))
                    # Fallback: use requested symbols directly
                    execution_symbols = list(desired_symbols)
                    symbol_mapping = {s: s for s in desired_symbols}

            symbols_changed = execution_symbols != current_symbols or rolled
            if symbols_changed:
                quotes = {}
                tick_serials = {}
                positions = {}
                local_time_record = {}
                market = {"ticks": {}, "quotes": {}}
                current_symbols = list(execution_symbols)
                for s in current_symbols:
                    try:
                        quotes[s] = api.get_quote(s)
                        tick_serials[s] = api.get_tick_serial(s)
                        if account is not None:
                            positions[s] = api.get_position(s, account=account)
                        local_time_record[s] = time.time() - 0.005
                    except Exception:
                        continue
                writer.set_effective(
                    mode=current_mode,
                    symbols=current_symbols,
                    executor=desired.executor,
                    symbol_mapping=dict(symbol_mapping),
                )

                # (Re)create executors (best-effort). Even in monitor-only, operators may use manual commands.
                try:
                    limits = RiskLimits(
                        max_abs_position=int(desired.max_abs_position),
                        max_order_size=int(desired.max_order_size),
                        max_ops_per_sec=int(desired.max_ops_per_sec),
                        max_daily_loss=desired.max_daily_loss,
                        enforce_trading_time=bool(desired.enforce_trading_time),
                    )
                    exec_direct = DirectOrderExecutor(api=api, account=account, limits=limits)
                except Exception:
                    exec_direct = None
                try:
                    exec_targetpos = TargetPosExecutor(api=api, symbols=list(current_symbols), account=account)
                except Exception:
                    exec_targetpos = None

            if risk_kill_active and risk_kill_pending_enforce:
                _enforce_risk_kill_actions()
                risk_kill_pending_enforce = False
                writer.append_event({"type": "risk_kill_enforced", "reason": str(risk_kill_reason or "persisted_state")})

            # ZMQ Hot Path: Check for incoming targets/commands
            try:
                while True:
                    msg = rep_socket.recv_json(flags=zmq.NOBLOCK)
                    resp = {"status": "ok"}
                    try:
                        mtype = msg.get("type")
                        if mtype == "set_targets":
                            # {type: "set_targets", targets: {sym: pos}}
                            ts = msg.get("targets", {})
                            if isinstance(ts, dict):
                                for k, v in ts.items():
                                    zmq_targets[str(k)] = int(v)
                    except Exception as e:
                        resp = {"status": "error", "error": str(e)}
                    rep_socket.send_json(resp)
            except zmq.Again:
                pass

            # Main update loop
            try:
                t_wait = time.perf_counter()
                wait_window = max(0.05, float(poll_interval_sec))
                deadline = time.time() + wait_window
                ok = bool(api.wait_update(deadline=deadline))
                _obs_store.observe(metric="wait_update", latency_s=(time.perf_counter() - t_wait), ok=bool(ok))
                if ok:
                    last_wait_ok_at = time.time()
                    writer.set_health(ok=True, connected=True, last_wait_update_at=_now_iso(), error="")
            except Exception as e:
                writer.set_health(ok=False, connected=True, last_wait_update_at=_now_iso(), error=str(e))
                writer.append_event({"type": "wait_update_failed", "error": str(e)})
                _obs_store.observe(metric="wait_update", ok=False)
                loop_ok = False

            # Real-time daily-loss guard: evaluate on every update loop, not only snapshots.
            try:
                if account is not None and desired.max_daily_loss is not None and not risk_kill_active:
                    bal_now = _safe_float(getattr(account, "balance", None))
                    if bal_now is not None:
                        if start_balance is None:
                            start_balance = float(bal_now)
                        if float(bal_now) < float(start_balance) - float(desired.max_daily_loss):
                            _activate_risk_kill(
                                reason="max_daily_loss_realtime",
                                details={
                                    "start_balance": float(start_balance),
                                    "balance": float(bal_now),
                                    "max_daily_loss": float(desired.max_daily_loss),
                                },
                            )
            except Exception:
                pass

            # Update market cache (for StrategyRunner): last tick + quote summary per symbol.
            try:
                ticks_out = market.get("ticks") if isinstance(market.get("ticks"), dict) else {}
                quotes_out = market.get("quotes") if isinstance(market.get("quotes"), dict) else {}
                for s in list(current_symbols):
                    try:
                        ts = tick_serials.get(s)
                        if ts is not None and api.is_changing(ts):
                            tick = ts.iloc[-1].to_dict()
                            ticks_out[str(s)] = _jsonable(tick)
                    except Exception:
                        pass
                    try:
                        q = quotes.get(s)
                        if q is not None and api.is_changing(q, ["datetime", "last_price", "bid_price1", "ask_price1"]):
                            quotes_out[str(s)] = {
                                "datetime": str(getattr(q, "datetime", "") or ""),
                                "last_price": _safe_float(getattr(q, "last_price", None)),
                                "bid_price1": _safe_float(getattr(q, "bid_price1", None)),
                                "ask_price1": _safe_float(getattr(q, "ask_price1", None)),
                            }
                            if s in local_time_record:
                                local_time_record[s] = time.time() - 0.005
                    except Exception:
                        pass
                market = {"ticks": dict(ticks_out), "quotes": dict(quotes_out)}
                if (time.time() - float(last_market_flush_at)) >= 1.0:
                    writer.set_market(market)
                    writer.flush_state()
                    last_market_flush_at = time.time()
            except Exception:
                pass

            # Kill-switch file (operator emergency stop).
            try:
                if (gateway_root(runs_dir=runs_dir, profile=prof) / "KILL").exists() and not risk_kill_active:
                    _activate_risk_kill(reason="kill_switch_file")
                    time.sleep(1.0)
            except Exception:
                pass

            # Process operator commands (Redis stream primary + file mirror fallback).
            try:
                incoming_commands: list[dict[str, Any]] = []

                # Primary command channel: Redis stream.
                if redis_client is not None:
                    try:
                        stream_rows = redis_client.xread({redis_cmd_stream: redis_cmd_id}, count=128, block=1)
                        for _, entries in (stream_rows or []):
                            for entry_id, fields in entries:
                                redis_cmd_id = str(entry_id)
                                params_obj: dict[str, Any] = {}
                                params_raw = fields.get("params")
                                if isinstance(params_raw, str) and params_raw.strip():
                                    try:
                                        dec = json.loads(params_raw)
                                        if isinstance(dec, dict):
                                            params_obj = dec
                                    except Exception:
                                        params_obj = {}
                                incoming_commands.append(
                                    {
                                        "ts": fields.get("ts"),
                                        "command_id": fields.get("command_id"),
                                        "account_profile": fields.get("account_profile"),
                                        "type": fields.get("type"),
                                        "params": params_obj,
                                        "symbol": fields.get("symbol"),
                                        "target": fields.get("target"),
                                    }
                                )
                    except Exception as e:
                        writer.append_event({"type": "gateway_commands_stream_failed", "error": str(e)})
                        redis_client = None
                        writer.redis_client = None
                        _set_warm_path_state(degraded=True, reason="redis_stream_failed")

                # Fallback/audit mirror: append-only file.
                cmd_p = commands_path(runs_dir=runs_dir, profile=prof)
                if cmd_p.exists():
                    with open(cmd_p, "rb") as f:
                        f.seek(0, 2)
                        size = int(f.tell())
                        if cmd_offset > size:
                            cmd_offset = 0
                        f.seek(int(cmd_offset))
                        chunk = f.read()
                    new_off = size
                    for raw in chunk.splitlines():
                        try:
                            obj = json.loads(raw.decode("utf-8", errors="ignore"))
                        except Exception:
                            continue
                        if isinstance(obj, dict):
                            incoming_commands.append(obj)
                    cmd_offset = int(new_off)
                    _write_commands_cursor(runs_dir=runs_dir, profile=prof, offset=int(cmd_offset))

                for obj in incoming_commands:
                    ctype = str(obj.get("type") or "").strip()
                    cid = str(obj.get("command_id") or "").strip()
                    if not ctype or not cid:
                        continue
                    if command_deduper.is_duplicate_and_remember(cid):
                        continue

                    writer.append_event({"type": "gateway_command", "command_type": ctype, "command_id": cid})

                    if ctype == "cancel_all":
                        try:
                            if exec_direct is not None:
                                n = exec_direct.cancel_all_alive()
                                writer.append_event({"type": "gateway_cancel_all_done", "count": int(n)})
                        except Exception as e:
                            writer.append_event({"type": "gateway_cancel_all_failed", "error": str(e)})

                    if ctype == "flatten":
                        try:
                            for s in list(current_symbols):
                                if exec_targetpos is not None:
                                    exec_targetpos.set_target(s, 0)
                                elif exec_direct is not None and s in positions and s in quotes:
                                    exec_direct.set_target(symbol=s, target_net=0, position=positions[s], quote=quotes[s])
                            writer.append_event({"type": "gateway_flatten_done", "symbols": list(current_symbols)})
                        except Exception as e:
                            writer.append_event({"type": "gateway_flatten_failed", "error": str(e)})

                    if ctype == "disarm_live":
                        try:
                            cur = read_gateway_desired(runs_dir=runs_dir, profile=prof, redis_client=redis_client)
                            nxt = GatewayDesired(
                                mode="live_monitor",
                                symbols=cur.symbols_list(),
                                executor=cur.executor,
                                sim_account=cur.sim_account,
                                confirm_live="",
                                max_abs_position=cur.max_abs_position,
                                max_order_size=cur.max_order_size,
                                max_ops_per_sec=cur.max_ops_per_sec,
                                max_daily_loss=cur.max_daily_loss,
                                enforce_trading_time=cur.enforce_trading_time,
                            )
                            write_gateway_desired(runs_dir=runs_dir, profile=prof, desired=nxt, redis_client=redis_client)
                            writer.append_event({"type": "gateway_disarmed"})
                        except Exception as e:
                            writer.append_event({"type": "gateway_disarm_failed", "error": str(e)})

                    if ctype == "reset_risk_kill":
                        _reset_risk_kill(reason="operator_reset")

                    if ctype == "set_target":
                        # Manual target setting (for one-lot testing)
                        try:
                            if risk_kill_active:
                                writer.append_event(
                                    {
                                        "type": "manual_target_failed",
                                        "error": "risk_kill_active",
                                        "risk_kill_reason": str(risk_kill_reason),
                                    }
                                )
                                continue
                            params = obj.get("params") if isinstance(obj.get("params"), dict) else {}
                            sym = str((obj.get("symbol") if "symbol" in obj else params.get("symbol")) or "").strip()
                            tgt_raw = obj.get("target") if "target" in obj else params.get("target")
                            try:
                                tgt = int(tgt_raw) if tgt_raw is not None else 0
                            except Exception:
                                tgt = 0
                            if sym and sym in current_symbols:
                                # Clamp to risk limits
                                tgt = int(clamp_target_position(tgt, max_abs_position=int(desired.max_abs_position)))
                                if desired.executor == "direct" and exec_direct is not None and sym in positions and sym in quotes:
                                    exec_direct.set_target(symbol=sym, target_net=tgt, position=positions[sym], quote=quotes[sym])
                                    writer.append_event({"type": "manual_target_set", "symbol": sym, "target": tgt, "executor": "direct"})
                                elif exec_targetpos is not None:
                                    exec_targetpos.set_target(sym, tgt)
                                    writer.append_event({"type": "manual_target_set", "symbol": sym, "target": tgt, "executor": "targetpos"})
                                else:
                                    writer.append_event({"type": "manual_target_failed", "symbol": sym, "error": "no_executor"})
                            else:
                                writer.append_event({"type": "manual_target_failed", "symbol": sym, "error": "symbol_not_in_current_symbols"})
                        except Exception as e:
                            writer.append_event({"type": "manual_target_failed", "error": str(e)})
            except Exception as e:
                writer.append_event({"type": "gateway_commands_failed", "error": str(e)})

            # Targets → execution (only when order-enabled).
            orders_enabled = bool(current_mode in {"sim", "live_trade"} and not risk_kill_active)
            if orders_enabled:
                try:
                    # Hot path target source is ZMQ only. Disk targets are audit mirrors.
                    targets = dict(zmq_targets)
                    desired_targets: dict[str, int] = {}
                    for sym, tgt in targets.items():
                        if sym not in current_symbols:
                            continue
                        desired_targets[str(sym)] = int(clamp_target_position(int(tgt), max_abs_position=int(desired.max_abs_position)))
                    if desired_targets != last_targets:
                        writer.append_event({"type": "target_change", "old": dict(last_targets), "new": dict(desired_targets)})
                        last_targets = dict(desired_targets)

                    for sym, tgt in last_targets.items():
                        try:
                            if desired.enforce_trading_time and sym in quotes and sym in local_time_record:
                                q = quotes[sym]
                                if not is_in_trading_time(q, getattr(q, "datetime", ""), local_time_record[sym]):
                                    continue
                            if desired.executor == "direct":
                                if exec_direct is None or sym not in quotes or sym not in positions:
                                    continue
                                exec_direct.set_target(symbol=sym, target_net=int(tgt), position=positions[sym], quote=quotes[sym])
                            else:
                                if exec_targetpos is None:
                                    continue
                                exec_targetpos.set_target(sym, int(tgt))
                        except Exception:
                            continue
                except Exception as e:
                    writer.append_event({"type": "gateway_targets_failed", "error": str(e)})

            # Periodic snapshot (account only; paper mode has no account object)
            if (time.time() - last_snapshot_at) >= float(max(1.0, snapshot_interval_sec)):
                last_snapshot_at = time.time()
                try:
                    # Attach minimal non-secret metadata.
                    account_meta = {
                        "mode": str(current_mode),
                        "account_profile": prof,
                        "orders_enabled": bool(current_mode in {"sim", "live_trade"} and not risk_kill_active),
                        "monitor_only": bool(current_mode in {"paper", "live_monitor"}),
                        "live_enabled": bool(is_live_enabled()),
                        "risk_kill_active": bool(risk_kill_active),
                        "risk_kill_reason": str(risk_kill_reason),
                    }
                    if account is not None:
                        snap = snapshot_account_state(
                            api=api,
                            symbols=list(current_symbols),
                            account=account,
                            account_meta=account_meta,
                        )
                        writer.append_snapshot(snap)
                        writer.append_event({"type": "gateway_snapshot", "age_sec": float(time.time() - last_wait_ok_at) if last_wait_ok_at else None})

                        # Risk: max daily loss (best-effort, based on account.balance).
                        try:
                            acct = snap.get("account") if isinstance(snap.get("account"), dict) else {}
                            bal = acct.get("balance") if isinstance(acct, dict) else None
                            if start_balance is None and bal is not None:
                                start_balance = float(bal)
                            if start_balance is not None and desired.max_daily_loss is not None and bal is not None:
                                if float(bal) < float(start_balance) - float(desired.max_daily_loss):
                                    _activate_risk_kill(
                                        reason="max_daily_loss",
                                        details={
                                            "start_balance": float(start_balance),
                                            "balance": float(bal),
                                            "max_daily_loss": float(desired.max_daily_loss),
                                        },
                                    )
                        except Exception:
                            pass
                    else:
                        # Paper mode: no account snapshot; still record heartbeat.
                        writer.append_event({"type": "gateway_heartbeat", "mode": "paper"})
                except Exception as e:
                    writer.append_event({"type": "gateway_snapshot_failed", "error": str(e)})

        else:
            # Idle / disconnected: keep state fresh for the dashboard.
            writer.set_health(ok=True, connected=False, last_wait_update_at=_now_iso(), error="")
            writer.set_effective(
                mode=current_mode,
                symbols=current_symbols,
                executor=desired.executor,
                risk_kill_active=bool(risk_kill_active),
                risk_kill_reason=str(risk_kill_reason),
                risk_kill_updated_at=str(risk_kill_updated_at),
                warm_path_degraded=bool(warm_path_degraded),
                warm_path_reason=str(warm_path_reason),
            )
            time.sleep(1.0)
        _obs_store.observe(metric="loop", latency_s=(time.perf_counter() - loop_started), ok=bool(loop_ok))


__all__ = [
    "GatewayDesired",
    "GatewayWriter",
    "commands_path",
    "commands_cursor_path",
    "desired_path",
    "events_path",
    "gateway_root",
    "read_gateway_desired",
    "read_gateway_targets",
    "risk_kill_state_path",
    "run_gateway",
    "snapshots_path",
    "state_path",
    "targets_path",
    "write_gateway_desired",
]

