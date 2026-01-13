from __future__ import annotations

import time
from dataclasses import dataclass
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any, Literal

import numpy as np
import structlog

from ghtrader.execution import (
    DirectOrderExecutor,
    OrderRateLimiter,
    RiskLimits,
    TargetPosExecutor,
    clamp_target_position,
)
from ghtrader.models import load_model
from ghtrader.symbol_resolver import is_continuous_alias, resolve_trading_symbol
from ghtrader.tq_runtime import (
    GracefulShutdown,
    TradeRunWriter,
    create_tq_account,
    create_tq_api,
    snapshot_account_state,
    trading_day_from_ts_ns,
)

log = structlog.get_logger()


ExecutorType = Literal["targetpos", "direct"]


@dataclass(frozen=True)
class TradeConfig:
    mode: Literal["paper", "sim", "live"] = "paper"
    # If true, connect/subscribe/snapshot but never send orders (even if mode=live or sim).
    monitor_only: bool = False
    sim_account: Literal["tqsim", "tqkq"] = "tqsim"
    executor: ExecutorType = "targetpos"

    model_name: str = "xgboost"
    symbols: list[str] = None  # type: ignore[assignment]
    horizon: int = 50
    threshold_up: float = 0.6
    threshold_down: float = 0.6
    position_size: int = 1

    data_dir: Path = Path("data")
    artifacts_dir: Path = Path("artifacts")
    runs_dir: Path = Path("runs")

    # Risk
    limits: RiskLimits = RiskLimits(max_abs_position=1, max_order_size=1, max_ops_per_sec=10)

    # Preflight (applies only when orders are enabled). If None, defaults are mode-dependent.
    require_no_alive_orders: bool | None = None
    require_flat_start: bool | None = None

    # Executor knobs
    targetpos_price: str = "ACTIVE"
    targetpos_offset_priority: str = "今昨,开"
    direct_price_mode: Literal["ACTIVE", "PASSIVE"] = "ACTIVE"
    direct_advanced: str | None = None

    # Ops
    snapshot_interval_sec: float = 10.0


@dataclass
class SymbolBinding:
    requested_symbol: str
    execution_symbol: str
    trading_day: date


def _unique_in_order(items: list[str]) -> list[str]:
    out: list[str] = []
    seen: set[str] = set()
    for x in items:
        if x in seen:
            continue
        out.append(x)
        seen.add(x)
    return out


def _expected_n_features(model: Any) -> int | None:
    """
    Best-effort: extract the trained feature dimension from a loaded model.

    Used to sanity-check that the online factor set matches the model artifact.
    """
    try:
        if hasattr(model, "n_features"):
            return int(getattr(model, "n_features"))
    except Exception:
        pass
    try:
        scaler = getattr(model, "scaler", None)
        if scaler is not None and hasattr(scaler, "n_features_in_"):
            return int(getattr(scaler, "n_features_in_"))
    except Exception:
        pass
    try:
        inner = getattr(model, "model", None)
        if inner is not None:
            # LightGBM Booster: num_feature(); XGBoost Booster: num_features()
            for name in ("num_feature", "num_features"):
                if hasattr(inner, name):
                    return int(getattr(inner, name)())
    except Exception:
        pass
    return None


def online_model_seq_len(*, model_name: str, model: Any) -> int:
    if model_name in {"logistic", "xgboost", "lightgbm"}:
        return 1
    try:
        return int(getattr(model, "seq_len", 100) or 100)
    except Exception:
        return 100


def online_history_required(*, model_name: str, model: Any) -> int:
    if model_name in {"logistic", "xgboost", "lightgbm"}:
        return 1
    return online_model_seq_len(model_name=model_name, model=model) + 1


def maybe_roll_execution_symbol(
    *,
    requested_symbol: str,
    current_execution_symbol: str,
    old_trading_day: date,
    new_trading_day: date,
    data_dir: Path,
) -> str | None:
    if new_trading_day == old_trading_day:
        return None
    if not is_continuous_alias(requested_symbol):
        return None
    new_exec = resolve_trading_symbol(symbol=requested_symbol, data_dir=data_dir, trading_day=new_trading_day)
    if new_exec == current_execution_symbol:
        return None
    return new_exec


def _load_models(model_name: str, symbols: list[str], artifacts_dir: Path, horizon: int) -> dict[str, Any]:
    models: dict[str, Any] = {}
    for sym in symbols:
        model_dir = artifacts_dir / sym / model_name
        model_files = list(model_dir.glob(f"model_h{horizon}.*"))
        if not model_files:
            raise FileNotFoundError(f"No model found for {sym} {model_name} horizon={horizon} under {model_dir}")
        models[sym] = load_model(model_name, model_files[0])
    return models


def _decide_target(
    probs: np.ndarray,
    *,
    threshold_up: float,
    threshold_down: float,
    position_size: int,
) -> int:
    # probs = [down, flat, up]
    if probs[2] > threshold_up:
        return int(position_size)
    if probs[0] > threshold_down:
        return -int(position_size)
    return 0


def run_trade(cfg: TradeConfig, *, confirm_live: str | None = None) -> None:
    """
    Long-running trading runner. Designed to be launched as a subprocess job by the dashboard.
    """
    if cfg.symbols is None or not cfg.symbols:
        raise ValueError("cfg.symbols is required")

    if cfg.mode == "live" and (not cfg.monitor_only):
        if confirm_live != "I_UNDERSTAND":
            raise RuntimeError("Refusing to run in live mode without --confirm-live I_UNDERSTAND")

    strategy_symbols = list(cfg.symbols)

    # Initial symbol binding using best-effort trading-day (handles night session via 18:00 boundary).
    startup_td = trading_day_from_ts_ns(int(time.time() * 1_000_000_000), data_dir=cfg.data_dir)
    bindings: dict[str, SymbolBinding] = {}
    for s in strategy_symbols:
        exec_sym = resolve_trading_symbol(symbol=s, data_dir=cfg.data_dir, trading_day=startup_td) if is_continuous_alias(s) else s
        bindings[s] = SymbolBinding(requested_symbol=s, execution_symbol=exec_sym, trading_day=startup_td)

    resolved = [bindings[s].execution_symbol for s in strategy_symbols]
    if resolved != strategy_symbols:
        log.info("trade.symbols_resolved", requested=strategy_symbols, resolved=resolved, trading_day=str(startup_td))

    def current_execution_symbols() -> list[str]:
        return _unique_in_order([bindings[s].execution_symbol for s in strategy_symbols])

    execution_symbols = current_execution_symbols()

    # Create api + account
    account = create_tq_account(mode=cfg.mode, sim_account=cfg.sim_account, monitor_only=bool(cfg.monitor_only))
    api = create_tq_api(account=account)

    # Subscriptions
    quotes = {s: api.get_quote(bindings[s].execution_symbol) for s in strategy_symbols}
    tick_serials = {s: api.get_tick_serial(bindings[s].execution_symbol) for s in strategy_symbols}
    positions = {s: api.get_position(bindings[s].execution_symbol) for s in strategy_symbols}
    local_time_record: dict[str, float] = {s: time.time() - 0.005 for s in strategy_symbols}

    # Models
    models = _load_models(cfg.model_name, strategy_symbols, cfg.artifacts_dir, cfg.horizon)

    # Feature state
    from ghtrader.features import DEFAULT_FACTORS, FactorEngine, read_features_manifest

    enabled_factors: dict[str, list[str]] = {}
    for s in strategy_symbols:
        manifest = read_features_manifest(cfg.data_dir, s)
        ef = manifest.get("enabled_factors")
        if isinstance(ef, list) and ef and all(isinstance(x, str) and x for x in ef):
            enabled_factors[s] = list(ef)
        else:
            # For live order routing, require an explicit feature spec (manifest is source of truth).
            if cfg.mode == "live" and (not cfg.monitor_only):
                raise RuntimeError(
                    f"Missing features manifest for {s}. Expected {cfg.data_dir}/features/symbol={s}/manifest.json"
                )
            enabled_factors[s] = DEFAULT_FACTORS.copy()
            log.warning("trade.features_manifest_missing", symbol=s)

    # Validate online factor dimension against the trained model artifact (best-effort).
    for s in strategy_symbols:
        exp = _expected_n_features(models[s])
        if exp is not None and int(exp) != len(enabled_factors[s]):
            raise RuntimeError(
                f"Feature count mismatch for {s}: model expects n_features={int(exp)} but enabled_factors has {len(enabled_factors[s])}"
            )

    engines = {s: FactorEngine(enabled_factors=enabled_factors[s]) for s in strategy_symbols}
    feature_buffers: dict[str, list[dict]] = {s: [] for s in strategy_symbols}

    is_tabular_model = cfg.model_name in {"logistic", "xgboost", "lightgbm"}
    model_seq_len: dict[str, int] = {}
    history_required: dict[str, int] = {}
    for s in strategy_symbols:
        model_seq_len[s] = online_model_seq_len(model_name=cfg.model_name, model=models[s])
        history_required[s] = online_history_required(model_name=cfg.model_name, model=models[s])

    # Executor
    limiter = OrderRateLimiter(cfg.limits.max_ops_per_sec)
    if cfg.mode == "paper" or cfg.monitor_only:
        exec_impl = None
    elif cfg.executor == "targetpos":
        exec_impl = TargetPosExecutor(
            api=api,
            symbols=execution_symbols,
            account=account,
            price=cfg.targetpos_price,
            offset_priority=cfg.targetpos_offset_priority,
        )
    else:
        exec_impl = DirectOrderExecutor(
            api=api,
            account=account,
            limits=cfg.limits,
            price_mode=cfg.direct_price_mode,
            advanced=cfg.direct_advanced,
            rate_limiter=limiter,
        )

    # Run writer
    run_id = time.strftime("%Y%m%d_%H%M%S")
    writer = TradeRunWriter(run_id=run_id, runs_dir=cfg.runs_dir)
    writer.write_config(
        {
            "mode": cfg.mode,
            "monitor_only": bool(cfg.monitor_only),
            "sim_account": cfg.sim_account,
            "executor": cfg.executor,
            "model_name": cfg.model_name,
            "symbols_requested": strategy_symbols,
            "symbols_resolved": resolved,
            "startup_trading_day": str(startup_td),
            "symbol_bindings": {s: bindings[s].execution_symbol for s in strategy_symbols},
            "enabled_factors": {s: enabled_factors[s] for s in strategy_symbols},
            "model_seq_len": {s: int(model_seq_len[s]) for s in strategy_symbols},
            "horizon": cfg.horizon,
            "threshold_up": cfg.threshold_up,
            "threshold_down": cfg.threshold_down,
            "position_size": cfg.position_size,
            "require_no_alive_orders": cfg.require_no_alive_orders,
            "require_flat_start": cfg.require_flat_start,
            "limits": cfg.limits.__dict__,
        }
    )

    shutdown = GracefulShutdown()
    shutdown.install()

    last_targets: dict[str, int] = {s: 0 for s in strategy_symbols}
    last_snapshot = 0.0
    start_balance: float | None = None

    log.info(
        "trade.start",
        run_id=run_id,
        mode=cfg.mode,
        monitor_only=bool(cfg.monitor_only),
        executor=cfg.executor,
        symbols_strategy=strategy_symbols,
        symbols_execution=execution_symbols,
    )

    armed = False  # becomes True only after startup preflight succeeds in order-enabled modes

    try:
        orders_enabled = cfg.mode != "paper" and (not cfg.monitor_only)
        require_no_alive = (
            cfg.require_no_alive_orders
            if cfg.require_no_alive_orders is not None
            else (cfg.mode == "live" and orders_enabled)
        )
        require_flat = cfg.require_flat_start if cfg.require_flat_start is not None else (cfg.mode == "live" and orders_enabled)

        # Startup snapshot + preflight (best-effort). This is important for live accounts.
        try:
            api.wait_update()
        except Exception as e:
            writer.append_event({"ts": now_utc(), "type": "wait_update_failed", "phase": "startup", "error": str(e)})
            log.error("trade.wait_update_failed", phase="startup", error=str(e))
            return

        try:
            exec_syms = current_execution_symbols()
            snap0 = snapshot_account_state(api=api, symbols=exec_syms, account=account)
            writer.append_snapshot(snap0)
            writer.append_event(
                {
                    "ts": snap0.get("ts") or now_utc(),
                    "type": "startup_snapshot",
                    "orders_enabled": bool(orders_enabled),
                    "require_no_alive_orders": bool(require_no_alive),
                    "require_flat_start": bool(require_flat),
                }
            )

            if orders_enabled:
                alive = list(snap0.get("orders_alive") or [])
                if require_no_alive and alive:
                    writer.append_event(
                        {
                            "ts": snap0.get("ts") or now_utc(),
                            "type": "preflight_failed",
                            "reason": "alive_orders",
                            "n_alive": len(alive),
                        }
                    )
                    log.error("trade.preflight_failed", reason="alive_orders", n_alive=len(alive))
                    return

                pos = dict(snap0.get("positions") or {})
                net_by_symbol: dict[str, int] = {}
                for ex in exec_syms:
                    p = dict(pos.get(ex) or {})
                    if "error" in p:
                        continue
                    net = int(p.get("volume_long", 0) or 0) - int(p.get("volume_short", 0) or 0)
                    net_by_symbol[ex] = net

                if require_flat and any(v != 0 for v in net_by_symbol.values()):
                    writer.append_event(
                        {
                            "ts": snap0.get("ts") or now_utc(),
                            "type": "preflight_failed",
                            "reason": "require_flat_start",
                            "net_by_symbol": net_by_symbol,
                        }
                    )
                    log.error("trade.preflight_failed", reason="require_flat_start", net_by_symbol=net_by_symbol)
                    return

                max_abs = int(cfg.limits.max_abs_position)
                if any(abs(v) > max_abs for v in net_by_symbol.values()):
                    writer.append_event(
                        {
                            "ts": snap0.get("ts") or now_utc(),
                            "type": "preflight_failed",
                            "reason": "start_position_out_of_limits",
                            "max_abs_position": max_abs,
                            "net_by_symbol": net_by_symbol,
                        }
                    )
                    log.error(
                        "trade.preflight_failed",
                        reason="start_position_out_of_limits",
                        max_abs_position=max_abs,
                        net_by_symbol=net_by_symbol,
                    )
                    return
        except Exception as e:
            writer.append_event({"ts": now_utc(), "type": "startup_snapshot_failed", "error": str(e)})
            log.error("trade.startup_snapshot_failed", error=str(e))
            if orders_enabled:
                return

        armed = bool(orders_enabled)

        while True:
            try:
                api.wait_update()
            except Exception as e:
                writer.append_event({"ts": now_utc(), "type": "wait_update_failed", "error": str(e)})
                log.error("trade.wait_update_failed", error=str(e))
                break

            if shutdown.requested:
                log.warning("trade.shutdown_requested")
                break

            # File-based kill switch (operators can `touch runs/trading/KILL` to stop).
            try:
                if (cfg.runs_dir / "trading" / "KILL").exists():
                    log.error("trade.risk_kill", reason="kill_switch_file")
                    writer.append_event({"ts": now_utc(), "type": "risk_kill", "reason": "kill_switch_file"})
                    break
            except Exception:
                pass

            # Periodic snapshot + risk checks
            now = time.time()
            if now - last_snapshot >= float(cfg.snapshot_interval_sec):
                exec_syms = current_execution_symbols()
                snap = snapshot_account_state(api=api, symbols=exec_syms, account=account)
                writer.append_snapshot(snap)
                last_snapshot = now

                bal = snap.get("account", {}).get("balance")
                if start_balance is None and isinstance(bal, (int, float)):
                    start_balance = float(bal)
                    writer.append_event({"ts": snap["ts"], "type": "start_balance", "balance": start_balance})
                if (
                    armed
                    and cfg.limits.max_daily_loss is not None
                    and start_balance is not None
                    and isinstance(bal, (int, float))
                ):
                    if float(bal) < start_balance - float(cfg.limits.max_daily_loss):
                        writer.append_event(
                            {
                                "ts": snap["ts"],
                                "type": "risk_kill",
                                "reason": "max_daily_loss",
                                "start_balance": start_balance,
                                "balance": float(bal),
                                "max_daily_loss": float(cfg.limits.max_daily_loss),
                            }
                        )
                        log.error("trade.risk_kill", reason="max_daily_loss", balance=float(bal), start=start_balance)
                        break

            for sym in strategy_symbols:
                if not api.is_changing(tick_serials[sym]):
                    continue

                tick = tick_serials[sym].iloc[-1]
                tick_dict = tick.to_dict()
                local_time_record[sym] = time.time()

                # Roll-aware binding: on trading-day change, re-resolve continuous aliases and reset state if needed.
                try:
                    ts_ns = int(tick_dict.get("datetime") or 0)
                except Exception:
                    ts_ns = 0
                if ts_ns:
                    td = trading_day_from_ts_ns(ts_ns, data_dir=cfg.data_dir)
                    if td != bindings[sym].trading_day:
                        old_td = bindings[sym].trading_day
                        bindings[sym].trading_day = td
                        if is_continuous_alias(sym):
                            old_exec = bindings[sym].execution_symbol
                            new_exec = maybe_roll_execution_symbol(
                                requested_symbol=sym,
                                current_execution_symbol=old_exec,
                                old_trading_day=old_td,
                                new_trading_day=td,
                                data_dir=cfg.data_dir,
                            )
                            if new_exec is not None:
                                writer.append_event(
                                    {
                                        "ts": now_utc(),
                                        "type": "roll",
                                        "requested_symbol": sym,
                                        "old_execution_symbol": old_exec,
                                        "new_execution_symbol": new_exec,
                                        "old_trading_day": str(old_td),
                                        "trading_day": str(td),
                                    }
                                )
                                log.info(
                                    "trade.roll",
                                    requested_symbol=sym,
                                    old_execution_symbol=old_exec,
                                    new_execution_symbol=new_exec,
                                    old_trading_day=str(old_td),
                                    trading_day=str(td),
                                )

                                # In order-enabled modes, flatten the old underlying before switching (best-effort).
                                if armed:
                                    try:
                                        if isinstance(exec_impl, TargetPosExecutor):
                                            exec_impl.set_target(old_exec, 0)
                                        elif isinstance(exec_impl, DirectOrderExecutor):
                                            exec_impl.cancel_all_alive()
                                            exec_impl.set_target(
                                                symbol=old_exec,
                                                target_net=0,
                                                position=positions[sym],
                                                quote=quotes[sym],
                                            )
                                    except Exception as e:
                                        writer.append_event(
                                            {
                                                "ts": now_utc(),
                                                "type": "roll_flatten_failed",
                                                "requested_symbol": sym,
                                                "old_execution_symbol": old_exec,
                                                "error": str(e),
                                            }
                                        )

                                # Reset online state and refresh subscriptions to the new underlying.
                                try:
                                    engines[sym].reset_states()
                                except Exception:
                                    pass
                                feature_buffers[sym] = []
                                last_targets[sym] = 0

                                bindings[sym].execution_symbol = new_exec
                                quotes[sym] = api.get_quote(new_exec)
                                tick_serials[sym] = api.get_tick_serial(new_exec)
                                positions[sym] = api.get_position(new_exec)
                                local_time_record[sym] = time.time() - 0.005
                                continue

                # Compute factors incrementally
                factors = engines[sym].compute_incremental(tick_dict)
                feature_buffers[sym].append(factors)
                req_len = int(history_required[sym])
                if len(feature_buffers[sym]) > req_len:
                    feature_buffers[sym] = feature_buffers[sym][-req_len:]
                if len(feature_buffers[sym]) < req_len:
                    continue

                # Prepare input
                feature_names = enabled_factors[sym]
                X = np.array([[fb[f] for f in feature_names] for fb in feature_buffers[sym]])
                X = np.nan_to_num(X, nan=0.0)

                model = models[sym]
                if is_tabular_model:
                    X_input = X[-1:, :]
                else:
                    X_input = X

                try:
                    probs = model.predict_proba(X_input)
                    if len(getattr(probs, "shape", ())) > 1:
                        probs = probs[-1]
                    probs = np.asarray(probs, dtype=float)
                    if probs.shape[0] != 3:
                        continue
                except Exception as e:
                    writer.append_event({"ts": now_utc(), "type": "predict_failed", "symbol": sym, "error": str(e)})
                    continue

                target = _decide_target(
                    probs,
                    threshold_up=cfg.threshold_up,
                    threshold_down=cfg.threshold_down,
                    position_size=cfg.position_size,
                )
                target = clamp_target_position(target, max_abs_position=int(cfg.limits.max_abs_position))

                if target == last_targets[sym]:
                    continue

                old = last_targets[sym]
                last_targets[sym] = target
                writer.append_event(
                    {
                        "ts": now_utc(),
                        "type": "target_change",
                        "symbol": sym,
                        "old": int(old),
                        "new": int(target),
                        "probs": [float(probs[0]), float(probs[1]), float(probs[2])],
                    }
                )

                if cfg.mode == "paper" or cfg.monitor_only:
                    continue

                # Best-effort trading-session guard (avoid sending orders outside trading time)
                if cfg.limits.enforce_trading_time:
                    try:
                        from tqsdk.datetime import _is_in_trading_time

                        q = quotes[sym]
                        if not _is_in_trading_time(q, getattr(q, "datetime", ""), local_time_record[sym]):
                            writer.append_event({"ts": now_utc(), "type": "skip_non_trading_time", "symbol": sym})
                            continue
                    except Exception:
                        # If guard fails, do not block trading (best-effort).
                        pass

                if isinstance(exec_impl, TargetPosExecutor):
                    exec_impl.set_target(bindings[sym].execution_symbol, target)
                elif isinstance(exec_impl, DirectOrderExecutor):
                    exec_impl.set_target(
                        symbol=bindings[sym].execution_symbol,
                        target_net=target,
                        position=positions[sym],
                        quote=quotes[sym],
                    )

    finally:
        # Best-effort safe shutdown behavior
        try:
            if armed:
                if isinstance(exec_impl, TargetPosExecutor):
                    seen_exec: set[str] = set()
                    for s in strategy_symbols:
                        ex = bindings[s].execution_symbol
                        if ex in seen_exec:
                            continue
                        seen_exec.add(ex)
                        exec_impl.set_target(ex, 0)
                elif isinstance(exec_impl, DirectOrderExecutor):
                    exec_impl.cancel_all_alive()
                    seen_exec = set()
                    for s in strategy_symbols:
                        ex = bindings[s].execution_symbol
                        if ex in seen_exec:
                            continue
                        seen_exec.add(ex)
                        exec_impl.set_target(symbol=ex, target_net=0, position=positions[s], quote=quotes[s])

                # Give a short window for orders/tasks to process
                deadline = time.time() + 10.0
                while time.time() < deadline:
                    try:
                        api.wait_update()
                    except Exception:
                        break
                    if shutdown.requested:
                        break
        except Exception:
            pass
        try:
            exec_syms = current_execution_symbols()
            snap = snapshot_account_state(api=api, symbols=exec_syms, account=account)
            writer.append_snapshot(snap)
        except Exception:
            pass
        api.close()
        log.info("trade.done", run_id=run_id)


def now_utc() -> str:
    return datetime.now(timezone.utc).isoformat()

