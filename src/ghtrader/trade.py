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
from ghtrader.symbol_resolver import resolve_trading_symbol
from ghtrader.tq_runtime import GracefulShutdown, TradeRunWriter, create_tq_account, create_tq_api, snapshot_account_state

log = structlog.get_logger()


ExecutorType = Literal["targetpos", "direct"]


@dataclass(frozen=True)
class TradeConfig:
    mode: Literal["paper", "sim", "live"] = "paper"
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

    # Executor knobs
    targetpos_price: str = "ACTIVE"
    targetpos_offset_priority: str = "今昨,开"
    direct_price_mode: Literal["ACTIVE", "PASSIVE"] = "ACTIVE"
    direct_advanced: str | None = None

    # Ops
    snapshot_interval_sec: float = 10.0


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

    if cfg.mode == "live":
        if confirm_live != "I_UNDERSTAND":
            raise RuntimeError("Refusing to run in live mode without --confirm-live I_UNDERSTAND")

    # Resolve continuous aliases at start (best-effort; users should prefer specific contracts for execution).
    trading_day = date.today()
    resolved = [resolve_trading_symbol(symbol=s, data_dir=cfg.data_dir, trading_day=trading_day) for s in cfg.symbols]
    if resolved != cfg.symbols:
        log.info("trade.symbols_resolved", requested=cfg.symbols, resolved=resolved, trading_day=str(trading_day))
    symbols = resolved

    # Create api + account
    account = create_tq_account(mode=cfg.mode, sim_account=cfg.sim_account)
    api = create_tq_api(account=account)

    # Subscriptions
    quotes = {s: api.get_quote(s) for s in symbols}
    tick_serials = {s: api.get_tick_serial(s) for s in symbols}
    positions = {s: api.get_position(s) for s in symbols}
    local_time_record: dict[str, float] = {s: time.time() - 0.005 for s in symbols}

    # Models
    models = _load_models(cfg.model_name, symbols, cfg.artifacts_dir, cfg.horizon)

    # Feature state
    from ghtrader.features import FactorEngine

    engines = {s: FactorEngine() for s in symbols}
    feature_buffers: dict[str, list[dict]] = {s: [] for s in symbols}
    seq_len = 100

    # Executor
    limiter = OrderRateLimiter(cfg.limits.max_ops_per_sec)
    if cfg.mode == "paper":
        exec_impl = None
    elif cfg.executor == "targetpos":
        exec_impl = TargetPosExecutor(
            api=api,
            symbols=symbols,
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
            "sim_account": cfg.sim_account,
            "executor": cfg.executor,
            "model_name": cfg.model_name,
            "symbols_requested": cfg.symbols,
            "symbols_resolved": symbols,
            "horizon": cfg.horizon,
            "threshold_up": cfg.threshold_up,
            "threshold_down": cfg.threshold_down,
            "position_size": cfg.position_size,
            "limits": cfg.limits.__dict__,
        }
    )

    shutdown = GracefulShutdown()
    shutdown.install()

    last_targets: dict[str, int] = {s: 0 for s in symbols}
    last_snapshot = 0.0
    start_balance: float | None = None

    log.info("trade.start", run_id=run_id, mode=cfg.mode, executor=cfg.executor, symbols=symbols)

    try:
        while True:
            api.wait_update()

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
                snap = snapshot_account_state(api=api, symbols=symbols, account=account)
                writer.append_snapshot(snap)
                last_snapshot = now

                bal = snap.get("account", {}).get("balance")
                if start_balance is None and isinstance(bal, (int, float)):
                    start_balance = float(bal)
                    writer.append_event({"ts": snap["ts"], "type": "start_balance", "balance": start_balance})
                if (
                    cfg.mode != "paper"
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

            for sym in symbols:
                if not api.is_changing(tick_serials[sym]):
                    continue

                tick = tick_serials[sym].iloc[-1]
                tick_dict = tick.to_dict()
                local_time_record[sym] = time.time()

                # Compute factors incrementally
                factors = engines[sym].compute_incremental(tick_dict)
                feature_buffers[sym].append(factors)
                if len(feature_buffers[sym]) > seq_len:
                    feature_buffers[sym] = feature_buffers[sym][-seq_len:]
                if len(feature_buffers[sym]) < seq_len:
                    continue

                # Prepare input
                feature_names = list(factors.keys())
                X = np.array([[fb[f] for f in feature_names] for fb in feature_buffers[sym]])
                X = np.nan_to_num(X, nan=0.0)

                model = models[sym]
                if cfg.model_name in ["logistic", "xgboost", "lightgbm"]:
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

                if cfg.mode == "paper":
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
                    exec_impl.set_target(sym, target)
                elif isinstance(exec_impl, DirectOrderExecutor):
                    exec_impl.set_target(symbol=sym, target_net=target, position=positions[sym], quote=quotes[sym])

    finally:
        # Best-effort safe shutdown behavior
        try:
            if cfg.mode != "paper":
                if isinstance(exec_impl, TargetPosExecutor):
                    for s in symbols:
                        exec_impl.set_target(s, 0)
                elif isinstance(exec_impl, DirectOrderExecutor):
                    exec_impl.cancel_all_alive()
                    for s in symbols:
                        exec_impl.set_target(symbol=s, target_net=0, position=positions[s], quote=quotes[s])

                # Give a short window for orders/tasks to process
                deadline = time.time() + 10.0
                while time.time() < deadline:
                    api.wait_update()
                    if shutdown.requested:
                        break
        except Exception:
            pass
        try:
            snap = snapshot_account_state(api=api, symbols=symbols, account=account)
            writer.append_snapshot(snap)
        except Exception:
            pass
        api.close()
        log.info("trade.done", run_id=run_id)


def now_utc() -> str:
    return datetime.now(timezone.utc).isoformat()

