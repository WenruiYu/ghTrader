"""
TqSdk Tier1 evaluation harness (PRD 5.8).

This module intentionally contains the direct TqSdk backtest integration.
Non-`ghtrader.tq.*` modules should not import `tqsdk` directly (PRD 5.1.3).
"""

from __future__ import annotations

import json
from datetime import date, datetime
from pathlib import Path

import numpy as np
import pandas as pd
import structlog

from ghtrader.config import get_tqsdk_auth
from ghtrader.research.eval import BacktestMetrics, SignalStrategy, compute_max_drawdown, compute_sharpe
from ghtrader.datasets.features import FactorEngine
from ghtrader.research.models import load_model

log = structlog.get_logger()


def run_backtest(
    model_name: str,
    symbol: str,
    start_date: date,
    end_date: date,
    data_dir: Path,
    artifacts_dir: Path,
    runs_dir: Path,
    horizon: int = 50,
    initial_balance: float = 1_000_000.0,
    threshold_up: float = 0.6,
    threshold_down: float = 0.6,
) -> BacktestMetrics:
    """
    Run a TqSdk backtest with a trained model.

    Uses TqBacktest for event-driven simulation with TqSim for execution.
    """
    try:
        from tqsdk import BacktestFinished, TqApi, TqBacktest, TqSim  # type: ignore
    except Exception as e:
        raise RuntimeError("tqsdk not installed. Install with: pip install tqsdk") from e

    _ = data_dir  # reserved for future replay providers

    log.info(
        "backtest.start",
        model=model_name,
        symbol=symbol,
        start=str(start_date),
        end=str(end_date),
    )

    # Load model
    model_dir = artifacts_dir / symbol / model_name
    model_files = sorted([p for p in model_dir.glob(f"model_h{horizon}.*") if not p.name.endswith(".meta.json")])
    if not model_files:
        raise FileNotFoundError(f"No model found for {model_name} horizon {horizon} in {model_dir}")
    model_path = model_files[0]

    model = load_model(model_name, model_path)
    log.info("backtest.model_loaded", path=str(model_path))

    # Create factor engine for online feature computation (prefer model metadata for parity).
    enabled_factors: list[str] | None = None
    try:
        from ghtrader.util.json_io import read_json

        meta = read_json(model_dir / f"model_h{horizon}.meta.json") or {}
        ef = (meta or {}).get("enabled_factors") if isinstance(meta, dict) else None
        if isinstance(ef, list) and ef and all(isinstance(x, str) and x.strip() for x in ef):
            enabled_factors = [str(x).strip() for x in ef if str(x).strip()]
    except Exception:
        enabled_factors = None
    factor_engine = FactorEngine(enabled_factors=enabled_factors or None) if enabled_factors else FactorEngine()

    # Strategy
    strategy = SignalStrategy(threshold_up=threshold_up, threshold_down=threshold_down)

    # TqSdk credentials
    auth = get_tqsdk_auth()

    # Create backtest
    sim = TqSim(init_balance=initial_balance)
    backtest = TqBacktest(start_dt=start_date, end_dt=end_date)

    api = TqApi(account=sim, backtest=backtest, auth=auth)

    # Subscribe to tick data
    tick_serial = api.get_tick_serial(symbol)
    quote = api.get_quote(symbol)

    # State
    current_pos = 0
    trade_log: list[dict] = []
    equity_curve: list[float] = []

    # Sequence buffer for deep models
    feature_buffer: list[dict] = []
    seq_len = int(getattr(model, "seq_len", 100) or 100)

    try:
        while True:
            api.wait_update()

            if api.is_changing(tick_serial):
                # Build tick dict
                tick = tick_serial.iloc[-1]
                tick_dict = tick.to_dict()

                # Compute features incrementally
                features = factor_engine.compute_incremental(tick_dict)
                feature_buffer.append(features)

                # Keep only last seq_len features
                if len(feature_buffer) > seq_len:
                    feature_buffer = feature_buffer[-seq_len:]

                # Need enough history for prediction
                if len(feature_buffer) < seq_len:
                    continue

                # Prepare input for model
                feature_names = list(features.keys())
                X = np.array([[fb[f] for f in feature_names] for fb in feature_buffer])
                X = np.nan_to_num(X, nan=0.0)

                # For tabular models, use last row; for sequence models, use whole sequence
                if model_name in ["logistic", "xgboost", "lightgbm"]:
                    X_input = X[-1:, :]
                else:
                    X_input = X

                # Get prediction
                try:
                    probs = model.predict_proba(X_input)
                    if len(getattr(probs, "shape", ())) > 1:
                        probs = probs[-1]  # Last prediction
                except Exception as e:
                    log.warning("backtest.predict_failed", error=str(e))
                    continue

                # Get target position
                target_pos = int(strategy.get_target_position(np.asarray(probs, dtype=float)))

                # Execute if position changed (best-effort; Tier1 harness)
                if target_pos != current_pos:
                    delta = target_pos - current_pos
                    direction = "BUY" if delta > 0 else "SELL"

                    price = quote.ask_price1 if direction == "BUY" else quote.bid_price1
                    api.insert_order(
                        symbol=symbol,
                        direction=direction,
                        offset="OPEN" if delta > 0 else "CLOSE",
                        volume=abs(int(delta)),
                        limit_price=price,
                    )

                    current_pos = target_pos

                    trade_log.append(
                        {
                            "datetime": int(tick_dict.get("datetime") or 0),
                            "direction": direction,
                            "volume": abs(int(delta)),
                            "price": float(price) if price is not None else None,
                            "position": int(current_pos),
                        }
                    )

                # Track equity
                account = api.get_account()
                equity_curve.append(float(getattr(account, "balance", 0.0) or 0.0))

    except BacktestFinished:
        log.info("backtest.finished")
    finally:
        api.close()

    # Compute metrics
    metrics = BacktestMetrics()
    metrics.n_trades = len(trade_log)
    metrics.total_volume = int(sum(int(t.get("volume") or 0) for t in trade_log))

    if equity_curve:
        equity_series = pd.Series(equity_curve)
        metrics.total_pnl = float(equity_series.iloc[-1] - float(initial_balance))
        metrics.max_drawdown = float(compute_max_drawdown(equity_series))

        # Daily returns approximation
        if len(equity_series) > 100:
            returns = equity_series.pct_change().dropna()
            metrics.sharpe_ratio = float(compute_sharpe(returns))

    # TqSdk stats
    metrics.tqsdk_stat = sim.tqsdk_stat if hasattr(sim, "tqsdk_stat") else {}

    # Save report
    run_id = datetime.now().strftime("%Y%m%d_%H%M%S")
    report_dir = runs_dir / symbol / model_name / run_id
    report_dir.mkdir(parents=True, exist_ok=True)

    with open(report_dir / "metrics.json", "w") as f:
        json.dump(metrics.to_dict(), f, indent=2, default=str)

    pd.DataFrame(trade_log).to_csv(report_dir / "trades.csv", index=False)
    pd.Series(equity_curve).to_frame("equity").to_csv(report_dir / "equity.csv", index=False)

    log.info(
        "backtest.report_saved",
        path=str(report_dir),
        pnl=metrics.total_pnl,
        sharpe=metrics.sharpe_ratio,
        max_dd=metrics.max_drawdown,
    )

    return metrics

