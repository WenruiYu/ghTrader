"""
Evaluation: TqSdk backtest harness, metrics, and promotion gates.

Tier1: run event-driven backtests using TqSdk's TqBacktest + TqSim.
Tier2 (later): offline L5 micro-sim for more realistic evaluation.
"""

from __future__ import annotations

import json
import os
import sys
from dataclasses import dataclass, field
from datetime import date, datetime
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd
import structlog

log = structlog.get_logger()

# Add vendored tqsdk to path
_TQSDK_PATH = Path(__file__).parent.parent.parent.parent / "tqsdk-python"
if _TQSDK_PATH.exists() and str(_TQSDK_PATH) not in sys.path:
    sys.path.insert(0, str(_TQSDK_PATH))


# ---------------------------------------------------------------------------
# Metrics
# ---------------------------------------------------------------------------

@dataclass
class BacktestMetrics:
    """Metrics from a backtest run."""
    
    # Basic stats
    n_trades: int = 0
    n_winning: int = 0
    n_losing: int = 0
    
    # PnL
    total_pnl: float = 0.0
    gross_profit: float = 0.0
    gross_loss: float = 0.0
    
    # Risk
    max_drawdown: float = 0.0
    sharpe_ratio: float = 0.0
    
    # Turnover
    total_volume: int = 0
    turnover_ratio: float = 0.0
    
    # TqSdk stats (from TqSim)
    tqsdk_stat: dict[str, Any] = field(default_factory=dict)
    
    def to_dict(self) -> dict[str, Any]:
        return {
            "n_trades": self.n_trades,
            "n_winning": self.n_winning,
            "n_losing": self.n_losing,
            "win_rate": self.n_winning / self.n_trades if self.n_trades > 0 else 0,
            "total_pnl": self.total_pnl,
            "gross_profit": self.gross_profit,
            "gross_loss": self.gross_loss,
            "profit_factor": self.gross_profit / abs(self.gross_loss) if self.gross_loss != 0 else float("inf"),
            "max_drawdown": self.max_drawdown,
            "sharpe_ratio": self.sharpe_ratio,
            "total_volume": self.total_volume,
            "turnover_ratio": self.turnover_ratio,
            "tqsdk_stat": self.tqsdk_stat,
        }


def compute_sharpe(returns: pd.Series, risk_free: float = 0.0, annualize: float = 252.0) -> float:
    """Compute annualized Sharpe ratio."""
    if len(returns) < 2:
        return 0.0
    excess = returns - risk_free
    mean_ret = excess.mean()
    std_ret = excess.std()
    if std_ret == 0:
        return 0.0
    return mean_ret / std_ret * np.sqrt(annualize)


def compute_max_drawdown(equity_curve: pd.Series) -> float:
    """Compute maximum drawdown as a fraction."""
    if len(equity_curve) < 2:
        return 0.0
    peak = equity_curve.expanding().max()
    drawdown = (equity_curve - peak) / peak
    return abs(drawdown.min())


# ---------------------------------------------------------------------------
# Simple strategy for backtest (signal-based)
# ---------------------------------------------------------------------------

@dataclass
class SignalStrategy:
    """
    Simple strategy that trades based on model predictions.
    
    - prob[UP] > threshold_up: go long
    - prob[DOWN] > threshold_down: go short
    - otherwise: flatten
    """
    
    threshold_up: float = 0.6
    threshold_down: float = 0.6
    position_size: int = 1
    
    def get_target_position(self, probs: np.ndarray) -> int:
        """
        Get target position from probabilities.
        
        Args:
            probs: Shape (3,) with [prob_down, prob_flat, prob_up]
        
        Returns:
            Target position: -1, 0, or 1
        """
        if probs[2] > self.threshold_up:
            return self.position_size
        elif probs[0] > self.threshold_down:
            return -self.position_size
        else:
            return 0


# ---------------------------------------------------------------------------
# TqSdk Backtest Harness
# ---------------------------------------------------------------------------

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
    
    Args:
        model_name: Model type (e.g., "xgboost", "deeplob")
        symbol: Instrument code
        start_date: Backtest start date
        end_date: Backtest end date
        data_dir: Data directory
        artifacts_dir: Model artifacts directory
        runs_dir: Output directory for reports
        horizon: Label horizon the model was trained on
        initial_balance: Starting account balance
        threshold_up: Probability threshold for long signal
        threshold_down: Probability threshold for short signal
    
    Returns:
        BacktestMetrics with results
    """
    from tqsdk import TqApi, TqSim, TqBacktest, BacktestFinished
    
    from ghtrader.config import get_tqsdk_auth
    from ghtrader.features import FactorEngine
    from ghtrader.models import load_model
    
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
        from ghtrader.json_io import read_json

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
    seq_len = getattr(model, "seq_len", 100)
    
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
                    # Sequence model - need to handle differently
                    # This is a simplified version; real impl would batch properly
                    X_input = X
                
                # Get prediction
                try:
                    probs = model.predict_proba(X_input)
                    if len(probs.shape) > 1:
                        probs = probs[-1]  # Last prediction
                except Exception as e:
                    log.warning("backtest.predict_failed", error=str(e))
                    continue
                
                # Get target position
                target_pos = strategy.get_target_position(probs)
                
                # Execute if position changed
                if target_pos != current_pos:
                    delta = target_pos - current_pos
                    direction = "BUY" if delta > 0 else "SELL"
                    offset = "OPEN" if current_pos == 0 else ("CLOSE" if target_pos == 0 else "CLOSEOPEN")
                    
                    # Submit order
                    price = quote.ask_price1 if direction == "BUY" else quote.bid_price1
                    api.insert_order(
                        symbol=symbol,
                        direction=direction,
                        offset="OPEN" if delta > 0 else "CLOSE",
                        volume=abs(delta),
                        limit_price=price,
                    )
                    
                    current_pos = target_pos
                    
                    trade_log.append({
                        "datetime": int(tick_dict["datetime"]),
                        "direction": direction,
                        "volume": abs(delta),
                        "price": price,
                        "position": current_pos,
                    })
                
                # Track equity
                account = api.get_account()
                equity_curve.append(account.balance)
    
    except BacktestFinished:
        log.info("backtest.finished")
    finally:
        api.close()
    
    # Compute metrics
    metrics = BacktestMetrics()
    metrics.n_trades = len(trade_log)
    metrics.total_volume = sum(t["volume"] for t in trade_log)
    
    if equity_curve:
        equity_series = pd.Series(equity_curve)
        metrics.total_pnl = equity_series.iloc[-1] - initial_balance
        metrics.max_drawdown = compute_max_drawdown(equity_series)
        
        # Daily returns approximation
        if len(equity_series) > 100:
            returns = equity_series.pct_change().dropna()
            metrics.sharpe_ratio = compute_sharpe(returns)
    
    # TqSdk stats
    metrics.tqsdk_stat = sim.tqsdk_stat if hasattr(sim, "tqsdk_stat") else {}
    
    # Save report
    run_id = datetime.now().strftime("%Y%m%d_%H%M%S")
    report_dir = runs_dir / symbol / model_name / run_id
    report_dir.mkdir(parents=True, exist_ok=True)
    
    # Save metrics
    with open(report_dir / "metrics.json", "w") as f:
        json.dump(metrics.to_dict(), f, indent=2, default=str)
    
    # Save trade log
    pd.DataFrame(trade_log).to_csv(report_dir / "trades.csv", index=False)
    
    # Save equity curve
    pd.Series(equity_curve).to_frame("equity").to_csv(report_dir / "equity.csv", index=False)
    
    log.info(
        "backtest.report_saved",
        path=str(report_dir),
        pnl=metrics.total_pnl,
        sharpe=metrics.sharpe_ratio,
        max_dd=metrics.max_drawdown,
    )
    
    return metrics


# ---------------------------------------------------------------------------
# Promotion gates
# ---------------------------------------------------------------------------

@dataclass
class PromotionGate:
    """Gate for model promotion decisions."""
    
    min_sharpe: float = 0.5
    max_drawdown: float = 0.2
    min_trades: int = 100
    min_pnl: float = 0.0
    
    def evaluate(self, metrics: BacktestMetrics) -> tuple[bool, list[str]]:
        """
        Evaluate if a model passes promotion gates.
        
        Returns:
            (passed, list of failure reasons)
        """
        failures: list[str] = []
        
        if metrics.sharpe_ratio < self.min_sharpe:
            failures.append(f"Sharpe {metrics.sharpe_ratio:.2f} < {self.min_sharpe}")
        
        if metrics.max_drawdown > self.max_drawdown:
            failures.append(f"MaxDD {metrics.max_drawdown:.2%} > {self.max_drawdown:.2%}")
        
        if metrics.n_trades < self.min_trades:
            failures.append(f"Trades {metrics.n_trades} < {self.min_trades}")
        
        if metrics.total_pnl < self.min_pnl:
            failures.append(f"PnL {metrics.total_pnl:.2f} < {self.min_pnl}")
        
        passed = len(failures) == 0
        return passed, failures


def evaluate_and_promote(
    candidate_path: Path,
    production_path: Path,
    metrics: BacktestMetrics,
    gate: PromotionGate | None = None,
) -> bool:
    """
    Evaluate a candidate model and promote if it passes gates.
    
    Args:
        candidate_path: Path to candidate model
        production_path: Path to production model (will be overwritten if promoted)
        metrics: Backtest metrics for candidate
        gate: Promotion gate (uses defaults if None)
    
    Returns:
        True if promoted, False otherwise
    """
    if gate is None:
        gate = PromotionGate()
    
    passed, failures = gate.evaluate(metrics)
    
    if passed:
        # Promote: copy candidate to production
        import shutil
        production_path.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(candidate_path, production_path)
        log.info("promotion.success", candidate=str(candidate_path), production=str(production_path))
        return True
    else:
        log.warning("promotion.failed", reasons=failures)
        return False
