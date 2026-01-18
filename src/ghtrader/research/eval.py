"""
Evaluation: metrics and promotion gates (PRD 5.8 / 5.9).

TqSdk Tier1 backtest integration lives in `ghtrader.tq.eval` to keep non-`ghtrader.tq.*`
modules free of direct `tqsdk` imports (PRD 5.1.3).
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date
from typing import Any

import numpy as np
import pandas as pd
import structlog

log = structlog.get_logger()


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
