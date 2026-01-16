"""
Online inference and paper trading loop with online calibrator.

The OnlineCalibrator stacks deep model logits with engineered factors
and updates intraday via SGD/FTRL using delayed event-time labels.
"""

from __future__ import annotations

import os
import sys
import time
from collections import deque
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import numpy as np
import structlog

log = structlog.get_logger()

# Add vendored tqsdk to path
_TQSDK_PATH = Path(__file__).parent.parent.parent.parent / "tqsdk-python"
if _TQSDK_PATH.exists() and str(_TQSDK_PATH) not in sys.path:
    sys.path.insert(0, str(_TQSDK_PATH))


# ---------------------------------------------------------------------------
# Online calibrator (SGD-based multinomial logistic regression)
# ---------------------------------------------------------------------------

@dataclass
class OnlineCalibrator:
    """
    Online multinomial logistic regression calibrator.
    
    Combines deep model logits + factor vector and updates weights
    intraday using SGD with delayed event-time labels.
    
    Features:
    - L2 regularization
    - Learning rate with decay
    - Drift detection and disable-on-instability
    """
    
    n_classes: int = 3
    n_inputs: int = 11  # deep logits (3) + factors (8)
    lr: float = 0.01
    lr_decay: float = 0.999
    l2_lambda: float = 0.001
    max_gradient_norm: float = 1.0
    
    # Weights: shape (n_inputs, n_classes)
    weights: np.ndarray = field(default_factory=lambda: np.zeros((11, 3)))
    bias: np.ndarray = field(default_factory=lambda: np.zeros(3))
    
    # State
    n_updates: int = 0
    current_lr: float = 0.01
    disabled: bool = False
    
    # Drift detection
    recent_losses: deque = field(default_factory=lambda: deque(maxlen=100))
    loss_threshold: float = 2.0
    
    def __post_init__(self) -> None:
        # Initialize weights properly
        self.weights = np.random.randn(self.n_inputs, self.n_classes) * 0.01
        self.bias = np.zeros(self.n_classes)
        self.current_lr = self.lr
        self.recent_losses = deque(maxlen=100)
    
    def predict_proba(self, x: np.ndarray) -> np.ndarray:
        """
        Predict class probabilities.
        
        Args:
            x: Input features, shape (n_inputs,)
        
        Returns:
            Probabilities, shape (n_classes,)
        """
        logits = x @ self.weights + self.bias
        # Softmax with numerical stability
        logits = logits - logits.max()
        exp_logits = np.exp(logits)
        return exp_logits / exp_logits.sum()
    
    def update(self, x: np.ndarray, y: int) -> float:
        """
        Update weights with a single sample using SGD.
        
        Args:
            x: Input features, shape (n_inputs,)
            y: True label (0, 1, or 2)
        
        Returns:
            Cross-entropy loss for this sample
        """
        if self.disabled:
            return 0.0
        
        # Forward pass
        probs = self.predict_proba(x)
        
        # Compute cross-entropy loss
        loss = -np.log(probs[y] + 1e-10)
        
        # Check for drift
        self.recent_losses.append(loss)
        if len(self.recent_losses) >= 100:
            avg_loss = np.mean(list(self.recent_losses))
            if avg_loss > self.loss_threshold:
                log.warning("calibrator.drift_detected", avg_loss=avg_loss)
                self.disabled = True
                return loss
        
        # Gradient of cross-entropy with softmax
        # d_loss/d_logits = probs - one_hot(y)
        grad_logits = probs.copy()
        grad_logits[y] -= 1.0
        
        # Gradient w.r.t. weights: outer product
        grad_weights = np.outer(x, grad_logits)
        grad_bias = grad_logits
        
        # Add L2 regularization gradient
        grad_weights += self.l2_lambda * self.weights
        
        # Gradient clipping
        grad_norm = np.linalg.norm(grad_weights)
        if grad_norm > self.max_gradient_norm:
            grad_weights = grad_weights * (self.max_gradient_norm / grad_norm)
        
        # Update
        self.weights -= self.current_lr * grad_weights
        self.bias -= self.current_lr * grad_bias
        
        # Learning rate decay
        self.n_updates += 1
        self.current_lr = self.lr * (self.lr_decay ** self.n_updates)
        
        return loss
    
    def reset(self) -> None:
        """Reset calibrator to initial state."""
        self.weights = np.random.randn(self.n_inputs, self.n_classes) * 0.01
        self.bias = np.zeros(self.n_classes)
        self.n_updates = 0
        self.current_lr = self.lr
        self.disabled = False
        self.recent_losses.clear()
    
    def save(self, path: Path) -> None:
        """Save calibrator state."""
        path.parent.mkdir(parents=True, exist_ok=True)
        np.savez(
            path,
            weights=self.weights,
            bias=self.bias,
            n_updates=self.n_updates,
            current_lr=self.current_lr,
        )
    
    def load(self, path: Path) -> None:
        """Load calibrator state."""
        data = np.load(path)
        self.weights = data["weights"]
        self.bias = data["bias"]
        self.n_updates = int(data["n_updates"])
        self.current_lr = float(data["current_lr"])


# ---------------------------------------------------------------------------
# Delayed label buffer for online learning
# ---------------------------------------------------------------------------

@dataclass
class DelayedLabelBuffer:
    """
    Buffer for tracking predictions and delayed labels.
    
    Stores (features, prediction) tuples and matches them with labels
    once the horizon is reached.
    """
    
    horizon_ticks: int = 50
    buffer: deque = field(default_factory=lambda: deque())
    
    # Mid price history for label computation
    mid_history: deque = field(default_factory=lambda: deque())
    price_tick: float = 1.0
    threshold_k: int = 1
    
    def __post_init__(self) -> None:
        self.buffer = deque()
        self.mid_history = deque(maxlen=self.horizon_ticks * 2)
    
    def add_prediction(self, features: np.ndarray, mid_price: float) -> None:
        """Add a prediction to the buffer."""
        self.buffer.append({
            "features": features,
            "mid_at_pred": mid_price,
            "tick_count": 0,
        })
        self.mid_history.append(mid_price)
    
    def tick(self, current_mid: float) -> list[tuple[np.ndarray, int]]:
        """
        Process a new tick and return any samples ready for training.
        
        Returns:
            List of (features, label) tuples ready for update
        """
        self.mid_history.append(current_mid)
        
        ready_samples: list[tuple[np.ndarray, int]] = []
        
        # Update tick counts and check for ready samples
        items_to_remove: list[int] = []
        for i, item in enumerate(self.buffer):
            item["tick_count"] += 1
            
            if item["tick_count"] >= self.horizon_ticks:
                # Compute label
                mid_at_pred = item["mid_at_pred"]
                delta = current_mid - mid_at_pred
                threshold = self.threshold_k * self.price_tick
                
                if delta >= threshold:
                    label = 2  # UP
                elif delta <= -threshold:
                    label = 0  # DOWN
                else:
                    label = 1  # FLAT
                
                ready_samples.append((item["features"], label))
                items_to_remove.append(i)
        
        # Remove processed items (in reverse to maintain indices)
        for i in reversed(items_to_remove):
            del self.buffer[i]
        
        return ready_samples


# ---------------------------------------------------------------------------
# Paper trading loop
# ---------------------------------------------------------------------------

def run_paper_trading(
    model_name: str,
    symbols: list[str],
    artifacts_dir: Path,
    horizon: int = 50,
    threshold_up: float = 0.6,
    threshold_down: float = 0.6,
    calibrator_lr: float = 0.01,
) -> None:
    """
    Run paper trading loop with online calibrator.
    
    NO REAL ORDERS are placed. This is for research and validation only.
    
    Args:
        model_name: Model type to use
        symbols: Instruments to trade
        artifacts_dir: Model artifacts directory
        horizon: Label horizon for online learning
        threshold_up: Probability threshold for long signal
        threshold_down: Probability threshold for short signal
        calibrator_lr: Learning rate for online calibrator
    """
    from tqsdk import TqApi
    
    from ghtrader.config import get_tqsdk_auth, is_live_enabled
    from ghtrader.features import FactorEngine
    from ghtrader.models import load_model
    
    # Safety check
    if is_live_enabled():
        raise RuntimeError(
            "GHTRADER_LIVE_ENABLED is set but this is the paper trading loop. "
            "Live trading is not implemented. Remove this env var."
        )
    
    log.info("paper.start", model=model_name, symbols=symbols)
    
    # Load models for each symbol
    models = {}
    for symbol in symbols:
        model_dir = artifacts_dir / symbol / model_name
        model_files = sorted([p for p in model_dir.glob(f"model_h{horizon}.*") if not p.name.endswith(".meta.json")])
        if model_files:
            models[symbol] = load_model(model_name, model_files[0])
            log.info("paper.model_loaded", symbol=symbol)
        else:
            log.warning("paper.no_model", symbol=symbol)
    
    if not models:
        raise RuntimeError("No models loaded")
    
    # Create factor engines and calibrators for each symbol (prefer model metadata for parity).
    enabled_factors: dict[str, list[str]] = {}
    try:
        from ghtrader.json_io import read_json

        for s in symbols:
            mdir = artifacts_dir / s / model_name
            meta = read_json(mdir / f"model_h{horizon}.meta.json") or {}
            ef = (meta or {}).get("enabled_factors") if isinstance(meta, dict) else None
            if isinstance(ef, list) and ef and all(isinstance(x, str) and x.strip() for x in ef):
                enabled_factors[s] = [str(x).strip() for x in ef if str(x).strip()]
    except Exception:
        enabled_factors = {}

    factor_engines = {s: FactorEngine(enabled_factors=enabled_factors.get(s) or None) if enabled_factors.get(s) else FactorEngine() for s in symbols}
    calibrators = {s: OnlineCalibrator(lr=calibrator_lr) for s in symbols}
    label_buffers = {s: DelayedLabelBuffer(horizon_ticks=horizon) for s in symbols}
    
    # Feature buffers for sequence models
    feature_buffers: dict[str, list[dict]] = {s: [] for s in symbols}
    seq_len = 100
    
    # Paper positions (no real orders)
    positions: dict[str, int] = {s: 0 for s in symbols}
    paper_pnl: dict[str, float] = {s: 0.0 for s in symbols}
    last_prices: dict[str, float] = {s: 0.0 for s in symbols}
    
    # TqSdk setup
    auth = get_tqsdk_auth()
    api = TqApi(auth=auth)
    
    # Subscribe to ticks
    tick_serials = {s: api.get_tick_serial(s) for s in symbols}
    quotes = {s: api.get_quote(s) for s in symbols}
    
    log.info("paper.subscribed", symbols=symbols)
    
    try:
        while True:
            api.wait_update()
            
            for symbol in symbols:
                if not api.is_changing(tick_serials[symbol]):
                    continue
                
                tick = tick_serials[symbol].iloc[-1]
                tick_dict = tick.to_dict()
                
                # Compute mid price
                mid = (tick_dict["bid_price1"] + tick_dict["ask_price1"]) / 2
                
                # Update paper PnL based on position
                if last_prices[symbol] > 0 and positions[symbol] != 0:
                    price_change = mid - last_prices[symbol]
                    paper_pnl[symbol] += positions[symbol] * price_change
                last_prices[symbol] = mid
                
                # Process delayed labels and update calibrator
                ready_samples = label_buffers[symbol].tick(mid)
                for features, label in ready_samples:
                    loss = calibrators[symbol].update(features, label)
                    if loss > 0:
                        log.debug("paper.calibrator_update", symbol=symbol, loss=f"{loss:.4f}")
                
                # Compute features
                factors = factor_engines[symbol].compute_incremental(tick_dict)
                feature_buffers[symbol].append(factors)
                if len(feature_buffers[symbol]) > seq_len:
                    feature_buffers[symbol] = feature_buffers[symbol][-seq_len:]
                
                if len(feature_buffers[symbol]) < seq_len:
                    continue
                
                # Prepare input
                feature_names = list(factors.keys())
                X = np.array([[fb[f] for f in feature_names] for fb in feature_buffers[symbol]])
                X = np.nan_to_num(X, nan=0.0)
                
                # Get base model prediction
                model = models[symbol]
                if model_name in ["logistic", "xgboost", "lightgbm"]:
                    X_input = X[-1:, :]
                else:
                    X_input = X
                
                try:
                    base_probs = model.predict_proba(X_input)
                    if len(base_probs.shape) > 1:
                        base_probs = base_probs[-1]
                except Exception as e:
                    log.warning("paper.predict_failed", symbol=symbol, error=str(e))
                    continue
                
                # Stack deep logits + factors for calibrator
                calibrator_input = np.concatenate([
                    base_probs,
                    X[-1, :min(8, len(feature_names))],  # First 8 factors
                ])
                
                # Resize calibrator if needed
                if len(calibrator_input) != calibrators[symbol].n_inputs:
                    calibrators[symbol].n_inputs = len(calibrator_input)
                    calibrators[symbol].weights = np.random.randn(len(calibrator_input), 3) * 0.01
                
                # Get calibrated probabilities
                cal_probs = calibrators[symbol].predict_proba(calibrator_input)
                
                # Add to delayed label buffer for online learning
                label_buffers[symbol].add_prediction(calibrator_input, mid)
                
                # Trading decision (paper only)
                if cal_probs[2] > threshold_up:
                    target = 1
                elif cal_probs[0] > threshold_down:
                    target = -1
                else:
                    target = 0
                
                if target != positions[symbol]:
                    old_pos = positions[symbol]
                    positions[symbol] = target
                    log.info(
                        "paper.position_change",
                        symbol=symbol,
                        old=old_pos,
                        new=target,
                        probs=f"[{cal_probs[0]:.2f},{cal_probs[1]:.2f},{cal_probs[2]:.2f}]",
                        paper_pnl=f"{paper_pnl[symbol]:.2f}",
                    )
    
    except KeyboardInterrupt:
        log.info("paper.interrupted")
    finally:
        api.close()
        
        # Final report
        for symbol in symbols:
            log.info(
                "paper.final",
                symbol=symbol,
                position=positions[symbol],
                paper_pnl=f"{paper_pnl[symbol]:.2f}",
                calibrator_updates=calibrators[symbol].n_updates,
            )
