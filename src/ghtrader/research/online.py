"""
Online inference and paper trading loop with online calibrator.

The OnlineCalibrator stacks deep model logits with engineered factors
and updates intraday via SGD/FTRL using delayed event-time labels.
"""

from __future__ import annotations

import os
import time
from collections import deque
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import numpy as np
import structlog

log = structlog.get_logger()


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

