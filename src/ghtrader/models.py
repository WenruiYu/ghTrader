"""
Models: tabular baselines (Logistic, XGBoost/LightGBM) and deep models (DeepLOB, Transformer).

All models follow a common interface for training/inference and support multi-horizon outputs.
"""

from __future__ import annotations

from contextlib import nullcontext
import json
import pickle
from abc import ABC, abstractmethod
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Literal

import numpy as np
import pandas as pd
import structlog
import torch
import torch.nn as nn
import torch.nn.functional as F
import ghtrader.distributed as distu
from sklearn.linear_model import LogisticRegression
from sklearn.preprocessing import StandardScaler
from torch.utils.data import DataLoader, Dataset

log = structlog.get_logger()

ModelType = Literal["logistic", "xgboost", "lightgbm", "deeplob", "transformer", "tcn", "tlob", "ssm"]


# ---------------------------------------------------------------------------
# Base model interface
# ---------------------------------------------------------------------------

class BaseModel(ABC):
    """Abstract base for all models."""
    
    name: str
    
    @abstractmethod
    def fit(self, X: np.ndarray, y: np.ndarray, **kwargs: Any) -> None:
        """Fit the model."""
        pass
    
    @abstractmethod
    def predict_proba(self, X: np.ndarray, **kwargs: Any) -> np.ndarray:
        """Predict class probabilities. Shape: (n_samples, n_classes)."""
        pass
    
    @abstractmethod
    def save(self, path: Path) -> None:
        """Save model to disk."""
        pass
    
    @abstractmethod
    def load(self, path: Path) -> None:
        """Load model from disk."""
        pass


# ---------------------------------------------------------------------------
# Tabular baselines
# ---------------------------------------------------------------------------

class LogisticModel(BaseModel):
    """Logistic regression baseline."""
    
    name = "logistic"
    
    def __init__(self, n_classes: int = 3, **kwargs: Any) -> None:
        self.n_classes = n_classes
        self.scaler = StandardScaler()
        self.model = LogisticRegression(
            solver="lbfgs",
            max_iter=1000,
            **kwargs,
        )
    
    def fit(self, X: np.ndarray, y: np.ndarray, **kwargs: Any) -> None:
        # Remove NaN rows
        mask = ~(np.isnan(X).any(axis=1) | np.isnan(y))
        X_clean = X[mask]
        y_clean = y[mask].astype(int)
        
        # Scale features
        X_scaled = self.scaler.fit_transform(X_clean)
        
        # Fit model
        self.model.fit(X_scaled, y_clean)
        log.info("logistic.fit_done", n_samples=len(y_clean))
    
    def predict_proba(self, X: np.ndarray, **kwargs: Any) -> np.ndarray:
        X_scaled = self.scaler.transform(X)
        return self.model.predict_proba(X_scaled)
    
    def save(self, path: Path) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        with open(path, "wb") as f:
            pickle.dump({"scaler": self.scaler, "model": self.model}, f)
    
    def load(self, path: Path) -> None:
        with open(path, "rb") as f:
            data = pickle.load(f)
        self.scaler = data["scaler"]
        self.model = data["model"]


class LightGBMModel(BaseModel):
    """LightGBM baseline."""
    
    name = "lightgbm"
    
    def __init__(self, n_classes: int = 3, **kwargs: Any) -> None:
        self.n_classes = n_classes
        self.params = {
            "objective": "multiclass",
            "num_class": n_classes,
            "metric": "multi_logloss",
            "boosting_type": "gbdt",
            "num_leaves": 63,
            "learning_rate": 0.05,
            "feature_fraction": 0.8,
            "bagging_fraction": 0.8,
            "bagging_freq": 5,
            "verbose": -1,
            "n_jobs": -1,
            **kwargs,
        }
        self.model = None
    
    def fit(self, X: np.ndarray, y: np.ndarray, n_rounds: int = 500, **kwargs: Any) -> None:
        import lightgbm as lgb
        
        # Remove NaN rows
        mask = ~(np.isnan(X).any(axis=1) | np.isnan(y))
        X_clean = X[mask]
        y_clean = y[mask].astype(int)
        
        # Create dataset
        train_data = lgb.Dataset(X_clean, label=y_clean)
        
        # Train
        self.model = lgb.train(
            self.params,
            train_data,
            num_boost_round=n_rounds,
        )
        log.info("lightgbm.fit_done", n_samples=len(y_clean), n_rounds=n_rounds)
    
    def predict_proba(self, X: np.ndarray, **kwargs: Any) -> np.ndarray:
        if self.model is None:
            raise RuntimeError("Model not fitted")
        return self.model.predict(X)
    
    def save(self, path: Path) -> None:
        if self.model is None:
            raise RuntimeError("Model not fitted")
        path.parent.mkdir(parents=True, exist_ok=True)
        self.model.save_model(str(path))
    
    def load(self, path: Path) -> None:
        import lightgbm as lgb
        self.model = lgb.Booster(model_file=str(path))


class XGBoostModel(BaseModel):
    """XGBoost baseline."""
    
    name = "xgboost"
    
    def __init__(self, n_classes: int = 3, **kwargs: Any) -> None:
        self.n_classes = n_classes
        self.params = {
            "objective": "multi:softprob",
            "num_class": n_classes,
            "eval_metric": "mlogloss",
            "max_depth": 6,
            "learning_rate": 0.05,
            "subsample": 0.8,
            "colsample_bytree": 0.8,
            "n_jobs": -1,
            **kwargs,
        }
        self.model = None
    
    def fit(self, X: np.ndarray, y: np.ndarray, n_rounds: int = 500, **kwargs: Any) -> None:
        import xgboost as xgb
        
        # Remove NaN rows
        mask = ~(np.isnan(X).any(axis=1) | np.isnan(y))
        X_clean = X[mask]
        y_clean = y[mask].astype(int)
        
        # Create DMatrix
        dtrain = xgb.DMatrix(X_clean, label=y_clean)
        
        # Train
        self.model = xgb.train(self.params, dtrain, num_boost_round=n_rounds)
        log.info("xgboost.fit_done", n_samples=len(y_clean), n_rounds=n_rounds)
    
    def predict_proba(self, X: np.ndarray, **kwargs: Any) -> np.ndarray:
        import xgboost as xgb
        if self.model is None:
            raise RuntimeError("Model not fitted")
        dtest = xgb.DMatrix(X)
        return self.model.predict(dtest)
    
    def save(self, path: Path) -> None:
        if self.model is None:
            raise RuntimeError("Model not fitted")
        path.parent.mkdir(parents=True, exist_ok=True)
        self.model.save_model(str(path))
    
    def load(self, path: Path) -> None:
        import xgboost as xgb
        self.model = xgb.Booster()
        self.model.load_model(str(path))


# ---------------------------------------------------------------------------
# Deep learning: Dataset and utilities
# ---------------------------------------------------------------------------

class TickSequenceDataset(Dataset):
    """Dataset for tick sequences (for DeepLOB/Transformer)."""
    
    def __init__(
        self,
        features: np.ndarray,
        labels: np.ndarray,
        seq_len: int = 100,
        *,
        segment_id: np.ndarray | None = None,
    ) -> None:
        """
        Args:
            features: Shape (n_samples, n_features)
            labels: Shape (n_samples,) or (n_samples, n_horizons)
            seq_len: Sequence length for each sample
        """
        self.features = features
        self.labels = labels
        self.seq_len = seq_len
        self.segment_id = segment_id
        
        # Valid indices: need seq_len history and valid label
        self.valid_indices = []
        for i in range(seq_len, len(features)):
            if not np.isnan(labels[i]).any():
                if self.segment_id is not None:
                    # Enforce "no cross-segment" windows: the whole history window and the
                    # label index must share the same segment id.
                    s = self.segment_id[i]
                    window = self.segment_id[i - seq_len : i + 1]
                    if not np.all(window == s):
                        continue
                self.valid_indices.append(i)
    
    def __len__(self) -> int:
        return len(self.valid_indices)
    
    def __getitem__(self, idx: int) -> tuple[torch.Tensor, torch.Tensor]:
        i = self.valid_indices[idx]
        # Sequence: [i - seq_len, i)
        seq = self.features[i - self.seq_len : i]
        label = self.labels[i]
        
        # Handle NaN in sequence by replacing with 0
        seq = np.nan_to_num(seq, nan=0.0)
        
        return torch.tensor(seq, dtype=torch.float32), torch.tensor(label, dtype=torch.long)


# ---------------------------------------------------------------------------
# DeepLOB model
# ---------------------------------------------------------------------------

class DeepLOBNet(nn.Module):
    """
    DeepLOB-style architecture: CNN over L5 book features + LSTM over time.
    
    Input: (batch, seq_len, n_features)
    Output: (batch, n_classes) or (batch, n_horizons, n_classes)
    """
    
    def __init__(
        self,
        n_features: int,
        n_classes: int = 3,
        n_horizons: int = 1,
        hidden_dim: int = 64,
        n_lstm_layers: int = 2,
        dropout: float = 0.2,
    ) -> None:
        super().__init__()
        self.n_horizons = n_horizons
        self.n_classes = n_classes
        
        # CNN layers (1D convolution over feature dimension)
        self.conv1 = nn.Conv1d(n_features, 32, kernel_size=3, padding=1)
        self.conv2 = nn.Conv1d(32, 64, kernel_size=3, padding=1)
        self.pool = nn.MaxPool1d(2)
        self.dropout = nn.Dropout(dropout)
        
        # LSTM
        self.lstm = nn.LSTM(
            input_size=64,
            hidden_size=hidden_dim,
            num_layers=n_lstm_layers,
            batch_first=True,
            dropout=dropout if n_lstm_layers > 1 else 0,
            bidirectional=False,
        )
        
        # Output heads (one per horizon if multi-horizon)
        self.heads = nn.ModuleList([
            nn.Linear(hidden_dim, n_classes) for _ in range(n_horizons)
        ])
    
    def forward(self, x: torch.Tensor) -> torch.Tensor:
        # x: (batch, seq_len, n_features)
        batch_size = x.size(0)
        
        # CNN expects (batch, channels, seq_len)
        x = x.permute(0, 2, 1)
        
        x = F.relu(self.conv1(x))
        x = self.dropout(x)
        x = F.relu(self.conv2(x))
        x = self.pool(x)
        x = self.dropout(x)
        
        # Back to (batch, seq_len, channels) for LSTM
        x = x.permute(0, 2, 1)
        
        # LSTM
        lstm_out, _ = self.lstm(x)
        
        # Use last hidden state
        last_hidden = lstm_out[:, -1, :]
        
        # Multi-head output
        if self.n_horizons == 1:
            return self.heads[0](last_hidden)
        else:
            outputs = [head(last_hidden) for head in self.heads]
            return torch.stack(outputs, dim=1)  # (batch, n_horizons, n_classes)


class DeepLOBModel(BaseModel):
    """DeepLOB wrapper with training loop."""
    
    name = "deeplob"
    
    def __init__(
        self,
        n_features: int = 11,
        n_classes: int = 3,
        n_horizons: int = 1,
        seq_len: int = 100,
        hidden_dim: int = 64,
        device: str = "cuda" if torch.cuda.is_available() else "cpu",
        **kwargs: Any,
    ) -> None:
        self.n_features = n_features
        self.n_classes = n_classes
        self.n_horizons = n_horizons
        self.seq_len = seq_len
        self.hidden_dim = hidden_dim
        self.device = device
        self.model: DeepLOBNet | None = None
        self.scaler = StandardScaler()
    
    def fit(
        self,
        X: np.ndarray,
        y: np.ndarray,
        epochs: int = 50,
        batch_size: int = 256,
        lr: float = 1e-3,
        ddp: bool = True,
        use_amp: bool | None = None,
        num_workers: int = 4,
        seed: int | None = None,
        **kwargs: Any,
    ) -> None:
        if seed is not None:
            distu.seed_everything(seed)

        # If launched via torchrun, initialize distributed. This is a safe no-op
        # when WORLD_SIZE=1 and enables DDP when WORLD_SIZE>1.
        if ddp:
            distu.setup_distributed()
        ddp_active = ddp and distu.is_distributed()

        # Resolve device (respect explicit cpu)
        if str(self.device).startswith("cpu"):
            device = torch.device("cpu")
        elif ddp_active:
            device = distu.get_device()
        else:
            device = torch.device(self.device)
        self.device = str(device)

        # Scale features
        X_scaled = self.scaler.fit_transform(X)

        # Create dataset + sampler
        seg = kwargs.pop("segment_id", None)
        dataset = TickSequenceDataset(X_scaled, y, self.seq_len, segment_id=seg)
        sampler = None
        if ddp_active:
            from torch.utils.data.distributed import DistributedSampler

            sampler = DistributedSampler(dataset, shuffle=True)

        dataloader = DataLoader(
            dataset,
            batch_size=batch_size,
            shuffle=(sampler is None),
            sampler=sampler,
            num_workers=num_workers,
            pin_memory=(device.type == "cuda"),
        )

        # Create model
        self.model = DeepLOBNet(
            n_features=self.n_features,
            n_classes=self.n_classes,
            n_horizons=self.n_horizons,
            hidden_dim=self.hidden_dim,
        ).to(device)

        train_model: nn.Module = self.model
        if ddp_active:
            from torch.nn.parallel import DistributedDataParallel as DDP

            if device.type == "cuda":
                train_model = DDP(self.model, device_ids=[device.index], output_device=device.index)
            else:
                train_model = DDP(self.model)

        optimizer = torch.optim.AdamW(train_model.parameters(), lr=lr)
        criterion = nn.CrossEntropyLoss()

        amp_enabled = bool(use_amp) if use_amp is not None else (device.type == "cuda")
        scaler = torch.amp.GradScaler("cuda", enabled=amp_enabled and device.type == "cuda")
        autocast_ctx = (
            torch.cuda.amp.autocast if device.type == "cuda" else (lambda **_kwargs: nullcontext())
        )

        train_model.train()
        for epoch in range(epochs):
            if sampler is not None:
                sampler.set_epoch(epoch)

            total_loss = 0.0
            n_batches = 0

            for batch_x, batch_y in dataloader:
                batch_x = batch_x.to(device, non_blocking=True)
                batch_y = batch_y.to(device, non_blocking=True)

                optimizer.zero_grad(set_to_none=True)

                with autocast_ctx(enabled=amp_enabled):
                    outputs = train_model(batch_x)

                    if self.n_horizons == 1:
                        loss = criterion(outputs, batch_y)
                    else:
                        loss = (
                            sum(criterion(outputs[:, h], batch_y[:, h]) for h in range(self.n_horizons))
                            / self.n_horizons
                        )

                if scaler.is_enabled():
                    scaler.scale(loss).backward()
                    scaler.step(optimizer)
                    scaler.update()
                else:
                    loss.backward()
                    optimizer.step()

                total_loss += float(loss.detach().cpu().item())
                n_batches += 1

            avg_loss = total_loss / max(1, n_batches)
            if distu.is_rank0() and (epoch + 1) % 10 == 0:
                log.info("deeplob.epoch", epoch=epoch + 1, loss=f"{avg_loss:.4f}")

        if distu.is_rank0():
            log.info("deeplob.fit_done", epochs=epochs)

        if ddp_active:
            distu.barrier()
    
    def predict_proba(self, X: np.ndarray, **kwargs: Any) -> np.ndarray:
        if self.model is None:
            raise RuntimeError("Model not fitted")
        
        X_scaled = self.scaler.transform(X)

        seg = kwargs.get("segment_id", None)
        seg_arr: np.ndarray | None = None
        if seg is not None:
            try:
                seg_arr = np.asarray(seg)
                if len(seg_arr) != len(X_scaled):
                    seg_arr = None
            except Exception:
                seg_arr = None

        out = np.full((len(X_scaled), self.n_classes), np.nan, dtype=float)
        self.model.eval()
        with torch.no_grad():
            for i in range(self.seq_len, len(X_scaled)):
                if seg_arr is not None:
                    s = seg_arr[i]
                    if not np.all(seg_arr[i - self.seq_len : i + 1] == s):
                        continue

                seq = X_scaled[i - self.seq_len : i]
                seq = np.nan_to_num(seq, nan=0.0)
                seq_tensor = torch.tensor(seq, dtype=torch.float32).unsqueeze(0).to(self.device)
                outputs = self.model(seq_tensor)
                probs = F.softmax(outputs, dim=-1).cpu().numpy()
                p0 = probs[0]
                if getattr(p0, "ndim", 1) == 2:
                    p0 = p0[0]
                out[i] = p0

        return out
    
    def save(self, path: Path) -> None:
        if self.model is None:
            raise RuntimeError("Model not fitted")
        path.parent.mkdir(parents=True, exist_ok=True)
        torch.save({
            "model_state": self.model.state_dict(),
            "scaler": self.scaler,
            "config": {
                "n_features": self.n_features,
                "n_classes": self.n_classes,
                "n_horizons": self.n_horizons,
                "seq_len": self.seq_len,
                "hidden_dim": self.hidden_dim,
            },
        }, path)
    
    def load(self, path: Path) -> None:
        data = torch.load(path, map_location=self.device, weights_only=False)
        self.scaler = data["scaler"]
        config = data["config"]
        self.n_features = config["n_features"]
        self.n_classes = config["n_classes"]
        self.n_horizons = config["n_horizons"]
        self.seq_len = config["seq_len"]
        self.hidden_dim = config["hidden_dim"]
        
        self.model = DeepLOBNet(
            n_features=self.n_features,
            n_classes=self.n_classes,
            n_horizons=self.n_horizons,
            hidden_dim=self.hidden_dim,
        ).to(self.device)
        self.model.load_state_dict(data["model_state"])


# ---------------------------------------------------------------------------
# Transformer model
# ---------------------------------------------------------------------------

class TransformerEncoder(nn.Module):
    """Small Transformer encoder for tick sequences."""
    
    def __init__(
        self,
        n_features: int,
        n_classes: int = 3,
        n_horizons: int = 1,
        d_model: int = 64,
        n_heads: int = 4,
        n_layers: int = 3,
        dropout: float = 0.1,
        max_seq_len: int = 200,
    ) -> None:
        super().__init__()
        self.n_horizons = n_horizons
        self.n_classes = n_classes
        self.d_model = d_model
        
        # Input projection
        self.input_proj = nn.Linear(n_features, d_model)
        
        # Positional encoding
        self.pos_encoding = nn.Parameter(torch.randn(1, max_seq_len, d_model) * 0.02)
        
        # Transformer encoder layers
        encoder_layer = nn.TransformerEncoderLayer(
            d_model=d_model,
            nhead=n_heads,
            dim_feedforward=d_model * 4,
            dropout=dropout,
            batch_first=True,
        )
        self.transformer = nn.TransformerEncoder(encoder_layer, num_layers=n_layers)
        
        # Output heads
        self.heads = nn.ModuleList([
            nn.Linear(d_model, n_classes) for _ in range(n_horizons)
        ])
        
        self.dropout = nn.Dropout(dropout)
    
    def forward(self, x: torch.Tensor) -> torch.Tensor:
        # x: (batch, seq_len, n_features)
        seq_len = x.size(1)
        
        # Project and add positional encoding
        x = self.input_proj(x)
        x = x + self.pos_encoding[:, :seq_len, :]
        x = self.dropout(x)
        
        # Transformer
        x = self.transformer(x)
        
        # Use last position output
        last = x[:, -1, :]
        
        # Multi-head output
        if self.n_horizons == 1:
            return self.heads[0](last)
        else:
            outputs = [head(last) for head in self.heads]
            return torch.stack(outputs, dim=1)


class TransformerModel(BaseModel):
    """Transformer wrapper with training loop."""
    
    name = "transformer"
    
    def __init__(
        self,
        n_features: int = 11,
        n_classes: int = 3,
        n_horizons: int = 1,
        seq_len: int = 100,
        d_model: int = 64,
        device: str = "cuda" if torch.cuda.is_available() else "cpu",
        **kwargs: Any,
    ) -> None:
        self.n_features = n_features
        self.n_classes = n_classes
        self.n_horizons = n_horizons
        self.seq_len = seq_len
        self.d_model = d_model
        self.device = device
        self.model: TransformerEncoder | None = None
        self.scaler = StandardScaler()
    
    def fit(
        self,
        X: np.ndarray,
        y: np.ndarray,
        epochs: int = 50,
        batch_size: int = 256,
        lr: float = 1e-3,
        ddp: bool = True,
        use_amp: bool | None = None,
        num_workers: int = 4,
        seed: int | None = None,
        **kwargs: Any,
    ) -> None:
        if seed is not None:
            distu.seed_everything(seed)

        if ddp:
            distu.setup_distributed()
        ddp_active = ddp and distu.is_distributed()

        if str(self.device).startswith("cpu"):
            device = torch.device("cpu")
        elif ddp_active:
            device = distu.get_device()
        else:
            device = torch.device(self.device)
        self.device = str(device)

        # Scale features
        X_scaled = self.scaler.fit_transform(X)

        seg = kwargs.pop("segment_id", None)
        dataset = TickSequenceDataset(X_scaled, y, self.seq_len, segment_id=seg)
        sampler = None
        if ddp_active:
            from torch.utils.data.distributed import DistributedSampler

            sampler = DistributedSampler(dataset, shuffle=True)

        dataloader = DataLoader(
            dataset,
            batch_size=batch_size,
            shuffle=(sampler is None),
            sampler=sampler,
            num_workers=num_workers,
            pin_memory=(device.type == "cuda"),
        )

        self.model = TransformerEncoder(
            n_features=self.n_features,
            n_classes=self.n_classes,
            n_horizons=self.n_horizons,
            d_model=self.d_model,
            max_seq_len=self.seq_len,
        ).to(device)

        train_model: nn.Module = self.model
        if ddp_active:
            from torch.nn.parallel import DistributedDataParallel as DDP

            if device.type == "cuda":
                train_model = DDP(self.model, device_ids=[device.index], output_device=device.index)
            else:
                train_model = DDP(self.model)

        optimizer = torch.optim.AdamW(train_model.parameters(), lr=lr)
        criterion = nn.CrossEntropyLoss()

        amp_enabled = bool(use_amp) if use_amp is not None else (device.type == "cuda")
        scaler = torch.amp.GradScaler("cuda", enabled=amp_enabled and device.type == "cuda")
        autocast_ctx = (
            torch.cuda.amp.autocast if device.type == "cuda" else (lambda **_kwargs: nullcontext())
        )

        train_model.train()
        for epoch in range(epochs):
            if sampler is not None:
                sampler.set_epoch(epoch)

            total_loss = 0.0
            n_batches = 0

            for batch_x, batch_y in dataloader:
                batch_x = batch_x.to(device, non_blocking=True)
                batch_y = batch_y.to(device, non_blocking=True)

                optimizer.zero_grad(set_to_none=True)
                with autocast_ctx(enabled=amp_enabled):
                    outputs = train_model(batch_x)

                    if self.n_horizons == 1:
                        loss = criterion(outputs, batch_y)
                    else:
                        loss = (
                            sum(criterion(outputs[:, h], batch_y[:, h]) for h in range(self.n_horizons))
                            / self.n_horizons
                        )

                if scaler.is_enabled():
                    scaler.scale(loss).backward()
                    scaler.step(optimizer)
                    scaler.update()
                else:
                    loss.backward()
                    optimizer.step()

                total_loss += float(loss.detach().cpu().item())
                n_batches += 1

            avg_loss = total_loss / max(1, n_batches)
            if distu.is_rank0() and (epoch + 1) % 10 == 0:
                log.info("transformer.epoch", epoch=epoch + 1, loss=f"{avg_loss:.4f}")

        if distu.is_rank0():
            log.info("transformer.fit_done", epochs=epochs)

        if ddp_active:
            distu.barrier()
    
    def predict_proba(self, X: np.ndarray, **kwargs: Any) -> np.ndarray:
        if self.model is None:
            raise RuntimeError("Model not fitted")
        
        X_scaled = self.scaler.transform(X)

        seg = kwargs.get("segment_id", None)
        seg_arr: np.ndarray | None = None
        if seg is not None:
            try:
                seg_arr = np.asarray(seg)
                if len(seg_arr) != len(X_scaled):
                    seg_arr = None
            except Exception:
                seg_arr = None

        out = np.full((len(X_scaled), self.n_classes), np.nan, dtype=float)
        self.model.eval()
        with torch.no_grad():
            for i in range(self.seq_len, len(X_scaled)):
                if seg_arr is not None:
                    s = seg_arr[i]
                    if not np.all(seg_arr[i - self.seq_len : i + 1] == s):
                        continue

                seq = X_scaled[i - self.seq_len : i]
                seq = np.nan_to_num(seq, nan=0.0)
                seq_tensor = torch.tensor(seq, dtype=torch.float32).unsqueeze(0).to(self.device)
                outputs = self.model(seq_tensor)
                probs = F.softmax(outputs, dim=-1).cpu().numpy()
                p0 = probs[0]
                if getattr(p0, "ndim", 1) == 2:
                    p0 = p0[0]
                out[i] = p0

        return out
    
    def save(self, path: Path) -> None:
        if self.model is None:
            raise RuntimeError("Model not fitted")
        path.parent.mkdir(parents=True, exist_ok=True)
        torch.save({
            "model_state": self.model.state_dict(),
            "scaler": self.scaler,
            "config": {
                "n_features": self.n_features,
                "n_classes": self.n_classes,
                "n_horizons": self.n_horizons,
                "seq_len": self.seq_len,
                "d_model": self.d_model,
            },
        }, path)
    
    def load(self, path: Path) -> None:
        data = torch.load(path, map_location=self.device, weights_only=False)
        self.scaler = data["scaler"]
        config = data["config"]
        self.n_features = config["n_features"]
        self.n_classes = config["n_classes"]
        self.n_horizons = config["n_horizons"]
        self.seq_len = config["seq_len"]
        self.d_model = config["d_model"]
        
        self.model = TransformerEncoder(
            n_features=self.n_features,
            n_classes=self.n_classes,
            n_horizons=self.n_horizons,
            d_model=self.d_model,
            max_seq_len=self.seq_len,
        ).to(self.device)
        self.model.load_state_dict(data["model_state"])


# ---------------------------------------------------------------------------
# TCN (Temporal Convolutional Network) model
# ---------------------------------------------------------------------------

class TemporalBlock(nn.Module):
    """Single temporal block with dilated causal convolution."""
    
    def __init__(
        self,
        in_channels: int,
        out_channels: int,
        kernel_size: int,
        dilation: int,
        dropout: float = 0.2,
    ) -> None:
        super().__init__()
        padding = (kernel_size - 1) * dilation
        
        self.conv1 = nn.Conv1d(
            in_channels, out_channels, kernel_size,
            padding=padding, dilation=dilation
        )
        self.conv2 = nn.Conv1d(
            out_channels, out_channels, kernel_size,
            padding=padding, dilation=dilation
        )
        
        self.dropout = nn.Dropout(dropout)
        self.relu = nn.ReLU()
        
        # Residual connection
        self.downsample = nn.Conv1d(in_channels, out_channels, 1) if in_channels != out_channels else None
        
        self.padding = padding
    
    def forward(self, x: torch.Tensor) -> torch.Tensor:
        # x: (batch, channels, seq_len)
        out = self.conv1(x)
        out = out[:, :, :-self.padding]  # Remove future padding (causal)
        out = self.relu(out)
        out = self.dropout(out)
        
        out = self.conv2(out)
        out = out[:, :, :-self.padding]
        out = self.relu(out)
        out = self.dropout(out)
        
        # Residual
        res = x if self.downsample is None else self.downsample(x)
        
        # Match sequence lengths
        if out.size(2) != res.size(2):
            min_len = min(out.size(2), res.size(2))
            out = out[:, :, -min_len:]
            res = res[:, :, -min_len:]
        
        return self.relu(out + res)


class TCNNet(nn.Module):
    """
    Temporal Convolutional Network for sequence classification.
    
    Uses dilated causal convolutions for efficient long-range dependencies.
    """
    
    def __init__(
        self,
        n_features: int,
        n_classes: int = 3,
        n_horizons: int = 1,
        n_channels: list[int] | None = None,
        kernel_size: int = 3,
        dropout: float = 0.2,
    ) -> None:
        super().__init__()
        self.n_horizons = n_horizons
        self.n_classes = n_classes
        
        if n_channels is None:
            n_channels = [64, 64, 64, 64]
        
        layers: list[nn.Module] = []
        in_channels = n_features
        
        for i, out_channels in enumerate(n_channels):
            dilation = 2 ** i
            layers.append(
                TemporalBlock(in_channels, out_channels, kernel_size, dilation, dropout)
            )
            in_channels = out_channels
        
        self.network = nn.Sequential(*layers)
        
        # Output heads
        self.heads = nn.ModuleList([
            nn.Linear(n_channels[-1], n_classes) for _ in range(n_horizons)
        ])
    
    def forward(self, x: torch.Tensor) -> torch.Tensor:
        # x: (batch, seq_len, n_features)
        x = x.permute(0, 2, 1)  # (batch, n_features, seq_len)
        
        out = self.network(x)
        
        # Use last time step
        last = out[:, :, -1]
        
        if self.n_horizons == 1:
            return self.heads[0](last)
        else:
            outputs = [head(last) for head in self.heads]
            return torch.stack(outputs, dim=1)


class TCNModel(BaseModel):
    """TCN wrapper with training loop."""
    
    name = "tcn"
    
    def __init__(
        self,
        n_features: int = 11,
        n_classes: int = 3,
        n_horizons: int = 1,
        seq_len: int = 100,
        n_channels: list[int] | None = None,
        device: str = "cuda" if torch.cuda.is_available() else "cpu",
        **kwargs: Any,
    ) -> None:
        self.n_features = n_features
        self.n_classes = n_classes
        self.n_horizons = n_horizons
        self.seq_len = seq_len
        self.n_channels = n_channels or [64, 64, 64, 64]
        self.device = device
        self.model: TCNNet | None = None
        self.scaler = StandardScaler()
    
    def fit(
        self,
        X: np.ndarray,
        y: np.ndarray,
        epochs: int = 50,
        batch_size: int = 256,
        lr: float = 1e-3,
        ddp: bool = True,
        use_amp: bool | None = None,
        num_workers: int = 4,
        seed: int | None = None,
        **kwargs: Any,
    ) -> None:
        if seed is not None:
            distu.seed_everything(seed)

        if ddp:
            distu.setup_distributed()
        ddp_active = ddp and distu.is_distributed()

        if str(self.device).startswith("cpu"):
            device = torch.device("cpu")
        elif ddp_active:
            device = distu.get_device()
        else:
            device = torch.device(self.device)
        self.device = str(device)

        X_scaled = self.scaler.fit_transform(X)

        seg = kwargs.pop("segment_id", None)
        dataset = TickSequenceDataset(X_scaled, y, self.seq_len, segment_id=seg)
        sampler = None
        if ddp_active:
            from torch.utils.data.distributed import DistributedSampler

            sampler = DistributedSampler(dataset, shuffle=True)

        dataloader = DataLoader(
            dataset,
            batch_size=batch_size,
            shuffle=(sampler is None),
            sampler=sampler,
            num_workers=num_workers,
            pin_memory=(device.type == "cuda"),
        )

        self.model = TCNNet(
            n_features=self.n_features,
            n_classes=self.n_classes,
            n_horizons=self.n_horizons,
            n_channels=self.n_channels,
        ).to(device)

        train_model: nn.Module = self.model
        if ddp_active:
            from torch.nn.parallel import DistributedDataParallel as DDP

            if device.type == "cuda":
                train_model = DDP(self.model, device_ids=[device.index], output_device=device.index)
            else:
                train_model = DDP(self.model)

        optimizer = torch.optim.AdamW(train_model.parameters(), lr=lr)
        criterion = nn.CrossEntropyLoss()

        amp_enabled = bool(use_amp) if use_amp is not None else (device.type == "cuda")
        scaler = torch.amp.GradScaler("cuda", enabled=amp_enabled and device.type == "cuda")
        autocast_ctx = (
            torch.cuda.amp.autocast if device.type == "cuda" else (lambda **_kwargs: nullcontext())
        )

        train_model.train()
        for epoch in range(epochs):
            if sampler is not None:
                sampler.set_epoch(epoch)

            total_loss = 0.0
            n_batches = 0

            for batch_x, batch_y in dataloader:
                batch_x = batch_x.to(device, non_blocking=True)
                batch_y = batch_y.to(device, non_blocking=True)

                optimizer.zero_grad(set_to_none=True)
                with autocast_ctx(enabled=amp_enabled):
                    outputs = train_model(batch_x)

                    if self.n_horizons == 1:
                        loss = criterion(outputs, batch_y)
                    else:
                        loss = (
                            sum(criterion(outputs[:, h], batch_y[:, h]) for h in range(self.n_horizons))
                            / self.n_horizons
                        )

                if scaler.is_enabled():
                    scaler.scale(loss).backward()
                    scaler.step(optimizer)
                    scaler.update()
                else:
                    loss.backward()
                    optimizer.step()

                total_loss += float(loss.detach().cpu().item())
                n_batches += 1

            avg_loss = total_loss / max(1, n_batches)
            if distu.is_rank0() and (epoch + 1) % 10 == 0:
                log.info("tcn.epoch", epoch=epoch + 1, loss=f"{avg_loss:.4f}")

        if distu.is_rank0():
            log.info("tcn.fit_done", epochs=epochs)

        if ddp_active:
            distu.barrier()
    
    def predict_proba(self, X: np.ndarray, **kwargs: Any) -> np.ndarray:
        if self.model is None:
            raise RuntimeError("Model not fitted")
        
        X_scaled = self.scaler.transform(X)

        seg = kwargs.get("segment_id", None)
        seg_arr: np.ndarray | None = None
        if seg is not None:
            try:
                seg_arr = np.asarray(seg)
                if len(seg_arr) != len(X_scaled):
                    seg_arr = None
            except Exception:
                seg_arr = None

        out = np.full((len(X_scaled), self.n_classes), np.nan, dtype=float)
        self.model.eval()
        with torch.no_grad():
            for i in range(self.seq_len, len(X_scaled)):
                if seg_arr is not None:
                    s = seg_arr[i]
                    if not np.all(seg_arr[i - self.seq_len : i + 1] == s):
                        continue

                seq = X_scaled[i - self.seq_len : i]
                seq = np.nan_to_num(seq, nan=0.0)
                seq_tensor = torch.tensor(seq, dtype=torch.float32).unsqueeze(0).to(self.device)
                outputs = self.model(seq_tensor)
                probs = F.softmax(outputs, dim=-1).cpu().numpy()
                p0 = probs[0]
                if getattr(p0, "ndim", 1) == 2:
                    p0 = p0[0]
                out[i] = p0

        return out
    
    def save(self, path: Path) -> None:
        if self.model is None:
            raise RuntimeError("Model not fitted")
        path.parent.mkdir(parents=True, exist_ok=True)
        torch.save({
            "model_state": self.model.state_dict(),
            "scaler": self.scaler,
            "config": {
                "n_features": self.n_features,
                "n_classes": self.n_classes,
                "n_horizons": self.n_horizons,
                "seq_len": self.seq_len,
                "n_channels": self.n_channels,
            },
        }, path)
    
    def load(self, path: Path) -> None:
        data = torch.load(path, map_location=self.device, weights_only=False)
        self.scaler = data["scaler"]
        config = data["config"]
        self.n_features = config["n_features"]
        self.n_classes = config["n_classes"]
        self.n_horizons = config["n_horizons"]
        self.seq_len = config["seq_len"]
        self.n_channels = config["n_channels"]
        
        self.model = TCNNet(
            n_features=self.n_features,
            n_classes=self.n_classes,
            n_horizons=self.n_horizons,
            n_channels=self.n_channels,
        ).to(self.device)
        self.model.load_state_dict(data["model_state"])


# ---------------------------------------------------------------------------
# TLOB (Transformer with dual attention for LOB)
# ---------------------------------------------------------------------------

class SpatialAttention(nn.Module):
    """Attention over price levels (spatial dimension of LOB)."""
    
    def __init__(self, n_features: int, n_heads: int = 4) -> None:
        super().__init__()
        self.attention = nn.MultiheadAttention(n_features, n_heads, batch_first=True)
        self.norm = nn.LayerNorm(n_features)
    
    def forward(self, x: torch.Tensor) -> torch.Tensor:
        # x: (batch, seq_len, n_features)
        attn_out, _ = self.attention(x, x, x)
        return self.norm(x + attn_out)


class TemporalAttention(nn.Module):
    """Attention over time steps."""
    
    def __init__(self, d_model: int, n_heads: int = 4, dropout: float = 0.1) -> None:
        super().__init__()
        self.attention = nn.MultiheadAttention(d_model, n_heads, batch_first=True, dropout=dropout)
        self.norm = nn.LayerNorm(d_model)
        self.dropout = nn.Dropout(dropout)
    
    def forward(self, x: torch.Tensor, mask: torch.Tensor | None = None) -> torch.Tensor:
        attn_out, _ = self.attention(x, x, x, attn_mask=mask)
        return self.norm(x + self.dropout(attn_out))


class TLOBNet(nn.Module):
    """
    TLOB: Transformer with dual attention for Limit Order Book prediction.
    
    Combines:
    1. Spatial attention over L5 price levels (book structure)
    2. Temporal attention over time steps (sequence dynamics)
    
    Reference: "TLOB: A Novel Transformer Model with Dual Attention for Stock Price
    Trend Prediction with Limit Order Book Data" (2025)
    """
    
    def __init__(
        self,
        n_features: int,
        n_classes: int = 3,
        n_horizons: int = 1,
        d_model: int = 64,
        n_spatial_heads: int = 4,
        n_temporal_heads: int = 4,
        n_temporal_layers: int = 3,
        dropout: float = 0.1,
        max_seq_len: int = 200,
    ) -> None:
        super().__init__()
        self.n_horizons = n_horizons
        self.n_classes = n_classes
        self.d_model = d_model
        
        # Input projection
        self.input_proj = nn.Linear(n_features, d_model)
        
        # Spatial attention (over features/price levels)
        self.spatial_attn = SpatialAttention(d_model, n_spatial_heads)
        
        # Positional encoding for temporal attention
        self.pos_encoding = nn.Parameter(torch.randn(1, max_seq_len, d_model) * 0.02)
        
        # Temporal attention layers
        self.temporal_layers = nn.ModuleList([
            TemporalAttention(d_model, n_temporal_heads, dropout)
            for _ in range(n_temporal_layers)
        ])
        
        # Feed-forward
        self.ffn = nn.Sequential(
            nn.Linear(d_model, d_model * 4),
            nn.GELU(),
            nn.Dropout(dropout),
            nn.Linear(d_model * 4, d_model),
            nn.Dropout(dropout),
        )
        self.ffn_norm = nn.LayerNorm(d_model)
        
        # Output heads
        self.heads = nn.ModuleList([
            nn.Linear(d_model, n_classes) for _ in range(n_horizons)
        ])
    
    def forward(self, x: torch.Tensor) -> torch.Tensor:
        # x: (batch, seq_len, n_features)
        batch_size, seq_len, _ = x.size()
        
        # Project to d_model
        x = self.input_proj(x)
        
        # Spatial attention (over features at each time step)
        x = self.spatial_attn(x)
        
        # Add positional encoding
        x = x + self.pos_encoding[:, :seq_len, :]
        
        # Temporal attention layers
        for temporal_layer in self.temporal_layers:
            x = temporal_layer(x)
        
        # Feed-forward
        x = self.ffn_norm(x + self.ffn(x))
        
        # Use last position
        last = x[:, -1, :]
        
        if self.n_horizons == 1:
            return self.heads[0](last)
        else:
            outputs = [head(last) for head in self.heads]
            return torch.stack(outputs, dim=1)


class TLOBModel(BaseModel):
    """TLOB wrapper with training loop."""
    
    name = "tlob"
    
    def __init__(
        self,
        n_features: int = 11,
        n_classes: int = 3,
        n_horizons: int = 1,
        seq_len: int = 100,
        d_model: int = 64,
        device: str = "cuda" if torch.cuda.is_available() else "cpu",
        **kwargs: Any,
    ) -> None:
        self.n_features = n_features
        self.n_classes = n_classes
        self.n_horizons = n_horizons
        self.seq_len = seq_len
        self.d_model = d_model
        self.device = device
        self.model: TLOBNet | None = None
        self.scaler = StandardScaler()
    
    def fit(
        self,
        X: np.ndarray,
        y: np.ndarray,
        epochs: int = 50,
        batch_size: int = 256,
        lr: float = 1e-3,
        ddp: bool = True,
        use_amp: bool | None = None,
        num_workers: int = 4,
        seed: int | None = None,
        **kwargs: Any,
    ) -> None:
        if seed is not None:
            distu.seed_everything(seed)

        if ddp:
            distu.setup_distributed()
        ddp_active = ddp and distu.is_distributed()

        if str(self.device).startswith("cpu"):
            device = torch.device("cpu")
        elif ddp_active:
            device = distu.get_device()
        else:
            device = torch.device(self.device)
        self.device = str(device)

        X_scaled = self.scaler.fit_transform(X)

        seg = kwargs.pop("segment_id", None)
        dataset = TickSequenceDataset(X_scaled, y, self.seq_len, segment_id=seg)
        sampler = None
        if ddp_active:
            from torch.utils.data.distributed import DistributedSampler

            sampler = DistributedSampler(dataset, shuffle=True)

        dataloader = DataLoader(
            dataset,
            batch_size=batch_size,
            shuffle=(sampler is None),
            sampler=sampler,
            num_workers=num_workers,
            pin_memory=(device.type == "cuda"),
        )

        self.model = TLOBNet(
            n_features=self.n_features,
            n_classes=self.n_classes,
            n_horizons=self.n_horizons,
            d_model=self.d_model,
            max_seq_len=self.seq_len,
        ).to(device)

        train_model: nn.Module = self.model
        if ddp_active:
            from torch.nn.parallel import DistributedDataParallel as DDP

            if device.type == "cuda":
                train_model = DDP(self.model, device_ids=[device.index], output_device=device.index)
            else:
                train_model = DDP(self.model)

        optimizer = torch.optim.AdamW(train_model.parameters(), lr=lr)
        criterion = nn.CrossEntropyLoss()

        amp_enabled = bool(use_amp) if use_amp is not None else (device.type == "cuda")
        scaler = torch.amp.GradScaler("cuda", enabled=amp_enabled and device.type == "cuda")
        autocast_ctx = (
            torch.cuda.amp.autocast if device.type == "cuda" else (lambda **_kwargs: nullcontext())
        )

        train_model.train()
        for epoch in range(epochs):
            if sampler is not None:
                sampler.set_epoch(epoch)

            total_loss = 0.0
            n_batches = 0

            for batch_x, batch_y in dataloader:
                batch_x = batch_x.to(device, non_blocking=True)
                batch_y = batch_y.to(device, non_blocking=True)

                optimizer.zero_grad(set_to_none=True)
                with autocast_ctx(enabled=amp_enabled):
                    outputs = train_model(batch_x)

                    if self.n_horizons == 1:
                        loss = criterion(outputs, batch_y)
                    else:
                        loss = (
                            sum(criterion(outputs[:, h], batch_y[:, h]) for h in range(self.n_horizons))
                            / self.n_horizons
                        )

                if scaler.is_enabled():
                    scaler.scale(loss).backward()
                    scaler.step(optimizer)
                    scaler.update()
                else:
                    loss.backward()
                    optimizer.step()

                total_loss += float(loss.detach().cpu().item())
                n_batches += 1

            avg_loss = total_loss / max(1, n_batches)
            if distu.is_rank0() and (epoch + 1) % 10 == 0:
                log.info("tlob.epoch", epoch=epoch + 1, loss=f"{avg_loss:.4f}")

        if distu.is_rank0():
            log.info("tlob.fit_done", epochs=epochs)

        if ddp_active:
            distu.barrier()
    
    def predict_proba(self, X: np.ndarray, **kwargs: Any) -> np.ndarray:
        if self.model is None:
            raise RuntimeError("Model not fitted")
        
        X_scaled = self.scaler.transform(X)

        seg = kwargs.get("segment_id", None)
        seg_arr: np.ndarray | None = None
        if seg is not None:
            try:
                seg_arr = np.asarray(seg)
                if len(seg_arr) != len(X_scaled):
                    seg_arr = None
            except Exception:
                seg_arr = None

        out = np.full((len(X_scaled), self.n_classes), np.nan, dtype=float)
        self.model.eval()
        with torch.no_grad():
            for i in range(self.seq_len, len(X_scaled)):
                if seg_arr is not None:
                    s = seg_arr[i]
                    if not np.all(seg_arr[i - self.seq_len : i + 1] == s):
                        continue

                seq = X_scaled[i - self.seq_len : i]
                seq = np.nan_to_num(seq, nan=0.0)
                seq_tensor = torch.tensor(seq, dtype=torch.float32).unsqueeze(0).to(self.device)
                outputs = self.model(seq_tensor)
                probs = F.softmax(outputs, dim=-1).cpu().numpy()
                p0 = probs[0]
                if getattr(p0, "ndim", 1) == 2:
                    p0 = p0[0]
                out[i] = p0

        return out
    
    def save(self, path: Path) -> None:
        if self.model is None:
            raise RuntimeError("Model not fitted")
        path.parent.mkdir(parents=True, exist_ok=True)
        torch.save({
            "model_state": self.model.state_dict(),
            "scaler": self.scaler,
            "config": {
                "n_features": self.n_features,
                "n_classes": self.n_classes,
                "n_horizons": self.n_horizons,
                "seq_len": self.seq_len,
                "d_model": self.d_model,
            },
        }, path)
    
    def load(self, path: Path) -> None:
        data = torch.load(path, map_location=self.device, weights_only=False)
        self.scaler = data["scaler"]
        config = data["config"]
        self.n_features = config["n_features"]
        self.n_classes = config["n_classes"]
        self.n_horizons = config["n_horizons"]
        self.seq_len = config["seq_len"]
        self.d_model = config["d_model"]
        
        self.model = TLOBNet(
            n_features=self.n_features,
            n_classes=self.n_classes,
            n_horizons=self.n_horizons,
            d_model=self.d_model,
            max_seq_len=self.seq_len,
        ).to(self.device)
        self.model.load_state_dict(data["model_state"])


# ---------------------------------------------------------------------------
# SSM/Mamba-style model (simplified S4-like implementation)
# ---------------------------------------------------------------------------

class S4Block(nn.Module):
    """
    Simplified State Space Model block.
    
    This is a lightweight SSM-inspired block that can be used as an alternative
    to attention for sequence modeling. It uses a structured state-space approach
    with linear complexity.
    """
    
    def __init__(
        self,
        d_model: int,
        d_state: int = 16,
        dropout: float = 0.1,
    ) -> None:
        super().__init__()
        self.d_model = d_model
        self.d_state = d_state
        
        # Simplified linear layers instead of explicit state-space matrices
        self.input_gate = nn.Linear(d_model, d_model)
        self.forget_gate = nn.Linear(d_model, d_model)
        self.output_gate = nn.Linear(d_model, d_model)
        
        # State mixing
        self.state_proj = nn.Linear(d_model, d_model)
        
        self.norm = nn.LayerNorm(d_model)
        self.dropout = nn.Dropout(dropout)
    
    def forward(self, x: torch.Tensor) -> torch.Tensor:
        # x: (batch, seq_len, d_model)
        batch, seq_len, d_model = x.size()
        
        # Simple gated RNN-style update (S4-inspired but simplified)
        h = torch.zeros(batch, d_model, device=x.device)
        outputs = []
        
        for t in range(seq_len):
            xt = x[:, t, :]  # (batch, d_model)
            
            # Gated update
            i = torch.sigmoid(self.input_gate(xt))
            f = torch.sigmoid(self.forget_gate(xt))
            
            # State update
            h = f * h + i * torch.tanh(self.state_proj(xt))
            
            # Output
            o = torch.sigmoid(self.output_gate(xt))
            y = o * h
            
            outputs.append(y)
        
        out = torch.stack(outputs, dim=1)
        return self.norm(x + self.dropout(out))


class SSMNet(nn.Module):
    """
    State Space Model network for sequence classification.
    
    Uses simplified S4-like blocks for efficient long-range modeling.
    """
    
    def __init__(
        self,
        n_features: int,
        n_classes: int = 3,
        n_horizons: int = 1,
        d_model: int = 64,
        d_state: int = 16,
        n_layers: int = 4,
        dropout: float = 0.1,
    ) -> None:
        super().__init__()
        self.n_horizons = n_horizons
        self.n_classes = n_classes
        
        # Input projection
        self.input_proj = nn.Linear(n_features, d_model)
        
        # SSM layers
        self.layers = nn.ModuleList([
            S4Block(d_model, d_state, dropout)
            for _ in range(n_layers)
        ])
        
        # Output heads
        self.heads = nn.ModuleList([
            nn.Linear(d_model, n_classes) for _ in range(n_horizons)
        ])
    
    def forward(self, x: torch.Tensor) -> torch.Tensor:
        # x: (batch, seq_len, n_features)
        x = self.input_proj(x)
        
        for layer in self.layers:
            x = layer(x)
        
        # Use last position
        last = x[:, -1, :]
        
        if self.n_horizons == 1:
            return self.heads[0](last)
        else:
            outputs = [head(last) for head in self.heads]
            return torch.stack(outputs, dim=1)


class SSMModel(BaseModel):
    """SSM/Mamba-style model wrapper."""
    
    name = "ssm"
    
    def __init__(
        self,
        n_features: int = 11,
        n_classes: int = 3,
        n_horizons: int = 1,
        seq_len: int = 100,
        d_model: int = 64,
        d_state: int = 16,
        device: str = "cuda" if torch.cuda.is_available() else "cpu",
        **kwargs: Any,
    ) -> None:
        self.n_features = n_features
        self.n_classes = n_classes
        self.n_horizons = n_horizons
        self.seq_len = seq_len
        self.d_model = d_model
        self.d_state = d_state
        self.device = device
        self.model: SSMNet | None = None
        self.scaler = StandardScaler()
    
    def fit(
        self,
        X: np.ndarray,
        y: np.ndarray,
        epochs: int = 50,
        batch_size: int = 256,
        lr: float = 1e-3,
        ddp: bool = True,
        use_amp: bool | None = None,
        num_workers: int = 4,
        seed: int | None = None,
        **kwargs: Any,
    ) -> None:
        if seed is not None:
            distu.seed_everything(seed)

        if ddp:
            distu.setup_distributed()
        ddp_active = ddp and distu.is_distributed()

        if str(self.device).startswith("cpu"):
            device = torch.device("cpu")
        elif ddp_active:
            device = distu.get_device()
        else:
            device = torch.device(self.device)
        self.device = str(device)

        X_scaled = self.scaler.fit_transform(X)

        seg = kwargs.pop("segment_id", None)
        dataset = TickSequenceDataset(X_scaled, y, self.seq_len, segment_id=seg)
        sampler = None
        if ddp_active:
            from torch.utils.data.distributed import DistributedSampler

            sampler = DistributedSampler(dataset, shuffle=True)

        dataloader = DataLoader(
            dataset,
            batch_size=batch_size,
            shuffle=(sampler is None),
            sampler=sampler,
            num_workers=num_workers,
            pin_memory=(device.type == "cuda"),
        )

        self.model = SSMNet(
            n_features=self.n_features,
            n_classes=self.n_classes,
            n_horizons=self.n_horizons,
            d_model=self.d_model,
            d_state=self.d_state,
        ).to(device)

        train_model: nn.Module = self.model
        if ddp_active:
            from torch.nn.parallel import DistributedDataParallel as DDP

            if device.type == "cuda":
                train_model = DDP(self.model, device_ids=[device.index], output_device=device.index)
            else:
                train_model = DDP(self.model)

        optimizer = torch.optim.AdamW(train_model.parameters(), lr=lr)
        criterion = nn.CrossEntropyLoss()

        amp_enabled = bool(use_amp) if use_amp is not None else (device.type == "cuda")
        scaler = torch.amp.GradScaler("cuda", enabled=amp_enabled and device.type == "cuda")
        autocast_ctx = (
            torch.cuda.amp.autocast if device.type == "cuda" else (lambda **_kwargs: nullcontext())
        )

        train_model.train()
        for epoch in range(epochs):
            if sampler is not None:
                sampler.set_epoch(epoch)

            total_loss = 0.0
            n_batches = 0

            for batch_x, batch_y in dataloader:
                batch_x = batch_x.to(device, non_blocking=True)
                batch_y = batch_y.to(device, non_blocking=True)

                optimizer.zero_grad(set_to_none=True)
                with autocast_ctx(enabled=amp_enabled):
                    outputs = train_model(batch_x)

                    if self.n_horizons == 1:
                        loss = criterion(outputs, batch_y)
                    else:
                        loss = (
                            sum(criterion(outputs[:, h], batch_y[:, h]) for h in range(self.n_horizons))
                            / self.n_horizons
                        )

                if scaler.is_enabled():
                    scaler.scale(loss).backward()
                    scaler.step(optimizer)
                    scaler.update()
                else:
                    loss.backward()
                    optimizer.step()

                total_loss += float(loss.detach().cpu().item())
                n_batches += 1

            avg_loss = total_loss / max(1, n_batches)
            if distu.is_rank0() and (epoch + 1) % 10 == 0:
                log.info("ssm.epoch", epoch=epoch + 1, loss=f"{avg_loss:.4f}")

        if distu.is_rank0():
            log.info("ssm.fit_done", epochs=epochs)

        if ddp_active:
            distu.barrier()
    
    def predict_proba(self, X: np.ndarray, **kwargs: Any) -> np.ndarray:
        if self.model is None:
            raise RuntimeError("Model not fitted")
        
        X_scaled = self.scaler.transform(X)

        seg = kwargs.get("segment_id", None)
        seg_arr: np.ndarray | None = None
        if seg is not None:
            try:
                seg_arr = np.asarray(seg)
                if len(seg_arr) != len(X_scaled):
                    seg_arr = None
            except Exception:
                seg_arr = None

        out = np.full((len(X_scaled), self.n_classes), np.nan, dtype=float)
        self.model.eval()
        with torch.no_grad():
            for i in range(self.seq_len, len(X_scaled)):
                if seg_arr is not None:
                    s = seg_arr[i]
                    if not np.all(seg_arr[i - self.seq_len : i + 1] == s):
                        continue

                seq = X_scaled[i - self.seq_len : i]
                seq = np.nan_to_num(seq, nan=0.0)
                seq_tensor = torch.tensor(seq, dtype=torch.float32).unsqueeze(0).to(self.device)
                outputs = self.model(seq_tensor)
                probs = F.softmax(outputs, dim=-1).cpu().numpy()
                p0 = probs[0]
                if getattr(p0, "ndim", 1) == 2:
                    p0 = p0[0]
                out[i] = p0

        return out
    
    def save(self, path: Path) -> None:
        if self.model is None:
            raise RuntimeError("Model not fitted")
        path.parent.mkdir(parents=True, exist_ok=True)
        torch.save({
            "model_state": self.model.state_dict(),
            "scaler": self.scaler,
            "config": {
                "n_features": self.n_features,
                "n_classes": self.n_classes,
                "n_horizons": self.n_horizons,
                "seq_len": self.seq_len,
                "d_model": self.d_model,
                "d_state": self.d_state,
            },
        }, path)
    
    def load(self, path: Path) -> None:
        data = torch.load(path, map_location=self.device, weights_only=False)
        self.scaler = data["scaler"]
        config = data["config"]
        self.n_features = config["n_features"]
        self.n_classes = config["n_classes"]
        self.n_horizons = config["n_horizons"]
        self.seq_len = config["seq_len"]
        self.d_model = config["d_model"]
        self.d_state = config["d_state"]
        
        self.model = SSMNet(
            n_features=self.n_features,
            n_classes=self.n_classes,
            n_horizons=self.n_horizons,
            d_model=self.d_model,
            d_state=self.d_state,
        ).to(self.device)
        self.model.load_state_dict(data["model_state"])


# ---------------------------------------------------------------------------
# Model factory and training entrypoint
# ---------------------------------------------------------------------------

def create_model(model_type: str, **kwargs: Any) -> BaseModel:
    """Factory function to create a model by type."""
    if model_type == "logistic":
        # Common callers pass deep-model-only knobs; strip them for baselines.
        kwargs.pop("n_features", None)
        kwargs.pop("seq_len", None)
        kwargs.pop("hidden_dim", None)
        kwargs.pop("d_model", None)
        kwargs.pop("n_heads", None)
        kwargs.pop("n_layers", None)
        kwargs.pop("n_channels", None)
        kwargs.pop("d_state", None)
        return LogisticModel(**kwargs)
    elif model_type == "lightgbm":
        kwargs.pop("n_features", None)
        kwargs.pop("seq_len", None)
        kwargs.pop("hidden_dim", None)
        kwargs.pop("d_model", None)
        kwargs.pop("n_heads", None)
        kwargs.pop("n_layers", None)
        kwargs.pop("n_channels", None)
        kwargs.pop("d_state", None)
        return LightGBMModel(**kwargs)
    elif model_type == "xgboost":
        kwargs.pop("n_features", None)
        kwargs.pop("seq_len", None)
        kwargs.pop("hidden_dim", None)
        kwargs.pop("d_model", None)
        kwargs.pop("n_heads", None)
        kwargs.pop("n_layers", None)
        kwargs.pop("n_channels", None)
        kwargs.pop("d_state", None)
        return XGBoostModel(**kwargs)
    elif model_type == "deeplob":
        return DeepLOBModel(**kwargs)
    elif model_type == "transformer":
        return TransformerModel(**kwargs)
    elif model_type == "tcn":
        return TCNModel(**kwargs)
    elif model_type == "tlob":
        return TLOBModel(**kwargs)
    elif model_type == "ssm":
        return SSMModel(**kwargs)
    else:
        raise ValueError(f"Unknown model type: {model_type}")


def train_model(
    model_type: ModelType,
    symbol: str,
    data_dir: Path,
    artifacts_dir: Path,
    horizon: int = 50,
    gpus: int = 1,
    epochs: int | None = None,
    batch_size: int | None = None,
    lr: float | None = None,
    ddp: bool = True,
    use_amp: bool | None = None,
    num_workers: int = 4,
    seed: int | None = None,
    **kwargs: Any,
) -> Path:
    """
    Train a model for a symbol.
    
    Args:
        model_type: Type of model to train
        symbol: Instrument code
        data_dir: Data directory with features and labels
        artifacts_dir: Output directory for model artifacts
        horizon: Label horizon to train on
        gpus: Number of GPUs (for deep models)
    
    Returns:
        Path to saved model
    """
    from ghtrader.features import read_features_for_symbol, read_features_manifest
    from ghtrader.labels import read_labels_for_symbol, read_labels_manifest
    
    log.info("train.start", model_type=model_type, symbol=symbol, horizon=horizon)

    # If launched via torchrun, initialize distributed once. This makes it safe to
    # run under torchrun without multiple ranks clobbering artifacts.
    ddp_active = False
    if ddp:
        ddp_active = distu.setup_distributed()
    
    # Load features and labels
    features_df = read_features_for_symbol(data_dir, symbol)
    labels_df = read_labels_for_symbol(data_dir, symbol)

    # Guardrails: ensure features/labels were built against the same tick source.
    feat_manifest = read_features_manifest(data_dir, symbol)
    lab_manifest = read_labels_manifest(data_dir, symbol)
    if not feat_manifest or not lab_manifest:
        raise ValueError(
            "Missing features/labels manifest.json. "
            "Run `ghtrader build` first (and re-run with --overwrite if needed)."
        )
    tl_f = str(feat_manifest.get("ticks_lake") or "")
    tl_l = str(lab_manifest.get("ticks_lake") or "")
    lv_f = str(feat_manifest.get("lake_version") or "v2")
    lv_l = str(lab_manifest.get("lake_version") or "v2")
    if tl_f != tl_l:
        raise ValueError(f"ticks_lake mismatch between features ({tl_f}) and labels ({tl_l}) for symbol={symbol}")
    if lv_f != lv_l:
        raise ValueError(f"lake_version mismatch between features ({lv_f}) and labels ({lv_l}) for symbol={symbol}")
    if str(symbol).startswith("KQ.m@") and tl_f != "main_l5":
        raise ValueError(
            "Continuous symbols (KQ.m@...) must be trained from derived main_l5 ticks. "
            "Rebuild with: `ghtrader build --ticks-lake main_l5 ...`"
        )
    if tl_f == "main_l5":
        sf = dict(feat_manifest.get("schedule") or {})
        sl = dict(lab_manifest.get("schedule") or {})
        hf = str(sf.get("hash") or "")
        hl = str(sl.get("hash") or "")
        if not hf or not hl:
            raise ValueError(
                "Missing schedule provenance in features/labels manifests. "
                "Rebuild features+labels with overwrite=True."
            )
        if hf != hl:
            raise ValueError(f"schedule_hash mismatch between features ({hf}) and labels ({hl}) for symbol={symbol}")
        d0f = str(sf.get("l5_start_date") or "")
        d0l = str(sl.get("l5_start_date") or "")
        if d0f and d0l and d0f != d0l:
            raise ValueError(f"l5_start_date mismatch between features ({d0f}) and labels ({d0l}) for symbol={symbol}")
    
    # Merge on datetime
    df = features_df.merge(labels_df[["datetime", f"label_{horizon}"]], on="datetime")
    
    # Prepare X and y
    manifest = feat_manifest
    feature_cols = list(manifest.get("enabled_factors") or [])
    if feature_cols:
        # Keep only factors that actually exist on disk (defensive for older caches).
        feature_cols = [c for c in feature_cols if c in features_df.columns]
    else:
        # Legacy fallback: infer from columns, but never treat metadata as features.
        exclude = {"symbol", "datetime", "underlying_contract", "segment_id"}
        feature_cols = [c for c in features_df.columns if c not in exclude]

    segment_id: np.ndarray | None = None
    if "segment_id" in df.columns:
        try:
            seg = pd.to_numeric(df["segment_id"], errors="coerce").fillna(-1).astype("int64").values
            if len(seg) > 0 and int(seg.min()) != int(seg.max()):
                segment_id = seg
        except Exception:
            segment_id = None
    X = df[feature_cols].values
    y = df[f"label_{horizon}"].values
    
    log.info("train.data_loaded", n_samples=len(df), n_features=len(feature_cols))
    
    # Create model
    model = create_model(
        model_type,
        n_features=len(feature_cols),
        **kwargs,
    )
    
    # Train
    fit_kwargs: dict[str, Any] = {
        "ddp": ddp,
        "use_amp": use_amp,
        "num_workers": num_workers,
        "seed": seed,
    }
    if epochs is not None:
        fit_kwargs["epochs"] = epochs
    if batch_size is not None:
        fit_kwargs["batch_size"] = batch_size
    if lr is not None:
        fit_kwargs["lr"] = lr
    if segment_id is not None:
        fit_kwargs["segment_id"] = segment_id

    model.fit(X, y, **fit_kwargs)
    
    # Save
    model_dir = artifacts_dir / symbol / model_type
    model_dir.mkdir(parents=True, exist_ok=True)
    
    if model_type in ["logistic"]:
        model_path = model_dir / f"model_h{horizon}.pkl"
    elif model_type in ["lightgbm", "xgboost"]:
        model_path = model_dir / f"model_h{horizon}.json"
    else:
        model_path = model_dir / f"model_h{horizon}.pt"
    
    if (not ddp_active) or distu.is_rank0():
        model.save(model_path)
        log.info("train.saved", path=str(model_path))

    if ddp_active:
        # Wait for rank0 to finish writing.
        distu.barrier()
    
    return model_path


def load_model(model_type: ModelType, path: Path, **kwargs: Any) -> BaseModel:
    """Load a trained model from disk."""
    model = create_model(model_type, **kwargs)
    model.load(path)
    return model
