"""
Shared model base types and sequence utilities.
"""

from __future__ import annotations

import os
from abc import ABC, abstractmethod
from typing import Any, Literal

import numpy as np
import torch
import torch.nn as nn
import torch.nn.functional as F
from torch.utils.data import Dataset

from ghtrader.config import env_bool, env_int

ModelType = Literal[
    "logistic",
    "xgboost",
    "lightgbm",
    "deeplob",
    "transformer",
    "tcn",
    "tlob",
    "ssm",
    "tkan",
    "lobert",
    "kanformer",
]


def _env_int(key: str, default: int) -> int:
    return env_int(key, default)


def _env_bool(key: str, default: bool) -> bool:
    return env_bool(key, default)


def resolve_dataloader_kwargs(
    *,
    num_workers: int,
    prefetch_factor: int | None,
    pin_memory: bool | None,
    device: torch.device,
) -> dict[str, Any]:
    workers = int(num_workers)
    if workers <= 0:
        auto_default = max(2, min(64, (os.cpu_count() or 8) // 8))
        workers = max(1, _env_int("GHTRADER_TRAIN_NUM_WORKERS_AUTO", auto_default))

    pin = bool(pin_memory) if pin_memory is not None else (device.type == "cuda")
    out: dict[str, Any] = {
        "num_workers": int(workers),
        "pin_memory": bool(pin),
    }
    if workers > 0:
        pf = int(prefetch_factor) if prefetch_factor is not None else _env_int("GHTRADER_TRAIN_PREFETCH_FACTOR", 2)
        out["prefetch_factor"] = max(1, int(pf))
        out["persistent_workers"] = _env_bool("GHTRADER_TRAIN_PERSISTENT_WORKERS", True)
    return out


def sequence_predict_proba_batched(
    *,
    model: nn.Module,
    X_scaled: np.ndarray,
    seq_len: int,
    n_classes: int,
    device: torch.device,
    seg_arr: np.ndarray | None = None,
    batch_size: int = 256,
) -> np.ndarray:
    out = np.full((len(X_scaled), n_classes), np.nan, dtype=float)
    if len(X_scaled) <= int(seq_len):
        return out

    valid_idx: list[int] = []
    for i in range(int(seq_len), len(X_scaled)):
        if seg_arr is not None:
            s = seg_arr[i]
            if not np.all(seg_arr[i - int(seq_len) : i + 1] == s):
                continue
        valid_idx.append(i)
    if not valid_idx:
        return out

    model.eval()
    bs = max(8, int(batch_size))
    with torch.no_grad():
        for start in range(0, len(valid_idx), bs):
            idxs = valid_idx[start : start + bs]
            seqs = np.asarray(
                [np.nan_to_num(X_scaled[i - int(seq_len) : i], nan=0.0) for i in idxs],
                dtype=np.float32,
            )
            seq_tensor = torch.from_numpy(seqs).to(device)
            probs = F.softmax(model(seq_tensor), dim=-1).detach().cpu().numpy()
            if probs.ndim == 3:
                probs = probs[:, 0, :]
            out[np.asarray(idxs, dtype=int)] = probs
    return out


class BaseModel(ABC):
    """Abstract base for all models."""

    name: str

    @abstractmethod
    def fit(self, X: np.ndarray, y: np.ndarray, **kwargs: Any) -> None:
        """Fit the model."""

    @abstractmethod
    def predict_proba(self, X: np.ndarray, **kwargs: Any) -> np.ndarray:
        """Predict class probabilities. Shape: (n_samples, n_classes)."""

    @abstractmethod
    def save(self, path: Any) -> None:
        """Save model to disk."""

    @abstractmethod
    def load(self, path: Any) -> None:
        """Load model from disk."""


class TickSequenceDataset(Dataset):
    """Dataset for tick sequences (for deep sequence models)."""

    def __init__(
        self,
        features: np.ndarray,
        labels: np.ndarray,
        seq_len: int = 100,
        *,
        segment_id: np.ndarray | None = None,
    ) -> None:
        self.features = features
        self.labels = labels
        self.seq_len = seq_len
        self.segment_id = segment_id

        self.valid_indices: list[int] = []
        for i in range(seq_len, len(features)):
            if not np.isnan(labels[i]).any():
                if self.segment_id is not None:
                    s = self.segment_id[i]
                    window = self.segment_id[i - seq_len : i + 1]
                    if not np.all(window == s):
                        continue
                self.valid_indices.append(i)

    def __len__(self) -> int:
        return len(self.valid_indices)

    def __getitem__(self, idx: int) -> tuple[torch.Tensor, torch.Tensor]:
        i = self.valid_indices[idx]
        seq = self.features[i - self.seq_len : i]
        label = self.labels[i]
        seq = np.nan_to_num(seq, nan=0.0)
        return torch.tensor(seq, dtype=torch.float32), torch.tensor(label, dtype=torch.long)
