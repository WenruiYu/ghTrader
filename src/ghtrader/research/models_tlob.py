"""TLOB model family."""

from __future__ import annotations

from contextlib import nullcontext
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd
import structlog
import torch
import torch.nn as nn
import torch.nn.functional as F
import ghtrader.research.distributed as distu
from sklearn.preprocessing import StandardScaler
from torch.utils.data import DataLoader

from .models_base import (
    BaseModel,
    TickSequenceDataset,
    _env_bool,
    _env_int,
    resolve_dataloader_kwargs as _resolve_dataloader_kwargs,
    sequence_predict_proba_batched as _sequence_predict_proba_batched,
)

log = structlog.get_logger()

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

        dl_prefetch = kwargs.pop("prefetch_factor", None)
        dl_pin_memory = kwargs.pop("pin_memory", None)
        loader_kwargs = _resolve_dataloader_kwargs(
            num_workers=num_workers,
            prefetch_factor=(int(dl_prefetch) if dl_prefetch is not None else None),
            pin_memory=(bool(dl_pin_memory) if dl_pin_memory is not None else None),
            device=device,
        )
        dataloader = DataLoader(
            dataset,
            batch_size=batch_size,
            shuffle=(sampler is None),
            sampler=sampler,
            **loader_kwargs,
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

        return _sequence_predict_proba_batched(
            model=self.model,
            X_scaled=X_scaled,
            seq_len=self.seq_len,
            n_classes=self.n_classes,
            device=self.device,
            seg_arr=seg_arr,
        )
    
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
