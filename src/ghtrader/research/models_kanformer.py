"""KANFormer model family."""

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
from .models_tkan import KANLinear

log = structlog.get_logger()

# ---------------------------------------------------------------------------
# KANFormer (Hybrid CNN + Transformer + KAN)
# ---------------------------------------------------------------------------

class KANFormer(nn.Module):
    """
    KANFormer: Hybrid architecture combining CNN feature extraction,
    Transformer temporal modeling, and KAN layers for non-linear interactions.
    
    Architecture:
    1. CNN encoder (spatial features)
    2. Transformer encoder (temporal features)
    3. KAN classification head
    """
    
    def __init__(
        self,
        n_features: int,
        n_classes: int = 3,
        n_horizons: int = 1,
        d_model: int = 64,
        n_heads: int = 4,
        n_layers: int = 2,
        grid_size: int = 5,
        spline_order: int = 3,
        dropout: float = 0.1,
        max_seq_len: int = 200,
    ) -> None:
        super().__init__()
        self.n_horizons = n_horizons
        
        # 1. CNN Feature Extractor (DeepLOB-style lightweight)
        self.conv1 = nn.Conv1d(n_features, 32, kernel_size=3, padding=1)
        self.conv2 = nn.Conv1d(32, d_model, kernel_size=3, padding=1)
        self.pool = nn.MaxPool1d(2)
        self.dropout = nn.Dropout(dropout)
        
        # 2. Transformer Encoder
        encoder_layer = nn.TransformerEncoderLayer(
            d_model=d_model,
            nhead=n_heads,
            dim_feedforward=d_model * 4,
            dropout=dropout,
            batch_first=True,
            norm_first=True,
        )
        self.transformer = nn.TransformerEncoder(encoder_layer, num_layers=n_layers)
        self.pos_encoding = nn.Parameter(torch.randn(1, max_seq_len, d_model) * 0.02)
        
        # 3. KAN Classification Head
        # Global average pooling over time -> KAN layer
        self.kan_head = KANLinear(
            d_model, 
            n_classes * n_horizons, 
            grid_size=grid_size, 
            spline_order=spline_order
        )
        
    def forward(self, x: torch.Tensor) -> torch.Tensor:
        # x: (batch, seq_len, n_features)
        
        # CNN (requires channels first)
        x = x.permute(0, 2, 1)
        x = F.relu(self.conv1(x))
        x = self.dropout(x)
        x = F.relu(self.conv2(x))
        x = self.pool(x)
        x = self.dropout(x)
        
        # Transformer (requires batch first: batch, seq_len, d_model)
        x = x.permute(0, 2, 1)
        seq_len = x.size(1)
        
        # Add position encoding (resized to pooled length)
        pos = F.interpolate(
            self.pos_encoding.permute(0, 2, 1), 
            size=seq_len, 
            mode='linear'
        ).permute(0, 2, 1)
        x = x + pos
        
        x = self.transformer(x)
        
        # Global Average Pooling over time
        x = x.mean(dim=1)
        
        # KAN Head
        out = self.kan_head(x)
        
        if self.n_horizons == 1:
            return out
        else:
            return out.view(-1, self.n_horizons, 3)


class KANFormerModel(BaseModel):
    """KANFormer wrapper with training loop."""
    
    name = "kanformer"
    
    def __init__(
        self,
        n_features: int = 11,
        n_classes: int = 3,
        n_horizons: int = 1,
        seq_len: int = 100,
        d_model: int = 64,
        grid_size: int = 5,
        device: str = "cuda" if torch.cuda.is_available() else "cpu",
        **kwargs: Any,
    ) -> None:
        self.n_features = n_features
        self.n_classes = n_classes
        self.n_horizons = n_horizons
        self.seq_len = seq_len
        self.d_model = d_model
        self.grid_size = grid_size
        self.device = device
        self.model: KANFormer | None = None
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

        self.model = KANFormer(
            n_features=self.n_features,
            n_classes=self.n_classes,
            n_horizons=self.n_horizons,
            d_model=self.d_model,
            grid_size=self.grid_size,
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
                log.info("kanformer.epoch", epoch=epoch + 1, loss=f"{avg_loss:.4f}")

        if distu.is_rank0():
            log.info("kanformer.fit_done", epochs=epochs)

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
                "grid_size": self.grid_size,
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
        self.grid_size = config["grid_size"]
        
        self.model = KANFormer(
            n_features=self.n_features,
            n_classes=self.n_classes,
            n_horizons=self.n_horizons,
            d_model=self.d_model,
            grid_size=self.grid_size,
            max_seq_len=self.seq_len,
        ).to(self.device)
        self.model.load_state_dict(data["model_state"])
