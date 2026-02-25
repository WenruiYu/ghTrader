"""LOBERT model family."""

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
# LOBERT (LOB Encoder Foundation Model)
# ---------------------------------------------------------------------------

class PLGSEmbedding(nn.Module):
    """
    Piecewise Linear-Geometric Scaling (PLGS) embedding.
    
    Handles extreme value distributions in LOB data (price/volume) by applying
    linear scaling for small values and geometric (log) scaling for large values.
    """
    
    def __init__(self, d_model: int, linear_limit: float = 10.0) -> None:
        super().__init__()
        self.d_model = d_model
        self.linear_limit = linear_limit
        self.proj = nn.Linear(1, d_model)
        
    def forward(self, x: torch.Tensor) -> torch.Tensor:
        # x: (batch, seq_len, 1)
        
        # Apply PLGS transformation
        # If |x| <= linear_limit: x
        # If |x| > linear_limit: sign(x) * (linear_limit + log(|x|/linear_limit))
        
        abs_x = torch.abs(x)
        mask = abs_x > self.linear_limit
        
        x_scaled = torch.where(
            mask,
            torch.sign(x) * (self.linear_limit + torch.log(abs_x / self.linear_limit + 1e-8)),
            x
        )
        
        return self.proj(x_scaled)


class LOBERTEncoder(nn.Module):
    """
    BERT-style encoder for Limit Order Book data.
    
    Features:
    - PLGS embeddings for numerical features
    - Transformer encoder backbone
    - Masked Message Modeling (MMM) pretraining head (optional)
    """
    
    def __init__(
        self,
        n_features: int,
        n_classes: int = 3,
        n_horizons: int = 1,
        d_model: int = 128,
        n_heads: int = 4,
        n_layers: int = 4,
        dropout: float = 0.1,
        max_seq_len: int = 200,
        use_plgs: bool = True,
    ) -> None:
        super().__init__()
        self.n_horizons = n_horizons
        self.n_classes = n_classes
        self.d_model = d_model
        self.use_plgs = use_plgs
        
        # Embeddings
        if use_plgs:
            # One PLGS embedder per feature
            self.feature_embedders = nn.ModuleList([
                PLGSEmbedding(d_model) for _ in range(n_features)
            ])
            # Combine feature embeddings (e.g., sum or concat+proj)
            # Here we sum them, similar to adding positional encodings
            self.feature_combiner = nn.Linear(d_model, d_model)
        else:
            self.input_proj = nn.Linear(n_features, d_model)
            
        self.pos_encoding = nn.Parameter(torch.randn(1, max_seq_len, d_model) * 0.02)
        self.dropout = nn.Dropout(dropout)
        
        # Transformer
        encoder_layer = nn.TransformerEncoderLayer(
            d_model=d_model,
            nhead=n_heads,
            dim_feedforward=d_model * 4,
            dropout=dropout,
            batch_first=True,
            norm_first=True,  # Pre-LN for stability
        )
        self.transformer = nn.TransformerEncoder(encoder_layer, num_layers=n_layers)
        
        # Classification heads
        self.heads = nn.ModuleList([
            nn.Sequential(
                nn.Linear(d_model, d_model),
                nn.GELU(),
                nn.LayerNorm(d_model),
                nn.Linear(d_model, n_classes)
            ) for _ in range(n_horizons)
        ])
        
    def forward(self, x: torch.Tensor) -> torch.Tensor:
        # x: (batch, seq_len, n_features)
        batch, seq_len, n_feat = x.size()
        
        if self.use_plgs:
            # Apply PLGS to each feature independently
            embeddings = []
            for i in range(n_feat):
                feat = x[:, :, i : i+1]
                emb = self.feature_embedders[i](feat)
                embeddings.append(emb)
            
            # Sum embeddings (additive feature composition)
            x_emb = sum(embeddings)
            x = self.feature_combiner(x_emb)
        else:
            x = self.input_proj(x)
            
        # Add positional encoding
        x = x + self.pos_encoding[:, :seq_len, :]
        x = self.dropout(x)
        
        # Transformer
        x = self.transformer(x)
        
        # Use CLS token equivalent (last position for causal, or pool)
        # Here using last position
        last = x[:, -1, :]
        
        if self.n_horizons == 1:
            return self.heads[0](last)
        else:
            outputs = [head(last) for head in self.heads]
            return torch.stack(outputs, dim=1)


class LOBERTModel(BaseModel):
    """LOBERT wrapper with training loop."""
    
    name = "lobert"
    
    def __init__(
        self,
        n_features: int = 11,
        n_classes: int = 3,
        n_horizons: int = 1,
        seq_len: int = 100,
        d_model: int = 128,
        n_layers: int = 4,
        use_plgs: bool = True,
        device: str = "cuda" if torch.cuda.is_available() else "cpu",
        **kwargs: Any,
    ) -> None:
        self.n_features = n_features
        self.n_classes = n_classes
        self.n_horizons = n_horizons
        self.seq_len = seq_len
        self.d_model = d_model
        self.n_layers = n_layers
        self.use_plgs = use_plgs
        self.device = device
        self.model: LOBERTEncoder | None = None
        self.scaler = StandardScaler()
    
    def fit(
        self,
        X: np.ndarray,
        y: np.ndarray,
        epochs: int = 50,
        batch_size: int = 256,
        lr: float = 1e-4,  # Lower LR for Transformers
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

        # Scale features (even with PLGS, scaling helps convergence)
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

        self.model = LOBERTEncoder(
            n_features=self.n_features,
            n_classes=self.n_classes,
            n_horizons=self.n_horizons,
            d_model=self.d_model,
            n_layers=self.n_layers,
            max_seq_len=self.seq_len,
            use_plgs=self.use_plgs,
        ).to(device)

        train_model: nn.Module = self.model
        if ddp_active:
            from torch.nn.parallel import DistributedDataParallel as DDP

            if device.type == "cuda":
                train_model = DDP(self.model, device_ids=[device.index], output_device=device.index)
            else:
                train_model = DDP(self.model)

        optimizer = torch.optim.AdamW(train_model.parameters(), lr=lr, weight_decay=0.01)
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
                log.info("lobert.epoch", epoch=epoch + 1, loss=f"{avg_loss:.4f}")

        if distu.is_rank0():
            log.info("lobert.fit_done", epochs=epochs)

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
                "n_layers": self.n_layers,
                "use_plgs": self.use_plgs,
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
        self.n_layers = config["n_layers"]
        self.use_plgs = config["use_plgs"]
        
        self.model = LOBERTEncoder(
            n_features=self.n_features,
            n_classes=self.n_classes,
            n_horizons=self.n_horizons,
            d_model=self.d_model,
            n_layers=self.n_layers,
            max_seq_len=self.seq_len,
            use_plgs=self.use_plgs,
        ).to(self.device)
        self.model.load_state_dict(data["model_state"])
