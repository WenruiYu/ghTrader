"""T-KAN model family."""

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
# T-KAN (Temporal Kolmogorov-Arnold Networks)
# ---------------------------------------------------------------------------

class KANLinear(nn.Module):
    """
    Linear layer with B-spline activations (Kolmogorov-Arnold Network).
    
    Instead of fixed weights w, each connection learns a spline function phi(x).
    Approximated here using a basis expansion: phi(x) = sum(c_i * B_i(x)).
    """
    
    def __init__(
        self,
        in_features: int,
        out_features: int,
        grid_size: int = 5,
        spline_order: int = 3,
        scale_noise: float = 0.1,
        scale_base: float = 1.0,
        scale_spline: float = 1.0,
        enable_standalone_scale_spline: bool = True,
        base_activation: nn.Module = nn.SiLU(),
        grid_eps: float = 0.02,
        grid_range: list[float] = [-1, 1],
    ) -> None:
        super().__init__()
        self.in_features = in_features
        self.out_features = out_features
        self.grid_size = grid_size
        self.spline_order = spline_order
        
        # Base weights (residual connection)
        self.base_weight = nn.Parameter(torch.Tensor(out_features, in_features))
        
        # Spline weights
        h = (grid_range[1] - grid_range[0]) / grid_size
        grid = (
            (
                torch.arange(-spline_order, grid_size + spline_order + 1) * h
                + grid_range[0]
            )
            .expand(in_features, -1)
            .contiguous()
        )
        self.register_buffer("grid", grid)
        
        self.spline_weight = nn.Parameter(
            torch.Tensor(out_features, in_features, grid_size + spline_order)
        )
        
        if enable_standalone_scale_spline:
            self.spline_scaler = nn.Parameter(
                torch.Tensor(out_features, in_features)
            )
        else:
            self.spline_scaler = None
            
        self.scale_noise = scale_noise
        self.scale_base = scale_base
        self.scale_spline = scale_spline
        self.enable_standalone_scale_spline = enable_standalone_scale_spline
        self.base_activation = base_activation
        self.grid_eps = grid_eps
        
        self.reset_parameters()
        
    def reset_parameters(self) -> None:
        nn.init.kaiming_uniform_(self.base_weight, a=np.sqrt(5) * self.scale_base)
        with torch.no_grad():
            noise = (
                (
                    torch.rand(self.grid_size + 1, self.in_features, self.out_features)
                    - 1 / 2
                )
                * self.scale_noise
                / self.grid_size
            )
            self.spline_weight.data.copy_(
                (self.scale_spline if not self.enable_standalone_scale_spline else 1.0)
                * self.curve2coeff(
                    self.grid.T[self.spline_order : -self.spline_order],
                    noise,
                )
            )
            if self.enable_standalone_scale_spline:
                nn.init.kaiming_uniform_(self.spline_scaler, a=np.sqrt(5) * self.scale_spline)

    def b_splines(self, x: torch.Tensor) -> torch.Tensor:
        """
        Compute the B-spline bases for the given input tensor.
        x: (batch, in_features)
        returns: (batch, in_features, grid_size + spline_order)
        """
        assert x.dim() == 2 and x.size(1) == self.in_features
        
        grid: torch.Tensor = self.grid  # (in_features, grid_size + 2*spline_order + 1)
        x = x.unsqueeze(-1)
        bases = ((x >= grid[:, :-1]) & (x < grid[:, 1:])).to(x.dtype)
        
        for k in range(1, self.spline_order + 1):
            bases = (
                (x - grid[:, : -(k + 1)])
                / (grid[:, k:-1] - grid[:, : -(k + 1)])
                * bases[:, :, :-1]
            ) + (
                (grid[:, k + 1 :] - x)
                / (grid[:, k + 1 :] - grid[:, 1:(-k)])
                * bases[:, :, 1:]
            )
            
        assert bases.size() == (
            x.size(0),
            self.in_features,
            self.grid_size + self.spline_order,
        )
        return bases

    def curve2coeff(self, x: torch.Tensor, y: torch.Tensor) -> torch.Tensor:
        """
        Compute the coefficients of the curve that interpolates the given points.
        x: (batch, in_features)
        y: (batch, in_features, out_features)
        returns: (out_features, in_features, grid_size + spline_order)
        """
        # x: (batch, in_features)
        # y: (batch, in_features, out_features)
        # A: (batch, in_features, grid_size + spline_order)
        A = self.b_splines(x).transpose(0, 1)  # (in_features, batch, grid_size + spline_order)
        B = y.transpose(0, 1)  # (in_features, batch, out_features)
        solution = torch.linalg.lstsq(A, B).solution  # (in_features, grid_size + spline_order, out_features)
        result = solution.permute(2, 0, 1)  # (out_features, in_features, grid_size + spline_order)
        return result.contiguous()

    def forward(self, x: torch.Tensor) -> torch.Tensor:
        # x: (batch, in_features)
        base_output = F.linear(self.base_activation(x), self.base_weight)
        
        # Spline output
        spline_basis = self.b_splines(x)  # (batch, in_features, grid_size + spline_order)
        
        # (batch, in_features, grid_size + spline_order) * (out_features, in_features, grid_size + spline_order)
        # -> (batch, out_features)
        spline_output = F.linear(
            spline_basis.view(x.size(0), -1),
            self.spline_weight.view(self.out_features, -1),
        )
        
        if self.enable_standalone_scale_spline:
            spline_output = spline_output + F.linear(x, self.spline_scaler)
            
        return base_output + spline_output


class TKANCell(nn.Module):
    """
    Temporal KAN Cell (LSTM-like but with KAN layers).
    """
    
    def __init__(self, input_size: int, hidden_size: int, grid_size: int = 5, spline_order: int = 3) -> None:
        super().__init__()
        self.hidden_size = hidden_size
        
        # Replace linear gates with KAN layers
        self.kan_i = KANLinear(input_size + hidden_size, hidden_size, grid_size, spline_order)
        self.kan_f = KANLinear(input_size + hidden_size, hidden_size, grid_size, spline_order)
        self.kan_c = KANLinear(input_size + hidden_size, hidden_size, grid_size, spline_order)
        self.kan_o = KANLinear(input_size + hidden_size, hidden_size, grid_size, spline_order)
        
    def forward(
        self, x: torch.Tensor, h_prev: torch.Tensor, c_prev: torch.Tensor
    ) -> tuple[torch.Tensor, torch.Tensor]:
        # x: (batch, input_size)
        # h_prev: (batch, hidden_size)
        # c_prev: (batch, hidden_size)
        
        combined = torch.cat([x, h_prev], dim=1)
        
        i = torch.sigmoid(self.kan_i(combined))
        f = torch.sigmoid(self.kan_f(combined))
        o = torch.sigmoid(self.kan_o(combined))
        c_tilde = torch.tanh(self.kan_c(combined))
        
        c = f * c_prev + i * c_tilde
        h = o * torch.tanh(c)
        
        return h, c


class TKANNet(nn.Module):
    """
    Temporal Kolmogorov-Arnold Network.
    
    Replaces standard LSTM linear layers with KAN layers for better
    interpretability and expressiveness.
    """
    
    def __init__(
        self,
        n_features: int,
        n_classes: int = 3,
        n_horizons: int = 1,
        hidden_dim: int = 64,
        n_layers: int = 2,
        grid_size: int = 5,
        spline_order: int = 3,
        dropout: float = 0.2,
    ) -> None:
        super().__init__()
        self.n_horizons = n_horizons
        self.hidden_dim = hidden_dim
        self.n_layers = n_layers
        
        self.cells = nn.ModuleList([
            TKANCell(
                input_size=n_features if i == 0 else hidden_dim,
                hidden_size=hidden_dim,
                grid_size=grid_size,
                spline_order=spline_order,
            )
            for i in range(n_layers)
        ])
        
        self.dropout = nn.Dropout(dropout)
        
        # Output heads
        self.heads = nn.ModuleList([
            nn.Linear(hidden_dim, n_classes) for _ in range(n_horizons)
        ])
        
    def forward(self, x: torch.Tensor) -> torch.Tensor:
        # x: (batch, seq_len, n_features)
        batch_size, seq_len, _ = x.size()
        
        h = [torch.zeros(batch_size, self.hidden_dim, device=x.device) for _ in range(self.n_layers)]
        c = [torch.zeros(batch_size, self.hidden_dim, device=x.device) for _ in range(self.n_layers)]
        
        # Iterate over time
        for t in range(seq_len):
            xt = x[:, t, :]
            
            for l in range(self.n_layers):
                h[l], c[l] = self.cells[l](xt, h[l], c[l])
                xt = self.dropout(h[l])
                
        # Use last hidden state of last layer
        last_hidden = h[-1]
        
        if self.n_horizons == 1:
            return self.heads[0](last_hidden)
        else:
            outputs = [head(last_hidden) for head in self.heads]
            return torch.stack(outputs, dim=1)


class TKANModel(BaseModel):
    """T-KAN wrapper with training loop."""
    
    name = "tkan"
    
    def __init__(
        self,
        n_features: int = 11,
        n_classes: int = 3,
        n_horizons: int = 1,
        seq_len: int = 100,
        hidden_dim: int = 64,
        grid_size: int = 5,
        device: str = "cuda" if torch.cuda.is_available() else "cpu",
        **kwargs: Any,
    ) -> None:
        self.n_features = n_features
        self.n_classes = n_classes
        self.n_horizons = n_horizons
        self.seq_len = seq_len
        self.hidden_dim = hidden_dim
        self.grid_size = grid_size
        self.device = device
        self.model: TKANNet | None = None
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

        self.model = TKANNet(
            n_features=self.n_features,
            n_classes=self.n_classes,
            n_horizons=self.n_horizons,
            hidden_dim=self.hidden_dim,
            grid_size=self.grid_size,
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
                log.info("tkan.epoch", epoch=epoch + 1, loss=f"{avg_loss:.4f}")

        if distu.is_rank0():
            log.info("tkan.fit_done", epochs=epochs)

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
                "hidden_dim": self.hidden_dim,
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
        self.hidden_dim = config["hidden_dim"]
        self.grid_size = config["grid_size"]
        
        self.model = TKANNet(
            n_features=self.n_features,
            n_classes=self.n_classes,
            n_horizons=self.n_horizons,
            hidden_dim=self.hidden_dim,
            grid_size=self.grid_size,
        ).to(self.device)
        self.model.load_state_dict(data["model_state"])
