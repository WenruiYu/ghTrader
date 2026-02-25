"""
Models facade: baseline + deep model exports, factory wrappers, and training entrypoint.

Implementation detail:
- Deep model families live in dedicated `models_<family>.py` modules.
- This file preserves backward-compatible imports for external callers.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

from .models_base import BaseModel, ModelType, TickSequenceDataset
from .models_baselines import LightGBMModel, LogisticModel, XGBoostModel
from .models_factory import (
    create_model as _create_model_impl,
    load_model as _load_model_impl,
)
from .models_deeplob import DeepLOBModel, DeepLOBNet
from .models_transformer import TransformerEncoder, TransformerModel
from .models_tcn import TemporalBlock, TCNModel, TCNNet
from .models_tlob import SpatialAttention, TemporalAttention, TLOBModel, TLOBNet
from .models_ssm import S4Block, SSMModel, SSMNet
from .models_tkan import KANLinear, TKANCell, TKANModel, TKANNet
from .models_lobert import LOBERTEncoder, LOBERTModel, PLGSEmbedding
from .models_kanformer import KANFormer, KANFormerModel
from .models_training import train_model as _train_model_impl

# ---------------------------------------------------------------------------
# Model factory and training entrypoint
# ---------------------------------------------------------------------------

def create_model(model_type: str, **kwargs: Any) -> BaseModel:
    return _create_model_impl(
        model_type=model_type,
        baseline_factories={
            "logistic": LogisticModel,
            "lightgbm": LightGBMModel,
            "xgboost": XGBoostModel,
        },
        deep_factories={
            "deeplob": DeepLOBModel,
            "transformer": TransformerModel,
            "tcn": TCNModel,
            "tlob": TLOBModel,
            "ssm": SSMModel,
            "tkan": TKANModel,
            "lobert": LOBERTModel,
            "kanformer": KANFormerModel,
        },
        **kwargs,
    )


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
    num_workers: int = 0,
    prefetch_factor: int | None = None,
    pin_memory: bool | None = None,
    seed: int | None = None,
    **kwargs: Any,
) -> Path:
    return _train_model_impl(
        model_type=model_type,
        symbol=symbol,
        data_dir=data_dir,
        artifacts_dir=artifacts_dir,
        horizon=horizon,
        gpus=gpus,
        epochs=epochs,
        batch_size=batch_size,
        lr=lr,
        ddp=ddp,
        use_amp=use_amp,
        num_workers=num_workers,
        prefetch_factor=prefetch_factor,
        pin_memory=pin_memory,
        seed=seed,
        create_model_fn=create_model,
        **kwargs,
    )


def load_model(model_type: ModelType, path: Path, **kwargs: Any) -> BaseModel:
    """Load a trained model from disk."""
    return _load_model_impl(create_model_fn=create_model, model_type=model_type, path=path, **kwargs)
