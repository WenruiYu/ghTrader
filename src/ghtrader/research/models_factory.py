"""Model factory helpers and load/save dispatch."""

from __future__ import annotations

from pathlib import Path
from typing import Any, Callable, Mapping

from .models_base import BaseModel, ModelType

ModelFactory = Callable[..., BaseModel]

_BASELINE_DEEP_ONLY_KWARGS = (
    "n_features",
    "seq_len",
    "hidden_dim",
    "d_model",
    "n_heads",
    "n_layers",
    "n_channels",
    "d_state",
    "grid_size",
)


def _strip_deep_only_kwargs(kwargs: dict[str, Any]) -> dict[str, Any]:
    out = dict(kwargs)
    for key in _BASELINE_DEEP_ONLY_KWARGS:
        out.pop(key, None)
    return out


def create_model(
    model_type: str,
    *,
    baseline_factories: Mapping[str, ModelFactory],
    deep_factories: Mapping[str, ModelFactory],
    **kwargs: Any,
) -> BaseModel:
    """Create a model instance by type."""
    if model_type in baseline_factories:
        return baseline_factories[model_type](**_strip_deep_only_kwargs(kwargs))
    if model_type in deep_factories:
        return deep_factories[model_type](**kwargs)
    raise ValueError(f"Unknown model type: {model_type}")


def load_model(
    *,
    create_model_fn: Callable[..., BaseModel],
    model_type: ModelType,
    path: Path,
    **kwargs: Any,
) -> BaseModel:
    model = create_model_fn(model_type, **kwargs)
    model.load(path)
    return model


def model_artifact_suffix(model_type: str) -> str:
    if model_type == "logistic":
        return ".pkl"
    if model_type in {"lightgbm", "xgboost"}:
        return ".json"
    return ".pt"


def model_artifact_path(*, model_dir: Path, model_type: str, horizon: int) -> Path:
    return model_dir / f"model_h{horizon}{model_artifact_suffix(model_type)}"
