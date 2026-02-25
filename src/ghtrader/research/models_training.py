"""Training orchestration for research models."""

from __future__ import annotations

from pathlib import Path
from typing import Any, Callable

import numpy as np
import pandas as pd
import structlog

import ghtrader.research.distributed as distu

from .models_base import BaseModel, ModelType
from .models_factory import model_artifact_path

log = structlog.get_logger()

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
    create_model_fn: Callable[..., BaseModel] | None = None,
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
    from ghtrader.datasets.features import read_features_for_symbol, read_features_manifest
    from ghtrader.datasets.labels import read_labels_for_symbol, read_labels_manifest
    
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
            "Missing QuestDB build metadata for features/labels. "
            "Run `ghtrader build` first (and re-run with --overwrite if needed)."
        )
    tk_f = str(feat_manifest.get("ticks_kind") or "")
    tk_l = str(lab_manifest.get("ticks_kind") or "")
    dv_f = str(feat_manifest.get("dataset_version") or "v2")
    dv_l = str(lab_manifest.get("dataset_version") or "v2")
    if tk_f != tk_l:
        raise ValueError(f"ticks_kind mismatch between features ({tk_f}) and labels ({tk_l}) for symbol={symbol}")
    if dv_f != dv_l:
        raise ValueError(f"dataset_version mismatch between features ({dv_f}) and labels ({dv_l}) for symbol={symbol}")
    rh_f = str(feat_manifest.get("row_hash_algo") or "")
    rh_l = str(lab_manifest.get("row_hash_algo") or "")
    if rh_f and rh_l and rh_f != rh_l:
        raise ValueError(f"row_hash_algo mismatch between features ({rh_f}) and labels ({rh_l}) for symbol={symbol}")
    if str(symbol).startswith("KQ.m@") and tk_f != "main_l5":
        raise ValueError(
            "Continuous symbols (KQ.m@...) must be trained from derived main_l5 ticks. "
            "Rebuild with: `ghtrader build --ticks-kind main_l5 ...`"
        )
    if tk_f == "main_l5":
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
    if create_model_fn is None:
        # Local import avoids circular dependency; facade passes create_model_fn explicitly.
        from .models import create_model as create_model_fn

    model = create_model_fn(
        model_type,
        n_features=len(feature_cols),
        **kwargs,
    )
    
    # Train
    fit_kwargs: dict[str, Any] = {
        "ddp": ddp,
        "use_amp": use_amp,
        "num_workers": num_workers,
        "prefetch_factor": prefetch_factor,
        "pin_memory": pin_memory,
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
    
    model_path = model_artifact_path(model_dir=model_dir, model_type=str(model_type), horizon=int(horizon))
    
    if (not ddp_active) or distu.is_rank0():
        model.save(model_path)
        log.info("train.saved", path=str(model_path))
        # Persist non-secret model metadata for training-serving parity (PRD §5.11.7).
        try:
            from datetime import datetime, timezone

            from ghtrader.util.json_io import write_json_atomic

            meta_path = model_dir / f"model_h{horizon}.meta.json"
            feat_q = dict(feat_manifest.get("questdb") or {}) if isinstance(feat_manifest, dict) else {}
            lab_q = dict(lab_manifest.get("questdb") or {}) if isinstance(lab_manifest, dict) else {}

            meta: dict[str, Any] = {
                "schema_version": 1,
                "created_at": datetime.now(timezone.utc).isoformat(),
                "symbol": str(symbol),
                "model_type": str(model_type),
                "horizon": int(horizon),
                "n_features": int(len(feature_cols)),
                # Canonical feature spec (model input order).
                "enabled_factors": list(feature_cols),
                # Provenance: which canonical tick source/features/labels build this model expects.
                "ticks_kind": str(tk_f),
                "dataset_version": str(dv_f),
                "feature_build_id": str(feat_q.get("build_id") or ""),
                "label_build_id": str(lab_q.get("build_id") or ""),
                "feature_schema_hash": str(feat_manifest.get("schema_hash") or "") if isinstance(feat_manifest, dict) else "",
                # Labels build table does not currently store a schema hash; keep best-effort.
                "label_schema_hash": str(lab_manifest.get("schema_hash") or "") if isinstance(lab_manifest, dict) else "",
                "feature_row_hash_algo": str(feat_manifest.get("row_hash_algo") or "") if isinstance(feat_manifest, dict) else "",
                "label_row_hash_algo": str(lab_manifest.get("row_hash_algo") or "") if isinstance(lab_manifest, dict) else "",
            }
            if str(tk_f) == "main_l5":
                # Required schedule provenance for roll-boundary safety.
                sched = dict(feat_manifest.get("schedule") or {}) if isinstance(feat_manifest, dict) else {}
                meta["schedule_hash"] = str(sched.get("hash") or "")
                meta["l5_start_date"] = str(sched.get("l5_start_date") or "")
                meta["schedule_end_date"] = str(sched.get("end_date") or "")

            write_json_atomic(meta_path, meta)
            log.info("train.saved_meta", path=str(meta_path))
        except Exception as e:
            log.warning("train.save_meta_failed", error=str(e))

    if ddp_active:
        # Wait for rank0 to finish writing.
        distu.barrier()
    
    return model_path
