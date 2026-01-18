"""
TqSdk paper-trading loop (PRD 5.7 Online calibrator, paper-only).

This module contains the direct TqSdk integration for the paper loop so that
non-`ghtrader.tq.*` modules remain free of direct `tqsdk` imports (PRD 5.1.3).
"""

from __future__ import annotations

from pathlib import Path

import numpy as np
import structlog

from ghtrader.config import get_tqsdk_auth, is_live_enabled
from ghtrader.datasets.features import FactorEngine
from ghtrader.research.models import load_model
from ghtrader.research.online import DelayedLabelBuffer, OnlineCalibrator

log = structlog.get_logger()


def run_paper_trading(
    model_name: str,
    symbols: list[str],
    artifacts_dir: Path,
    horizon: int = 50,
    threshold_up: float = 0.6,
    threshold_down: float = 0.6,
    calibrator_lr: float = 0.01,
) -> None:
    """
    Run paper trading loop with online calibrator.

    NO REAL ORDERS are placed. This is for research and validation only.
    """
    try:
        from tqsdk import TqApi  # type: ignore
    except Exception as e:
        raise RuntimeError("tqsdk not installed. Install with: pip install tqsdk") from e

    # Safety check
    if is_live_enabled():
        raise RuntimeError(
            "GHTRADER_LIVE_ENABLED is set but this is the paper trading loop. "
            "Refusing to run to avoid confusion. Unset GHTRADER_LIVE_ENABLED, or use AccountGateway/StrategyRunner."
        )

    log.info("paper.start", model=model_name, symbols=symbols)

    # Load models for each symbol
    models = {}
    for symbol in symbols:
        model_dir = artifacts_dir / symbol / model_name
        model_files = sorted([p for p in model_dir.glob(f"model_h{horizon}.*") if not p.name.endswith(".meta.json")])
        if model_files:
            models[symbol] = load_model(model_name, model_files[0])
            log.info("paper.model_loaded", symbol=symbol)
        else:
            log.warning("paper.no_model", symbol=symbol)

    if not models:
        raise RuntimeError("No models loaded")

    # Create factor engines and calibrators for each symbol (prefer model metadata for parity).
    enabled_factors: dict[str, list[str]] = {}
    try:
        from ghtrader.util.json_io import read_json

        for s in symbols:
            mdir = artifacts_dir / s / model_name
            meta = read_json(mdir / f"model_h{horizon}.meta.json") or {}
            ef = (meta or {}).get("enabled_factors") if isinstance(meta, dict) else None
            if isinstance(ef, list) and ef and all(isinstance(x, str) and x.strip() for x in ef):
                enabled_factors[s] = [str(x).strip() for x in ef if str(x).strip()]
    except Exception:
        enabled_factors = {}

    factor_engines = {
        s: FactorEngine(enabled_factors=enabled_factors.get(s) or None) if enabled_factors.get(s) else FactorEngine()
        for s in symbols
    }
    calibrators = {s: OnlineCalibrator(lr=calibrator_lr) for s in symbols}
    label_buffers = {s: DelayedLabelBuffer(horizon_ticks=horizon) for s in symbols}

    # Feature buffers for sequence models
    feature_buffers: dict[str, list[dict]] = {s: [] for s in symbols}
    seq_len = 100

    # Paper positions (no real orders)
    positions: dict[str, int] = {s: 0 for s in symbols}
    paper_pnl: dict[str, float] = {s: 0.0 for s in symbols}
    last_prices: dict[str, float] = {s: 0.0 for s in symbols}

    # TqSdk setup
    auth = get_tqsdk_auth()
    api = TqApi(auth=auth)

    # Subscribe to ticks
    tick_serials = {s: api.get_tick_serial(s) for s in symbols}

    log.info("paper.subscribed", symbols=symbols)

    try:
        while True:
            api.wait_update()

            for symbol in symbols:
                if not api.is_changing(tick_serials[symbol]):
                    continue

                tick = tick_serials[symbol].iloc[-1]
                tick_dict = tick.to_dict()

                # Compute mid price
                mid = (tick_dict["bid_price1"] + tick_dict["ask_price1"]) / 2

                # Update paper PnL based on position
                if last_prices[symbol] > 0 and positions[symbol] != 0:
                    price_change = mid - last_prices[symbol]
                    paper_pnl[symbol] += positions[symbol] * price_change
                last_prices[symbol] = mid

                # Process delayed labels and update calibrator
                ready_samples = label_buffers[symbol].tick(mid)
                for features, label in ready_samples:
                    loss = calibrators[symbol].update(features, label)
                    if loss > 0:
                        log.debug("paper.calibrator_update", symbol=symbol, loss=f"{loss:.4f}")

                # Compute features
                factors = factor_engines[symbol].compute_incremental(tick_dict)
                feature_buffers[symbol].append(factors)
                if len(feature_buffers[symbol]) > seq_len:
                    feature_buffers[symbol] = feature_buffers[symbol][-seq_len:]

                if len(feature_buffers[symbol]) < seq_len:
                    continue

                # Prepare input
                feature_names = list(factors.keys())
                X = np.array([[fb[f] for f in feature_names] for fb in feature_buffers[symbol]])
                X = np.nan_to_num(X, nan=0.0)

                # Get base model prediction
                model = models[symbol]
                if model_name in ["logistic", "xgboost", "lightgbm"]:
                    X_input = X[-1:, :]
                else:
                    X_input = X

                try:
                    base_probs = model.predict_proba(X_input)
                    if len(getattr(base_probs, "shape", ())) > 1:
                        base_probs = base_probs[-1]
                except Exception as e:
                    log.warning("paper.predict_failed", symbol=symbol, error=str(e))
                    continue

                # Stack deep logits + factors for calibrator
                calibrator_input = np.concatenate(
                    [
                        np.asarray(base_probs, dtype=float),
                        X[-1, : min(8, len(feature_names))],
                    ]
                )

                # Resize calibrator if needed
                if len(calibrator_input) != calibrators[symbol].n_inputs:
                    calibrators[symbol].n_inputs = len(calibrator_input)
                    calibrators[symbol].weights = np.random.randn(len(calibrator_input), 3) * 0.01

                # Get calibrated probabilities
                cal_probs = calibrators[symbol].predict_proba(calibrator_input)

                # Add to delayed label buffer for online learning
                label_buffers[symbol].add_prediction(calibrator_input, mid)

                # Trading decision (paper only)
                if cal_probs[2] > threshold_up:
                    target = 1
                elif cal_probs[0] > threshold_down:
                    target = -1
                else:
                    target = 0

                if target != positions[symbol]:
                    old_pos = positions[symbol]
                    positions[symbol] = target
                    log.info(
                        "paper.position_change",
                        symbol=symbol,
                        old=old_pos,
                        new=target,
                        probs=f"[{cal_probs[0]:.2f},{cal_probs[1]:.2f},{cal_probs[2]:.2f}]",
                        paper_pnl=f"{paper_pnl[symbol]:.2f}",
                    )

    except KeyboardInterrupt:
        log.info("paper.interrupted")
    finally:
        api.close()

        # Final report
        for symbol in symbols:
            log.info(
                "paper.final",
                symbol=symbol,
                position=positions[symbol],
                paper_pnl=f"{paper_pnl[symbol]:.2f}",
                calibrator_updates=calibrators[symbol].n_updates,
            )

