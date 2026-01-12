---
name: ghTrader Model+Tests Update
overview: Update the existing ghTrader plan to (1) add tests-first/TDD with backfilled unit tests for current core modules, (2) load TqSdk credentials from a repo-local `.env` file, and (3) expand the model research track to evaluate newer LOB-focused architectures (e.g., TLOB) and efficient sequence backbones (TCN, state-space/Mamba) against our DeepLOB/Transformer baselines.
todos:
  - id: tests-backfill-core
    content: Backfill pytest unit tests for existing core modules (lake/features/labels/models/online/pipeline/cli), add fixtures and synthetic tick data, and enforce a minimal coverage gate.
    status: completed
  - id: dotenv-creds
    content: "Add repo-local `.env` support via `python-dotenv`: create `.env.example`, load dotenv at CLI startup, centralize `TqAuth` creation in a config helper, and update all call sites."
    status: completed
    dependencies:
      - tests-backfill-core
  - id: bench-harness
    content: Add a benchmarking harness that computes offline metrics + trading metrics + latency stats for a given model/dataset configuration; produce a single JSON report per run.
    status: completed
    dependencies:
      - tests-backfill-core
      - dotenv-creds
  - id: model-tcn
    content: Implement a TCN (dilated ConvNet) sequence model for LOB/tick classification and wire it into train/eval/bench flows.
    status: completed
    dependencies:
      - bench-harness
  - id: model-tlob
    content: Implement a TLOB-style dual-attention transformer and wire it into train/eval/bench flows; compare to DeepLOB/Transformer/TCN.
    status: completed
    dependencies:
      - bench-harness
  - id: model-ssm-mamba-spike
    content: "Optional spike: evaluate an SSM/Mamba backbone for tick sequences (dependency permitting), focusing on latency vs accuracy tradeoffs."
    status: completed
    dependencies:
      - bench-harness
      - model-tcn
      - model-tlob
---

# ghTrader Plan Update: Model Zoo Research + Tests-First + `.env` Credentials

## What changed vs the current plan

- **Tests-first is now a hard rule**: we will adopt TDD for all future core logic, and **backfill tests immediately** for the already-implemented modules (characterization tests first, then refactor).
- **TqSdk credentials will be loaded from a repo-local `.env` file** (gitignored) via `python-dotenv`, while the code continues to read `TQSDK_USER/TQSDK_PASSWORD` from env.
- **Model research track expands beyond DeepLOB + vanilla Transformer** to include LOB-specialized Transformer designs (notably **TLOB, 2025**) and more efficient sequence backbones (TCN, state-space/Mamba-style) where they can improve latency/accuracy.

## Model landscape for SHFE L5 tick micro-alpha (what to evaluate next)

### Strong non-deep baselines (still worth keeping)

- **Regularized linear**: Logistic/Ridge/ElasticNet (fast, stable, best for stacked/online calibrators).
- **Tree ensembles**: LightGBM/XGBoost (strong tabular baselines; handles nonlinearities in engineered factors).
- **Online/streaming** (optional): `river` library for drift detectors + incremental models (alternative to custom calibrator).

### LOB/tick deep models (candidates that may beat our current DeepLOB/Transformer)

- **TLOB (Transformer with dual attention)**: explicitly models **spatial (price-level)** and **temporal** dependencies; reported strong results on FI-2010. Reference: [TLOB paper (2025)](https://hf.co/papers/2502.15757).
- **TCN / dilated ConvNets**: often strong on high-frequency sequences, **very fast inference**, linear complexity, easier to deploy.
- **State-space sequence models (S4/Mamba family)**: near-linear sequence modeling; promising for **longer context at lower latency** (not LOB-specific, but good candidate for tick streams).
- **Hybrid CNN→(Transformer/SSM)**: keep local “book-image” inductive bias but use stronger temporal backbone.

### Recommendation (model ladder v2)

1. **Tabular baselines + calibration** (fast iteration, best debugging signal)
2. **DeepLOB + TCN** (CNN-heavy inductive bias, low latency)
3. **TLOB-style transformer** (spatial+temporal attention)
4. **SSM/Mamba backbone** (if it delivers accuracy/latency improvements in practice)
5. Optional: **self-supervised pretraining** on unlabeled ticks (masked modeling / contrastive) once we have enough data

## Plan updates (files + implementation sequence)

### 1) Tests-first + backfill unit tests

- Add `pytest` harness and write **unit tests for core functions** in:
- [`/home/ops/ghTrader/src/ghtrader/lake.py`](/home/ops/ghTrader/src/ghtrader/lake.py): schema hash, partition write/read roundtrip, manifest serialization.
- [`/home/ops/ghTrader/src/ghtrader/labels.py`](/home/ops/ghTrader/src/ghtrader/labels.py): label boundaries, NaN tail handling, walk-forward splits properties.
- [`/home/ops/ghTrader/src/ghtrader/features.py`](/home/ops/ghTrader/src/ghtrader/features.py): factor batch vs incremental consistency on synthetic ticks, rolling-window behavior.
- [`/home/ops/ghTrader/src/ghtrader/models.py`](/home/ops/ghTrader/src/ghtrader/models.py): smoke tests for train/save/load/predict_proba for each model type on tiny synthetic data.
- [`/home/ops/ghTrader/src/ghtrader/online.py`](/home/ops/ghTrader/src/ghtrader/online.py): calibrator update reduces loss on a separable toy stream; drift-disable triggers.
- [`/home/ops/ghTrader/src/ghtrader/pipeline.py`](/home/ops/ghTrader/src/ghtrader/pipeline.py): latency tracker stats + budget checks.
- [`/home/ops/ghTrader/src/ghtrader/cli.py`](/home/ops/ghTrader/src/ghtrader/cli.py): CLI smoke tests (help, argument parsing) using Click’s runner.

- Testing principles:
- **No network calls** in unit tests.
- TqSdk integration tests (if any) will be `pytest.mark.integration` and skipped unless `.env` creds exist.

### 2) `.env` credentials support (repo-local, gitignored)

- Add `.env.example` documenting required keys (`TQSDK_USER`, `TQSDK_PASSWORD`).
- Add a small config helper module (e.g. `src/ghtrader/config.py`) that:
- calls `dotenv.load_dotenv()` once at CLI entry,
- exposes `get_tqsdk_auth()` used by ingest/backtest/paper code.
- Update code paths currently reading env directly to use the helper.

### 3) Model zoo research & benchmarking harness

- Add a “model benchmark” harness that runs:
- **offline metrics** (cross-entropy, F1, calibration) and
- **trading metrics** (Tier1 backtest + Tier2 microsim), and
- **latency gates** via `LatencyTracker`.

- Implement and compare these additional deep candidates:
- **TCN baseline** (dilated conv) for sequence classification.
- **TLOB-style dual-attention transformer** (spatial attention over L5 levels + temporal attention over ticks).
- Optional spike: **Mamba/SSM backbone** (only if dependency install is stable on your GPU stack).

- Decision rule: promote the architecture family that is best under **(PnL, DD, turnover, latency)** gates.

## Deliverables

- A tested core with reproducible local runs (`pytest` green) and a clear tests-first workflow.
- `.env`-driven credential loading (no more manual exports required).
- A structured model evaluation path that can credibly answer “is there a better model than DeepLOB/vanilla Transformer for SHFE L5 ticks?”