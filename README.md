# ghTrader

AI-centric SHFE tick trading system for copper (CU), gold (AU), and silver (AG) using TqSdk Pro.

## PRD (single source of truth)

The canonical product/spec/plan for this repo is [`PRD.md`](PRD.md). All future planning should start by reading and updating `PRD.md` before any implementation.

## Project Rules (MUST READ)

### Safety constraints

- **Research-only by default**: no live trading is enabled until explicitly configured
- All order routing code paths are gated behind `GHTRADER_LIVE_ENABLED=true` environment variable
- Default mode is backtest/paper; real orders require explicit enablement

### Data integrity and lineage

- Raw market data is **append-only** in the Parquet lake; never mutate raw data
- Every dataset/build writes a manifest (symbols, date range, schema version, code hash)
- Timestamps are stored in **epoch-nanoseconds** (Beijing time, as provided by TqSdk)

### Reproducibility

- Time-series splits only (walk-forward); no random shuffles across time
- Features and labels must be computable **causally** at tick-time (no future data)
- Each experiment records: config + git commit hash + data manifest IDs + metrics

### AI-centric but production-minded

- Start with strong baselines (Logistic/XGBoost) before deep models
- Training uses GPUs (DDP); backtests are CPU-parallel
- Inference is designed for export (TorchScript/ONNX) for low-latency deployment later

### Performance and scale

- Use **columnar storage** (Parquet/Arrow, ZSTD) partitioned by symbol/date
- Use multi-process / Ray for feature generation and batched backtests
- Avoid single-process bottlenecks

### Engineering hygiene

- Python-only MVP; type hints where they add value
- Keep modules consolidated (minimal file count)
- Add dependencies only when they pay for themselves
- Secrets never committed; credentials come from environment variables / `.env`

---

## Quick start

```bash
# Create virtual environment
python3 -m venv .venv
source .venv/bin/activate

# Install in editable mode (includes vendored tqsdk-python)
pip install -e ./tqsdk-python
pip install -e ./akshare
pip install -e .[dev,control]

# Configure TqSdk credentials (Pro account with tq_dl required)
# Option 1: Create .env file (recommended)
cp env.example .env
# Edit .env and fill in your credentials

# Option 2: Set environment variables directly
export TQSDK_USER="your_username"
export TQSDK_PASSWORD="your_password"

# Recommended: use the trading-day partitioned lake (lake_v2) by default
# (alternative: pass --lake-version v2 on relevant commands)
export GHTRADER_LAKE_VERSION=v2

# Download historical L5 ticks for a symbol and date range
ghtrader download --symbol SHFE.cu2502 --start 2024-01-01 --end 2024-01-31 --lake-version v2

# Build features and labels
ghtrader build --symbol SHFE.cu2502 --lake-version v2

# Train baseline model
ghtrader train --model xgboost --symbol SHFE.cu2502

# Train a deep model with DDP across 4 GPUs (recommended on this server)
# (run inside the venv; requires torchrun available in PATH)
torchrun --nproc_per_node=4 -m ghtrader.cli train --model deeplob --symbol SHFE.cu2502 --epochs 50 --batch-size 256 --seq-len 100 --lr 1e-3

# Run backtest
ghtrader backtest --model xgboost --symbol SHFE.cu2502
```

---

## SSH-only web dashboard (control plane)

The server is headless (no GUI OS). ghTrader includes an **SSH-only web dashboard** to manage long-running jobs and view logs/data coverage without manually running every CLI command.

Start the dashboard on the server (binds to `127.0.0.1` by default):

```bash
ghtrader dashboard --host 127.0.0.1 --port 8000
```

From your local machine, open an SSH tunnel and access it in your browser:

```bash
ssh -L 8000:127.0.0.1:8000 ops@<server>
```

Then open `http://127.0.0.1:8000` locally.

Explorer:
- SQL Explorer page: `http://127.0.0.1:8000/explorer`

Operational notes:
- Job metadata: `runs/control/jobs.db`
- Logs: `runs/control/logs/job-<id>.log`
- Optional token (defense-in-depth): `ghtrader dashboard --token <TOKEN>` and access with `?token=<TOKEN>`

---

## Project structure

```
ghTrader/
  README.md              # This file (project rules + setup)
  pyproject.toml         # Dependencies, entrypoints, formatting
  .gitignore             # Ignore data/, runs/, artifacts/, secrets
  tqsdk-python/          # Vendored TqSdk (do not modify unless patching)
  src/ghtrader/
    __init__.py
    cli.py               # CLI entrypoint (download, record, build, train, backtest, paper)
    tq_ingest.py         # TqSdk integration: historical download + live recorder
    lake.py              # Parquet schema, partitioning, manifest writing/reading
    db.py                # DuckDB lakehouse query layer (Parquet views + metrics index)
    db_bench.py          # QuestDB/ClickHouse benchmark harness (optional)
    serving_db.py        # Optional serving DB sync (backfill + incremental)
    features.py          # FactorEngine + registry
    labels.py            # Event-time labels, horizons
    models.py            # DeepLOB, Transformer, tabular wrappers
    online.py            # OnlineCalibrator + paper online loop
    eval.py              # Backtest harness + metrics + promotion gates
  data/                  # Parquet lake, manifests (gitignored)
  runs/                  # Configs, metrics, reports (gitignored)
  artifacts/             # Models, scalers, feature specs (gitignored)
```

---

## Data lake schema (L5 ticks)

Partitioning (two lake roots are supported):
- **lake_v1 (legacy)**: `data/lake/ticks/symbol=.../date=YYYY-MM-DD/part-....parquet`
- **lake_v2 (trading-day)**: `data/lake_v2/ticks/symbol=.../date=YYYY-MM-DD/part-....parquet`
  - For **lake_v2**, `date=YYYY-MM-DD` is the **trading day** (night session after ~18:00 local maps to the next trading day).

Derived main-with-depth ticks (optional, for continuous series + L5 depth):
- `data/lake{,_v2}/main_l5/ticks/symbol=KQ.m@SHFE.cu/date=YYYY-MM-DD/part-....parquet`
  - `lake_v2` main_l5 includes `underlying_contract` and `segment_id` to prevent cross-roll leakage in sequence models.

Columns:
- `symbol` (string): instrument code (e.g., `SHFE.cu2502`)
- `datetime` (int64): epoch-nanoseconds (Beijing time)
- `last_price`, `average`, `highest`, `lowest` (float64)
- `volume`, `amount`, `open_interest` (float64)
- `bid_price1..5`, `bid_volume1..5` (float64)
- `ask_price1..5`, `ask_volume1..5` (float64)

---

## DuckDB SQL layer (optional)

DuckDB provides fast **read-only SQL** over the Parquet lake and powers the dashboard Explorer.

Install:
- If you installed `pip install -e ".[dev,control]"`, DuckDB is included.
- Otherwise install it with: `pip install -e ".[db]"`

Initialize/refresh Parquet-backed views (writes `data/ghtrader.duckdb` by default):

```bash
ghtrader db init
```

Run SQL (SELECT/WITH only) and export:

```bash
ghtrader sql --query "SELECT COUNT(*) AS n FROM ticks_raw_v2"
ghtrader sql --query "SELECT * FROM run_metrics ORDER BY created_at DESC" --out runs_metrics.csv
```

Optional serving DB hooks (require external daemons):
- `ghtrader db benchmark ...` (QuestDB/ClickHouse ingest/query benchmark)
- `ghtrader db serve-sync ...` (best-effort backfill/incremental sync from Parquet partitions)

## Labels (event-time, multi-horizon)

- Mid price: `mid = (bid_price1 + ask_price1) / 2`
- Horizons: `N in {10, 50, 200}` ticks (configurable)
- Threshold: `k` ticks (default `k=1`)
- Classes: `{DOWN, FLAT, UP}` based on `mid[t+N] - mid[t]` relative to `k * price_tick`

---

## Models

1. **Baselines**: Logistic regression, XGBoost/LightGBM on engineered factors
2. **DeepLOB-style**: CNN over L5 snapshot + LSTM/GRU over time
3. **Transformer encoder**: longer context, multi-horizon heads
4. **Online calibrator**: stacks deep logits + factor vector; updates intraday via SGD/FTRL

---

## License

MIT
