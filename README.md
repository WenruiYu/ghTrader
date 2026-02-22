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

- Canonical tick data (**main_l5**) is **append-only** in **QuestDB**; never mutate historical tick rows
- Ingest writes a lightweight manifest under `data/manifests/` (run id, symbols, date range, schema hash, code hash)
- Timestamps are stored in **epoch-nanoseconds** (`datetime_ns`, as provided by TqSdk)

### Reproducibility

- Time-series splits only (walk-forward); no random shuffles across time
- Features and labels must be computable **causally** at tick-time (no future data)
- Each experiment records: config + git commit hash + data manifest IDs + metrics

### AI-centric but production-minded

- Start with strong baselines (Logistic/XGBoost) before deep models
- Training uses GPUs (DDP); backtests are CPU-parallel
- Inference is designed for export (TorchScript/ONNX) for low-latency deployment later

### Performance and scale

- Use QuestDB partitioning + cached manifests/summaries for coverage/completeness (avoid full scans on interactive paths)
- Use subprocess jobs (dashboard/CLI) for long-running operations
- Parallelize CPU-heavy stages (ingest/build/backtests) across symbols/partitions

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

# Install ghTrader in editable mode (dev + dashboard + QuestDB client)
pip install -e ".[dev,control,questdb]"

# Install TqSdk (required for data download/backtest)
# Option A (recommended): install from PyPI
pip install tqsdk

# Optional: add tabular baselines (xgboost/lightgbm)
# pip install -e ".[tabular]"

# Configure TqSdk credentials (Pro account with tq_dl required)
# Option 1: Create .env file (recommended)
cp env.example .env
# Edit .env and fill in your credentials

# Option 2: Set environment variables directly
export TQSDK_USER="your_username"
export TQSDK_PASSWORD="your_password"

# Optional: override trading calendar holiday list (no akshare)
export TQ_CHINESE_HOLIDAY_URL="https://files.shinnytech.com/shinny_chinese_holiday.json"

# Start QuestDB (canonical store) and initialize tables
docker compose -f infra/questdb/docker-compose.yml up -d
ghtrader db questdb-health
ghtrader db questdb-init

# Download historical L5 ticks for a symbol and date range
ghtrader download --symbol SHFE.cu2502 --start 2024-01-01 --end 2024-01-31

# Build features and labels
ghtrader build --symbol SHFE.cu2502

# Train baseline model (works with default deps)
ghtrader train --model logistic --symbol SHFE.cu2502

# Train a deep model with DDP across 4 GPUs (recommended on this server)
# (run inside the venv; requires torchrun available in PATH)
torchrun --nproc_per_node=4 -m ghtrader.cli train --model deeplob --symbol SHFE.cu2502 --epochs 50 --batch-size 256 --seq-len 100 --lr 1e-3

# Run backtest (requires a trained model artifact for the same symbol/model/horizon)
ghtrader backtest --model logistic --symbol SHFE.cu2502 --start 2024-01-01 --end 2024-01-31
```

---

## SSH-only web dashboard (control plane)

The server is headless (no GUI OS). ghTrader includes an **SSH-only web dashboard** to manage long-running jobs, view data coverage, train models, and monitor trading—all without manually running CLI commands.

Start the dashboard on the server (binds to `127.0.0.1` by default):

```bash
ghtrader dashboard --host 127.0.0.1 --port 8000
```

From your local machine, open an SSH tunnel and access it in your browser:

```bash
ssh -L 8000:127.0.0.1:8000 ops@<server>
```

Then open `http://127.0.0.1:8000` locally.

### Dashboard pages

The control plane is variety-centric with fixed SHFE varieties: `cu`, `au`, `ag`.

| Page | Canonical URL | Description |
|------|---------------|-------------|
| **Dashboard** | `/v/{var}/dashboard` | Command center with KPIs, pipeline status, quick actions |
| **Jobs** | `/v/{var}/jobs` | Variety-scoped jobs and management |
| **Data** | `/v/{var}/data` | Main schedule + main_l5 build/validation for current variety |
| **Models** | `/v/{var}/models` | Variety-scoped model inventory, training, benchmarks |
| **Trading** | `/v/{var}/trading` | Trading console filtered to current variety context |
| **SQL** | `/explorer` | QuestDB SQL explorer (read-only; canonical ticks) |
| **System** | `/system` | CPU, memory, disk, GPU monitoring |

Legacy routes remain for compatibility and redirect to default variety `cu`:
`/`, `/jobs`, `/data`, `/models`, `/trading`, `/ops*` -> `/v/cu/...`

Examples:

```bash
# Copper workspace
http://127.0.0.1:8000/v/cu/dashboard

# Gold workspace
http://127.0.0.1:8000/v/au/data
```

### Key features

- **Status indicators**: QuestDB and GPU status shown in the topbar
- **Running jobs badge**: See at-a-glance how many jobs are running
- **Tabbed layouts**: Each page organizes related functionality into tabs
- **Toast notifications**: Async feedback for job submissions and actions
- **Auto-refresh**: Live data updates without manual page reload
- **Quick actions**: One-click pipeline operations from the dashboard

### Operational notes

- Job metadata: `runs/control/jobs.db`
- Logs: `runs/control/logs/job-<id>.log`
- Optional token (defense-in-depth): `ghtrader dashboard --token <TOKEN>` and access with `?token=<TOKEN>`
- Layered data-quality runbook: `reports/data_quality_operations_manual.md`

### Data quality APIs

The Data Hub exposes layered readiness endpoints for operators and alerts:

- `GET /api/data/quality/readiness`
- `GET /api/data/quality/anomalies`
- `GET /api/data/quality/profiles`

These complement `main-l5-validate` reports with machine-readable fields:
`engineering_state`, `source_state`, `policy_state`, `overall_state`, `reason_code`, `action_hint`.

---

## Project structure

```
ghTrader/
  README.md              # This file (project rules + setup)
  PRD.md                 # Canonical requirements/spec (single source of truth)
  pyproject.toml         # Dependencies, entrypoints, formatting
  .gitignore             # Ignore data/, runs/, artifacts/, secrets
  src/ghtrader/
    __init__.py
    cli.py               # CLI entrypoint (thin; delegates to cli_commands)
    cli_commands/        # CLI subcommands by domain
    control/             # FastAPI UI + APIs + job runner
    tq/                  # TqSdk-only provider implementation
    questdb/             # QuestDB schema/query/ingest helpers
    data/                # Data lifecycle (calendar, schedule, main_l5 builders)
    datasets/            # Derived datasets (features/labels)
    trading/             # Trading runtime modules
    research/            # Training/eval/benchmark utilities
    util/                # Shared helpers
    config.py            # Env/config + TqAuth
    data_provider.py     # Provider interfaces
  data/                  # Local caches + ingest manifests (gitignored)
  runs/                  # Reports + control-plane DB/logs (gitignored)
  artifacts/             # Model artifacts (gitignored)
```

---

## QuestDB schema (canonical; v2-only)

Key tables (see `PRD.md` for the authoritative architecture):

- **Ticks (canonical)**:
  - `ghtrader_ticks_main_l5_v2` (`ticks_kind='main_l5'`, derived main-with-depth)
- **Roll schedule**:
  - `ghtrader_main_schedule_v2` (canonical schedule for continuous alias resolution)
- **Derived datasets** (optional):
  - `ghtrader_features_v2`, `ghtrader_labels_v2` + build-metadata tables

Core tick columns:

- `symbol`, `ts`, `datetime_ns`, `trading_day`, `dataset_version='v2'`, `ticks_kind`, `row_hash`
- L5 numeric columns: `bid/ask_price1..5`, `bid/ask_volume1..5`, plus `last_price`, `average`, `highest`, `lowest`, `volume`, `amount`, `open_interest`
- Derived `main_l5` rows also include: `underlying_contract`, `segment_id`, `schedule_hash`

---

## QuestDB (required)

ghTrader is **QuestDB-first/only** for ticks, schedules, features, and labels.

Install the QuestDB extras:

```bash
pip install -e ".[questdb]"
```

Start QuestDB (docker compose):

```bash
docker compose -f infra/questdb/docker-compose.yml up -d
```

Verify connectivity and initialize required tables:

```bash
ghtrader db questdb-health
ghtrader db questdb-init
```

Configure connection (see `env.example`):
- `GHTRADER_QUESTDB_HOST`
- `GHTRADER_QUESTDB_ILP_PORT` (default `9009`)
- `GHTRADER_QUESTDB_PG_PORT` (default `8812`)
- `GHTRADER_QUESTDB_PG_USER` / `GHTRADER_QUESTDB_PG_PASSWORD` / `GHTRADER_QUESTDB_PG_DBNAME`

Common flows:

```bash
# Probe L5 start (manual) and copy the env line
ghtrader data l5-start --exchange SHFE --var cu
# Then set in .env:
#   GHTRADER_L5_START_DATE_CU=YYYY-MM-DD
# Repeat for AU/AG:
#   ghtrader data l5-start --exchange SHFE --var au
#   ghtrader data l5-start --exchange SHFE --var ag

# Build main schedule from TqSdk KQ.m@ mapping
# (uses per-var env start key -> latest trading day)
ghtrader main-schedule --var cu

# Build derived main_l5 ticks for the continuous alias (writes to QuestDB)
ghtrader main-l5 --var cu --symbol KQ.m@SHFE.cu

# (Optional) Build features + labels from main_l5
ghtrader build --symbol KQ.m@SHFE.cu --ticks-kind main_l5
```

Pipeline robustness defaults (recommended in `.env`):
- `GHTRADER_PIPELINE_ENFORCE_HEALTH=true` (step success implies persisted state is healthy)
- `GHTRADER_LOCK_WAIT_TIMEOUT_S=120` + `GHTRADER_LOCK_FORCE_CANCEL_ON_TIMEOUT=true` (recover from stuck lock owners)
- Optionally tune `GHTRADER_MAIN_L5_TOTAL_WORKERS` / `GHTRADER_MAIN_L5_SEGMENT_WORKERS` to cap ingest fan-out

## Data maintenance (Update)

Manual update (active + recently expired contracts):

```bash
ghtrader update --exchange SHFE --var cu --recent-expired-days 10 --refresh-catalog
```

Dashboard update:
- Data → Contracts → **Update** (bulk or per-contract)

Optional daily Update scheduler (dashboard):
- Set `GHTRADER_DAILY_UPDATE_TARGETS=SHFE:cu` (and optionally `GHTRADER_DAILY_UPDATE_RECENT_EXPIRED_DAYS=10`) in `.env` and restart the dashboard.

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
