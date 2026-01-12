---
name: Akshare+DataPlan Restore
overview: Integrate akshare to build an SHFE-rule main-contract schedule (date→underlying contract), plan L1-vs-L5 tick storage and a main-with-depth dataset, and restore accidentally deleted project scaffolding (pyproject/.gitignore/tests) before implementation.
todos:
  - id: prd-main-depth-update
    content: Update PRD.md with L1 vs L5 data reality, SHFE main continuous rule, and definition of main-with-depth dataset semantics.
    status: completed
  - id: restore-project-files
    content: Recreate root pyproject.toml, root .gitignore, and restore the full tests/ suite; ensure pytest passes.
    status: completed
    dependencies:
      - prd-main-depth-update
  - id: akshare-install-and-adapter
    content: Make vendored akshare usable (install guidance + small adapter module to fetch daily OI and normalize symbols).
    status: completed
    dependencies:
      - restore-project-files
  - id: main-schedule-builder
    content: Implement SHFE main contract schedule builder using the 1.1 OI threshold next-day rule; persist schedule in data/rolls/ with manifest.
    status: completed
    dependencies:
      - akshare-install-and-adapter
  - id: exhaustive-l5-downloader
    content: Add downloader/CLI to backfill L5 ticks for a full CU contract range (cu1601→cu2701) using inferred active date ranges from daily data.
    status: completed
    dependencies:
      - main-schedule-builder
  - id: main-with-depth-materialization
    content: Build and persist derived main-with-depth L5 dataset driven by schedule + underlying contract ticks, with provenance manifest.
    status: completed
    dependencies:
      - exhaustive-l5-downloader
  - id: tests-new-core
    content: Add tests-first coverage for schedule logic, symbol normalization, contract-range generation, and optional integration smoke tests.
    status: completed
    dependencies:
      - main-schedule-builder
      - exhaustive-l5-downloader
      - main-with-depth-materialization
---

# Akshare integration + SHFE main-with-depth dataset + repo recovery

## Key findings (from repo + data inspection)

- **Repo recovery needed**: root `pyproject.toml`, root `.gitignore`, and the entire `tests/` suite are currently missing.
- **No git history available**: `git status` fails because `/home/ops/ghTrader` is **not a git repo**, so recovery must be **reconstruction** (not `git restore`).
- **Tick schema reality** (Parquet inspection):
- `KQ.m@SHFE.cu` partitions have the L5 columns present but **levels 2–5 are 100% NaN** (true **L1-only** stream).
- `SHFE.cu2602` partitions have **levels 1–5 populated** (true L5 depth), though some rows may still contain NaNs.
- **akshare is vendored but not currently importable as `import akshare as ak`** because the repo contains `/home/ops/ghTrader/akshare/akshare/*` and Python is currently resolving `akshare` as a **namespace package** rooted at `/home/ops/ghTrader/akshare`. The correct integration is to **install the vendored akshare repo** (editable) so `akshare` resolves to `/home/ops/ghTrader/akshare/akshare/__init__.py`.
- **SHFE main continuous rule is fixed** (provided by you):
- main starts from day 2 based on day-1 close OI ranking
- switch effective **next trading day** only
- switch trigger: other contract OI > current_main_OI * 1.1
- 88 = splice (no smoothing), 888 = back-adjusted prices (not suitable for tick microstructure)

## Decisions (aligned with your instruction)

- **Main rule**: implement the SHFE rule exactly (OI-based + 1.1 threshold + next-day effect; no intraday switching).
- **L5 backfill**: per your choice, implement an **exhaustive contract-range downloader** (e.g. `SHFE.cu1601` → `SHFE.cu2701`) but still compute **actual active trading ranges** per contract from daily data to avoid pointless empty-range downloads.

## PRD-first changes

Update [`/home/ops/ghTrader/PRD.md`](/home/ops/ghTrader/PRD.md) to add:

- **Data sources & depth levels**: explicitly document that `KQ.m@...` is L1-only (levels 2–5 NaN) while specific contracts provide L5.
- **Main-with-depth dataset**:
- define it as a **derived dataset** built from specific-contract L5 ticks + SHFE roll schedule
- define that sequences/labels must **not cross roll boundaries**
- explicitly state we use **88-style splice semantics** for tick research (no 888 smoothing)
- **akshare role**: used as a **daily OI data provider** (and/or daily contract universe extraction), with caching and reproducible schedule outputs.

## Implementation plan

### 1) Restore deleted project scaffolding (so we can safely develop)

Recreate:

- Root [`/home/ops/ghTrader/pyproject.toml`](/home/ops/ghTrader/pyproject.toml): restore package metadata, dependencies, dev deps, `ghtrader` console script.
- Root [`/home/ops/ghTrader/.gitignore`](/home/ops/ghTrader/.gitignore): restore ignores for `data/`, `runs/`, `artifacts/`, `.env`, `.venv/`, caches.
- [`/home/ops/ghTrader/tests/`](/home/ops/ghTrader/tests): restore the full unit test suite previously used (lake/features/labels/models/online/pipeline/cli/distributed/benchmark/ddp-smoke).

### 2) Make vendored akshare usable in ghTrader

- Add install guidance: `pip install -e ./akshare` (same style as `tqsdk-python`).
- In ghTrader code, avoid `import akshare` top-level (it imports huge surface area). Instead import the specific module:
- `from akshare.futures.futures_daily_bar import get_futures_daily`
- Add a small adapter module (e.g. `src/ghtrader/akshare_daily.py`) that:
- calls `get_futures_daily(market="SHFE")`
- normalizes symbols to TqSdk format (e.g. `CU2602` → `SHFE.cu2602`)
- provides caching of fetched daily data to Parquet for reproducibility.

### 3) Implement SHFE main-contract schedule builder (date → underlying contract)

Add a module (e.g. `src/ghtrader/main_contract.py`) implementing:

- `build_shfe_main_schedule(var: str, start: date, end: date, rule_threshold: float=1.1) -> pd.DataFrame`
- Uses daily OI per contract and applies the **next-day switch** rule.
- Outputs:
- `date`, `main_contract`, `main_oi`, `next_best_contract`, `next_best_oi`, `switch_flag`
- Persist schedule to: `data/rolls/shfe_main_schedule/var=cu/schedule.parquet` (and similarly for `au/ag`).

### 4) Implement exhaustive contract-range L5 downloader (your requested approach)

Extend [`/home/ops/ghTrader/src/ghtrader/tq_ingest.py`](/home/ops/ghTrader/src/ghtrader/tq_ingest.py) + CLI to support:

- `ghtrader download-contract-range --exchange SHFE --var cu --start-contract 1601 --end-contract 2701`
- Use akshare daily data to infer each contract’s **first/last active trading day**, and download only that range.
- Retain resume behavior (skip already-present partitions).

### 5) Build “main-with-depth” continuity dataset (materialized)

Create a derived dataset builder (e.g. `src/ghtrader/main_depth.py`):

- Input: `schedule.parquet` + underlying contract ticks in `data/lake/ticks/`.
- Output: a **separate derived lake** (recommended):
- `data/lake/main_l5/ticks/symbol=KQ.m@SHFE.cu/date=YYYY-MM-DD/part-....parquet`
- Store a manifest linking:
- schedule hash
- list of underlying contracts used
- row counts per day

### 6) Tests-first for all new core behavior

- Add unit tests (no network):
- schedule rule logic using synthetic daily OI tables
- contract-range generator (cu1601→cu2701)
- symbol normalization (akshare → TqSdk)
- Add optional integration tests (marked) that run only with credentials:
- smoke download for 1 contract-day

## Files expected to change/add

- **Restore**: [`/home/ops/ghTrader/pyproject.toml`](/home/ops/ghTrader/pyproject.toml), [`/home/ops/ghTrader/.gitignore`](/home/ops/ghTrader/.gitignore), [`/home/ops/ghTrader/tests/`](/home/ops/ghTrader/tests)
- **Update**: [`/home/ops/ghTrader/PRD.md`](/home/ops/ghTrader/PRD.md), [`/home/ops/ghTrader/README.md`](/home/ops/ghTrader/README.md), [`/home/ops/ghTrader/src/ghtrader/tq_ingest.py`](/home/ops/ghTrader/src/ghtrader/tq_ingest.py)
- **Add**: `src/ghtrader/akshare_daily.py`, `src/ghtrader/main_contract.py`, `src/ghtrader/main_depth.py` (names can be adjusted)

## Validation

- Run `pytest` (unit tests) and ensure green.
- Validate schedule correctness by spot-checking several historical dates vs SHFE rule outputs.
- Validate derived main-with-depth dataset:
- no L2–L5 NaNs (except occasional data gaps)
- roll days correctly switch underlying contract next day