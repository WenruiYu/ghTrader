---
name: Audit+Checksums+DashboardParity
overview: Add a first-class lake audit/validation command, write-time file checksums, formal manifests for features/labels (and ticks/derived), and expand the SSH-only dashboard so every CLI feature is available and usable via dedicated forms.
todos:
  - id: prd-integrity-dashboard
    content: Update PRD.md with integrity validation, checksum, manifest, and dashboard parity requirements.
    status: completed
  - id: integrity-utils
    content: Add integrity utilities for sha256 sidecars and manifest helpers.
    status: completed
    dependencies:
      - prd-integrity-dashboard
  - id: write-time-checksums
    content: Integrate write-time checksums + manifests into ticks/features/labels/main_l5 writes.
    status: completed
    dependencies:
      - integrity-utils
  - id: audit-cli
    content: Implement `ghtrader audit` command and audit report output.
    status: completed
    dependencies:
      - write-time-checksums
  - id: dashboard-parity
    content: Expand dashboard with dedicated forms/pages for every CLI command (including audit).
    status: completed
    dependencies:
      - audit-cli
  - id: integrity-tests
    content: Add tests for checksums/manifests/audit and run full pytest suite.
    status: completed
    dependencies:
      - dashboard-parity
---

# Data integrity audit + write-time checksums + dashboard feature parity

## Goal

Implement:

1) **Data integrity verification/validation**

- A dedicated `ghtrader` command to audit the data lake(s): schema conformity, duplicates/out-of-order `datetime`, per-day row count mismatches vs manifests, NaN/negative sanity, and derived-vs-raw equivalence checks.
- **Write-time file-level checksums** for Parquet outputs.
- **Formal manifests for features/labels outputs** (and extend manifests for ticks/derived where needed for audit).

2) **Dashboard feature parity**

- Ensure **every CLI command** is available and usable in the dashboard via **dedicated forms** (not a single dropdown).

## PRD-first updates

Update [`/home/ops/ghTrader/PRD.md`](/home/ops/ghTrader/PRD.md) to add/extend sections covering:

- **Data integrity & validation**:
- New `ghtrader audit` command and what it checks (errors vs warnings, exit codes).
- Required checks: schema, datetime monotonicity/duplicates, basic sanity rules, manifest consistency, derived-vs-raw equivalence.
- **Checksums**:
- Every Parquet file written by ghTrader must produce a sibling checksum file `*.parquet.sha256` computed at write time.
- **Manifests**:
- Add dataset manifests for:
- raw ticks partitions (date-level or symbol-level)
- derived `main_l5` ticks
- features
- labels
- Each manifest must include per-day row counts, file list, and checksum references.
- **Dashboard parity**:
- The dashboard must expose forms for each CLI command in [`/home/ops/ghTrader/src/ghtrader/cli.py`](/home/ops/ghTrader/src/ghtrader/cli.py):
`download`, `download-contract-range`, `record`, `build`, `train`, `backtest`, `paper`, `benchmark`, `compare`, `daily-train`, `sweep`, `main-schedule`, `main-depth`, and the new `audit`.

## Implementation plan

### 1) Integrity utilities (new module)

Add `src/ghtrader/integrity.py` (or similar) with:

- `sha256_file(path) -> str` (streaming)
- `write_sha256_sidecar(parquet_path)` writes `parquet_path.with_suffix(parquet_path.suffix + '.sha256')`
- helpers to read/verify sidecar

### 2) Write-time checksums everywhere

- **Ticks**: update [`/home/ops/ghTrader/src/ghtrader/lake.py`](/home/ops/ghTrader/src/ghtrader/lake.py) `write_ticks_partition()` to:
- compute sha256 of the written parquet
- write `part-....parquet.sha256`
- update/emit a **partition manifest** for the day (e.g. `date=YYYY-MM-DD/_manifest.json`) containing file list + rows + sha256
- **Derived main_l5**: update [`/home/ops/ghTrader/src/ghtrader/main_depth.py`](/home/ops/ghTrader/src/ghtrader/main_depth.py) to write checksums and include file list in manifest.
- **Features/Labels**: update [`/home/ops/ghTrader/src/ghtrader/features.py`](/home/ops/ghTrader/src/ghtrader/features.py) and [`/home/ops/ghTrader/src/ghtrader/labels.py`](/home/ops/ghTrader/src/ghtrader/labels.py) to:
- write `*.parquet.sha256` for each partition file
- write a **symbol-level manifest.json** capturing build parameters (horizons/ticks_lake), schema hash, row_counts per date, and file checksums.

### 3) `ghtrader audit` command (new)

- Add `src/ghtrader/audit.py` implementing scan + report generation.
- Add CLI entrypoint in [`/home/ops/ghTrader/src/ghtrader/cli.py`](/home/ops/ghTrader/src/ghtrader/cli.py): `ghtrader audit ...`
- Default audit coverage:
- raw ticks (`data/lake/ticks`)
- derived main_l5 ticks (`data/lake/main_l5/ticks`)
- features (`data/features`)
- labels (`data/labels`)
- Checks (configurable via flags):
- **Checksum verification**: parquet bytes match `*.sha256`
- **Schema conformity**:
- ticks vs `TICK_ARROW_SCHEMA`
- features/labels match their recorded schema (from manifest)
- **Datetime integrity**: within each partition file: monotonic non-decreasing; duplicate ratio thresholds (warn)
- **Sanity rules** (ticks): non-negative volume/OI, prices non-negative, `ask_price1 >= bid_price1` when both present
- **Manifest consistency**: per-date row counts equal sum of parquet metadata rows; manifest file list exists
- **Derived-vs-raw equivalence** (main_l5): for each derived day, locate underlying raw files by schedule and verify:
- same file name exists
- same row count
- same `datetime` column
- numeric columns match (excluding `symbol`) for either full compare on small files or sampled rows on large files
- Output:
- JSON report under `runs/audit/<run_id>.json`
- exit code 0 if no errors, 1 if errors

### 4) Dashboard parity (full forms)

Update dashboard under `src/ghtrader/control/`:

- Add dedicated pages (and templates) for each command group:
- **Ingest**: download, download-contract-range, record
- **Build**: build (raw/main_l5), main-schedule, main-depth
- **Modeling**: train, sweep
- **Evaluation**: benchmark, compare, backtest, paper, daily-train
- **Integrity**: audit
- Extend `src/ghtrader/control/views.py` to construct argv for each command based on form inputs.
- Extend data/status pages to show:
- raw ticks coverage and derived main_l5 coverage
- latest audit reports and a link to the most recent report file

### 5) Tests-first

Add tests to `tests/` for:

- checksum sidecar creation and verification for ticks/features/labels/derived
- manifest creation for features/labels (and tick-date manifests)
- audit command on synthetic lakes:
- passes on clean data
- fails when a `.sha256` is tampered
- derived-vs-raw mismatch detection

## Files expected to change/add

- **Update**: [`/home/ops/ghTrader/PRD.md`](/home/ops/ghTrader/PRD.md)
- **Add**: `src/ghtrader/integrity.py`, `src/ghtrader/audit.py`
- **Update**: [`/home/ops/ghTrader/src/ghtrader/cli.py`](/home/ops/ghTrader/src/ghtrader/cli.py)
- **Update**: [`/home/ops/ghTrader/src/ghtrader/lake.py`](/home/ops/ghTrader/src/ghtrader/lake.py)
- **Update**: [`/home/ops/ghTrader/src/ghtrader/features.py`](/home/ops/ghTrader/src/ghtrader/features.py)
- **Update**: [`/home/ops/ghTrader/src/ghtrader/labels.py`](/home/ops/ghTrader/src/ghtrader/labels.py)
- **Update**: dashboard files under `src/ghtrader/control/` (routes/templates)
- **Add/Update tests**: new `tests/test_audit_*.py`, checksum/manifest tests

## Acceptance criteria

- Every Parquet output written by ghTrader has a matching `*.parquet.sha256` written at write time.
- Features/labels have `manifest.json` with file list + per-day counts and audit can verify them.
- `ghtrader audit` produces a JSON report and fails on checksum/schema/derived equivalence errors.
- Dashboard includes usable forms for every CLI command and can launch each as a subprocess job.