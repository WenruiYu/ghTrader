---
name: database-framework-improvements
overview: Evaluate and introduce a database layer for ghTrader that supports fast SQL over ticks/features/labels/metrics while preserving the Parquet lake as canonical storage, with optional escalation to a server time-series/analytics DB (QuestDB/ClickHouse) if/when needed.
todos:
  - id: audit-current-db-usage
    content: Inventory all current persistence points (lake_v2, features/labels, runs metrics, sqlite jobs/locks) and codify desired DB responsibilities in PRD.md.
    status: completed
  - id: duckdb-layer
    content: Implement DuckDB query layer over Parquet (views + CLI query command) and add a minimal metrics schema to query runs/results.
    status: completed
  - id: dashboard-explorer
    content: Add optional dashboard page/API to run read-only DuckDB queries with guardrails (limits, timeouts, allowlist).
    status: completed
  - id: server-db-benchmark
    content: Benchmark QuestDB vs ClickHouse on representative tick partitions (ingest speed, query latency, storage) and decide whether to add a serving DB backend.
    status: completed
  - id: optional-serving-backend
    content: If needed, implement a serving DB backend (QuestDB or ClickHouse) with backfill + incremental ingest jobs that respect lake_version + segment metadata.
    status: completed
---

# Database framework improvements (QuestDB-style) for ghTrader

## Current state (from codebase)

- **Canonical market data store**: Parquet/Arrow lake under `data/lake` (legacy) and `data/lake_v2` (trading-day partitions).
- Paths and IO live in [`/home/ops/ghTrader/src/ghtrader/lake.py`](/home/ops/ghTrader/src/ghtrader/lake.py)
- **Derived continuous L5 (“main_l5”)**: Parquet under lake root; v2 includes `underlying_contract` + `segment_id`.
- Materialization lives in [`/home/ops/ghTrader/src/ghtrader/main_depth.py`](/home/ops/ghTrader/src/ghtrader/main_depth.py)
- **Features/labels**: Parquet in `data/features/`, `data/labels/` with manifests; main_l5 propagates segment metadata.
- [`/home/ops/ghTrader/src/ghtrader/features.py`](/home/ops/ghTrader/src/ghtrader/features.py), [`/home/ops/ghTrader/src/ghtrader/labels.py`](/home/ops/ghTrader/src/ghtrader/labels.py)
- **Control-plane DB**: SQLite (`runs/control/jobs.db`) for jobs + locks.
- [`/home/ops/ghTrader/src/ghtrader/control/db.py`](/home/ops/ghTrader/src/ghtrader/control/db.py), [`/home/ops/ghTrader/src/ghtrader/control/locks.py`](/home/ops/ghTrader/src/ghtrader/control/locks.py)
- **Metrics / reports**: written as JSON + Parquet files under `runs/`.
- Example: [`/home/ops/ghTrader/src/ghtrader/eval.py`](/home/ops/ghTrader/src/ghtrader/eval.py)

## Why a DB layer helps here

- **Fast ad-hoc SQL** over multi-year tick history without hand-writing pandas/pyarrow scans.
- **Dashboards** become “query + render” rather than filesystem crawls.
- **Unified experiment/metrics querying** (compare runs, track promotion gates) instead of many JSON files.
- **Optional serving** for near-real-time inspection and time-series analytics.

## DB options (researched) and fit

### Embedded analytics DB (recommended baseline): DuckDB

- **Best fit when**: you want SQL over Parquet immediately, minimal ops, single-host workflows.
- **Strengths**:
- Queries Parquet directly (no ingest duplication), strong columnar performance.
- Easy integration into CLI + FastAPI dashboard.
- **Limitations**:
- Not a multi-tenant OLTP DB; multi-process writes are restricted.

### Time-series DB (QuestDB)

- **Best fit when**: you want a dedicated time-series daemon, high ingest, time-series SQL.
- **Strengths**:
- High-throughput ingestion via ILP; query via PGWire.
- Native time partitioning; WAL tables improve out-of-order ingest behavior.
- **Notable limitations** (per docs):
- PGWire lacks SSL; `DELETE` not supported; SQL dialect differs from PostgreSQL.
- Timestamps are stored UTC but transmitted as `timestamp without timezone`; clients must handle TZ carefully.

### Columnar analytics DB (ClickHouse)

- **Best fit when**: very large historical tick corpora and many concurrent analytical queries.
- **Strengths**:
- Excellent compression/codecs and query performance at scale.
- Can ingest Parquet efficiently; MergeTree partitioning by date with `ORDER BY (symbol, datetime)`.
- **Limitations**:
- Not transactional/OLTP; job locking should stay in SQLite/Postgres.

### Postgres + TimescaleDB

- **Best fit when**: you want a single relational DB for metadata + time-series, and ingest volume is moderate.
- **Strengths**:
- Transactions + strong ecosystem.
- Hypertables + compression + continuous aggregates.
- **Limitations**:
- For high-frequency L5 ticks, may be more operational tuning and lower ingest headroom than dedicated column stores.

## Recommended architecture (staged, low-risk)

### Stage A (deliver value fast): DuckDB “lakehouse query layer”

- Keep **Parquet lake_v2 as canonical**; add DuckDB as query engine.
- Provide:
- SQL views over `data/lake_v2/**.parquet`, `data/features/**.parquet`, `data/labels/**.parquet`.
- Tables for **runs/metrics** (ingest JSON summaries into DuckDB for easy comparisons).
- CLI command to run SQL and export results.
- Optional dashboard “Data Explorer” page powered by DuckDB read-only queries.

### Stage B (if you truly need a daemon DB): QuestDB or ClickHouse as a serving/index layer

- Add a pluggable backend for “serving DB”.
- Keep Parquet canonical; provide jobs to:
- Backfill Parquet partitions into DB.
- Incrementally ingest new partitions (or double-write from live recorder).
- Choose QuestDB vs ClickHouse based on benchmark results on your real tick volumes.

### Stage C (only if needed): migrate control-plane metadata DB

- Keep SQLite for now (it’s already WAL+busy_timeout hardened).
- If you go multi-host or need stronger transactional semantics, move **jobs/locks** to Postgres.

## Key files that would change (Stage A)

- [`/home/ops/ghTrader/PRD.md`](/home/ops/ghTrader/PRD.md): specify DB layer semantics and canonical store remains Parquet.
- [`/home/ops/ghTrader/pyproject.toml`](/home/ops/ghTrader/pyproject.toml): add optional deps (e.g., `duckdb`, optional `psycopg`/QuestDB client, optional ClickHouse client).
- Add new module (suggested): `src/ghtrader/db.py` or `src/ghtrader/db/`:
- `DatabaseBackend` interface
- `DuckDBBackend` implementation
- [`/home/ops/ghTrader/src/ghtrader/cli.py`](/home/ops/ghTrader/src/ghtrader/cli.py): add commands like `ghtrader sql` / `ghtrader db init`.
- [`/home/ops/ghTrader/src/ghtrader/control/views.py`](/home/ops/ghTrader/src/ghtrader/control/views.py) + new template: optional “Data Explorer” route.

## Validation strategy

- Unit tests for:
- View definitions (correct columns, partition columns, lake_version semantics).
- Queries against synthetic lakes (ticks/features/labels join by datetime).
- Metrics ingest roundtrip.
- If adding QuestDB/ClickHouse later: integration tests using containers in CI.