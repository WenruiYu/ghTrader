# System Optimization Roadmap (P0/P1/P2)

Last updated: 2026-02-23

## Prioritization Rules

- `P0`: trading safety / data correctness / release gate integrity
- `P1`: reliability hardening and architectural decoupling
- `P2`: maintainability and observability maturity

Complexity scale: `S` (1-2 files), `M` (3-8 files), `L` (cross-domain refactor).

---

## P0 (Immediate) - Minimal Safe Batches

### Batch `P0-A` Trading Safety Envelope

| ID | Optimization | Primary files | Risk removed | Complexity | Verification |
|---|---|---|---|---|---|
| `P0-A1` | Enforce strategy lock symmetry (`trade:strategy:account` + account lock) | `src/ghtrader/cli_commands/runtime.py`, `src/ghtrader/control/locks.py` | Dual strategy runners writing conflicting targets | S | Start two strategy processes for same account; second must fail lock |
| `P0-A2` | Persist risk-kill state and require explicit reset | `src/ghtrader/tq/gateway.py`, `src/ghtrader/control/routes/gateway.py`, `src/ghtrader/control/static/trading.js` | Restart clears kill state unexpectedly | M | Trigger kill, restart gateway, verify kill persists until reset action |
| `P0-A3` | Warm-path degraded mode surfacing for Redis/WS failures | `src/ghtrader/tq/gateway.py`, `src/ghtrader/trading/strategy_runner.py`, `src/ghtrader/control/app.py`, `src/ghtrader/control/static/trading.js` | Silent observability blind spot | M | Disable Redis; UI must show degraded badge and recovery events |

### Batch `P0-B` Data Contract & Schema Hardening

| ID | Optimization | Primary files | Risk removed | Complexity | Verification |
|---|---|---|---|---|---|
| `P0-B1` | Replace best-effort schema evolution with explicit migration ledger + required-column checks | `src/ghtrader/questdb/migrate.py`, `src/ghtrader/questdb/serving_db.py`, `src/ghtrader/cli_commands/db.py` | Silent schema drift | L | Simulate missing column; build should fail-fast with migration guidance |
| `P0-B2` | Strict provenance requirement for `main_l5` labels/features (`segment_id` + `schedule_hash`) | `src/ghtrader/datasets/labels.py`, `src/ghtrader/datasets/features.py`, `src/ghtrader/research/pipeline.py` | Temporal leakage under missing provenance | M | Inject missing provenance day; strict mode must block and report |
| `P0-B3` | Row-hash algorithm versioning in metadata/manifests | `src/ghtrader/data/ticks_schema.py`, `src/ghtrader/data/manifest.py`, `src/ghtrader/questdb/features_labels.py` | Idempotency ambiguity after hash function change | M | Reingest same dataset; dedupe counters and hash-version checks must pass |

### Batch `P0-C` Control Plane Quality Gate Integrity

| ID | Optimization | Primary files | Risk removed | Complexity | Verification |
|---|---|---|---|---|---|
| `P0-C1` | Normalize CI workflow to one canonical pipeline | `.github/workflows/ci.yml` | Duplicate workflow definitions and ambiguous gate behavior | S | Run CI in PR and push contexts; verify single deterministic job graph |
| `P0-C2` | Enforce env-only key rejection at config store boundary | `src/ghtrader/config_service/store.py`, `src/ghtrader/config_service/schema.py`, `src/ghtrader/cli_commands/configuration.py`, `src/ghtrader/control/routes/core.py` | Secret/config boundary bypass | M | Attempt to set env-only keys via API and CLI; both must reject |

### P0 Implementation Status (2026-02-23)

- `P0-A1`: Implemented (`strategy run` now acquires account + strategy locks).
- `P0-A2`: Implemented (gateway risk-kill state persisted and explicit `reset_risk_kill` command added).
- `P0-A3`: Implemented (warm-path degraded state surfaced in gateway/strategy state and Trading Console UI/API).
- `P0-B1`: Implemented (QuestDB schema migration ledger + required-column fail-fast checks in `ensure_table`/db init path).
- `P0-B2`: Implemented (strict main_l5 provenance gate for features/labels and research pipeline guardrail).
- `P0-B3`: Implemented (row-hash algorithm version propagated to manifests/build metadata with parity checks).
- `P0-C1`: Implemented (CI workflow normalized to a single deterministic pipeline).
- `P0-C2`: Implemented (ConfigStore rejects env-only/unmanaged keys at storage boundary; CLI/API tests updated).

---

## P1 (Near-Term) - Reliability + Decoupling

| ID | Workstream | Primary files | Expected gain | Complexity | Verification |
|---|---|---|---|---|---|
| `P1-T1` | Bounded command dedupe window (TTL/LRU) | `src/ghtrader/tq/gateway.py` | Prevent long-run memory growth | S | 24h soak test; resident memory should stabilize |
| `P1-T2` | Explicit ZMQ connection-health FSM | `src/ghtrader/trading/strategy_runner.py`, `src/ghtrader/tq/gateway.py` | Fewer oscillating safe-halt states | M | Fault-inject IPC interruptions; verify deterministic recovery transitions |
| `P1-T3` | Multi-sink state revision contract (Redis/file/ZMQ) | `src/ghtrader/tq/gateway.py`, `src/ghtrader/control/state_helpers.py` | Consistent observer view | M | Verify monotonic revision IDs across sinks under partial failures |
| `P1-D1` | Ingest maintenance circuit breaker and skip ledger | `src/ghtrader/tq/ingest.py`, `src/ghtrader/data/main_l5.py` | Better throughput during provider incidents | M | Simulate repeated maintenance errors; pipeline continues with explicit skip report |
| `P1-D2` | Validation and build gate integration (`enforce_health`) | `src/ghtrader/data/main_l5.py`, `src/ghtrader/data/main_l5_validation.py`, `src/ghtrader/cli_commands/data.py` | Prevent unhealthy dataset promotion | M | Build with injected gap anomalies; pipeline must stop with actionable diagnostics |
| `P1-C1` | Split `control/app.py` composition responsibilities | `src/ghtrader/control/app.py`, `src/ghtrader/control/*` | Lower regression blast radius | L | API contract regression tests + websocket behavior parity |
| `P1-C2` | Supervisor health attribution and restart reason telemetry | `src/ghtrader/control/supervisors.py`, `src/ghtrader/control/jobs.py`, `src/ghtrader/control/slo.py` | Faster incident triage | M | Kill child processes; verify restart counters and reason tags |
| `P1-C3` | WebSocket broadcast failure accounting + dead-client pruning | `src/ghtrader/control/app.py` | Improved warm-path resilience | S | WS disconnect storm test; dead clients removed, counters increase |
| `P1-M1` | `research/models.py` modular split by model families | `src/ghtrader/research/models.py`, `src/ghtrader/research/models_*.py` | Better maintainability and faster reviews | L | Existing `test_models.py` and `test_sota_models.py` pass unchanged |

---

## P2 (Mid-Term) - Maintainability & Operations Maturity

| ID | Workstream | Primary files | Expected gain | Complexity | Verification |
|---|---|---|---|---|---|
| `P2-O1` | Incident response runbook + SLO threshold docs | `reports/data_quality_operations_manual.md`, `reports/incident_response_runbook.md` | Stronger ops readiness | S | Runbook dry-run drill with simulated outage |
| `P2-O2` | Full end-to-end pipeline integration tests | `tests/test_e2e_data_pipeline.py` (new), related fixtures | Catch cross-module regressions | M | CI integration job with optional credentials |
| `P2-O3` | Coverage governance cleanup and exclusion review | `pyproject.toml`, `tests/` | More truthful quality signal | S | Coverage report by package and exclusion rationale |
| `P2-A1` | `control/views.py` decomposition | `src/ghtrader/control/views.py`, `views_*` modules | UI route maintainability | M | Route parity snapshots for core pages |
| `P2-A2` | Query budget + pagination standards across control APIs | `src/ghtrader/control/routes/*.py` | Better latency predictability | M | Load-test list endpoints with bounded query cost |
| `P2-D1` | Data contract checks in CI (schema/null-rate/hash integrity) | `.github/workflows/ci.yml`, data validation utilities | Earlier data regression detection | M | CI preflight data-contract job passes/fails deterministically |

---

## Recommended Execution Order

1. `P0-A` (trading safety)
2. `P0-B` (data contract/schema)
3. `P0-C` (gate integrity)
4. `P1-T` + `P1-D` in parallel
5. `P1-C` modularization
6. `P2` operational maturity

## Exit Criteria Per Priority

- `P0`: all items merged with regression tests and runbook deltas
- `P1`: architecture boundaries extracted and reliability counters in place
- `P2`: process and observability maturity documented and automated where possible

