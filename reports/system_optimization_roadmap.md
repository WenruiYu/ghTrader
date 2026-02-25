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

- `P0-A1`: Implemented (`strategy run` now acquires strategy-specific account lock only; no longer conflicts with gateway account lock).
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
| `P1-M2` | Move model training orchestration to dedicated module | `src/ghtrader/research/models.py`, `src/ghtrader/research/models_training.py` | Clearer boundary and lower merge-conflict risk | M | Existing imports stay unchanged; model/e2e tests and full non-integration suite stay green |

### P1 Progress Update (2026-02-23)

- `P1-T1`: Implemented (`gateway` command dedupe upgraded from unbounded in-memory `set` to bounded TTL/LRU behavior with configurable `GHTRADER_GATEWAY_CMD_DEDUPE_MAX_IDS` and `GHTRADER_GATEWAY_CMD_DEDUPE_TTL_S`; regression tests added in `tests/test_tq_gateway_mvp.py`).
- `P1-T2`: Implemented (added explicit ZMQ hot-path connection FSM in `strategy_runner` with bounded fail/recover transitions, and periodic gateway hot-path heartbeat publish to reduce timeout flapping; regression tests added in `tests/test_strategy_runner.py` and `tests/test_tq_gateway_mvp.py`).
- `P1-T3`: Implemented (`GatewayWriter` now emits monotonic `state_revision` per state flush and isolates Redis `publish`/`set` failures; control-plane state helpers now select freshest state by revision (fallback `updated_at`) across Redis/file sinks, with regression tests in `tests/test_tq_gateway_mvp.py` and `tests/test_control_state_helpers.py`).
- `P1-C3`: Implemented (`ConnectionManager` now tracks WebSocket broadcast attempts/successes/failures, prunes dead clients on send failures, and logs prune events; regression tests added in `tests/test_control_websocket_manager.py`).
- `P1-D1`: Implemented (`download_main_l5_for_days` now degrades maintenance-window failures into per-day skip ledger entries, with a shared maintenance circuit breaker to avoid repeated provider-hit retries under outage windows; aggregated skip ledger is now surfaced in `main_l5` update reports. Regression tests added in `tests/test_tq_ingest_maintenance_retry.py` and `tests/test_main_l5_update.py`).
- `P1-D2`: Implemented (`build_main_l5(enforce_health=True)` now invokes `validate_main_l5` as a promotion gate and fails on validation `overall_state=error` with actionable `reason_code/action_hint` diagnostics; gate summary is persisted into `main_l5` update reports. Regression coverage added in `tests/test_main_l5_update.py`).
- `P1-C1`: Implemented (finalized `control/app.py` composition split across `src/ghtrader/control/app_lifecycle.py`, `src/ghtrader/control/bootstrap.py`, `src/ghtrader/control/route_mounts.py`, and `src/ghtrader/control/app_factory.py`; `src/ghtrader/control/app.py` remains compatibility-shim only, with explicit shim-boundary regression in `tests/test_control_app_factory.py::test_control_app_module_is_compatibility_shim`).
- `P1-T3` (follow-up hardening): Implemented (`GatewayWriter` now rehydrates persisted revision on restart and records Redis state-write failure counters/streak; gateway warm-path degraded reason now reflects sustained Redis state-write failures as `redis_state_write_failed`, with new regression coverage in `tests/test_tq_gateway_mvp.py` and `tests/test_control_state_helpers.py`).
- `P1-C2`: Implemented (supervisor loops now publish restart/stop reason telemetry and tick-failure counters; supervisor-started jobs include restart reason metadata; telemetry is surfaced via SLO snapshot (`control_plane.supervisors`) and Trading Console payload (`supervisor_telemetry`) with targeted regression tests).
- `P1-M2`: Implemented (moved `train_model` orchestration into `src/ghtrader/research/models_training.py`; `src/ghtrader/research/models.py` remains the backward-compatible facade and now delegates to the new module; validated via `tests/test_models.py`, `tests/test_sota_models.py`, `tests/test_e2e_data_pipeline.py`, and full `pytest -m "not integration and not ddp_integration"`).

---

### P2 Progress Update (2026-02-23)

- `P2-A1`: In progress (first `views.py` decomposition slice landed: moved pipeline/calendar helper functions into `src/ghtrader/control/views_pipeline_helpers.py`, rewired `views.py` imports, and added module-boundary regression tests in `tests/test_control_views_pipeline_helpers.py` while preserving control-plane route parity tests).
- `P2-A1`: In progress (second slice landed: moved training-launch helper logic (`build_train_job_argv` policy + active DDP training detection) into `src/ghtrader/control/views_training_helpers.py`, rewired `views.py` to use thin wrappers, and added module-boundary regression tests in `tests/test_control_views_training_helpers.py`).
- `P2-A1`: In progress (third slice landed: moved model/eval action routes (`/models/model/*` + `/models/eval/*`) into `src/ghtrader/control/views_model_actions.py`, rewired `views.py` to mount via `register_model_action_routes`, and added route-module boundary tests in `tests/test_control_views_model_actions.py`).
- `P2-A1`: In progress (fourth slice landed: moved jobs action routes (`/jobs/start`, `/jobs/{job_id}/cancel`) into `src/ghtrader/control/views_job_actions.py`, rewired `views.py` to mount via `register_job_action_routes`, and added dedicated route-module boundary tests in `tests/test_control_views_job_actions.py`; existing `/jobs/start` dedupe behavior remains covered in `tests/test_control_main_build_dedupe.py`).
- `P2-A1`: In progress (fifth slice landed: moved Data Hub page routes (`/data`, `/v/{variety}/data`) and cache/query orchestration into `src/ghtrader/control/views_data_page.py`, rewired `views.py` to mount via `register_data_page_routes`, and updated `route_mounts` to consume the new data-page cache clear entrypoint; route-module boundary tests added in `tests/test_control_views_data_page.py`).
- `P2-A1`: In progress (sixth slice landed: moved Data Hub loader internals (reports/QuestDB status/coverage/validation snapshot loaders) into `src/ghtrader/control/views_data_page_loaders.py`, rewired `views_data_page.py` orchestration to use shared loaders, and added loader-boundary tests in `tests/test_control_views_data_page_loaders.py`).
- `P2-A1`: In progress (seventh slice landed: moved Data Hub orchestration internals (cache fan-out scheduling + async loader dispatch + snapshot merge/fallback) into `src/ghtrader/control/views_data_page_orchestrator.py`, rewired `views_data_page.py` to orchestration-only composition, and added orchestrator-boundary tests in `tests/test_control_views_data_page_orchestrator.py`).
- `P2-A1`: In progress (hardening follow-up landed: Data Hub orchestrator now normalizes fallback payloads so loader exceptions propagate into `coverage_error` / `validation_error`, `cache_hits` timing metric now reports preload hits before async fill, and regression coverage was expanded for both behaviors while residual unused imports were removed from `src/ghtrader/control/views.py`).
- `P2-A1`: In progress (eighth slice landed: moved Data Hub action routes (`/data/integrity/report/*`, `/data/main-l5-validate/report/*`, `/data/settings/tqsdk_scheduler`, `/data/build/*`) from `src/ghtrader/control/views.py` into dedicated `src/ghtrader/control/views_data_actions.py`, rewired `views.py` to mount via `register_data_action_routes`, and added route-module boundary regression coverage in `tests/test_control_views_data_actions.py`).
- `P2-A1`: In progress (ninth slice landed: moved workspace page routes (`/`, `/v/{variety}/dashboard`, `/jobs`, `/v/{variety}/jobs`, `/models`, `/v/{variety}/models`, `/trading`, `/v/{variety}/trading`) from `src/ghtrader/control/views.py` into dedicated `src/ghtrader/control/views_workspace_pages.py`, rewired `views.py` to mount via `register_workspace_page_routes`, and added route-module boundary regression coverage in `tests/test_control_views_workspace_pages.py`).
- `P2-A1`: In progress (tenth slice landed: moved job detail route (`/jobs/{job_id}`) from `src/ghtrader/control/views.py` into dedicated `src/ghtrader/control/views_job_detail.py`, rewired `views.py` to mount via `register_job_detail_routes`, and added route-module boundary regression coverage in `tests/test_control_views_job_detail.py`).
- `P2-A1`: Completed (`src/ghtrader/control/views.py` is now a composition/registration layer with no inline page/action route handlers; decomposed-route boundary tests and variety/page behavior regressions pass).
- `P2-A2`: Completed (introduced shared query budget/pagination helpers in `src/ghtrader/control/routes/query_budget.py`, standardized limit/offset/max-bytes clamping across control route modules, bounded jobs scan window/offset cost, and enforced `max_rows` in models inventory; regression coverage added in `tests/test_control_query_budget_pagination.py` with broad control suite verification).
- `P2-D1`: Completed (added deterministic data-contract preflight utility `src/ghtrader/data/contract_checks.py`, wired CLI entrypoint `ghtrader data contract-check`, added regression coverage in `tests/test_data_contract_checks.py`, and enforced the preflight in dedicated CI `data-contract` job).
- `P2-O2`: Completed (added `tests/test_e2e_data_pipeline.py` with deterministic end-to-end daily-pipeline orchestration coverage plus optional live provider smoke behind explicit CI flag/credentials, and added dedicated `e2e-pipeline` CI job).
- `P2-O1`: Completed (expanded `reports/data_quality_operations_manual.md` with explicit SLO thresholds + drill checklist and added `reports/incident_response_runbook.md` with outage/data/regression response playbooks).
- `P2-O3`: Completed (introduced coverage governance review baseline in `reports/coverage_governance_review.md`, added enforcement test `tests/test_coverage_governance.py`, and linked `pyproject.toml` coverage exclusions to documented rationale).
- P2 retrospective hardening (2026-02-24): consolidated 7 duplicated `_now_iso()` definitions into shared `util/time.now_iso`; extracted shared `null_rate()` to `ticks_schema.py` resolving behavioral divergence between `contract_checks` (fail-closed) and `field_quality` (fail-open); extracted duplicated progress helpers from `main_l5.py`/`main_schedule.py` into shared `data/progress_helpers.py`; removed redundant `int()` conversion in `questdb_query.py`; added 4 missing test cases for contract checks (rows-minimum, L5 null-rate, extra-columns, shared null_rate); added monkeypatch guards to e2e pipeline tests; extracted `_variety_nav_ctx`/`_job_summary`/`_start_or_reuse_job_by_command` from `views.py` into `views_helpers.py` reducing views.py to 268 lines; fixed stale `test_data_page_cache.py` module reference; cleaned up dead `structlog`/`datetime`/`timezone` imports across 4 files. Full suite: 397 passed.
- QuestDB query/perf hardening (2026-02-24): introduced configurable tick-table partition strategy in `src/ghtrader/questdb/serving_db.py` (`GHTRADER_QDB_TICKS_PARTITION_BY=DAY|MONTH|YEAR`, default `DAY`) and removed cast-based `trading_day` predicates from hot query filters in `src/ghtrader/questdb/queries.py` (`fetch_ticks_for_day`, `list_trading_days_for_symbol`, `query_symbol_recent_last`) to reduce query predicate overhead while preserving API contracts; added regression coverage in `tests/test_questdb_query_sql_filters.py` and `tests/test_questdb_sender_compat.py`.
- QuestDB `trading_day` predicate normalization follow-up (2026-02-24): removed remaining cast-based `trading_day` filters/order clauses in `src/ghtrader/questdb/main_l5_validate.py`, `src/ghtrader/questdb/main_schedule.py`, and `src/ghtrader/data/gap_detection.py`; aligned freshness SQL in `src/ghtrader/questdb/queries.py` (`query_symbol_latest` + `query_symbol_recent_last`) to direct `trading_day` expressions; updated SQL Explorer sample aggregation query (`src/ghtrader/control/templates/explorer.html`) to direct `trading_day` usage; added SQL-shape regression coverage in `tests/test_questdb_trading_day_predicates.py` and updated row-cleanup assertion in `tests/test_questdb_row_cleanup_strategy.py`.
- QuestDB common-aggregation materialization foundation (2026-02-23): added `src/ghtrader/questdb/main_l5_daily_agg.py` with idempotent DDL + rebuild pipeline for `ghtrader_main_l5_daily_agg_v2`, added `ghtrader db refresh-main-l5-daily-agg` maintenance command, updated `dashboard_status` to prefer aggregate-based symbol-count reads with raw-table fallback, routed `query_symbol_day_bounds`/`query_symbol_latest`/`query_symbol_recent_last` through daily-aggregate fast paths with missing-symbol raw fallback in `src/ghtrader/questdb/queries.py`, and integrated `main_l5` build-path auto-refresh (`src/ghtrader/data/main_l5.py`) with config gate (`GHTRADER_MAIN_L5_DAILY_AGG_AUTO_REFRESH`) + report telemetry + best-effort degradation on refresh errors; regression coverage expanded in `tests/test_main_l5_daily_agg.py`, `tests/test_questdb_daily_agg_path.py`, and `tests/test_main_l5_update.py`.

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

